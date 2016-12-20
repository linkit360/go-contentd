// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/cqr"
	m "github.com/vostrok/utils/metrics"
)

var ContentSvc ContentService
var callsSuccess m.Gauge
var errs m.Gauge

type ContentSentProperties struct {
	SentAt         time.Time `json:"sent_at,omitempty"`
	Msisdn         string    `json:"msisdn,omitempty"`
	Tid            string    `json:"tid,omitempty"`
	Price          int       `json:"price,omitempty"`
	ContentPath    string    `json:"content_path,omitempty"`
	ContentName    string    `json:"content_name,omitempty"`
	CapmaignHash   string    `json:"capmaign_hash,omitempty"`
	CampaignId     int64     `json:"campaign_id,omitempty"`
	ContentId      int64     `json:"content_id,omitempty"`
	ServiceId      int64     `json:"service_id,omitempty"`
	SubscriptionId int64     `json:"subscription_id,omitempty"`
	CountryCode    int64     `json:"country_code,omitempty"`
	OperatorCode   int64     `json:"operator_code,omitempty"`
	PaidHours      int       `json:"paid_hours,omitempty"`
	DelayHours     int       `json:"delay_hours,omitempty"`
	Publisher      string    `json:"publisher,omitempty"`
	Pixel          string    `json:"pixel,omitempty"`
	Error          string    `json:"error,omitempty"`
	Type           string    `json:"type,omitempty"`
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service+id, then unique content per sevice
func (t ContentSentProperties) key() string {
	return t.Msisdn + "-" + strconv.FormatInt(t.ServiceId, 10)
}

func InitService(
	metricInstancePrefix string,
	appName string,
	sConf ContentServiceConfig,
	inMemConfig inmem_client.RPCClientConfig,
	notifConf NotifierConfig,
) {
	initMetrics(metricInstancePrefix, appName)

	log.SetLevel(log.DebugLevel)
	inmem_client.Init(inMemConfig)

	ContentSvc.sConfig = sConf
	ContentSvc.notifier = NewNotifierService(notifConf)

	log.Debug()
}

type ContentService struct {
	sConfig   ContentServiceConfig
	notifier  Notifier
	cqrConfig []*cqr.CQRConfig
}

type ContentServiceConfig struct {
	SearchRetryCount int `default:"10" yaml:"retry_count"`
}

type GetUrlByCampaignHashParams struct {
	Msisdn       string `json:"msisdn"`
	CampaignHash string `json:"campaign_hash"`
	Tid          string `json:"tid"`
	CountryCode  int64  `json:"country_code"`
	OperatorCode int64  `json:"operator_code"`
	Publisher    string `json:"publisher"`
	Pixel        string `json:"pixel"`
}

// Get Content by campaign hash
// 1) find first avialable contentId
// 2) reset cache if nothing found
// 3) record that the content is shown to the user
func GetUrlByCampaignHash(p GetUrlByCampaignHashParams) (msg ContentSentProperties, err error) {
	logCtx := log.WithFields(log.Fields{
		"msisdn":       p.Msisdn,
		"campaignHash": p.CampaignHash,
		"countryCode":  p.CountryCode,
		"operatorCode": p.OperatorCode,
		"tid":          p.Tid,
	})
	if p.Msisdn == "" ||
		p.CampaignHash == "" ||
		p.CountryCode == 0 ||
		p.OperatorCode == 0 ||
		p.Tid == "" {
		errs.Inc()

		err = errors.New("Empty required params")
		logCtx.WithFields(log.Fields{
			"error":  err.Error(),
			"params": p,
		}).Errorf("required params are empty")

		return msg, errors.New("Required params not found")
	}
	campaign, err := inmem_client.GetCampaignByHash(p.CampaignHash)
	if err != nil {
		errs.Inc()

		err = fmt.Errorf("inmem_client.Call: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("couldn't get campaign by campaign hash")
		return msg, err
	}

	serviceId := campaign.ServiceId

	usedContentIds, err := inmem_client.SentContentGet(p.Msisdn, serviceId)
	if err != nil {
		errs.Inc()

		err = fmt.Errorf("inmem_client.Call: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("couldn't get used content ids")
		return msg, err
	}
	logCtx.WithFields(log.Fields{
		"tid":            p.Tid,
		"usedContentIds": usedContentIds,
		"serviceId":      serviceId,
	}).Debug("got used content ids")

	svc, err := inmem_client.GetServiceById(serviceId)
	if err != nil {
		errs.Inc()

		err = fmt.Errorf("inmem_client.Call: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("couldn't get service by id")
		return msg, err
	}
	avialableContentIds := svc.ContentIds
	logCtx.WithFields(log.Fields{
		"avialableContentIds": avialableContentIds,
	}).Debug("got avialable content ids")

	if len(avialableContentIds) == 0 {
		errs.Inc()

		err = fmt.Errorf("No content for campaign %s at all", p.CampaignHash)
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("No content avialabale at all")
		return msg, err
	}

	retry := 0
findContentId:
	// find first avialable contentId
	contentId := int64(0)
	for _, id := range avialableContentIds {
		if usedContentIds != nil {
			if usedId, ok := usedContentIds[id]; ok {
				logCtx.WithFields(log.Fields{
					"contentId": usedId,
				}).Debug("contentId already used, next..")

				continue
			}
		}
		contentId = int64(id)
		logCtx.WithFields(log.Fields{
			"contentId": id,
		}).Debug("contentId found")
		break
	}

	// reset if nothing
	if contentId == 0 {
		logCtx.Debug("No content avialable, reset remembered cache..")
		if err = inmem_client.SentContentClear(p.Msisdn, serviceId); err != nil {
			errs.Inc()

			err = fmt.Errorf("inmem_client.Call: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Debug("cannot clear sent content")
			return msg, err
		}
		usedContentIds, err := inmem_client.SentContentGet(p.Msisdn, serviceId)
		if err != nil {
			errs.Inc()

			err = fmt.Errorf("inmem_client.Call: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Errorf("couldn't get used content ids")
			return msg, err
		}
		logCtx.WithFields(log.Fields{
			"usedContentIds": usedContentIds,
		}).Debug("now used content ids is")
		for _, id := range avialableContentIds {
			contentId = int64(id)
			break
		}
	}
	// update in-memory cache usedContentIds
	if err = inmem_client.SentContentPush(p.Msisdn, serviceId, contentId); err != nil {
		errs.Inc()
		err = fmt.Errorf("inmem_client.Call: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Debug("cannot push sent content")
		return msg, err
	}

	logCtx.WithField("contentId", contentId).Debug("choosen content")

	contentInfo, err := inmem_client.GetContentById(contentId)
	if err != nil {
		if retry < ContentSvc.sConfig.SearchRetryCount {
			retry++
			logCtx.WithFields(log.Fields{
				"contentId": contentId,
				"retry":     retry,
			}).Error("contentId not found in content")
			goto findContentId
		} else {
			errs.Inc()

			err = fmt.Errorf("Failed to find valid contentId: campaign: %s, msisdn: %s", p.CampaignHash, p.Msisdn)
			logCtx.WithFields(log.Fields{
				"contentId": contentId,
				"retry":     retry,
				"error":     err.Error(),
			}).Error("fail")
			return msg, err
		}
	}

	// XXX: dispatcher needs path only,
	// and, as we got this variables during "get path" process
	// we save them into database, not to get them again
	// anyway, it is possible to find a better way in future
	msg = ContentSentProperties{
		Msisdn:       p.Msisdn,
		Price:        int(svc.Price),
		Tid:          p.Tid,
		ContentPath:  contentInfo.Path,
		ContentName:  contentInfo.Name,
		CampaignId:   campaign.Id,
		CapmaignHash: p.CampaignHash,
		ContentId:    contentId,
		ServiceId:    serviceId,
		CountryCode:  p.CountryCode,
		OperatorCode: p.OperatorCode,
		PaidHours:    svc.PaidHours,
		DelayHours:   svc.DelayHours,
		Publisher:    p.Publisher,
		Pixel:        p.Pixel,
	}

	// record sent content
	if err = ContentSvc.notifier.ContentSentNotify(msg); err != nil {
		errs.Inc()
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Info("notify content sent error")
	} else {
		logCtx.Info("notified")
	}
	logCtx.WithFields(log.Fields{"contentSentProperties": fmt.Sprintf("%#v", msg)}).Info("success")
	callsSuccess.Inc()
	return msg, nil
}

func initMetrics(metricInstancePrefix, appName string) {
	m.Init(metricInstancePrefix)

	callsSuccess = m.NewGauge("", appName, "success", "success overall")
	errs = m.NewGauge("", appName, "errors", "errors overall")

	go func() {
		for range time.Tick(time.Minute) {
			callsSuccess.Update()
			errs.Update()
		}
	}()
}
