// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	log "github.com/Sirupsen/logrus"

	inmem "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/cqr"
	"github.com/vostrok/utils/db"
	m "github.com/vostrok/utils/metrics"
)

var ContentSvc ContentService

type ContentSentProperties struct {
	Msisdn         string `json:"msisdn"`
	Tid            string `json:"tid"`
	Price          int    `json:"price"`
	ContentPath    string `json:"content_path"`
	ContentName    string `json:"content_name"`
	CapmaignHash   string `json:"capmaign_hash"`
	CampaignId     int64  `json:"campaign_id"`
	ContentId      int64  `json:"content_id"`
	ServiceId      int64  `json:"service_id"`
	SubscriptionId int64  `json:"subscription_id"`
	CountryCode    int64  `json:"country_code"`
	OperatorCode   int64  `json:"operator_code"`
	PaidHours      int    `json:"paid_hours"`
	DelayHours     int    `json:"delay_hours"`
	Publisher      string `json:"publisher"`
	Pixel          string `json:"pixel"`
	Error          string `json:"error"`
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service+id, then unique content per sevice
func (t ContentSentProperties) key() string {
	return t.Msisdn + "-" + strconv.FormatInt(t.ServiceId, 10)
}

func InitService(
	appName string,
	sConf ContentServiceConfig,
	inMemConfig inmem.RPCClientConfig,
	dbConf db.DataBaseConfig,
	notifConf NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)
	inmem.Init(inMemConfig)

	ContentSvc.db = db.Init(dbConf)
	ContentSvc.dbConf = dbConf
	m.Init(appName)

	initMetrics()

	ContentSvc.sConfig = sConf
	ContentSvc.notifier = NewNotifierService(notifConf)

	log.Debug()
}

type ContentService struct {
	db        *sql.DB
	dbConf    db.DataBaseConfig
	sConfig   ContentServiceConfig
	notifier  Notifier
	cqrConfig []*cqr.CQRConfig
}

type ContentServiceConfig struct {
	SubscriptionsLoadDays int      `default:"10" yaml:"subscriptions_load_days"`
	SearchRetryCount      int      `default:"10" yaml:"retry_count"`
	UniqDays              int      `default:"10" yaml:"uniq_days"` // content would be uniq in these days
	Tables                []string `yaml:"tables"`
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
	calls.Inc()
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
		err = errors.New("Empty required params")
		logCtx.WithFields(log.Fields{
			"error":  err.Error(),
			"params": p,
		}).Errorf("required params are empty")
		errs.Inc()
		return msg, errors.New("Required params not found")
	}
	campaign, err := inmem.GetCampaignByHash(p.CampaignHash)
	if err != nil {
		errs.Inc()
		campaignNotFound.Inc()
		err = fmt.Errorf("inmem.Call: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("couldn't get campaign by campaign hash")
		return msg, err
	}

	serviceId := campaign.ServiceId

	usedContentIds, err := inmem.SentContentGet(p.Msisdn, serviceId)
	if err != nil {
		errs.Inc()
		//campaignNotFound.Inc()
		err = fmt.Errorf("inmem.Call: %s", err.Error())
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

	svc, err := inmem.GetServiceById(serviceId)
	if err != nil {
		errs.Inc()
		// todo: insert specific metric

		err = fmt.Errorf("inmem.Call: %s", err.Error())
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
		// todo: insert specific metric

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
		inmem.SentContentClear(p.Msisdn, serviceId)
		usedContentIds, err := inmem.SentContentGet(p.Msisdn, serviceId)
		if err != nil {
			errs.Inc()
			// todo: insert specific metric

			err = fmt.Errorf("inmem.Call: %s", err.Error())
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
	inmem.SentContentPush(p.Msisdn, serviceId, contentId)

	logCtx.WithField("contentId", contentId).Debug("choosen content")

	contentInfo, err := inmem.GetContentById(contentId)
	if err != nil {
		if retry < ContentSvc.sConfig.SearchRetryCount {
			retry++
			logCtx.WithFields(log.Fields{
				"contentId": contentId,
				"retry":     retry,
			}).Error("contentId not found in content")
			goto findContentId
		} else {
			err = fmt.Errorf("Failed to find valid contentId: campaign: %s, msisdn: %s", p.CampaignHash, p.Msisdn)
			logCtx.WithFields(log.Fields{
				"contentId": contentId,
				"retry":     retry,
				"error":     err.Error(),
			}).Error("fail")
			errs.Inc()
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
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Info("notify content sent error")
		errs.Inc()
	} else {
		logCtx.Info("notified")
	}
	logCtx.WithFields(log.Fields{"contentSentProperties": fmt.Sprintf("%#v", msg)}).Info("success")
	return msg, nil
}
