// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"database/sql"
	"errors"
	"fmt"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
)

const ACTIVE_STATUS = 1

var ContentSvc ContentService

func InitService(sConf ContentServiceConfig) {
	log.SetLevel(log.DebugLevel)

	ContentSvc.db = db.Init(sConf.DbConf)

	ContentSvc.sConfig = sConf
	ContentSvc.notifier = NewNotifierService(sConf.Notifier)
	if err := initCQR(); err != nil {
		log.WithField("error", err.Error()).Fatal("Init CQR")
	}
}

type ContentService struct {
	db       *sql.DB
	sConfig  ContentServiceConfig
	notifier Notifier
	tables   map[string]struct{}
}
type ContentServiceConfig struct {
	DbConf                db.DataBaseConfig `yaml:"db"`
	Notifier              NotifierConfig    `notifier:"notifier"`
	SubscriptionsLoadDays int               `default:"10" yaml:"subscriptions_load_days"`
	SearchRetryCount      int               `default:"10" yaml:"retry_count"`
	TablePrefix           string            `default:"xmp_" yaml:"table_prefix"`
	UniqDays              int               `default:"10" yaml:"uniq_days"` // content would be uniq in these days
	Tables                []string          `yaml:"tables"`
}

type GetUrlByCampaignHashParams struct {
	Msisdn       string `json:"msisdn"`
	CampaignHash string `json:"campaign_hash"`
	Tid          string `json:"tid"`
	CountryCode  int64  `json:"country_code"`
	OperatorCode int64  `json:"operator_code"`
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
		err = errors.New("Empty required params")
		logCtx.WithFields(log.Fields{
			"error":  err.Error(),
			"params": p,
		}).Errorf("required params are empty")
		return msg, errors.New("Required params not found")
	}
	campaign, ok := campaign.Map[p.CampaignHash]
	if !ok {
		err = errors.New("Not found")
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("couldn't get campaign by campaign hash")
		return msg, fmt.Errorf("CampaignHash: %s", "Not found")
	}

	serviceId := campaign.ServiceId
	usedContentIds := contentSent.Get(p.Msisdn, serviceId)
	logCtx.WithFields(log.Fields{
		"usedContentIds": usedContentIds,
		"serviceId":      serviceId,
	}).Debug("got used content ids")

	avialableContentIds := service.Get(serviceId)
	logCtx.WithFields(log.Fields{
		"avialableContentIds": avialableContentIds,
	}).Debug("got avialable content ids")

	if len(avialableContentIds) == 0 {
		err = fmt.Errorf("No content for campaign %s at all", p.CampaignHash)
		logCtx.WithField("error", err.Error()).Errorf("No content avialabale at all")
		return msg, err
	}

	// todo: platform compatibility check
	// todo: subcategory check

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
		contentSent.Clear(p.Msisdn, serviceId)
		usedContentIds = contentSent.Get(p.Msisdn, serviceId)
		logCtx.WithFields(log.Fields{
			"usedContentIds": usedContentIds,
		}).Debug("now used content ids is")
		for _, id := range avialableContentIds {
			contentId = int64(id)
			break
		}
	}
	// update in-memory cache usedContentIds
	contentSent.Push(p.Msisdn, serviceId, contentId)

	logCtx.WithField("contentId", contentId).Debug("choosen content")

	contentInfo, ok := content.Map[contentId]
	if !ok {
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
				"ContentId": contentId,
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
		Price:        int(service.Map[serviceId].Price),
		Tid:          p.Tid,
		ContentPath:  contentInfo.Path,
		ContentName:  contentInfo.Name,
		CampaignId:   campaign.Id,
		CapmaignHash: p.CampaignHash,
		ContentId:    contentId,
		ServiceId:    serviceId,
		CountryCode:  p.CountryCode,
		OperatorCode: p.OperatorCode,
	}

	// record sent content
	if err = ContentSvc.notifier.ContentSentNotify(msg); err != nil {
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Info("notify content sent error")
	} else {
		logCtx.Info("notiied")
	}
	logCtx.Info("success")
	return msg, nil
}
