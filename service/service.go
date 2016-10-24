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

type ContentInterface interface {
	InitService(sConf ContentServiceConfig)

	GetUrlByCampaignHash(msisdn, campaignHash string) (path string, err error)
}

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
	DbConf           db.DataBaseConfig `yaml:"db"`
	Notifier         NotifierConfig    `notifier:"notifier"`
	SearchRetryCount int               `default:"10" yaml:"retry_count"`
	TablePrefix      string            `default:"xmp_" yaml:"table_prefix"`
	UniqDays         int               `default:"10" yaml:"uniq_days"` // content would be uniq in these days
	Tables           []string          `default:"campaign,service,content,service_content,subscriptions,content_sent" yaml:"tables"`
}

type GetUrlByCampaignHashParams struct {
	Msisdn       string
	CampaignHash string
	Tid          string
	CountryCode  int64
	OperatorCode int64
}

// Get Content by campaign hash
// 1) find first avialable contentId
// 2) reset cache if nothing found
// 3) record that the content is shown to the user
func GetUrlByCampaignHash(p GetUrlByCampaignHashParams) (msg *ContentSentProperties, err error) {
	logCtx := log.WithFields(log.Fields{
		"msisdn":       p.Msisdn,
		"campaignHash": p.CampaignHash,
		"CountryCode":  p.CountryCode,
		"OperatorCode": p.OperatorCode,
		"Tid":          p.Tid,
	})
	if p.Msisdn == "" || p.CampaignHash == "" || p.CountryCode == 0 || p.OperatorCode == 0 || p.Tid == "" {
		err = errors.New("Empty required params")
		logCtx.WithField("error", err.Error()).Errorf("No required params")
	}
	campaign, ok := campaign.Map[p.CampaignHash]
	if !ok {
		err = errors.New("Not found")
		logCtx.WithField("error", err.Error()).Errorf("get campaign by campaign hash")
		return msg, fmt.Errorf("CampaignHash: %s", "Not found")
	}

	serviceId := campaign.ServiceId
	usedContentIds := contentSent.Get(p.Msisdn, serviceId)
	logCtx.WithField("usedContentIds", usedContentIds)

	avialableContentIds := service.Get(serviceId)
	logCtx.WithField("avialableContentIds", avialableContentIds)
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
	for id, _ := range avialableContentIds {
		if usedContentIds != nil {
			if _, ok := usedContentIds[int64(id)]; ok {
				continue
			}
		}
		contentId = int64(id)
		break
	}
	// reset if nothing
	if contentId == 0 {
		logCtx.Debug("No content avialable, reset remembered cache..")
		contentSent.Clear(p.Msisdn, serviceId)
		for id, _ := range avialableContentIds {
			contentId = int64(id)
			break
		}

	}
	// update in-memory cache usedContentIds
	contentSent.Push(p.Msisdn, serviceId, contentId)
	logCtx.WithField("ContentId", contentId).Debug("choosen content")

	path, ok := content.Map[contentId]
	if !ok {
		if retry < ContentSvc.sConfig.SearchRetryCount {
			retry++
			logCtx.WithFields(log.Fields{"ContentId": contentId, "Retry": retry}).
				Error("Service ContentId not found in content")
			goto findContentId
		} else {
			err = fmt.Errorf("Failed to find valid contentId: campaign: %s, msisdn: %s", p.CampaignHash, p.Msisdn)
			logCtx.WithFields(log.Fields{"ContentId": contentId, "Retry": retry}).
				Error(err.Error())
			return msg, err
		}
	}

	// XXX: dispatcher needs path only,
	// and, as we got this variables during "get path" process
	// we save them into database, not to get them again
	// anyway, it is possible to find a better way in future
	msg = &ContentSentProperties{
		Msisdn:       p.Msisdn,
		Price:        int(service.Map[serviceId].Price),
		Tid:          p.Tid,
		ContentPath:  path,
		CampaignId:   campaign.Id,
		ContentId:    contentId,
		ServiceId:    serviceId,
		CountryCode:  p.CountryCode,
		OperatorCode: p.OperatorCode,
	}
	// record sent content
	ContentSvc.notifier.ContentSentNotify(msg)

	return msg, nil
}
