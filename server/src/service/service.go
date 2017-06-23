package service

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"encoding/json"
	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-utils/structs"
)

// does nothing but genetrates unique url
// save in db via qlistener
// update cache in midory
func CreateUniqueUrl(msg structs.ContentSentProperties) (string, error) {
	uniqueUrl, err := ContentSvc.sid.Generate()
	if err != nil {
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("sid.Generate: %s", err.Error())
		log.WithFields(log.Fields{
			"tid": msg.Tid,
		}).Error("cannot generate unique url")

		return "", err
	}
	msg.UniqueUrl = uniqueUrl
	log.WithFields(log.Fields{
		"url": uniqueUrl,
		"tid": msg.Tid,
		"msg": fmt.Sprintf("%#v", msg),
	}).Debug("set cache")

	if err := mid_client.SetUniqueUrlCache(msg); err != nil {
		log.WithFields(log.Fields{
			"tid":   msg.Tid,
			"error": err.Error(),
		}).Error("set cache")
	}
	if err := notifyUniqueContentURL("create", msg); err != nil {
		log.WithFields(log.Fields{
			"tid":   msg.Tid,
			"error": err.Error(),
		}).Error("notifyUniqueContentURL")
	}
	return msg.UniqueUrl, nil
}

// get unique url from midory (and delete it)
// remove from db
// and return url
func GetByUniqueUrl(uniqueUrl string) (msg structs.ContentSentProperties, err error) {
	defer func() {
		if err = mid_client.DeleteUniqueUrlCache(msg); err != nil {
			log.WithFields(log.Fields{
				"tid":   msg.Tid,
				"error": err.Error(),
			}).Error("mid_client.DeleteUniqueUrlCache")
		}
		if err = notifyUniqueContentURL("delete", msg); err != nil {
			log.WithFields(log.Fields{
				"tid":   msg.Tid,
				"error": err.Error(),
			}).Error("notifyUniqueContentURL")
		}
	}()

	msg, err = mid_client.GetUniqueUrlCache(uniqueUrl)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
			"url":   uniqueUrl,
		}).Debug("mid_client.GetUniqueUrlCache")
		return
	}
	log.WithFields(log.Fields{
		"url": uniqueUrl,
		"tid": msg.Tid,
		"msg": fmt.Sprintf("%#v", msg),
	}).Debug("got from cache")
	return
}

// get content, not url for content.
// filters already shown content
// reset cache if nothing found and show again
func GetContent(p GetContentParams) (msg structs.ContentSentProperties, err error) {
	msg = structs.ContentSentProperties{
		Msisdn:         p.Msisdn,
		Tid:            p.Tid,
		ServiceCode:    p.ServiceCode,
		CampaignId:     p.CampaignId,
		OperatorCode:   p.OperatorCode,
		CountryCode:    p.CountryCode,
		SubscriptionId: p.SubscriptionId,
	}
	defer func() {
		if err != nil {
			msg.Error = err.Error()
		}
	}()
	logCtx := log.WithFields(log.Fields{
		"service_code": p.ServiceCode,
		"tid":          p.Tid,
	})
	if p.ServiceCode == "" || p.CampaignId == "" {
		ContentSvc.m.errs.Inc()

		err = errors.New("Empty required params")
		logCtx.WithFields(log.Fields{
			"error":  err.Error(),
			"params": fmt.Sprintf("%#v", p),
		}).Error("required params (service code /campaign code) are empty")

		return msg, errors.New("Required params not found")
	}

	serviceCode := p.ServiceCode

	usedContentIds := make(map[string]struct{})
	if len(p.Msisdn) > 5 {
		usedContentIds, err = mid_client.SentContentGet(p.Msisdn, serviceCode)
		if err != nil {
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("mid_client.SentContentGet: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Errorf("couldn't get used content ids")
			return msg, err
		}
	}
	logCtx.WithFields(log.Fields{
		"tid":            p.Tid,
		"usedContentIds": usedContentIds,
		"service_code":   serviceCode,
	}).Debug("used content ids")

	svc, err := mid_client.GetServiceByCode(serviceCode)
	if err != nil {
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("mid_client.GetServiceByCode: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error":      err.Error(),
			"service_id": serviceCode,
		}).Errorf("couldn't get service by code")
		return msg, err
	}
	sJson, err := json.Marshal(svc)
	avialableContentIds := svc.ContentIds
	logCtx.WithFields(log.Fields{
		"avialableContentIds": avialableContentIds,
		"svc": string(sJson),
	}).Debug("got avialable content ids")

	if len(avialableContentIds) == 0 {
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("No content for service %s at all", p.ServiceCode)
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("No content avialabale at all")
		return msg, err
	}

	retry := 0
findContentId:
	// find first avialable contentId
	contentId := ""
	for _, id := range avialableContentIds {
		if usedContentIds != nil {
			if _, ok := usedContentIds[id]; ok {
				logCtx.WithFields(log.Fields{
					"contentId": id,
				}).Debug("contentId already used, next..")

				continue
			}
		}
		contentId = id
		logCtx.WithFields(log.Fields{
			"contentId": id,
		}).Debug("contentId found")
		break
	}

	// reset if nothing
	if contentId == "" {
		logCtx.Debug("No content avialable, reset remembered cache..")
		if err = mid_client.SentContentClear(p.Msisdn, serviceCode); err != nil {
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("mid_client.SentContentClear: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Debug("cannot clear sent content")
			return msg, err
		}
		usedContentIds, err := mid_client.SentContentGet(p.Msisdn, serviceCode)
		if err != nil {
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("mid_client.SentContentGet: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": svc.Id,
			}).Errorf("couldn't get used content ids")
			return msg, err
		}
		logCtx.WithFields(log.Fields{
			"service_id":     svc.Id,
			"usedContentIds": usedContentIds,
		}).Debug("now used content ids is")

		for _, id := range avialableContentIds {
			contentId = id
			break
		}
	}
	// update in-memory cache usedContentIds
	if err = mid_client.SentContentPush(p.Msisdn, serviceCode, contentId); err != nil {
		ContentSvc.m.errs.Inc()
		err = fmt.Errorf("mid_client.SentContentPush: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Debug("cannot push sent content")
		return msg, err
	}

	logCtx.WithField("contentId", contentId).Debug("choosen content")

	contentInfo, err := mid_client.GetContentById(contentId)
	if err != nil {
		if retry < ContentSvc.conf.SearchRetryCount {
			retry++
			logCtx.WithFields(log.Fields{
				"contentId": contentId,
				"retry":     retry,
				"error":     err.Error(),
			}).Error("contentId not found in content")
			goto findContentId
		} else {
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("Failed to find valid contentId: serviceId %d", p.ServiceCode)
			logCtx.WithFields(log.Fields{
				"contentId": contentId,
				"retry":     retry,
				"error":     err.Error(),
			}).Error("fail")
			return msg, err
		}
	}

	msg.ContentPath = contentInfo.Name
	msg.ContentName = contentInfo.Title
	msg.ContentId = contentId

	logCtx.WithFields(log.Fields{
		"tid":       msg.Tid,
		"path":      msg.ContentPath,
		"contentID": msg.ContentId,
	}).Info("success")

	ContentSvc.m.callsSuccess.Inc()
	return msg, nil
}
