package service

import (
	"errors"
	"fmt"

	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-utils/structs"
)

// does nothing but genetrates unique url
// save in db via qlistener
// update cache in inmemory
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

	if err := inmem_client.SetUniqueUrlCache(msg); err != nil {
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

// get unique url from inmemory (and delete it)
// remove from db
// and return url
func GetByUniqueUrl(uniqueUrl string) (msg structs.ContentSentProperties, err error) {
	defer func() {
		if err = inmem_client.DeleteUniqueUrlCache(msg); err != nil {
			log.WithFields(log.Fields{
				"tid":   msg.Tid,
				"error": err.Error(),
			}).Error("inmem_client.DeleteUniqueUrlCache")
		}
		if err = notifyUniqueContentURL("delete", msg); err != nil {
			log.WithFields(log.Fields{
				"tid":   msg.Tid,
				"error": err.Error(),
			}).Error("notifyUniqueContentURL")
		}
	}()

	msg, err = inmem_client.GetUniqueUrlCache(uniqueUrl)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
			"url":   uniqueUrl,
		}).Debug("inmem_client.GetUniqueUrlCache")
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
		CampaignCode:   p.CampaignCode,
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
		"service_id": p.ServiceCode,
		"tid":        p.Tid,
	})
	if p.ServiceCode == "" || p.CampaignCode == "" {
		ContentSvc.m.errs.Inc()

		err = errors.New("Empty required params")
		logCtx.WithFields(log.Fields{
			"error":  err.Error(),
			"params": p,
		}).Error("required params are empty")

		return msg, errors.New("Required params not found")
	}

	serviceCode := p.ServiceCode

	usedContentIds, err := inmem_client.SentContentGet(p.Msisdn, serviceCode)
	if err != nil {
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("inmem_client.SentContentGet: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("couldn't get used content ids")
		return msg, err
	}
	logCtx.WithFields(log.Fields{
		"tid":            p.Tid,
		"usedContentIds": usedContentIds,
		"serviceId":      serviceCode,
	}).Debug("got used content ids")

	svc, err := inmem_client.GetServiceByCode(serviceCode)
	if err != nil {
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("couldn't get service by id")
		return msg, err
	}
	avialableContentCodes := svc.ContentCodes
	logCtx.WithFields(log.Fields{
		"avialableContentIds": avialableContentCodes,
	}).Debug("got avialable content ids")

	if len(avialableContentCodes) == 0 {
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("No content for service %d at all", p.ServiceCode)
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Errorf("No content avialabale at all")
		return msg, err
	}

	retry := 0
findContentId:
	// find first avialable contentId
	contentId := ""
	for _, id := range avialableContentCodes {
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
		if err = inmem_client.SentContentClear(p.Msisdn, serviceCode); err != nil {
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("inmem_client.SentContentClear: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Debug("cannot clear sent content")
			return msg, err
		}
		usedContentIds, err := inmem_client.SentContentGet(p.Msisdn, serviceCode)
		if err != nil {
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("inmem_client.SentContentGet: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Errorf("couldn't get used content ids")
			return msg, err
		}
		logCtx.WithFields(log.Fields{
			"usedContentIds": usedContentIds,
		}).Debug("now used content ids is")
		for _, id := range avialableContentCodes {
			contentId = id
			break
		}
	}
	// update in-memory cache usedContentIds
	if err = inmem_client.SentContentPush(p.Msisdn, serviceCode, contentId); err != nil {
		ContentSvc.m.errs.Inc()
		err = fmt.Errorf("inmem_client.SentContentPush: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Debug("cannot push sent content")
		return msg, err
	}

	logCtx.WithField("contentId", contentId).Debug("choosen content")

	contentInfo, err := inmem_client.GetContentById(contentId)
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
	msg.ContentCode = contentId

	logCtx.WithFields(log.Fields{
		"tid":       msg.Tid,
		"path":      msg.ContentPath,
		"contentID": msg.ContentCode,
	}).Info("success")

	ContentSvc.m.callsSuccess.Inc()
	return msg, nil
}
