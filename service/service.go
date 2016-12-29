// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"errors"
	"fmt"

	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/vostrok/inmem/rpcclient"
)

func CreateUniqueUrl(msg ContentSentProperties) (string, error) {
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
	ContentSvc.setUniqueUrlCache(msg)
	notifyNewUniqueContentURL(msg)
	return msg.UniqueUrl, nil
}

func GetByUniqueUrl(uniqueUrl string) (msg ContentSentProperties, err error) {
	msg, err = ContentSvc.getUniqueUrlCache(uniqueUrl)
	if err != nil {
		log.WithFields(log.Fields{
			"url": uniqueUrl,
		}).Debug("failed to get from cache")

		msg, err = ContentSvc.loadUniqueUrl(uniqueUrl)
		if err != nil {
			log.WithFields(log.Fields{
				"url": uniqueUrl,
			}).Debug("failed to load from db")
			return
		}
		log.WithFields(log.Fields{
			"url": uniqueUrl,
			"tid": msg.Tid,
		}).Debug("got from db")
		return
	}
	log.WithFields(log.Fields{
		"url": uniqueUrl,
		"tid": msg.Tid,
	}).Debug("got from cache")
	ContentSvc.deleteUniqueUrlCache(msg)
	return
}

// Get Content by campaign hash
// 1) find first avialable contentId
// 2) reset cache if nothing found
// 3) record that the content is shown to the user
func GetContent(p GetContentParams) (msg ContentSentProperties, err error) {
	defer func() {
		if err != nil {
			msg.Error = err.Error()
		}
	}()

	logCtx := log.WithFields(log.Fields{
		"msisdn":     p.Msisdn,
		"service_id": p.ServiceId,
		"tid":        p.Tid,
	})
	if p.Msisdn == "" || p.Tid == "" ||
		p.ServiceId == 0 || p.CampaignId == 0 {
		ContentSvc.m.errs.Inc()

		err = errors.New("Empty required params")
		logCtx.WithFields(log.Fields{
			"error":  err.Error(),
			"params": p,
		}).Error("required params are empty")

		return msg, errors.New("Required params not found")
	}

	serviceId := p.ServiceId

	usedContentIds, err := inmem_client.SentContentGet(p.Msisdn, serviceId)
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
		"serviceId":      serviceId,
	}).Debug("got used content ids")

	svc, err := inmem_client.GetServiceById(serviceId)
	if err != nil {
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
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
		ContentSvc.m.errs.Inc()

		err = fmt.Errorf("No content for service %d at all", p.ServiceId)
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
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("inmem_client.SentContentClear: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Debug("cannot clear sent content")
			return msg, err
		}
		usedContentIds, err := inmem_client.SentContentGet(p.Msisdn, serviceId)
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
		for _, id := range avialableContentIds {
			contentId = int64(id)
			break
		}
	}
	// update in-memory cache usedContentIds
	if err = inmem_client.SentContentPush(p.Msisdn, serviceId, contentId); err != nil {
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
			}).Error("contentId not found in content")
			goto findContentId
		} else {
			ContentSvc.m.errs.Inc()

			err = fmt.Errorf("Failed to find valid contentId: serviceId %d", p.ServiceId)
			logCtx.WithFields(log.Fields{
				"contentId": contentId,
				"retry":     retry,
				"error":     err.Error(),
			}).Error("fail")
			return msg, err
		}
	}

	msg = ContentSentProperties{
		Msisdn:      p.Msisdn,
		Tid:         p.Tid,
		ContentPath: contentInfo.Path,
		ContentName: contentInfo.Name,
		ContentId:   contentId,
		ServiceId:   serviceId,
	}

	logCtx.WithFields(log.Fields{
		"tid":       msg.Tid,
		"path":      msg.ContentPath,
		"contentID": msg.ContentId,
	}).Info("success")

	ContentSvc.m.callsSuccess.Inc()
	return msg, nil
}
