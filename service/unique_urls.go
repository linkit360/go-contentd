package service

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"

	"github.com/vostrok/utils/db"
)

// cache for unique urls
func (svc *ContentService) initUniqueUrlsCache() {
	svc.db = db.Init(svc.conf.DBConfig)
	prev, err := svc.loadUniqueUrls()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load uniq urls")
	}
	log.WithField("count", len(prev)).Debug("loaded uniq urls")
	svc.uniqUrlCache = cache.New(8760*time.Hour, time.Hour)
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.ServiceId, 10)
		svc.uniqUrlCache.Set(key, v, 8760*time.Hour)
	}
}
func (svc *ContentService) getUniqueUrlCache(uniqueUrl string) (ContentSentProperties, error) {
	v, found := svc.uniqUrlCache.Get(uniqueUrl)
	if !found {
		return ContentSentProperties{}, fmt.Errorf("Not found: %s", uniqueUrl)
	}
	if property, ok := v.(ContentSentProperties); ok {
		return property, nil
	}
	return ContentSentProperties{}, fmt.Errorf("Cannot decifer: %s, %v", uniqueUrl, v)
}

func (svc *ContentService) setUniqueUrlCache(r ContentSentProperties) {
	_, found := svc.uniqUrlCache.Get(r.UniqueUrl)
	if !found {
		svc.uniqUrlCache.Set(r.UniqueUrl, r, 8760*time.Hour)
		log.WithFields(log.Fields{
			"tid": r.Tid,
			"key": r.UniqueUrl,
		}).Debug("set url cache")
	}
}

func (svc *ContentService) deleteUniqueUrlCache(r ContentSentProperties) {
	svc.uniqUrlCache.Delete(r.UniqueUrl)
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": r.UniqueUrl,
	}).Debug("deleted url cache")
}

// warn cache for unique urls
func (svc *ContentService) loadUniqueUrls() (prop []ContentSentProperties, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("load uniq urls failed")
			} else {
				fields["count"] = len(prop)
				log.WithFields(fields).Debug("load uniq urls ")
			}
		}()
	}()
	query := fmt.Sprintf("SELECT "+
		"sent_at, "+
		"msisdn, "+
		"tid, "+
		"content_path, "+
		"unique_url, "+
		"content_name, "+
		"id_campaign, "+
		"id_service, "+
		"operator_code, "+
		"country_code "+
		"FROM %scontent_unique_urls",
		svc.conf.DBConfig.TablePrefix)

	rows, err := svc.db.Query(query)
	if err != nil {
		svc.m.dbErrors.Inc()

		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return prop, err
	}
	defer rows.Close()

	for rows.Next() {
		var p ContentSentProperties
		if err := rows.Scan(
			&p.SentAt,
			&p.Msisdn,
			&p.Tid,
			&p.ContentPath,
			&p.UniqueUrl,
			&p.ContentName,
			&p.CampaignId,
			&p.ServiceId,
			&p.OperatorCode,
			&p.CountryCode,
		); err != nil {
			svc.m.dbErrors.Inc()

			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return prop, err
		}
		prop = append(prop, p)
	}

	if rows.Err() != nil {
		svc.m.dbErrors.Inc()

		err = fmt.Errorf("Rows.Err: %s", err.Error())
		return
	}
	return
}

// warn cache for unique urls
func (svc *ContentService) loadUniqueUrl(uniqueUrl string) (p ContentSentProperties, err error) {
	begin := time.Now()
	defer func() {
		defer func() {
			fields := log.Fields{
				"took": time.Since(begin),
			}
			if err != nil {
				fields["error"] = err.Error()
				log.WithFields(fields).Error("load uniq url failed")
			} else {
				fields["tid"] = p.Tid
				log.WithFields(fields).Debug("load uniq url ")
			}
		}()
	}()
	query := fmt.Sprintf("SELECT "+
		"sent_at, "+
		"msisdn, "+
		"tid, "+
		"content_path, "+
		"unique_url, "+
		"content_name, "+
		"id_campaign, "+
		"id_service, "+
		"operator_code, "+
		"country_code, "+
		"FROM %scontent_unique_urls WHERE unique_url = $1 LIMIT 1",
		svc.conf.DBConfig.TablePrefix)

	rows, err := svc.db.Query(query, uniqueUrl)
	if err != nil {
		svc.m.dbErrors.Inc()

		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(
			&p.SentAt,
			&p.Msisdn,
			&p.Tid,
			&p.ContentPath,
			&p.UniqueUrl,
			&p.ContentName,
			&p.CampaignId,
			&p.ServiceId,
			&p.OperatorCode,
			&p.CountryCode,
		); err != nil {
			svc.m.dbErrors.Inc()

			err = fmt.Errorf("Rows.Next: %s", err.Error())
			return
		}
	}

	if rows.Err() != nil {
		svc.m.dbErrors.Inc()

		err = fmt.Errorf("Rows.Err: %s", err.Error())
		return
	}

	if p.Tid == "" || p.ContentId == 0 {
		err = fmt.Errorf("Not found: %s", uniqueUrl)
	}

	return
}
