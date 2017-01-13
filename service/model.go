// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
	shortid "github.com/ventu-io/go-shortid"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	inmem_service "github.com/vostrok/inmem/service"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
	m "github.com/vostrok/utils/metrics"
)

var ContentSvc *ContentService

type ContentService struct {
	conf         ContentServiceConfig
	n            *amqp.Notifier
	m            *Metrics
	sid          *shortid.Shortid
	db           *sql.DB
	uniqUrlCache *cache.Cache
}

type ContentServiceConfig struct {
	SearchRetryCount int               `default:"10" yaml:"retry_count"`
	Queues           QueuesConfig      `yaml:"queues"`
	DBConfig         db.DataBaseConfig `yaml:"db"`
}

type QueuesConfig struct {
	UniqueUrl string `default:"contentd_unique_url" yaml:"contentd_unique_url"`
}

func InitService(
	appName string,
	sConf ContentServiceConfig,
	inMemConfig inmem_client.RPCClientConfig,
	notifierConfig amqp.NotifierConfig,
) {
	ContentSvc = &ContentService{
		m:    initMetrics(appName),
		sid:  shortid.MustNew(1, shortid.DefaultABC, uint64(time.Now().UnixNano())),
		conf: sConf,
	}
	log.SetLevel(log.DebugLevel)
	inmem_client.Init(inMemConfig)

	ContentSvc.n = amqp.NewNotifier(notifierConfig)

	log.Debug("init done")
}

type GetContentParams struct {
	Msisdn       string `json:"msisdn"`
	Tid          string `json:"tid"`
	ServiceId    int64  `json:"service_id"`
	CampaignId   int64  `json:"campaign_id"`
	OperatorCode int64  `json:"operator_code"`
	CountryCode  int64  `json:"country_code"`
}

type Metrics struct {
	callsSuccess m.Gauge
	errs         m.Gauge
	notifyErrors m.Gauge
	dbErrors     m.Gauge
}

func initMetrics(appName string) *Metrics {

	metrics := &Metrics{
		callsSuccess: m.NewGauge("", appName, "success", "success"),
		errs:         m.NewGauge("", appName, "errors", "errors"),
		notifyErrors: m.NewGauge("", appName, "notify_errors", "notify errors "),
		dbErrors:     m.NewGauge("", "", "db_errors", "db errors "),
	}
	go func() {
		for range time.Tick(time.Minute) {
			metrics.callsSuccess.Update()
			metrics.errs.Update()
			metrics.notifyErrors.Update()
			metrics.dbErrors.Update()
		}
	}()
	return metrics
}

func notifyUniqueContentURL(eventName string, msg inmem_service.ContentSentProperties) (err error) {
	priority := uint8(0)
	if eventName == "create" {
		priority = uint8(1)
	} else if eventName == "delete" {
	} else {
		log.WithField("event", eventName).Fatal("unknown event name")
	}

	msg.SentAt = time.Now().UTC()
	defer func() {
		fields := log.Fields{
			"q":     ContentSvc.conf.Queues.UniqueUrl,
			"event": eventName,
			"tid":   msg.Tid,
		}
		if err != nil {
			ContentSvc.m.notifyErrors.Inc()

			fields["data"] = fmt.Sprintf("%#v", msg)
			fields["error"] = fmt.Errorf(eventName+": %s", err.Error())
			log.WithFields(fields).Error("cannot send")
		} else {
			log.WithFields(fields).Info("sent")
		}
	}()

	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	var body []byte
	body, err = json.Marshal(event)

	if err != nil {
		err = fmt.Errorf(eventName+" json.Marshal: %s", err.Error())
		return
	}

	ContentSvc.n.Publish(amqp.AMQPMessage{
		QueueName: ContentSvc.conf.Queues.UniqueUrl,
		Body:      body,
		Priority:  priority,
	})
	return nil
}
