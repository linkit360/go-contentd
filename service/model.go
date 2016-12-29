// this file makes 2 things:
// 1) - get content url by campaign hash and msisdn
// 2) - add a record to the content_sent table
// the file itself is big enough, bcz we keep all required tables in memory (as a in-memory cache)

package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
	shortid "github.com/ventu-io/go-shortid"

	inmem_client "github.com/vostrok/inmem/rpcclient"
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
	metricInstancePrefix string,
	appName string,
	sConf ContentServiceConfig,
	inMemConfig inmem_client.RPCClientConfig,
	notifierConfig amqp.NotifierConfig,
	dbConfig db.DataBaseConfig,
) {

	sConf.DBConfig = dbConfig
	ContentSvc = &ContentService{
		m:    initMetrics(metricInstancePrefix, appName),
		sid:  shortid.MustNew(1, shortid.DefaultABC, uint64(time.Now().UnixNano())),
		db:   db.Init(dbConfig),
		conf: sConf,
	}
	log.SetLevel(log.DebugLevel)
	inmem_client.Init(inMemConfig)

	ContentSvc.n = amqp.NewNotifier(notifierConfig)

	ContentSvc.initUniqueUrlsCache()

	log.Debug("init done")
}

type ContentSentProperties struct {
	SentAt       time.Time `json:"sent_at,omitempty"`
	Msisdn       string    `json:"msisdn,omitempty"`
	Tid          string    `json:"tid,omitempty"`
	ContentPath  string    `json:"content_path,omitempty"`
	UniqueUrl    string    `json:"unique_url,omitempty"`
	ContentName  string    `json:"content_name,omitempty"`
	CampaignId   int64     `json:"campaign_id,omitempty"`
	ContentId    int64     `json:"content_id,omitempty"`
	ServiceId    int64     `json:"service_id,omitempty"`
	OperatorCode int64     `json:"operator_code,omitempty"`
	CountryCode  int64     `json:"country_code,omitempty"`
	Error        string    `json:"error,omitempty"`
}

// Used to get a key of used content ids
// when key == msisdn, then uniq content exactly
// when key == msisdn + service_id, then unique content per sevice
func (t ContentSentProperties) key() string {
	return t.Msisdn + "-" + strconv.FormatInt(t.ServiceId, 10)
}

type GetContentParams struct {
	Msisdn       string `json:"msisdn"`
	Tid          string `json:"tid"`
	ServiceId    int64  `json:"service_id"`
	CampaignId   int64  `json:"campaign_id"`
	OperatorCode int64  `json:"operator_code"`
	CountryCode  int64  `json:"country_code"`
}

type GetUniqueUrlParams struct {
	Msisdn     string `json:"msisdn"`
	CampaignId int64  `json:"campaign_id"`
}

type Metrics struct {
	callsSuccess m.Gauge
	errs         m.Gauge
	notifyErrors m.Gauge
	dbErrors     m.Gauge
}

func initMetrics(metricInstancePrefix, appName string) *Metrics {
	m.Init(metricInstancePrefix)

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

func notifyNewUniqueContentURL(msg ContentSentProperties) (err error) {
	msg.SentAt = time.Now().UTC()
	eventName := "unique_url_created"
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
	})
	return nil
}
