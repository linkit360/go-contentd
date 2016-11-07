package metrics

import (
	"time"

	log "github.com/Sirupsen/logrus"
	kitmetrics "github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
)

var M AppMetrics

type AppMetrics struct {
	RequestsOverall  LocationMetric
	CQRRequest       LocationMetric
	CampaignNotFound kitmetrics.Gauge
	Counters         Counters
}
type Counters struct {
	CampaignNotFound float64
}

func Init() AppMetrics {

	M = AppMetrics{
		RequestsOverall:  NewLocationMetric("requests_overall"),
		CQRRequest:       NewLocationMetric("cqr_request"),
		CampaignNotFound: expvar.NewGauge("campaign_not_found"),
	}
	go func() {
		for range time.Tick(60 * time.Second) {
			M.CampaignNotFound.Set(M.Counters.CampaignNotFound)
			M.Counters.CampaignNotFound = 0
		}
	}()
	return M
}

type MethodTimeMetric struct {
	overtime kitmetrics.Counter
}

func (m MethodTimeMetric) CatchOverTime(dur time.Duration, max time.Duration) {
	if dur > max {
		m.overtime.Add(1)
	}
}

type LocationMetric struct {
	Time   MethodTimeMetric
	Count  kitmetrics.Counter
	Errors kitmetrics.Counter
}

func NewLocationMetric(name string) (lm LocationMetric) {
	if name == "" {
		log.WithField("error", "NewLocationMetric").Fatal("no name for location metric")
	}
	lm.Time = MethodTimeMetric{
		expvar.NewCounter("overtime_" + name),
	}
	lm.Count = expvar.NewCounter("access_" + name)
	lm.Errors = expvar.NewCounter("errors_" + name)
	return lm
}
