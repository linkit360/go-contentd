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

//var quantiles = []int{50, 90, 95, 99}

type MethodTimeMetric struct {
	//th       kitmetrics.TimeHistogram
	overtime kitmetrics.Counter
}

func (m MethodTimeMetric) CatchOverTime(dur time.Duration, max time.Duration) {
	if dur > max {
		m.overtime.Add(1)
	}
	//m.th.Observe(dur)
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
		//kitmetrics.NewTimeHistogram(time.Millisecond,
		//	expvar.NewHistogram("duration_ms_"+name, 0, 10000, 3, quantiles...)),
		expvar.NewCounter("overtime_" + name),
	}
	lm.Count = expvar.NewCounter("access_" + name)
	lm.Errors = expvar.NewCounter("errors_" + name)
	return lm
}
