package metrics

import (
	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"

	"time"
)

var M AppMetrics

type AppMetrics struct {
	RequestsOverall LocationMetric
}

func Init() AppMetrics {
	M = AppMetrics{
		RequestsOverall: NewLocationMetric("requests_overall"),
	}
}

var quantiles = []int{50, 90, 95, 99}

type MethodTimeMetric struct {
	th       metrics.TimeHistogram
	overtime metrics.Counter
}

func (m MethodTimeMetric) CatchOverTime(dur time.Duration, max time.Duration) {
	if dur > max {
		m.overtime.Add(1)
	}
	m.th.Observe(dur)
}

type LocationMetric struct {
	Time   MethodTimeMetric
	Count  metrics.Counter
	Errors metrics.Counter
}

func NewLocationMetric(name string) (lm LocationMetric) {
	if name == "" {
		log.WithField("error", "NewLocationMetric").Fatal("no name for location metric")
	}
	lm.Time = MethodTimeMetric{
		metrics.NewTimeHistogram(time.Millisecond,
			expvar.NewHistogram("duration_ms_"+name, 0, 10000, 3, quantiles...)),
		expvar.NewCounter("overtime_" + name),
	}
	lm.Count = expvar.NewCounter("access_" + name)
	lm.Errors = expvar.NewCounter("errors_" + name)
	return lm
}
