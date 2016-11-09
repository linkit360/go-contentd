package service

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/metrics"
)

var metr metrics

type metrics struct {
	campaignNotFound prometheus.Gauge
	calls            prometheus.Gauge
	errs             prometheus.Gauge
}

var (
	campaignNotFound int64
	calls            int64
	errs             int64
)

func initMetrics(appName string) {
	m.Init(appName)

	metr = metrics{
		campaignNotFound: m.NewGauge(
			"",
			"",
			"campaign_not_found_rpm",
			"Number of requests with campaign not found error",
		),
		calls: m.NewGauge(
			"",
			"",
			"call_rpm",
			"Number of GetUrlByCampaignHash calls",
		),
		errs: m.NewGauge(
			"",
			"",
			"call_errors_rpm",
			"Number of errors inside GetUrlByCampaignHash calls",
		),
	}
	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {
			metr.campaignNotFound.Set(float64(campaignNotFound))
			metr.calls.Set(float64(calls))
			metr.errs.Set(float64(errs))
		}
	}()
}
