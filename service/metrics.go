package service

import (
	"time"

	m "github.com/vostrok/metrics"
)

var (
	campaignNotFound m.Gauge
	calls            m.Gauge
	errs             m.Gauge
)

func initMetrics() {

	campaignNotFound = m.NewGaugeMetric(
		"service",
		"campaign_not_found",
		"Number of requests with campaign not found error",
	)
	calls = m.NewGaugeMetric(
		"service",
		"call",
		"Number of GetUrlByCampaignHash calls",
	)
	errs = m.NewGaugeMetric(
		"service",
		"call_errors",
		"Number of errors inside GetUrlByCampaignHash calls",
	)

	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {
			campaignNotFound.Update()
			calls.Update()
			errs.Update()
		}
	}()
}
