package service

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/vostrok/metrics"
)

var (
	campaignNotFound prometheus.Counter
	calls            prometheus.Counter
	errs             prometheus.Counter
)

func initMetrics(appName string) {
	metrics.Init(appName)

	campaignNotFound = metrics.NewCounter(
		"campaign_not_found_total",
		"Number of requests with campaign not found error",
	)

	calls = metrics.NewCounter(
		"call_total",
		"Number of GetUrlByCampaignHash calls",
	)
	errs = metrics.NewCounter(
		"call_errors_total",
		"Number of errors inside GetUrlByCampaignHash calls",
	)
}
