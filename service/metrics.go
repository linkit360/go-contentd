package service

import (
	m "github.com/vostrok/utils/metrics"
)

var (
	campaignNotFound m.Gauge
	calls            m.Gauge
	errs             m.Gauge
)

func initMetrics() {
	campaignNotFound = m.NewGauge("", "service", "campaign_not_found", "Number of requests with campaign not found error")
	calls = m.NewGauge("", "service", "call", "Number of GetUrlByCampaignHash calls")
	errs = m.NewGauge("", "service", "call_errors", "Number of errors inside GetUrlByCampaignHash calls")
}
