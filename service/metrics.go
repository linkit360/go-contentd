package service

import (
	m "github.com/vostrok/utils/metrics"
)

var (
	calls  m.Gauge
	errs   m.Gauge
	rpcErr m.Gauge
)

func initMetrics() {
	calls = m.NewGauge("", "service", "call", "Number of calls")
	errs = m.NewGauge("", "service", "call_errors", "Number of errors in calls")

	rpcErr = m.NewGauge("rpc", "inmem", "errors", "Number of RPC call errors (inmem)")
}
