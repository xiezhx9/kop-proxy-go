package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PulsarSendSuccessCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "produce", "pulsar_send_success_total")},
		[]string{"kafka_topic", "pulsar_topic"},
	)
	PulsarSendFailCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "produce", "pulsar_send_fail_total")},
		[]string{"kafka_topic", "pulsar_topic"},
	)
	PulsarSendLatency = promauto.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "produce", "pulsar_send_latency_ms"),
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}},
		[]string{"kafka_topic", "pulsar_topic"},
	)
)
