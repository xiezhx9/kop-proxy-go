package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PulsarSendSuccessCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "pulsar", "produce_topic_send_success_total")},
	)
	PulsarSendFailCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "pulsar", "produce_topic_send_fail_total")},
	)
	PulsarSendLatency = promauto.NewSummary(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "pulsar", "produce_topic_send_latency_ms"),
			Objectives: objectives},
	)
	PulsarTopicSendSuccessCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "pulsar", "produce_send_success_total")},
		[]string{"kafka_topic", "pulsar_topic"},
	)
	PulsarTopicSendFailCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "pulsar", "produce_send_fail_total")},
		[]string{"kafka_topic", "pulsar_topic"},
	)
	PulsarTopicSendLatency = promauto.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       prometheus.BuildFQName(namespace, "pulsar", "produce_send_latency_ms"),
			Objectives: objectives},
		[]string{"kafka_topic", "pulsar_topic"},
	)
)
