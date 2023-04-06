package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "kafka_pulsar"
)

func Init() {
	prometheus.MustRegister()
}
