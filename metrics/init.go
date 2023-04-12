package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	namespace = "kop"
)

var objectives = map[float64]float64{
	0.5: 0.05, 0.9: 0.01, 0.99: 0.001,
}

func init() {
	prometheus.MustRegister()
}
