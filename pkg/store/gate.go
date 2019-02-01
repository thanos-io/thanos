package store

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/gate"
)

// Gate wraps the Prometheus gate with extra metrics.
type Gate struct {
	g              *gate.Gate
	currentQueries prometheus.Gauge
}

// NewGate returns a new gate.
func NewGate(maxConcurrent int, reg prometheus.Registerer) *Gate {
	g := &Gate{
		g: gate.New(maxConcurrent),
	}
	g.currentQueries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_queries_total",
		Help: "Total number of currently executing queries.",
	})

	if reg != nil {
		reg.MustRegister(g.currentQueries)
	}

	return g
}

// Start iniates a new query.
func (g *Gate) Start(ctx context.Context) error {
	g.currentQueries.Inc()
	return g.g.Start(ctx)
}

// Done finishes a query.
func (g *Gate) Done() {
	g.currentQueries.Dec()
	g.g.Done()
}
