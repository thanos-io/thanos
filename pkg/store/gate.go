package store

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/gate"
)

// Gate wraps the Prometheus gate with extra metrics.
type Gate struct {
	g              *gate.Gate
	currentQueries prometheus.Gauge
	gateTiming     prometheus.Histogram
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
	g.gateTiming = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_gate_seconds",
		Help: "How many seconds it took for a query to wait at the gate.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 120,
		},
	})

	if reg != nil {
		reg.MustRegister(g.currentQueries, g.gateTiming)
	}

	return g
}

// IsMyTurn iniates a new query and waits until it's our turn to fulfill a query request.
func (g *Gate) IsMyTurn(ctx context.Context) error {
	start := time.Now()
	defer func() {
		g.gateTiming.Observe(float64(time.Now().Sub(start)))
	}()

	if err := g.g.Start(ctx); err != nil {
		return err
	}

	g.currentQueries.Inc()
	return nil
}

// Done finishes a query.
func (g *Gate) Done() {
	g.currentQueries.Dec()
	g.g.Done()
}
