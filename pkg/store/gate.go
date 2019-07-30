package store

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/gate"
)

// Gate wraps the Prometheus gate with extra metrics.
type Gate struct {
	g               *gate.Gate
	inflightQueries prometheus.Gauge
	gateTiming      prometheus.Histogram
}

// NewGate returns a new query gate.
func NewGate(maxConcurrent int, reg prometheus.Registerer) *Gate {
	g := &Gate{
		g: gate.New(maxConcurrent),
		inflightQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gate_queries_in_flight",
			Help: "Number of queries that are currently in flight.",
		}),
		gateTiming: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "gate_duration_seconds",
			Help: "How many seconds it took for queries to wait at the gate.",
			Buckets: []float64{
				0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 10,
			},
		}),
	}

	if reg != nil {
		reg.MustRegister(g.inflightQueries, g.gateTiming)
	}

	return g
}

// IsMyTurn iniates a new query and waits until it's our turn to fulfill a query request.
func (g *Gate) IsMyTurn(ctx context.Context) error {
	start := time.Now()
	defer func() {
		g.gateTiming.Observe(float64(time.Since(start)))
	}()

	if err := g.g.Start(ctx); err != nil {
		return err
	}

	g.inflightQueries.Inc()
	return nil
}

// Done finishes a query.
func (g *Gate) Done() {
	g.inflightQueries.Dec()
	g.g.Done()
}
