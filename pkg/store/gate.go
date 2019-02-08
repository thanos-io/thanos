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
	gateTiming     prometheus.Summary
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
	g.gateTiming = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_gate_seconds",
		Help: "How many seconds it took for a query to pass through the gate.",
	})

	if reg != nil {
		reg.MustRegister(g.currentQueries, g.gateTiming)
	}

	return g
}

// IsMyTurn iniates a new query and wait untils its our turn to fulfill a query request.
func (g *Gate) IsMyTurn(ctx context.Context) error {
	g.currentQueries.Inc()
	start := time.Now()
	if err := g.g.Start(ctx); err != nil {
		return err
	}
	g.gateTiming.Observe(float64(time.Now().Sub(start)))
	return nil
}

// Done finishes a query.
func (g *Gate) Done() {
	g.currentQueries.Dec()
	g.g.Done()
}
