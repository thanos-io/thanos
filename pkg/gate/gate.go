// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package gate

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promgate "github.com/prometheus/prometheus/util/gate"
)

// Gate controls the maximum number of concurrently running and waiting queries.
//
// Example of use:
//
//	g := gate.New(r, 5)
//
//	if err := g.Start(ctx); err != nil {
//	   return
//	}
//	defer g.Done()
type Gate interface {
	// Start initiates a new request and waits until it's our turn to fulfill a request.
	Start(ctx context.Context) error
	// Done finishes a query.
	Done()
}

// Keeper is used to create multiple gates sharing the same metrics.
//
// Deprecated: when Keeper is used to create several gates, the metric tracking
// the number of in-flight metric isn't meaningful because it is hard to say
// whether requests are being blocked or not. For clients that call
// gate.(*Keeper).NewGate only once, it is recommended to use gate.New()
// instead. Otherwise it is recommended to use the
// github.com/prometheus/prometheus/util/gate package directly and wrap the
// returned gate with gate.InstrumentGateDuration().
type Keeper struct {
	reg prometheus.Registerer
}

// NewKeeper creates a new Keeper.
//
// Deprecated: see Keeper.
func NewKeeper(reg prometheus.Registerer) *Keeper {
	return &Keeper{
		reg: reg,
	}
}

// NewGate returns a new Gate ready for use.
//
// Deprecated: see Keeper.
func (k *Keeper) NewGate(maxConcurrent int) Gate {
	return New(k.reg, maxConcurrent, Queries)
}

type OperationName string

const (
	Queries OperationName = "queries"
	Selects OperationName = "selects"
	Gets    OperationName = "gets"
	Sets    OperationName = "sets"
	Writes  OperationName = "writes"
)

// New returns an instrumented gate limiting the number of requests being
// executed concurrently.
//
// The gate implementation is based on the
// github.com/prometheus/prometheus/util/gate package.
//
// It can be called several times but not with the same registerer otherwise it
// will panic when trying to register the same metric multiple times.
func New(reg prometheus.Registerer, maxConcurrent int, opName OperationName) Gate {
	var (
		maxGaugeOpts = prometheus.GaugeOpts{
			Name: fmt.Sprintf("gate_%s_max", opName),
			Help: fmt.Sprintf("Maximum number of concurrent %s.", opName),
		}
		inFlightGaugeOpts = prometheus.GaugeOpts{
			Name: fmt.Sprintf("gate_%s_in_flight", opName),
			Help: fmt.Sprintf("Number of %s that are currently in flight.", opName),
		}
		totalCounterOpts = prometheus.CounterOpts{
			Name: fmt.Sprintf("gate_%s_total", opName),
			Help: fmt.Sprintf("Total number of %s.", opName),
		}
		durationHistogramOpts = prometheus.HistogramOpts{
			Name:    fmt.Sprintf("gate_%s_duration_seconds", opName),
			Help:    fmt.Sprintf("How many seconds it took for %s to wait at the gate.", opName),
			Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120, 240, 360, 720},
		}
	)

	promauto.With(reg).NewGauge(maxGaugeOpts).Set(float64(maxConcurrent))

	var gate Gate
	if maxConcurrent <= 0 {
		gate = NewNoop()
	} else {
		gate = promgate.New(maxConcurrent)
	}

	return InstrumentGateDuration(
		promauto.With(reg).NewHistogram(durationHistogramOpts),
		InstrumentGateTotal(
			promauto.With(reg).NewCounter(totalCounterOpts),
			InstrumentGateInFlight(
				promauto.With(reg).NewGauge(inFlightGaugeOpts),
				gate,
			),
		),
	)
}

type noopGate struct{}

func (noopGate) Start(context.Context) error { return nil }
func (noopGate) Done()                       {}

func NewNoop() Gate { return noopGate{} }

type instrumentedDurationGate struct {
	g        Gate
	duration prometheus.Observer
}

// InstrumentGateDuration instruments the provided Gate to track how much time
// the request has been waiting in the gate.
func InstrumentGateDuration(duration prometheus.Observer, g Gate) Gate {
	return &instrumentedDurationGate{
		g:        g,
		duration: duration,
	}
}

// Start implements the Gate interface.
func (g *instrumentedDurationGate) Start(ctx context.Context) error {
	start := time.Now()
	defer func() {
		g.duration.Observe(time.Since(start).Seconds())
	}()

	return g.g.Start(ctx)
}

// Done implements the Gate interface.
func (g *instrumentedDurationGate) Done() {
	g.g.Done()
}

type instrumentedInFlightGate struct {
	g        Gate
	inflight prometheus.Gauge
}

// InstrumentGateInFlight instruments the provided Gate to track how many
// requests are currently in flight.
func InstrumentGateInFlight(inflight prometheus.Gauge, g Gate) Gate {
	return &instrumentedInFlightGate{
		g:        g,
		inflight: inflight,
	}
}

// Start implements the Gate interface.
func (g *instrumentedInFlightGate) Start(ctx context.Context) error {
	if err := g.g.Start(ctx); err != nil {
		return err
	}

	g.inflight.Inc()
	return nil
}

// Done implements the Gate interface.
func (g *instrumentedInFlightGate) Done() {
	g.inflight.Dec()
	g.g.Done()
}

type instrumentedTotalGate struct {
	g     Gate
	total prometheus.Counter
}

// InstrumentGateTotal instruments the provided Gate to track total requests.
func InstrumentGateTotal(total prometheus.Counter, g Gate) Gate {
	return &instrumentedTotalGate{
		g:     g,
		total: total,
	}
}

// Start implements the Gate interface.
func (g *instrumentedTotalGate) Start(ctx context.Context) error {
	g.total.Inc()
	if err := g.g.Start(ctx); err != nil {
		return err
	}

	return nil
}

// Done implements the Gate interface.
func (g *instrumentedTotalGate) Done() {
	g.g.Done()
}
