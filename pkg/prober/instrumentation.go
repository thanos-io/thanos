// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

import (
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/pkg/component"
)

const (
	ready    = "ready"
	notReady = "not-ready"
	healthy  = "healthy"
)

// InstrumentationProbe stores instrumentation state of Probe.
// This is created with an intention to combine with other Probe's using prober.Combine.
type InstrumentationProbe struct {
	component component.Component
	logger    log.Logger

	statusMetric *prometheus.GaugeVec
	mu           sync.Mutex
	statusString string
}

// NewInstrumentation returns InstrumentationProbe records readiness and healthiness for given component.
func NewInstrumentation(component component.Component, logger log.Logger, reg prometheus.Registerer) *InstrumentationProbe {
	p := InstrumentationProbe{
		component: component,
		logger:    logger,
		statusMetric: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "status",
			Help:        "Represents status (0 indicates failure, 1 indicates success) of the component.",
			ConstLabels: map[string]string{"component": component.String()},
		},
			[]string{"check"},
		),
	}
	return &p
}

// Ready records the component status when Ready is called, if combined with other Probes.
func (p *InstrumentationProbe) Ready() {
	p.statusMetric.WithLabelValues(ready).Set(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statusString != ready {
		level.Info(p.logger).Log("msg", "changing probe status", "status", ready)
		p.statusString = ready
	}
}

// NotReady records the component status when NotReady is called, if combined with other Probes.
func (p *InstrumentationProbe) NotReady(err error) {
	p.statusMetric.WithLabelValues(ready).Set(0)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statusString != notReady {
		level.Warn(p.logger).Log("msg", "changing probe status", "status", notReady, "reason", err)
		p.statusString = notReady
	}
}

// Healthy records the component status when Healthy is called, if combined with other Probes.
func (p *InstrumentationProbe) Healthy() {
	p.statusMetric.WithLabelValues(healthy).Set(1)
	level.Info(p.logger).Log("msg", "changing probe status", "status", "healthy")
}

// NotHealthy records the component status when NotHealthy is called, if combined with other Probes.
func (p *InstrumentationProbe) NotHealthy(err error) {
	p.statusMetric.WithLabelValues(healthy).Set(0)
	level.Info(p.logger).Log("msg", "changing probe status", "status", "not-healthy", "reason", err)
}
