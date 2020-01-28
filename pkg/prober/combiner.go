// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

import "sync"

type combined struct {
	mu     sync.Mutex
	probes []Probe
}

// Combine folds given probes into one, reflects their statuses in a thread-safe way.
func Combine(probes ...Probe) Probe {
	return &combined{probes: probes}
}

// Ready sets components status to ready.
func (p *combined) Ready() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, probe := range p.probes {
		probe.Ready()
	}
}

// NotReady sets components status to not ready with given error as a cause.
func (p *combined) NotReady(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, probe := range p.probes {
		probe.NotReady(err)
	}
}

// Healthy sets components status to healthy.
func (p *combined) Healthy() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, probe := range p.probes {
		probe.Healthy()
	}
}

// NotHealthy sets components status to not healthy with given error as a cause.
func (p *combined) NotHealthy(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, probe := range p.probes {
		probe.NotHealthy(err)
	}
}
