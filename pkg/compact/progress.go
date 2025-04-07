// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const ParallelLimit = 1024

type CompactorState int64

// Use a gauge to track the state of a compactor pod.
// DO not inject new state in the middle of the enum otherwise it will break the old state syntax.
const (
	Initializing CompactorState = iota
	Idle
	SyncMeta
	ApplyRetention
	Compacting
	GarbageCollect
	DownSampling
	CleanBlocks
	Grouping
	CalculateProgress
)

// Register all the compactor important threads here.
const (
	Main      = "main"      // Main compaction goroutine
	Web       = "web"       // Web server goroutine
	Cleanup   = "cleanup"   // Cleanup goroutine
	Calculate = "Calculate" // Calculate calculation compactor progress goroutine
)

type Progress struct {
	state  prometheus.Gauge
	logger log.Logger
}

func (p *Progress) Set(state CompactorState) {
	level.Info(p.logger).Log("msg", "Setting compactor progress state", "state", state)
	p.state.Set(float64(state))
}

func (p *Progress) Idle() {
	p.Set(Idle)
}

type ProgressRegistry struct {
	*prometheus.GaugeVec
	logger log.Logger
}

func NewProgressRegistry(reg *prometheus.Registry, logger log.Logger) *ProgressRegistry {
	registry := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "thanos_compactor_progress_state",
		Help: "The compaction progress state of the compactor.",
	}, []string{"thread"})
	level.Info(logger).Log("msg", "Registering compactor progress state gauge")
	registry.WithLabelValues(Main).Set(float64(Initializing))
	registry.WithLabelValues(Web).Set(float64(Initializing))
	registry.WithLabelValues(Cleanup).Set(float64(Initializing))
	return &ProgressRegistry{
		registry,
		logger,
	}
}

func (pr *ProgressRegistry) Get(thread string) *Progress {
	return &Progress{
		state:  pr.WithLabelValues(thread),
		logger: pr.logger,
	}
}

// SetProgress is a helper function to set the progress state of a compactor and can be no-op if the progress is nil.
func SetProgress(progress *Progress, state CompactorState) {
	if progress == nil {
		return
	}
	progress.Set(state)
}
