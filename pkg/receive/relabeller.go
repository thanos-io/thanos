// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"
)

// Relabeller is responsible for managing the configuration and initialization of
// different types that apply relabel configurations to the Receive instance.
// The new config is atomically swapped in, so we don't need to use locks.
type Relabeller struct {
	configPathOrContent       fileContent
	relabelConfigs            *atomic.Pointer[RelabelConfig]
	logger                    log.Logger
	configReloadCounter       prometheus.Counter
	configReloadFailedCounter prometheus.Counter
	configReloadTimer         time.Duration
}

// RelabelConfig is a collection of relabel configurations.
type RelabelConfig []*relabel.Config

// NewRelabeller creates a new relabeller and loads the configuration to make sure loading is possible.
func NewRelabeller(configFile fileContent, reg prometheus.Registerer, logger log.Logger, configReloadTimer time.Duration) (*Relabeller, error) {
	var relabelConfigs atomic.Pointer[RelabelConfig]
	relabelConfigs.Store(&RelabelConfig{})
	relabeller := &Relabeller{
		configPathOrContent: configFile,
		relabelConfigs:      &relabelConfigs,
		logger:              logger,
		configReloadTimer:   configReloadTimer,
	}

	if reg != nil {
		relabeller.configReloadCounter = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "relabel_config_reload_total",
				Help:      "How many times the relabel configuration was reloaded",
			},
		)
		relabeller.configReloadFailedCounter = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "relabel_config_reload_err_total",
				Help:      "How many times the relabel configuration failed to reload.",
			},
		)
	}
	if configFile == nil {
		return relabeller, nil
	}
	relabeller.configPathOrContent = configFile
	if err := relabeller.loadConfig(); err != nil {
		return nil, errors.Wrap(err, "load relabel config")
	}
	return relabeller, nil
}

// simply returns the provided config. This is useful for testing.
func newRelabelerWithConstantConfig(config RelabelConfig, logger log.Logger) *Relabeller {
	var relabelConfigs atomic.Pointer[RelabelConfig]
	relabelConfigs.Store(&config)
	return &Relabeller{nil, &relabelConfigs, logger, nil, nil, 0}
}

// RelabelConfig returns the current relabel config.
// This is concurrent safe.
func (r *Relabeller) RelabelConfig() RelabelConfig {
	if r == nil {
		var relabelConfig RelabelConfig
		return relabelConfig
	}
	return *r.relabelConfigs.Load()
}

// setRelabelConfig sets the relabel config to the provided array.
// This is concurrent safe.
func (r *Relabeller) setRelabelConfig(configs RelabelConfig) {
	r.relabelConfigs.Store(&configs)
}

func (r *Relabeller) loadConfig() error {
	relabelContentYaml, err := r.configPathOrContent.Content()
	if err != nil {
		return errors.Wrap(err, "getting content of relabel config")
	}
	var relabelConfig RelabelConfig
	if err := yaml.Unmarshal(relabelContentYaml, &relabelConfig); err != nil {
		return errors.Wrap(err, "parsing relabel config")
	}
	r.setRelabelConfig(relabelConfig)
	return nil
}

// StartConfigReloader starts the automatic configuration reloader based off of
// the file indicated by pathOrContent.
func (r *Relabeller) StartConfigReloader(ctx context.Context) error {
	if !r.CanReload() {
		return nil
	}

	return extkingpin.PathContentReloader(ctx, r.configPathOrContent, r.logger, func() {
		level.Info(r.logger).Log("msg", "reloading relabel config.")

		if err := r.loadConfig(); err != nil {
			if failedReload := r.configReloadFailedCounter; failedReload != nil {
				failedReload.Inc()
			}
			errMsg := fmt.Sprintf("error reloading relabel config from %s", r.configPathOrContent.Path())
			level.Error(r.logger).Log("msg", errMsg, "err", err)
		}
		if reloadCounter := r.configReloadCounter; reloadCounter != nil {
			reloadCounter.Inc()
		}

	}, r.configReloadTimer)
}

func (r *Relabeller) CanReload() bool {
	if r.configPathOrContent == nil {
		return false
	}
	if r.configPathOrContent.Path() == "" {
		return false
	}
	return true
}
