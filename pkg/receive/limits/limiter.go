// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package limits

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/extkingpin"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
)

// Limiter is responsible for managing the configuration and initialization of
// different types that apply limits to the Receive instance.
type Limiter struct {
	sync.RWMutex
	requestLimiter            requestLimiter
	writeGate                 gate.Gate
	registerer                prometheus.Registerer
	configPathOrContent       fileContent
	logger                    log.Logger
	configReloadCounter       prometheus.Counter
	configReloadFailedCounter prometheus.Counter
}

type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
}

// fileContent is an interface to avoid a direct dependency on kingpin or extkingpin.
type fileContent interface {
	Content() ([]byte, error)
	Path() string
}

// NewLimiter creates a new *Limiter given a configuration and prometheus
// registerer.
func NewLimiter(configFile fileContent, reg prometheus.Registerer, logger log.Logger) (*Limiter, error) {
	limiter := &Limiter{
		writeGate:      gate.NewNoop(),
		requestLimiter: &noopRequestLimiter{},
		registerer:     reg,
		logger:         logger,
	}

	if reg != nil {
		limiter.configReloadCounter = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "limits_config_reload_total",
				Help:      "How many times the limit configuration was reloaded",
			},
		)
		limiter.configReloadFailedCounter = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "limits_config_reload_err_total",
				Help:      "How many times the limit configuration failed to reload.",
			},
		)
	}

	if configFile == nil {
		return limiter, nil
	}

	limiter.configPathOrContent = configFile
	if err := limiter.loadConfig(); err != nil {
		// TODO: wrap error.
		return nil, err
	}

	return limiter, nil
}

// StartConfigReloader starts the automatic configuration reloader based off of
// the file indicated by pathOrContent. It starts a Go routine in the given
// *run.Group. Pushes error parsing the reloaded configuration into errChan.
func (l *Limiter) StartConfigReloader(ctx context.Context, errChan chan<- error) error {
	if l.configPathOrContent == nil {
		return nil
	}
	if l.configPathOrContent.Path() == "" {
		return nil
	}

	return extkingpin.PathContentReloader(ctx, l.configPathOrContent, l.logger, func() {
		level.Info(l.logger).Log("msg", "reloading limit config")
		if err := l.loadConfig(); err != nil {
			if failedReload := l.configReloadCounter; failedReload != nil {
				failedReload.Inc()
			}
			if errChan != nil {
				errChan <- err
			}
			errMsg := fmt.Sprintf("error reloading tenant limits config from %s", l.configPathOrContent.Path())
			level.Error(l.logger).Log("msg", errMsg, "err", err)
		}
		if reloadCounter := l.configReloadCounter; reloadCounter != nil {
			reloadCounter.Inc()
		}
	})
}

func (l *Limiter) loadConfig() error {
	config, err := ParseLimitConfigContent(l.configPathOrContent)
	if err != nil {
		return err
	}
	l.Lock()
	defer l.Unlock()
	maxWriteConcurrency := config.WriteLimits.GlobalLimits.MaxConcurrency
	if maxWriteConcurrency > 0 {
		l.writeGate = gate.New(
			extprom.WrapRegistererWithPrefix(
				"thanos_receive_write_request_concurrent_",
				l.registerer,
			),
			int(maxWriteConcurrency),
		)
	}
	l.requestLimiter = newConfigRequestLimiter(
		l.registerer,
		&config.WriteLimits,
	)
	return nil
}

// RequestLimiter is a safe getter for the request limiter.
func (l *Limiter) RequestLimiter() requestLimiter {
	l.RLock()
	defer l.RUnlock()
	return l.requestLimiter
}

// WriteGate is a safe getter for the write gate.
func (l *Limiter) WriteGate() gate.Gate {
	l.RLock()
	defer l.RUnlock()
	return l.writeGate
}

// ParseLimitConfigContent parses the limit configuration from the path or
// content.
func ParseLimitConfigContent(limitsConfig fileContent) (*RootLimitsConfig, error) {
	if limitsConfig == nil {
		return &RootLimitsConfig{}, nil
	}
	limitsContentYaml, err := limitsConfig.Content()
	if err != nil {
		return nil, errors.Wrap(err, "get content of limit configuration")
	}
	parsedConfig, err := ParseRootLimitConfig(limitsContentYaml)
	if err != nil {
		return nil, errors.Wrap(err, "parse limit configuration")
	}
	return parsedConfig, nil
}

type nopConfigContent struct{}

var _ fileContent = (*nopConfigContent)(nil)

// Content returns no content and no error.
func (n nopConfigContent) Content() ([]byte, error) {
	return nil, nil
}

// Path returns an empty path.
func (n nopConfigContent) Path() string {
	return ""
}

// NewNopConfig creates a no-op config content (no configuration).
func NewNopConfig() nopConfigContent {
	return nopConfigContent{}
}
