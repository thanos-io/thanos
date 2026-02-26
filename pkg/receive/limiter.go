// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Limiter is responsible for managing the configuration and initialization of
// different types that apply limits to the Receive instance.
type Limiter struct {
	sync.RWMutex
	requestLimiter            requestLimiter
	headSeriesLimiterMtx      sync.Mutex
	headSeriesLimiter         headSeriesLimiter
	writeGate                 gate.Gate
	registerer                prometheus.Registerer
	configPathOrContent       fileContent
	logger                    log.Logger
	configReloadCounter       prometheus.Counter
	configReloadFailedCounter prometheus.Counter
	receiverMode              ReceiverMode
	configReloadTimer         time.Duration
}

// headSeriesLimiter encompasses active/head series limiting logic.
type headSeriesLimiter interface {
	QueryMetaMonitoring(context.Context) error
	isUnderLimit(tenant string) (bool, error)
}

type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
	AllowNativeHistogram(tenant string, h prompb.Histogram) (prompb.Histogram, bool)
}

// fileContent is an interface to avoid a direct dependency on kingpin or extkingpin.
type fileContent interface {
	Content() ([]byte, error)
	Path() string
}

func (l *Limiter) HeadSeriesLimiter() headSeriesLimiter {
	l.headSeriesLimiterMtx.Lock()
	defer l.headSeriesLimiterMtx.Unlock()

	return l.headSeriesLimiter
}

// NewLimiter creates a new *Limiter given a configuration and prometheus
// registerer.
func NewLimiter(configFile fileContent, reg prometheus.Registerer, r ReceiverMode, logger log.Logger, configReloadTimer time.Duration) (*Limiter, error) {
	limiter := &Limiter{
		writeGate:         gate.NewNoop(),
		requestLimiter:    &noopRequestLimiter{},
		headSeriesLimiter: NewNopSeriesLimit(),
		logger:            logger,
		receiverMode:      r,
		configReloadTimer: configReloadTimer,
	}

	if reg != nil {
		limiter.registerer = NewUnRegisterer(reg)
		limiter.configReloadCounter = promauto.With(limiter.registerer).NewCounter(
			prometheus.CounterOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      "limits_config_reload_total",
				Help:      "How many times the limit configuration was reloaded",
			},
		)
		limiter.configReloadFailedCounter = promauto.With(limiter.registerer).NewCounter(
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
		return nil, errors.Wrap(err, "load tenant limits config")
	}

	return limiter, nil
}

// StartConfigReloader starts the automatic configuration reloader based off of
// the file indicated by pathOrContent. It starts a Go routine in the given
// *run.Group.
func (l *Limiter) StartConfigReloader(ctx context.Context) error {
	if !l.CanReload() {
		return nil
	}

	return extkingpin.PathContentReloader(ctx, l.configPathOrContent, l.logger, func() {
		level.Info(l.logger).Log("msg", "reloading limit config")
		if err := l.loadConfig(); err != nil {
			if failedReload := l.configReloadCounter; failedReload != nil {
				failedReload.Inc()
			}
			errMsg := fmt.Sprintf("error reloading tenant limits config from %s", l.configPathOrContent.Path())
			level.Error(l.logger).Log("msg", errMsg, "err", err)
		}
		if reloadCounter := l.configReloadCounter; reloadCounter != nil {
			reloadCounter.Inc()
		}
	}, l.configReloadTimer)
}

func (l *Limiter) CanReload() bool {
	if l.configPathOrContent == nil {
		return false
	}
	if l.configPathOrContent.Path() == "" {
		return false
	}
	return true
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
			gate.WriteRequests,
		)
	}
	l.requestLimiter = newConfigRequestLimiter(
		l.registerer,
		&config.WriteLimits,
	)
	seriesLimitIsActivated := func() bool {
		if config.WriteLimits.DefaultLimits.HeadSeriesLimit != 0 {
			return true
		}
		for _, tenant := range config.WriteLimits.TenantsLimits {
			if tenant.HeadSeriesLimit != nil && *tenant.HeadSeriesLimit != 0 {
				return true
			}
		}
		return false
	}
	if (l.receiverMode == RouterOnly || l.receiverMode == RouterIngestor) && seriesLimitIsActivated() {
		l.headSeriesLimiterMtx.Lock()
		l.headSeriesLimiter = NewHeadSeriesLimit(config.WriteLimits, l.registerer, l.logger)
		l.headSeriesLimiterMtx.Unlock()
	}
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
