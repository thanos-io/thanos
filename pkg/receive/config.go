// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"gopkg.in/fsnotify.v1"
)

// HashringConfig represents the configuration for a hashring
// a receive node knows about.
type HashringConfig struct {
	Hashring  string   `json:"hashring,omitempty"`
	Tenants   []string `json:"tenants,omitempty"`
	Endpoints []string `json:"endpoints"`
}

// ConfigWatcher is able to watch a file containing a hashring configuration
// for updates.
type ConfigWatcher struct {
	ch       chan []HashringConfig
	path     string
	interval time.Duration
	logger   log.Logger
	watcher  *fsnotify.Watcher

	hashGauge            prometheus.Gauge
	successGauge         prometheus.Gauge
	lastSuccessTimeGauge prometheus.Gauge
	changesCounter       prometheus.Counter
	errorCounter         prometheus.Counter
	refreshCounter       prometheus.Counter
	hashringNodesGauge   *prometheus.GaugeVec
	hashringTenantsGauge *prometheus.GaugeVec

	// last is the last known configuration.
	last []HashringConfig
}

// NewConfigWatcher creates a new ConfigWatcher.
func NewConfigWatcher(logger log.Logger, r prometheus.Registerer, path string, interval model.Duration) (*ConfigWatcher, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "creating file watcher")
	}
	if err := watcher.Add(path); err != nil {
		return nil, errors.Wrapf(err, "adding path %s to file watcher", path)
	}
	c := &ConfigWatcher{
		ch:       make(chan []HashringConfig),
		path:     path,
		interval: time.Duration(interval),
		logger:   logger,
		watcher:  watcher,
		hashGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_config_hash",
				Help: "Hash of the currently loaded hashring configuration file.",
			}),
		successGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_config_last_reload_successful",
				Help: "Whether the last hashring configuration file reload attempt was successful.",
			}),
		lastSuccessTimeGauge: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_config_last_reload_success_timestamp_seconds",
				Help: "Timestamp of the last successful hashring configuration file reload.",
			}),
		changesCounter: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_changes_total",
				Help: "The number of times the hashrings configuration file has changed.",
			}),
		errorCounter: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_errors_total",
				Help: "The number of errors watching the hashrings configuration file.",
			}),
		refreshCounter: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_refreshes_total",
				Help: "The number of refreshes of the hashrings configuration file.",
			}),
		hashringNodesGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "thanos_receive_hashring_nodes",
				Help: "The number of nodes per hashring.",
			},
			[]string{"name"}),
		hashringTenantsGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "thanos_receive_hashring_tenants",
				Help: "The number of tenants per hashring.",
			},
			[]string{"name"}),
	}

	if r != nil {
		r.MustRegister(
			c.hashGauge,
			c.successGauge,
			c.lastSuccessTimeGauge,
			c.changesCounter,
			c.errorCounter,
			c.refreshCounter,
			c.hashringNodesGauge,
			c.hashringTenantsGauge,
		)
	}

	return c, nil
}

// Run starts the ConfigWatcher until the given context is cancelled.
func (cw *ConfigWatcher) Run(ctx context.Context) {
	defer cw.stop()

	cw.refresh(ctx)

	ticker := time.NewTicker(cw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case event := <-cw.watcher.Events:
			// fsnotify sometimes sends a bunch of events without name or operation.
			// It's unclear what they are and why they are sent - filter them out.
			if len(event.Name) == 0 {
				break
			}
			// Everything but a CHMOD requires rereading.
			// If the file was removed, we can't read it, so skip.
			if event.Op^(fsnotify.Chmod|fsnotify.Remove) == 0 {
				break
			}
			// Changes to a file can spawn various sequences of events with
			// different combinations of operations. For all practical purposes
			// this is inaccurate.
			// The most reliable solution is to reload everything if anything happens.
			cw.refresh(ctx)

		case <-ticker.C:
			// Setting a new watch after an update might fail. Make sure we don't lose
			// those files forever.
			cw.refresh(ctx)

		case err := <-cw.watcher.Errors:
			if err != nil {
				cw.errorCounter.Inc()
				level.Error(cw.logger).Log("msg", "error watching file", "err", err)
			}
		}
	}
}

// C returns a chan that gets hashring configuration updates.
func (cw *ConfigWatcher) C() <-chan []HashringConfig {
	return cw.ch
}

// readFile reads the configured file and returns content of configuration file.
func (cw *ConfigWatcher) readFile() ([]byte, error) {
	fd, err := os.Open(cw.path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := fd.Close(); err != nil {
			level.Error(cw.logger).Log("msg", "failed to close file", "err", err, "path", cw.path)
		}
	}()

	return ioutil.ReadAll(fd)
}

// loadConfig loads raw configuration content and returns a configuration.
func (cw *ConfigWatcher) loadConfig(content []byte) ([]HashringConfig, error) {
	var config []HashringConfig
	err := json.Unmarshal(content, &config)
	return config, err
}

// refresh reads the configured file and sends the hashring configuration on the channel.
func (cw *ConfigWatcher) refresh(ctx context.Context) {
	cw.refreshCounter.Inc()
	cfgContent, err := cw.readFile()
	if err != nil {
		cw.errorCounter.Inc()
		level.Error(cw.logger).Log("msg", "failed to read configuration file", "err", err, "path", cw.path)
		return
	}

	config, err := cw.loadConfig(cfgContent)
	if err != nil {
		cw.errorCounter.Inc()
		level.Error(cw.logger).Log("msg", "failed to load configuration file", "err", err, "path", cw.path)
		return
	}

	// If there was no change to the configuration, return early.
	if reflect.DeepEqual(cw.last, config) {
		return
	}
	cw.changesCounter.Inc()
	// Save the last known configuration.
	cw.last = config
	cw.successGauge.Set(1)
	cw.lastSuccessTimeGauge.SetToCurrentTime()
	cw.hashGauge.Set(hashAsMetricValue(cfgContent))

	for _, c := range config {
		cw.hashringNodesGauge.WithLabelValues(c.Hashring).Set(float64(len(c.Endpoints)))
		cw.hashringTenantsGauge.WithLabelValues(c.Hashring).Set(float64(len(c.Tenants)))
	}

	level.Debug(cw.logger).Log("msg", "refreshed hashring config")
	select {
	case <-ctx.Done():
		return
	case cw.ch <- config:
		return
	}
}

// stop shuts down the config watcher.
func (cw *ConfigWatcher) stop() {
	level.Debug(cw.logger).Log("msg", "stopping hashring configuration watcher...", "path", cw.path)

	done := make(chan struct{})
	defer close(done)

	// Closing the watcher will deadlock unless all events and errors are drained.
	go func() {
		for {
			select {
			case <-cw.watcher.Errors:
			case <-cw.watcher.Events:
			// Drain all events and errors.
			case <-done:
				return
			}
		}
	}()
	if err := cw.watcher.Close(); err != nil {
		level.Error(cw.logger).Log("msg", "error closing file watcher", "path", cw.path, "err", err)
	}

	close(cw.ch)
	level.Debug(cw.logger).Log("msg", "hashring configuration watcher stopped")
}

// hashAsMetricValue generates metric value from hash of data.
func hashAsMetricValue(data []byte) float64 {
	sum := md5.Sum(data)
	// We only want 48 bits as a float64 only has a 53 bit mantissa.
	smallSum := sum[0:6]
	var bytes = make([]byte, 8)
	copy(bytes, smallSum)
	return float64(binary.LittleEndian.Uint64(bytes))
}
