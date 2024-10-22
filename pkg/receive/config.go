// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

var (
	// An errParseConfigurationFile is returned by the ConfigWatcher when parsing failed.
	errParseConfigurationFile = errors.New("configuration file is not parsable")
	// An errEmptyConfigurationFile is returned by the ConfigWatcher when attempting to load an empty configuration file.
	errEmptyConfigurationFile = errors.New("configuration file is empty")
)

type ReceiverMode string

const (
	RouterOnly     ReceiverMode = "RouterOnly"
	IngestorOnly   ReceiverMode = "IngestorOnly"
	RouterIngestor ReceiverMode = "RouterIngestor"

	DefaultCapNProtoPort string = "19391"
)

type Endpoint struct {
	Address          string `json:"address"`
	CapNProtoAddress string `json:"capnproto_address"`
	AZ               string `json:"az"`
}

func (e *Endpoint) String() string {
	return fmt.Sprintf("addr: %s, capnp_addr: %s, az: %s", e.Address, e.CapNProtoAddress, e.AZ)
}

func (e *Endpoint) HasAddress(addr string) bool {
	return e.Address == addr || e.CapNProtoAddress == addr
}

func (e *Endpoint) UnmarshalJSON(data []byte) error {
	if err := e.unmarshal(data); err != nil {
		return err
	}
	if e.Address == "" {
		return errors.New("endpoint address must be set")
	}

	// If the Cap'n proto address is not set, initialize it
	// to the existing address using the default cap'n proto server port.
	if e.CapNProtoAddress != "" {
		return nil
	}
	if parts := strings.SplitN(e.Address, ":", 2); len(parts) <= 2 {
		e.CapNProtoAddress = parts[0] + ":" + DefaultCapNProtoPort
	}
	return nil
}

func (e *Endpoint) unmarshal(data []byte) error {
	// First try to unmarshal as a string.
	err := json.Unmarshal(data, &e.Address)
	if err == nil {
		return nil
	}

	// If that fails, try to unmarshal as an endpoint object.
	type endpointAlias Endpoint
	var configEndpoint endpointAlias
	if err := json.Unmarshal(data, &configEndpoint); err != nil {
		return err
	}

	e.Address = configEndpoint.Address
	e.AZ = configEndpoint.AZ
	e.CapNProtoAddress = configEndpoint.CapNProtoAddress
	return nil
}

// HashringConfig represents the configuration for a hashring
// a receive node knows about.
type HashringConfig struct {
	Hashring          string            `json:"hashring,omitempty"`
	Tenants           []string          `json:"tenants,omitempty"`
	TenantMatcherType tenantMatcher     `json:"tenant_matcher_type,omitempty"`
	Endpoints         []Endpoint        `json:"endpoints"`
	Algorithm         HashringAlgorithm `json:"algorithm,omitempty"`
	ExternalLabels    labels.Labels     `json:"external_labels,omitempty"`
}

type tenantMatcher string

const (
	// TenantMatcherTypeExact matches tenants exactly. This is also the default one.
	TenantMatcherTypeExact tenantMatcher = "exact"
	// TenantMatcherGlob matches tenants using glob patterns.
	TenantMatcherGlob tenantMatcher = "glob"
)

func isExactMatcher(m tenantMatcher) bool {
	return m == TenantMatcherTypeExact || m == ""
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

	// lastLoadedConfigHash is the hash of the last successfully loaded configuration.
	lastLoadedConfigHash float64
}

// NewConfigWatcher creates a new ConfigWatcher.
func NewConfigWatcher(logger log.Logger, reg prometheus.Registerer, path string, interval model.Duration) (*ConfigWatcher, error) {
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
		hashGauge: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_config_hash",
				Help: "Hash of the currently loaded hashring configuration file.",
			}),
		successGauge: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_config_last_reload_successful",
				Help: "Whether the last hashring configuration file reload attempt was successful.",
			}),
		lastSuccessTimeGauge: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "thanos_receive_config_last_reload_success_timestamp_seconds",
				Help: "Timestamp of the last successful hashring configuration file reload.",
			}),
		changesCounter: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_changes_total",
				Help: "The number of times the hashrings configuration file has changed.",
			}),
		errorCounter: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_errors_total",
				Help: "The number of errors watching the hashrings configuration file.",
			}),
		refreshCounter: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_refreshes_total",
				Help: "The number of refreshes of the hashrings configuration file.",
			}),
		hashringNodesGauge: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "thanos_receive_hashring_nodes",
				Help: "The number of nodes per hashring.",
			},
			[]string{"name"}),
		hashringTenantsGauge: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "thanos_receive_hashring_tenants",
				Help: "The number of tenants per hashring.",
			},
			[]string{"name"}),
	}
	return c, nil
}

// Run starts the ConfigWatcher until the given context is canceled.
func (cw *ConfigWatcher) Run(ctx context.Context) {
	defer cw.Stop()

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
			if event.Name == "" {
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

// ValidateConfig returns an error if the configuration that's being watched is not valid.
func (cw *ConfigWatcher) ValidateConfig() error {
	_, _, err := loadConfig(cw.logger, cw.path)
	return err
}

// Stop shuts down the config watcher.
func (cw *ConfigWatcher) Stop() {
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

// refresh reads the configured file and sends the hashring configuration on the channel.
func (cw *ConfigWatcher) refresh(ctx context.Context) {
	cw.refreshCounter.Inc()

	config, cfgHash, err := loadConfig(cw.logger, cw.path)
	if err != nil {
		cw.errorCounter.Inc()
		level.Error(cw.logger).Log("msg", "failed to load configuration file", "err", err, "path", cw.path)
		return
	}

	// If there was no change to the configuration, return early.
	if cw.lastLoadedConfigHash == cfgHash {
		return
	}

	cw.changesCounter.Inc()

	// Save the last known configuration.
	cw.lastLoadedConfigHash = cfgHash
	cw.hashGauge.Set(cfgHash)
	cw.successGauge.Set(1)
	cw.lastSuccessTimeGauge.SetToCurrentTime()

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

func ConfigFromWatcher(ctx context.Context, updates chan<- []HashringConfig, cw *ConfigWatcher) error {
	defer close(updates)
	go cw.Run(ctx)

	for {
		select {
		case cfg, ok := <-cw.C():
			if !ok {
				return errors.New("hashring config watcher stopped unexpectedly")
			}
			updates <- cfg
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ParseConfig parses the raw configuration content and returns a HashringConfig.
func ParseConfig(content []byte) ([]HashringConfig, error) {
	var config []HashringConfig
	err := json.Unmarshal(content, &config)
	return config, err
}

// loadConfig loads raw configuration content and returns a configuration.
func loadConfig(logger log.Logger, path string) ([]HashringConfig, float64, error) {
	cfgContent, err := readFile(logger, path)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to read configuration file")
	}

	config, err := ParseConfig(cfgContent)
	if err != nil {
		return nil, 0, errors.Wrapf(errParseConfigurationFile, "failed to parse configuration file: %v", err)
	}

	// If hashring is empty, return an error.
	if len(config) == 0 {
		return nil, 0, errors.Wrapf(errEmptyConfigurationFile, "failed to load configuration file, path: %s", path)
	}

	return config, hashAsMetricValue(cfgContent), nil
}

// readFile reads the configuration file and returns content of configuration file.
func readFile(logger log.Logger, path string) ([]byte, error) {
	fd, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := fd.Close(); err != nil {
			level.Error(logger).Log("msg", "failed to close file", "err", err, "path", path)
		}
	}()

	return io.ReadAll(fd)
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
