// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// An errParseConfigurationFile is returned by the HashringConfigLoader when parsing failed.
	errParseConfigurationFile = errors.New("configuration file is not parsable")
	// An errEmptyConfigurationFile is returned by the HashringConfigLoader when attempting to load an empty configuration file.
	errEmptyConfigurationFile = errors.New("configuration file is empty")
)

type ReceiverMode string

const (
	RouterOnly     ReceiverMode = "RouterOnly"
	IngestorOnly   ReceiverMode = "IngestorOnly"
	RouterIngestor ReceiverMode = "RouterIngestor"
)

type Endpoint struct {
	Address string `json:"address"`
	AZ      string `json:"az"`
}

func (e *Endpoint) UnmarshalJSON(data []byte) error {
	// First try to unmarshal as a string.
	err := json.Unmarshal(data, &e.Address)
	if err == nil {
		return nil
	}

	// If that fails, try to unmarshal as an endpoint object.
	type endpointAlias Endpoint
	var configEndpoint endpointAlias
	err = json.Unmarshal(data, &configEndpoint)
	if err == nil {
		e.Address = configEndpoint.Address
		e.AZ = configEndpoint.AZ
	}
	return err
}

// HashringConfig represents the configuration for a hashring
// a Receive node knows about.
type HashringConfig struct {
	Hashring       string            `json:"hashring,omitempty"`
	Tenants        []string          `json:"tenants,omitempty"`
	Endpoints      []Endpoint        `json:"endpoints"`
	Algorithm      HashringAlgorithm `json:"algorithm,omitempty"`
	ExternalLabels map[string]string `json:"external_labels,omitempty"`
}

// HashringConfigLoader is responsible for loading the hashring configuration. It also does runs validations on the
// configuration and exports metrics about it.
type HashringConfigLoader struct {
	path   string
	logger log.Logger

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

// NewHashringConfigLoader creates a new HashringConfigLoader.
func NewHashringConfigLoader(logger log.Logger, reg prometheus.Registerer, path string) *HashringConfigLoader {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	c := &HashringConfigLoader{
		path:   path,
		logger: logger,
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
	return c
}

// ParseConfig reads the configured file and return the hashring configuration.
func (hr *HashringConfigLoader) ParseConfig() ([]HashringConfig, error) {
	hr.refreshCounter.Inc()

	hashrings, cfgHash, err := loadConfig(hr.logger, hr.path)
	if err != nil {
		hr.errorCounter.Inc()
		level.Error(hr.logger).Log("msg", "failed to load configuration file", "err", err, "path", hr.path)
		return nil, err
	}

	// If there was no change to the configuration, return early.
	if hr.lastLoadedConfigHash == cfgHash {
		return nil, errors.New("configuration not changed")
	}

	hr.changesCounter.Inc()

	// Save the last known configuration.
	hr.lastLoadedConfigHash = cfgHash
	hr.hashGauge.Set(cfgHash)
	hr.successGauge.Set(1)
	hr.lastSuccessTimeGauge.SetToCurrentTime()

	for _, c := range hashrings {
		hr.hashringNodesGauge.WithLabelValues(c.Hashring).Set(float64(len(c.Endpoints)))
		hr.hashringTenantsGauge.WithLabelValues(c.Hashring).Set(float64(len(c.Tenants)))
	}

	level.Debug(hr.logger).Log("msg", "refreshed hashring config")
	return hashrings, nil
}

// loadConfig loads raw configuration content and returns a configuration.
func loadConfig(logger log.Logger, path string) ([]HashringConfig, float64, error) {
	cfgContent, err := readFile(logger, path)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to read configuration file")
	}

	var config []HashringConfig
	err = json.Unmarshal(cfgContent, &config)
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
