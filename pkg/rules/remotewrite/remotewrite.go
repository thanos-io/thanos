// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package remotewrite

import (
	"time"

	"github.com/pkg/errors"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"gopkg.in/yaml.v2"
)

// LoadRemoteWriteConfig prepares a RemoteWriteConfig instance from a given YAML config.
func LoadRemoteWriteConfig(configYAML []byte) (config.RemoteWriteConfig, error) {
	var cfg config.RemoteWriteConfig
	if err := yaml.Unmarshal(configYAML, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// NewFanoutStorage creates a storage that fans-out to both the WAL and a configured remote storage.
// The remote storage tails the WAL and sends the metrics it reads using Prometheus' remote_write.
func NewFanoutStorage(logger log.Logger, reg prometheus.Registerer, walDir string, rwConfig *config.RemoteWriteConfig) (storage.Storage, error) {
	walStore, err := NewStorage(logger, reg, walDir)
	if err != nil {
		return nil, err
	}
	remoteStore := remote.NewStorage(logger, reg, walStore.StartTime, walStore.Directory(), 1*time.Minute, nil)
	if err := remoteStore.ApplyConfig(&config.Config{
		GlobalConfig:       config.DefaultGlobalConfig,
		RemoteWriteConfigs: []*config.RemoteWriteConfig{rwConfig},
	}); err != nil {
		return nil, errors.Wrap(err, "applying config to remote storage")
	}
	return storage.NewFanout(logger, walStore, remoteStore), nil
}
