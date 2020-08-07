// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"time"

	cortexcache "github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"gopkg.in/yaml.v2"
)

type InMemoryResponseCacheConfig struct {
	// MaxSize represents overall maximum number of bytes cache can contain.
	MaxSize string `yaml:"max_size"`
	// MaxSizeItems represents the maximum number of entries in the cache.
	MaxSizeItems int `yaml:"max_size_items"`
	// Validity represents the expiry duration for the cache.
	Validity time.Duration `yaml:"validity"`
}

// newInMemoryResponseCacheConfig is an adapter function
// to creates Cortex fifo results cache config.
func newInMemoryResponseCacheConfig(conf []byte) (*queryrange.ResultsCacheConfig, error) {
	var config InMemoryResponseCacheConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, err
	}
	return &queryrange.ResultsCacheConfig{
		CacheConfig: cortexcache.Config{
			EnableFifoCache: true,
			Fifocache: cortexcache.FifoCacheConfig{
				MaxSizeBytes: config.MaxSize,
				MaxSizeItems: config.MaxSizeItems,
				Validity:     config.Validity,
			},
		},
	}, nil
}
