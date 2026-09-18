// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/cacheutil"
)

type IndexCacheProvider string

const (
	INMEMORY  IndexCacheProvider = "IN-MEMORY"
	MEMCACHED IndexCacheProvider = "MEMCACHED"
	REDIS     IndexCacheProvider = "REDIS"
)

// IndexCacheConfig specifies the index cache config.
type IndexCacheConfig struct {
	Type   IndexCacheProvider `yaml:"type"`
	Config any                `yaml:"config"`

	// Available item types are Postings, Series and ExpandedPostings.
	EnabledItems []string `yaml:"enabled_items"`
	// TTL for storing items in remote cache. Not supported for inmemory cache.
	// Default value is 24h.
	TTL time.Duration `yaml:"ttl"`
}

// NewIndexCache initializes and returns new index cache.
func NewIndexCache(logger log.Logger, confContentYaml []byte, reg prometheus.Registerer) (IndexCache, error) {
	level.Info(logger).Log("msg", "loading index cache configuration")
	cacheConfig := &IndexCacheConfig{}
	cacheMetrics := NewCommonMetrics(reg)
	if err := yaml.UnmarshalStrict(confContentYaml, cacheConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(cacheConfig.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of cache backend configuration")
	}

	if cacheConfig.TTL == 0 {
		cacheConfig.TTL = memcachedDefaultTTL
	}

	var cache IndexCache
	switch strings.ToUpper(string(cacheConfig.Type)) {
	case string(INMEMORY):
		cache, err = NewInMemoryIndexCache(logger, cacheMetrics, reg, backendConfig)
	case string(MEMCACHED):
		var memcached cacheutil.ReadThroughRemoteCache
		memcached, err = cacheutil.NewMemcachedClient(logger, "index-cache", backendConfig, reg)
		if err == nil {
			cache, err = NewRemoteIndexCache(logger, memcached, cacheMetrics, reg, cacheConfig.TTL)
		}
	case string(REDIS):
		var redisCache cacheutil.ReadThroughRemoteCache
		redisCache, err = cacheutil.NewRedisClient(logger, "index-cache", backendConfig, reg)
		if err == nil {
			cache, err = NewRemoteIndexCache(logger, redisCache, cacheMetrics, reg, cacheConfig.TTL)
		}
	default:
		return nil, errors.Errorf("index cache with type %s is not supported", cacheConfig.Type)
	}
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s index cache", cacheConfig.Type))
	}

	cache = NewTracingIndexCache(string(cacheConfig.Type), cache)
	if len(cacheConfig.EnabledItems) > 0 {
		if err = ValidateEnabledItems(cacheConfig.EnabledItems); err != nil {
			return nil, err
		}
		cache = NewFilteredIndexCache(cache, cacheConfig.EnabledItems)
	}

	return cache, nil
}
