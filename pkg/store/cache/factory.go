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
	INMEMORY   IndexCacheProvider = "IN-MEMORY"
	MEMCACHED  IndexCacheProvider = "MEMCACHED"
	REDIS      IndexCacheProvider = "REDIS"
	MULTILEVEL IndexCacheProvider = "MULTILEVEL"
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
	if err := yaml.UnmarshalStrict(confContentYaml, cacheConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	factory := &indexCacheFactory{logger: logger}
	cache, err := factory.newCache(*cacheConfig, NewCommonMetrics(reg), reg)
	if err != nil {
		// A later level may fail after earlier levels have started remote clients.
		for _, client := range factory.clients {
			client.Stop()
		}
	}
	return cache, err
}

type indexCacheFactory struct {
	logger  log.Logger
	clients []cacheutil.RemoteCacheClient
}

func (f *indexCacheFactory) newCache(cacheConfig IndexCacheConfig, cacheMetrics *CommonMetrics, reg prometheus.Registerer) (IndexCache, error) {
	if err := ValidateEnabledItems(cacheConfig.EnabledItems); err != nil {
		return nil, err
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
		cache, err = NewInMemoryIndexCache(f.logger, cacheMetrics, reg, backendConfig)
	case string(MEMCACHED):
		var memcached cacheutil.RemoteCacheClient
		memcached, err = cacheutil.NewMemcachedClient(f.logger, "index-cache", backendConfig, reg)
		if err == nil {
			f.clients = append(f.clients, memcached)
			cache, err = NewRemoteIndexCache(f.logger, memcached, cacheMetrics, reg, cacheConfig.TTL)
		}
	case string(REDIS):
		var redisCache cacheutil.RemoteCacheClient
		redisCache, err = cacheutil.NewRedisClient(f.logger, "index-cache", backendConfig, reg)
		if err == nil {
			f.clients = append(f.clients, redisCache)
			cache, err = NewRemoteIndexCache(f.logger, redisCache, cacheMetrics, reg, cacheConfig.TTL)
		}
	case string(MULTILEVEL):
		config := MultiLevelIndexCacheConfig{}
		if err := yaml.UnmarshalStrict(backendConfig, &config); err != nil {
			return nil, errors.Wrap(err, "parsing multi-level index cache config")
		}
		if len(config.Caches) < 2 {
			return nil, errors.New("multi-level index cache requires at least two caches")
		}
		for _, level := range config.Caches {
			if strings.EqualFold(string(level.Type), string(MULTILEVEL)) {
				return nil, errors.New("nested multi-level index caches are not supported")
			}
		}
		caches := make([]IndexCache, 0, len(config.Caches))
		for i, level := range config.Caches {
			if level.Config == nil {
				// Preserve backend defaults when a level omits its optional config.
				level.Config = map[string]any{}
			}
			if level.TTL == 0 {
				level.TTL = cacheConfig.TTL
			}
			levelReg := reg
			if reg != nil {
				levelReg = prometheus.WrapRegistererWith(prometheus.Labels{"level": fmt.Sprintf("L%d", i+1)}, reg)
			}
			levelCache, err := f.newCache(level, newCommonMetrics(levelReg, "thanos_store_index_cache_level_"), levelReg)
			if err != nil {
				return nil, errors.Wrapf(err, "create index cache level L%d", i+1)
			}
			caches = append(caches, levelCache)
		}
		cache = newMultiLevelIndexCache(caches, cacheMetrics)
	default:
		return nil, errors.Errorf("index cache with type %s is not supported", cacheConfig.Type)
	}
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s index cache", cacheConfig.Type))
	}

	cache = NewTracingIndexCache(string(cacheConfig.Type), cache)
	if len(cacheConfig.EnabledItems) > 0 {
		cache = NewFilteredIndexCache(cache, cacheConfig.EnabledItems)
	}

	return cache, nil
}
