// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/thanos/pkg/errors"
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
	Config interface{}        `yaml:"config"`
}

// NewIndexCache initializes and returns new index cache.
func NewIndexCache(logger log.Logger, confContentYaml []byte, reg prometheus.Registerer) (IndexCache, error) {
	level.Info(logger).Log("msg", "loading index cache configuration")
	cacheConfig := &IndexCacheConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, cacheConfig); err != nil {
		return nil, errors.Wrapf(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(cacheConfig.Config)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal content of cache backend configuration")
	}

	var cache IndexCache
	switch strings.ToUpper(string(cacheConfig.Type)) {
	case string(INMEMORY):
		cache, err = NewInMemoryIndexCache(logger, reg, backendConfig)
	case string(MEMCACHED):
		var memcached cacheutil.RemoteCacheClient
		memcached, err = cacheutil.NewMemcachedClient(logger, "index-cache", backendConfig, reg)
		if err == nil {
			cache, err = NewRemoteIndexCache(logger, memcached, reg)
		}
	case string(REDIS):
		var redisCache cacheutil.RemoteCacheClient
		redisCache, err = cacheutil.NewRedisClient(logger, "index-cache", backendConfig, reg)
		if err == nil {
			cache, err = NewRemoteIndexCache(logger, redisCache, reg)
		}
	default:
		return nil, errors.Newf("index cache with type %s is not supported", cacheConfig.Type)
	}
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("create %s index cache", cacheConfig.Type))
	}
	return cache, nil
}
