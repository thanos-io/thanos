// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"strings"
	"time"

	cortexcache "github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/pkg/errors"
)

type ResponseCacheProvider string

const (
	INMEMORY  ResponseCacheProvider = "IN-MEMORY"
	MEMCACHED ResponseCacheProvider = "MEMCACHED"
)

// InMemoryResponseCacheConfig holds the configs for the in-memory cache provider.
type InMemoryResponseCacheConfig struct {
	// MaxSize represents overall maximum number of bytes cache can contain.
	MaxSize string `yaml:"max_size"`
	// MaxSizeItems represents the maximum number of entries in the cache.
	MaxSizeItems int `yaml:"max_size_items"`
	// Validity represents the expiry duration for the cache.
	Validity time.Duration `yaml:"validity"`
}

// MemcachedResponseCacheConfig holds the configs for the memcache cache provider.
type MemcachedResponseCacheConfig struct {
	Memcached cacheutil.MemcachedClientConfig `yaml:",inline"`
	Validity  time.Duration                   `yaml:"validity"`
}

// CacheProviderConfig is the initial CacheProviderConfig struct holder before parsing it into a specific cache provider.
// Based on the config type the config is then parsed into a specific cache provider.
type CacheProviderConfig struct {
	Type   ResponseCacheProvider `yaml:"type"`
	Config interface{}           `yaml:"config"`
}

// NewCacheConfig is a parser that converts a Thanos cache config yaml into a cortex cache config struct.
func NewCacheConfig(confContentYaml []byte) (*queryrange.ResultsCacheConfig, error) {
	cacheConfig := &CacheProviderConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, cacheConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(cacheConfig.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of cache backend configuration")
	}

	var cache cortexcache.Config

	switch strings.ToUpper(string(cacheConfig.Type)) {
	case string(INMEMORY):

		var config InMemoryResponseCacheConfig
		if err := yaml.Unmarshal(backendConfig, &config); err != nil {
			return nil, err
		}

		cache = cortexcache.Config{
			EnableFifoCache: true,
			Fifocache: cortexcache.FifoCacheConfig{
				MaxSizeBytes: config.MaxSize,
				MaxSizeItems: config.MaxSizeItems,
				Validity:     config.Validity,
			},
		}
	case string(MEMCACHED):
		var config MemcachedResponseCacheConfig
		if err := yaml.UnmarshalStrict(backendConfig, &config); err != nil {
			return nil, err
		}

		cache = cortexcache.Config{
			Memcache: cortexcache.MemcachedConfig{
				Expiration:  config.Validity,
				Parallelism: config.Memcached.MaxAsyncConcurrency,
				BatchSize:   config.Memcached.MaxGetMultiBatchSize,
			},
			MemcacheClient: cortexcache.MemcachedClientConfig{
				Timeout:        config.Memcached.Timeout,
				MaxIdleConns:   config.Memcached.MaxIdleConnections,
				Addresses:      strings.Join(config.Memcached.Addresses, ","),
				UpdateInterval: config.Memcached.DNSProviderUpdateInterval,
			},
			Background: cortexcache.BackgroundConfig{
				WriteBackBuffer:     10000,
				WriteBackGoroutines: 10,
			},

			// ???????????????
		}
	default:
		return nil, errors.Errorf("index cache with type %s is not supported", cacheConfig.Type)
	}

	return &queryrange.ResultsCacheConfig{
		CacheConfig: cache,
	}, nil
}

// Config holds the query frontend configs.
type Config struct {
	// PartialResponseStrategy is the default strategy used
	// when parsing thanos query request.
	PartialResponseStrategy bool

	CachePathOrContent     extflag.PathOrContent
	RequestLoggingDecision string

	MaxQueryLength      time.Duration
	MaxQueryParallelism int
	MaxCacheFreshness   time.Duration

	SplitQueriesByInterval time.Duration
	MaxRetries             int

	CompressResponses    bool
	DownstreamURL        string
	LogQueriesLongerThan time.Duration

	ResultsCacheConfig queryrange.ResultsCacheConfig
}

// Validate a fully initialized config.
func (cfg *Config) Validate() error {
	if cfg.ResultsCacheConfig != (queryrange.ResultsCacheConfig{}) {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("split queries interval should be greater then 0")
		}
		if err := cfg.ResultsCacheConfig.CacheConfig.Validate(); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config")
		}
	}
	if len(cfg.DownstreamURL) == 0 {
		return errors.New("downstream URL should be configured")
	}
	if cfg.ResultsCacheConfig.CacheConfig.DefaultValidity == 0 {
		level.Warn(logger).Log("msg", "memcached cache valid time set to 0, use 24 hours instead")
		c.validity = memcachedDefaultTTL
	}
	return nil
}
