// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"strings"
	"time"

	cortexcache "github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/extflag"
	"gopkg.in/yaml.v2"

	cortexfrontend "github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	cortexvalidation "github.com/cortexproject/cortex/pkg/util/validation"
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
	// Expiration sets a global expiration limit for all cached items.
	Expiration time.Duration `yaml:"expiration"`
}

// CacheProviderConfig is the initial CacheProviderConfig struct holder before parsing it into a specific cache provider.
// Based on the config type the config is then parsed into a specific cache provider.
type CacheProviderConfig struct {
	Type   ResponseCacheProvider `yaml:"type"`
	Config interface{}           `yaml:"config"`
}

// NewCacheConfig is a parser that converts a Thanos cache config yaml into a cortex cache config struct.
func NewCacheConfig(logger log.Logger, confContentYaml []byte) (*cortexcache.Config, error) {
	cacheConfig := &CacheProviderConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, cacheConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(cacheConfig.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of cache backend configuration")
	}

	switch strings.ToUpper(string(cacheConfig.Type)) {
	case string(INMEMORY):
		var config InMemoryResponseCacheConfig
		if err := yaml.Unmarshal(backendConfig, &config); err != nil {
			return nil, err
		}

		return &cortexcache.Config{
			EnableFifoCache: true,
			Fifocache: cortexcache.FifoCacheConfig{
				MaxSizeBytes: config.MaxSize,
				MaxSizeItems: config.MaxSizeItems,
				Validity:     config.Validity,
			},
		}, nil
	case string(MEMCACHED):
		var config MemcachedResponseCacheConfig
		if err := yaml.UnmarshalStrict(backendConfig, &config); err != nil {
			return nil, err
		}
		// TODO(krasi) Add support for it in the cortex module.
		if config.Memcached.MaxItemSize > 0 {
			level.Warn(logger).Log("message", "MaxItemSize is not yet supported by the memcached client")
		}

		return &cortexcache.Config{
			Memcache: cortexcache.MemcachedConfig{
				Expiration:  config.Expiration,
				Parallelism: config.Memcached.MaxGetMultiConcurrency,
				BatchSize:   config.Memcached.MaxGetMultiBatchSize,
			},
			MemcacheClient: cortexcache.MemcachedClientConfig{
				Timeout:        config.Memcached.Timeout,
				MaxIdleConns:   config.Memcached.MaxIdleConnections,
				Addresses:      strings.Join(config.Memcached.Addresses, ","),
				UpdateInterval: config.Memcached.DNSProviderUpdateInterval,
			},
			Background: cortexcache.BackgroundConfig{
				WriteBackBuffer:     config.Memcached.MaxAsyncBufferSize,
				WriteBackGoroutines: config.Memcached.MaxAsyncConcurrency,
			},
		}, nil
	default:
		return nil, errors.Errorf("response cache with type %s is not supported", cacheConfig.Type)
	}
}

// Config holds the query frontend configs.
type Config struct {
	// PartialResponseStrategy is the default strategy used
	// when parsing thanos query request.
	PartialResponseStrategy bool

	CortexFrontendConfig     *cortexfrontend.Config
	CortexLimits             *cortexvalidation.Limits
	CortexResultsCacheConfig *queryrange.ResultsCacheConfig

	CachePathOrContent     extflag.PathOrContent
	CacheCompression       string
	RequestLoggingDecision string

	SplitQueriesByInterval time.Duration
	MaxRetries             int
}

// Validate a fully initialized config.
func (cfg *Config) Validate() error {
	if cfg.CortexResultsCacheConfig != nil {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("split queries interval should be greater then 0")
		}
		if err := cfg.CortexResultsCacheConfig.Validate(); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config")
		}
	}
	if len(cfg.CortexFrontendConfig.DownstreamURL) == 0 {
		return errors.New("downstream URL should be configured")
	}

	return nil
}
