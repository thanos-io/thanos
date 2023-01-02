// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"strings"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	prommodel "github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"

	cortexcache "github.com/thanos-io/thanos/internal/cortex/chunk/cache"
	"github.com/thanos-io/thanos/internal/cortex/frontend/transport"
	"github.com/thanos-io/thanos/internal/cortex/querier"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
	cortexvalidation "github.com/thanos-io/thanos/internal/cortex/util/validation"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/model"
)

type ResponseCacheProvider string

const (
	INMEMORY  ResponseCacheProvider = "IN-MEMORY"
	MEMCACHED ResponseCacheProvider = "MEMCACHED"
	REDIS     ResponseCacheProvider = "REDIS"
)

var (
	defaultMemcachedConfig = MemcachedResponseCacheConfig{
		Memcached: cacheutil.MemcachedClientConfig{
			Timeout:                   500 * time.Millisecond,
			MaxIdleConnections:        100,
			MaxAsyncConcurrency:       10,
			MaxAsyncBufferSize:        10000,
			MaxGetMultiConcurrency:    100,
			MaxGetMultiBatchSize:      0,
			MaxItemSize:               model.Bytes(1024 * 1024),
			DNSProviderUpdateInterval: 10 * time.Second,
		},
		Expiration: 24 * time.Hour,
	}
	// DefaultRedisConfig is default redis config for queryfrontend.
	DefaultRedisConfig = RedisResponseCacheConfig{
		Redis:      cacheutil.DefaultRedisClientConfig,
		Expiration: 24 * time.Hour,
	}
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

// RedisResponseCacheConfig holds the configs for the redis cache provider.
type RedisResponseCacheConfig struct {
	Redis cacheutil.RedisClientConfig `yaml:",inline"`
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
		config := defaultMemcachedConfig
		if err := yaml.UnmarshalStrict(backendConfig, &config); err != nil {
			return nil, err
		}
		if config.Expiration == 0 {
			level.Warn(logger).Log("msg", "memcached cache valid time set to 0, so using a default of 24 hours expiration time")
			config.Expiration = 24 * time.Hour
		}

		if config.Memcached.DNSProviderUpdateInterval <= 0 {
			level.Warn(logger).Log("msg", "memcached dns provider update interval time set to invalid value, defaulting to 10s")
			config.Memcached.DNSProviderUpdateInterval = 10 * time.Second
		}

		if config.Memcached.MaxAsyncConcurrency <= 0 {
			level.Warn(logger).Log("msg", "memcached max async concurrency must be positive, defaulting to 10")
			config.Memcached.MaxAsyncConcurrency = 10
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
				MaxItemSize:    int(config.Memcached.MaxItemSize),
			},
			Background: cortexcache.BackgroundConfig{
				WriteBackBuffer:     config.Memcached.MaxAsyncBufferSize,
				WriteBackGoroutines: config.Memcached.MaxAsyncConcurrency,
			},
		}, nil
	case string(REDIS):
		config := DefaultRedisConfig
		if err := yaml.UnmarshalStrict(backendConfig, &config); err != nil {
			return nil, err
		}
		if config.Expiration <= 0 {
			level.Warn(logger).Log("msg", "redis cache valid time set to 0, so using a default of 24 hours expiration time")
			config.Expiration = 24 * time.Hour
		}
		return &cortexcache.Config{
			Redis: cortexcache.RedisConfig{
				Endpoint:    config.Redis.Addr,
				Timeout:     config.Redis.ReadTimeout,
				MasterName:  config.Redis.MasterName,
				Expiration:  config.Expiration,
				DB:          config.Redis.DB,
				PoolSize:    config.Redis.PoolSize,
				Password:    flagext.Secret{Value: config.Redis.Password},
				IdleTimeout: config.Redis.IdleTimeout,
				MaxConnAge:  config.Redis.MaxConnAge,
			},
			Background: cortexcache.BackgroundConfig{
				WriteBackBuffer:     config.Redis.MaxSetMultiConcurrency * config.Redis.SetMultiBatchSize,
				WriteBackGoroutines: config.Redis.MaxSetMultiConcurrency,
			},
		}, nil
	default:
		return nil, errors.Errorf("response cache with type %s is not supported", cacheConfig.Type)
	}
}

// DownstreamTripperConfig stores the http.Transport configuration for query-frontend's HTTP downstream tripper.
type DownstreamTripperConfig struct {
	IdleConnTimeout       prommodel.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout prommodel.Duration `yaml:"response_header_timeout"`
	TLSHandshakeTimeout   prommodel.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout prommodel.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          *int               `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   *int               `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       *int               `yaml:"max_conns_per_host"`

	CachePathOrContent extflag.PathOrContent
}

// Config holds the query frontend configs.
type Config struct {
	QueryRangeConfig
	LabelsConfig
	DownstreamTripperConfig

	CortexHandlerConfig    *transport.HandlerConfig
	CompressResponses      bool
	CacheCompression       string
	RequestLoggingDecision string
	DownstreamURL          string
	ForwardHeaders         []string
	NumShards              int
}

// QueryRangeConfig holds the config for query range tripperware.
type QueryRangeConfig struct {
	// PartialResponseStrategy is the default strategy used
	// when parsing thanos query request.
	PartialResponseStrategy bool

	ResultsCacheConfig *queryrange.ResultsCacheConfig
	CachePathOrContent extflag.PathOrContent

	AlignRangeWithStep     bool
	RequestDownsampled     bool
	SplitQueriesByInterval time.Duration
	MinQuerySplitInterval  time.Duration
	MaxQuerySplitInterval  time.Duration
	HorizontalShards       int64
	MaxRetries             int
	Limits                 *cortexvalidation.Limits
}

// LabelsConfig holds the config for labels tripperware.
type LabelsConfig struct {
	// PartialResponseStrategy is the default strategy used
	// when parsing thanos query request.
	PartialResponseStrategy bool
	DefaultTimeRange        time.Duration

	ResultsCacheConfig *queryrange.ResultsCacheConfig
	CachePathOrContent extflag.PathOrContent

	SplitQueriesByInterval time.Duration
	MaxRetries             int

	Limits *cortexvalidation.Limits
}

// Validate a fully initialized config.
func (cfg *Config) Validate() error {
	if cfg.QueryRangeConfig.ResultsCacheConfig != nil {
		if cfg.QueryRangeConfig.SplitQueriesByInterval <= 0 && !cfg.isDynamicSplitSet() {
			return errors.New("split queries or split threshold interval should be greater than 0 when caching is enabled")
		}
		if err := cfg.QueryRangeConfig.ResultsCacheConfig.Validate(querier.Config{}); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config for query_range tripperware")
		}
	}

	if cfg.isDynamicSplitSet() && cfg.isStaticSplitSet() {
		return errors.New("split queries interval and dynamic query split interval cannot be set at the same time")
	}

	if cfg.isDynamicSplitSet() {

		if err := cfg.validateDynamicSplitParams(); err != nil {
			return err
		}
	}

	if cfg.LabelsConfig.ResultsCacheConfig != nil {
		if cfg.LabelsConfig.SplitQueriesByInterval <= 0 {
			return errors.New("split queries interval should be greater than 0  when caching is enabled")
		}
		if err := cfg.LabelsConfig.ResultsCacheConfig.Validate(querier.Config{}); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config for labels tripperware")
		}
	}

	if cfg.LabelsConfig.DefaultTimeRange == 0 {
		return errors.New("labels.default-time-range cannot be set to 0")
	}

	if cfg.DownstreamURL == "" {
		return errors.New("downstream URL should be configured")
	}

	return nil
}

func (cfg *Config) validateDynamicSplitParams() error {
	if cfg.QueryRangeConfig.HorizontalShards <= 0 {
		return errors.New("min horizontal shards should be greater than 0 when query split threshold is enabled")
	}

	if cfg.QueryRangeConfig.MaxQuerySplitInterval <= 0 {
		return errors.New("max query split interval should be greater than 0 when query split threshold is enabled")
	}

	if cfg.QueryRangeConfig.MinQuerySplitInterval <= 0 {
		return errors.New("min query split interval should be greater than 0 when query split threshold is enabled")
	}
	return nil
}

func (cfg *Config) isStaticSplitSet() bool {
	return cfg.QueryRangeConfig.SplitQueriesByInterval != 0
}

func (cfg *Config) isDynamicSplitSet() bool {
	return cfg.QueryRangeConfig.MinQuerySplitInterval > 0 ||
		cfg.QueryRangeConfig.HorizontalShards > 0 ||
		cfg.QueryRangeConfig.MaxQuerySplitInterval > 0
}
