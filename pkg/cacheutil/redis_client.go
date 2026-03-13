// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/rueidis"
	"gopkg.in/yaml.v3"

	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/model"
	thanos_tls "github.com/thanos-io/thanos/pkg/tls"
)

var (
	// DefaultRedisClientConfig is default redis config.
	DefaultRedisClientConfig = RedisClientConfig{
		DialTimeout:            time.Second * 5,
		ReadTimeout:            time.Second * 3,
		WriteTimeout:           time.Second * 3,
		MaxGetMultiConcurrency: 100,
		GetMultiBatchSize:      100,
		MaxSetMultiConcurrency: 100,
		SetMultiBatchSize:      100,
		TLSEnabled:             false,
		TLSConfig:              TLSConfig{},
		MaxAsyncConcurrency:    20,
		MaxAsyncBufferSize:     10000,

		SetAsyncCircuitBreaker: defaultCircuitBreakerConfig,
	}
)

// TLSConfig configures TLS connections.
type TLSConfig struct {
	// The CA cert to use for the targets.
	CAFile string `yaml:"ca_file"`
	// The client cert file for the targets.
	CertFile string `yaml:"cert_file"`
	// The client key file for the targets.
	KeyFile string `yaml:"key_file"`
	// Used to verify the hostname for the targets. See https://tools.ietf.org/html/rfc4366#section-3.1
	ServerName string `yaml:"server_name"`
	// Disable target certificate validation.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
}

// RedisClientConfig is the config accepted by RedisClient.
type RedisClientConfig struct {
	// Addr specifies the addresses of redis server.
	Addr string `yaml:"addr"`

	// Use the specified Username to authenticate the current connection
	// with one of the connections defined in the ACL list when connecting
	// to a Redis 6.0 instance, or greater, that is using the Redis ACL system.
	Username string `yaml:"username"`
	// Optional password. Must match the password specified in the
	// requirepass server configuration option (if connecting to a Redis 5.0 instance, or lower),
	// or the User Password when connecting to a Redis 6.0 instance, or greater,
	// that is using the Redis ACL system.
	Password string `yaml:"password"`

	// DB Database to be selected after connecting to the server.
	DB int `yaml:"db"`

	// DialTimeout specifies the client dial timeout.
	DialTimeout time.Duration `yaml:"dial_timeout"`

	// ReadTimeout specifies the client read timeout.
	ReadTimeout time.Duration `yaml:"read_timeout"`

	// WriteTimeout specifies the client write timeout.
	WriteTimeout time.Duration `yaml:"write_timeout"`

	// MaxGetMultiConcurrency specifies the maximum number of concurrent GetMulti() operations.
	// If set to 0, concurrency is unlimited.
	MaxGetMultiConcurrency int `yaml:"max_get_multi_concurrency"`

	// GetMultiBatchSize specifies the maximum size per batch for mget.
	GetMultiBatchSize int `yaml:"get_multi_batch_size"`

	// MaxSetMultiConcurrency specifies the maximum number of concurrent SetMulti() operations.
	// If set to 0, concurrency is unlimited.
	MaxSetMultiConcurrency int `yaml:"max_set_multi_concurrency"`

	// SetMultiBatchSize specifies the maximum size per batch for pipeline set.
	SetMultiBatchSize int `yaml:"set_multi_batch_size"`

	// TLSEnabled enable tls for redis connection.
	TLSEnabled bool `yaml:"tls_enabled"`

	// TLSConfig to use to connect to the redis server.
	TLSConfig TLSConfig `yaml:"tls_config"`

	// If not zero then client-side caching is enabled.
	// Client-side caching is when data is stored in memory
	// instead of fetching data each time.
	// See https://redis.io/docs/manual/client-side-caching/ for info.
	CacheSize model.Bytes `yaml:"cache_size"`

	// MasterName specifies the master's name. Must be not empty
	// for Redis Sentinel.
	MasterName string `yaml:"master_name"`

	// MaxAsyncBufferSize specifies the queue buffer size for SetAsync operations.
	MaxAsyncBufferSize int `yaml:"max_async_buffer_size"`

	// MaxAsyncConcurrency specifies the maximum number of SetAsync goroutines.
	MaxAsyncConcurrency int `yaml:"max_async_concurrency"`

	// SetAsyncCircuitBreaker configures the circuit breaker for SetAsync operations.
	SetAsyncCircuitBreaker CircuitBreakerConfig `yaml:"set_async_circuit_breaker_config"`
}

func (c *RedisClientConfig) validate() error {
	if c.Addr == "" {
		return errors.New("no redis addr provided")
	}

	if c.TLSEnabled {
		if (c.TLSConfig.CertFile != "") != (c.TLSConfig.KeyFile != "") {
			return errors.New("both client key and certificate must be provided")
		}
	}

	if err := c.SetAsyncCircuitBreaker.validate(); err != nil {
		return err
	}
	return nil
}

type RedisClient struct {
	client rueidis.Client

	config RedisClientConfig

	// getMultiGate used to enforce the max number of concurrent GetMulti() operations.
	getMultiGate gate.Gate
	// setMultiGate used to enforce the max number of concurrent SetMulti() operations.
	setMultiGate gate.Gate

	logger           log.Logger
	durationSet      prometheus.Observer
	durationSetMulti prometheus.Observer
	durationGetMulti prometheus.Observer

	p *AsyncOperationProcessor

	setAsyncCircuitBreaker CircuitBreaker
}

// GrabKeys implements ReadThroughRemoteCache.
func (c *RedisClient) GrabKeys(ctx context.Context, keys []string) map[string][]byte {
	panic("unimplemented")
}

// NewRedisClient makes a new RedisClient.
func NewRedisClient(logger log.Logger, name string, conf []byte, reg prometheus.Registerer) (*RedisClient, error) {
	config, err := parseRedisClientConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewRedisClientWithConfig(logger, name, config, reg)
}

// NewRedisClientWithConfig makes a new RedisClient.
func NewRedisClientWithConfig(logger log.Logger, name string, config RedisClientConfig,
	reg prometheus.Registerer) (*RedisClient, error) {

	if err := config.validate(); err != nil {
		return nil, err
	}

	if reg != nil {
		reg = prometheus.WrapRegistererWith(prometheus.Labels{"name": name}, reg)
	}

	var tlsConfig *tls.Config
	if config.TLSEnabled {
		userTLSConfig := config.TLSConfig

		tlsClientConfig, err := thanos_tls.NewClientConfig(logger, userTLSConfig.CertFile, userTLSConfig.KeyFile,
			userTLSConfig.CAFile, userTLSConfig.ServerName, userTLSConfig.InsecureSkipVerify)

		if err != nil {
			return nil, err
		}

		tlsConfig = tlsClientConfig
	}

	clientSideCacheDisabled := config.CacheSize == 0

	clientOpts := rueidis.ClientOption{
		InitAddress:       strings.Split(config.Addr, ","),
		ShuffleInit:       true,
		Username:          config.Username,
		Password:          config.Password,
		SelectDB:          config.DB,
		CacheSizeEachConn: int(config.CacheSize),
		Dialer:            net.Dialer{Timeout: config.DialTimeout},
		ConnWriteTimeout:  config.WriteTimeout,
		DisableCache:      clientSideCacheDisabled,
		TLSConfig:         tlsConfig,
	}

	if config.MasterName != "" {
		clientOpts.Sentinel = rueidis.SentinelOption{
			MasterSet: config.MasterName,
		}
	}

	client, err := rueidis.NewClient(clientOpts)
	if err != nil {
		return nil, err
	}

	c := &RedisClient{
		client: client,
		config: config,
		logger: logger,
		p:      NewAsyncOperationProcessor(config.MaxAsyncBufferSize, config.MaxAsyncConcurrency),
		getMultiGate: gate.New(
			extprom.WrapRegistererWithPrefix("thanos_redis_getmulti_", reg),
			config.MaxGetMultiConcurrency,
			gate.Gets,
		),
		setMultiGate: gate.New(
			extprom.WrapRegistererWithPrefix("thanos_redis_setmulti_", reg),
			config.MaxSetMultiConcurrency,
			gate.Sets,
		),
		setAsyncCircuitBreaker: newCircuitBreaker("redis-set-async", config.SetAsyncCircuitBreaker),
	}

	duration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_redis_operation_duration_seconds",
		Help:    "Duration of operations against redis.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 3, 6, 10},
	}, []string{"operation"})
	c.durationSet = duration.WithLabelValues(opSet)
	c.durationSetMulti = duration.WithLabelValues(opSetMulti)
	c.durationGetMulti = duration.WithLabelValues(opGetMulti)

	return c, nil
}

// SetAsync implement ReadThroughRemoteCache.
func (c *RedisClient) SetAsync(key string, value []byte, ttl time.Duration) error {
	return c.p.EnqueueAsync(func() {
		start := time.Now()
		err := c.setAsyncCircuitBreaker.Execute(func() error {
			return c.client.Do(context.Background(), c.client.B().Set().Key(key).Value(rueidis.BinaryString(value)).ExSeconds(int64(ttl.Seconds())).Build()).Error()
		})
		if err != nil {
			level.Warn(c.logger).Log("msg", "failed to set item into redis", "err", err, "key", key, "value_size", len(value))
			return
		}
		c.durationSet.Observe(time.Since(start).Seconds())
	})
}

// SetMulti set multiple keys and value.
func (c *RedisClient) SetMulti(data map[string][]byte, ttl time.Duration) {
	if len(data) == 0 {
		return
	}
	start := time.Now()
	sets := make(rueidis.Commands, 0, len(data))
	ittl := int64(ttl.Seconds())
	for k, v := range data {
		sets = append(sets, c.client.B().Setex().Key(k).Seconds(ittl).Value(rueidis.BinaryString(v)).Build())
	}
	for _, resp := range c.client.DoMulti(context.Background(), sets...) {
		if err := resp.Error(); err != nil {
			level.Warn(c.logger).Log("msg", "failed to set multi items from redis", "err", err, "items", len(data))
			return
		}
	}
	c.durationSetMulti.Observe(time.Since(start).Seconds())
}

// GetMulti implement ReadThroughRemoteCache.
func (c *RedisClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}
	start := time.Now()
	results := make(map[string][]byte, len(keys))

	if c.config.ReadTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, c.config.ReadTimeout)
		defer cancel()
		ctx = timeoutCtx
	}

	// NOTE(GiedriusS): TTL is the default one in case PTTL fails. 8 hours should be good enough IMHO.
	resps, err := rueidis.MGetCache(c.client, ctx, 8*time.Hour, keys)
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to mget items from redis", "err", err, "items", len(resps))
	}
	for key, resp := range resps {
		if val, err := resp.ToString(); err == nil {
			results[key] = stringToBytes(val)
		}
	}
	c.durationGetMulti.Observe(time.Since(start).Seconds())
	return results
}

// Stop implement ReadThroughRemoteCache.
func (c *RedisClient) Stop() {
	c.p.Stop()
	c.client.Close()
}

// stringToBytes converts string to byte slice (copied from vendor/github.com/go-redis/redis/v8/internal/util/unsafe.go).
func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

// parseRedisClientConfig unmarshals a buffer into a RedisClientConfig with default values.
func parseRedisClientConfig(conf []byte) (RedisClientConfig, error) {
	config := DefaultRedisClientConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return RedisClientConfig{}, err
	}
	return config, nil
}
