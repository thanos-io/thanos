// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

var (
	// DefaultRedisClientConfig is default redis config.
	DefaultRedisClientConfig = RedisClientConfig{
		DialTimeout:            time.Second * 5,
		ReadTimeout:            time.Second * 3,
		WriteTimeout:           time.Second * 3,
		PoolSize:               100,
		MinIdleConns:           10,
		IdleTimeout:            time.Minute * 5,
		MaxGetMultiConcurrency: 100,
		GetMultiBatchSize:      100,
		MaxSetMultiConcurrency: 100,
		SetMultiBatchSize:      100,
	}
)

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

	// Maximum number of socket connections.
	PoolSize int `yaml:"pool_size"`

	// MinIdleConns specifies the minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int `yaml:"min_idle_conns"`

	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// -1 disables idle timeout check.
	IdleTimeout time.Duration `yaml:"idle_timeout"`

	// Connection age at which client retires (closes) the connection.
	// Default 0 is to not close aged connections.
	MaxConnAge time.Duration `yaml:"max_conn_age"`

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
}

func (c *RedisClientConfig) validate() error {
	if c.Addr == "" {
		return errors.New("no redis addr provided")
	}
	return nil
}

// RedisClient is a wrap of redis.Client.
type RedisClient struct {
	*redis.Client
	config RedisClientConfig

	// getMultiGate used to enforce the max number of concurrent GetMulti() operations.
	getMultiGate gate.Gate
	// setMultiGate used to enforce the max number of concurrent SetMulti() operations.
	setMultiGate gate.Gate

	logger           log.Logger
	durationSet      prometheus.Observer
	durationSetMulti prometheus.Observer
	durationGetMulti prometheus.Observer
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
	redisClient := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Username:     config.Username,
		Password:     config.Password,
		DB:           config.DB,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		MinIdleConns: config.MinIdleConns,
		MaxConnAge:   config.MaxConnAge,
		IdleTimeout:  config.IdleTimeout,
	})

	if reg != nil {
		reg = prometheus.WrapRegistererWith(prometheus.Labels{"name": name}, reg)
	}

	c := &RedisClient{
		Client: redisClient,
		config: config,
		logger: logger,
		getMultiGate: gate.New(
			extprom.WrapRegistererWithPrefix("thanos_redis_getmulti_", reg),
			config.MaxGetMultiConcurrency,
		),
		setMultiGate: gate.New(
			extprom.WrapRegistererWithPrefix("thanos_redis_setmulti_", reg),
			config.MaxSetMultiConcurrency,
		),
	}
	duration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_redis_operation_duration_seconds",
		Help:    "Duration of operations against memcached.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 3, 6, 10},
	}, []string{"operation"})
	c.durationSet = duration.WithLabelValues(opSet)
	c.durationSetMulti = duration.WithLabelValues(opSetMulti)
	c.durationGetMulti = duration.WithLabelValues(opGetMulti)
	return c, nil
}

// SetAsync implement RemoteCacheClient.
func (c *RedisClient) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	start := time.Now()
	if _, err := c.Set(ctx, key, value, ttl).Result(); err != nil {
		level.Warn(c.logger).Log("msg", "failed to set item into redis", "err", err, "key", key,
			"value_size", len(value))
		return nil
	}
	c.durationSet.Observe(time.Since(start).Seconds())
	return nil
}

// SetMulti set multiple keys and value.
func (c *RedisClient) SetMulti(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	if len(data) == 0 {
		return
	}
	start := time.Now()
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	err := doWithBatch(ctx, len(data), c.config.SetMultiBatchSize, c.setMultiGate, func(startIndex, endIndex int) error {
		_, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
			for _, key := range keys {
				p.SetEX(ctx, key, data[key], ttl)
			}
			return nil
		})
		if err != nil {
			level.Warn(c.logger).Log("msg", "failed to set multi items from redis",
				"err", err, "items", len(data))
			return nil
		}
		return nil
	})
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to set multi items from redis", "err", err,
			"items", len(data))
		return
	}
	c.durationSetMulti.Observe(time.Since(start).Seconds())
}

// GetMulti implement RemoteCacheClient.
func (c *RedisClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}
	start := time.Now()
	results := make(map[string][]byte, len(keys))
	var mu sync.Mutex
	err := doWithBatch(ctx, len(keys), c.config.GetMultiBatchSize, c.getMultiGate, func(startIndex, endIndex int) error {
		currentKeys := keys[startIndex:endIndex]
		resp, err := c.MGet(ctx, currentKeys...).Result()
		if err != nil {
			level.Warn(c.logger).Log("msg", "failed to mget items from redis", "err", err, "items", len(resp))
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < len(resp); i++ {
			key := currentKeys[i]
			switch val := resp[i].(type) {
			case string:
				results[key] = stringToBytes(val)
			case nil: // miss
			default:
				level.Warn(c.logger).Log("msg",
					fmt.Sprintf("unexpected redis mget result type:%T %v", resp[i], resp[i]))
			}
		}
		return nil
	})
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to mget items from redis", "err", err, "items", len(keys))
		return nil
	}
	c.durationGetMulti.Observe(time.Since(start).Seconds())
	return results
}

// Stop implement RemoteCacheClient.
func (c *RedisClient) Stop() {
	if err := c.Close(); err != nil {
		level.Error(c.logger).Log("msg", "redis close err")
	}
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

// doWithBatch do func with batch and gate. batchSize==0 means one batch. gate==nil means no gate.
func doWithBatch(ctx context.Context, totalSize int, batchSize int, ga gate.Gate, f func(startIndex, endIndex int) error) error {
	if totalSize == 0 {
		return nil
	}
	if batchSize <= 0 {
		return f(0, totalSize)
	}
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < totalSize; i += batchSize {
		j := i + batchSize
		if j > totalSize {
			j = totalSize
		}
		if ga != nil {
			if err := ga.Start(ctx); err != nil {
				return nil
			}
		}
		startIndex, endIndex := i, j
		g.Go(func() error {
			if ga != nil {
				defer ga.Done()
			}
			return f(startIndex, endIndex)
		})
	}
	return g.Wait()
}

// parseRedisClientConfig unmarshals a buffer into a RedisClientConfig with default values.
func parseRedisClientConfig(conf []byte) (RedisClientConfig, error) {
	config := DefaultRedisClientConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return RedisClientConfig{}, err
	}
	return config, nil
}
