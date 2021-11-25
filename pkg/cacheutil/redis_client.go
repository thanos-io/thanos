package cacheutil

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"gopkg.in/yaml.v3"
)

var (
	defaultRedisClientConfig = RedisClientConfig{
		MaxGetMultiConcurrency: 100,
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

	// DialTimeout specifies the client dial timeout.
	// Default is 5 seconds.
	DialTimeout time.Duration `yaml:"dial_timeout"`

	// ReadTimeout specifies the client read timeout.
	// Default is 3 seconds.
	ReadTimeout time.Duration `yaml:"read_timeout"`

	// WriteTimeout specifies the client write timeout.
	// Default is ReadTimeout.
	WriteTimeout time.Duration `yaml:"write_timeout"`

	// Maximum number of socket connections.
	// Default is 10 connections per every available CPU as reported by runtime.GOMAXPROCS.
	PoolSize int `yaml:"pool_size"`

	// MinIdleConns specifies the minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int `yaml:"min_idle_conns"`

	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout time.Duration `yaml:"idle_timeout"`

	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration `yaml:"max_conn_age"`

	// MaxGetMultiConcurrency specifies the maximum number of concurrent GetMulti() operations.
	// If set to 0, concurrency is unlimited.
	MaxGetMultiConcurrency int `yaml:"max_get_multi_concurrency"`
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
		logger: logger,
		getMultiGate: gate.New(
			extprom.WrapRegistererWithPrefix("thanos_redis_getmulti_", reg),
			config.MaxGetMultiConcurrency,
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
	start := time.Now()
	_, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for key, val := range data {
			p.SetEX(ctx, key, val, ttl)
		}
		return nil
	})
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to mset items into redis", "err", err, "items", len(data))
		return
	}
	c.durationSetMulti.Observe(time.Since(start).Seconds())
}

// GetMulti implement RemoteCacheClient.
func (c *RedisClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}
	// Wait until we get a free slot from the gate, if the max
	// concurrency should be enforced.
	if c.config.MaxGetMultiConcurrency > 0 {
		if err := c.getMultiGate.Start(ctx); err != nil {
			level.Warn(c.logger).Log("msg", "getMultiGate err", "err", err, "items", len(keys))
			return nil
		}
		defer c.getMultiGate.Done()
	}
	start := time.Now()
	resp, err := c.MGet(ctx, keys...).Result()
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to mget items from redis", "err", err, "items", len(resp))
		return nil
	}
	var hits int
	for i := 0; i < len(resp); i++ {
		if resp[i] != nil {
			hits++
		}
	}
	results := make(map[string][]byte, hits)
	for i := 0; i < len(resp); i++ {
		v := resp[i]
		switch vv := v.(type) {
		case string:
			results[keys[i]] = []byte(vv)
		case []byte:
			results[keys[i]] = vv
		case nil: // miss
		default:
			level.Warn(c.logger).Log("msg",
				fmt.Sprintf("unexpected redis mget result type:%T %v", v, v))
		}
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

// parseRedisClientConfig unmarshals a buffer into a RedisClientConfig with default values.
func parseRedisClientConfig(conf []byte) (RedisClientConfig, error) {
	config := defaultRedisClientConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return RedisClientConfig{}, err
	}
	return config, nil
}
