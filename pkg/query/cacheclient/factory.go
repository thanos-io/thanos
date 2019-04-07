package cacheclient

import (
	"context"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk/cache"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	yaml "gopkg.in/yaml.v2"
)

type Provider string

const (
	INMEM    Provider = "INMEM"
	MEMCACHE Provider = "MEMCACHE"

	// TODO: REDIS Provider = "REDIS"
)

type Config struct {
	Type Provider      `yaml:"type"`
	TTL  time.Duration `yaml:"ttl"`

	Config interface{} `yaml:"config"`
}

// NewChunk initializes and returns new chunk cache client.
// NOTE: confContentYaml can contain secrets.
func NewChunk(logger log.Logger, confContentYaml []byte, reg prometheus.Registerer) (cache.Cache, error) {
	const name = "querier-chunk"

	level.Info(logger).Log("msg", "loading chunk cache configuration")
	conf := &Config{}
	if err := yaml.UnmarshalStrict(confContentYaml, conf); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	config, err := yaml.Marshal(conf.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of bucket configuration")
	}

	var c cache.Cache
	switch strings.ToUpper(string(conf.Type)) {
	case string(INMEM):
		var imc InMemConfig
		if err := yaml.Unmarshal(config, &imc); err != nil {
			return nil, err
		}
		c = cache.NewFifoCache(name, cache.FifoCacheConfig{
			Size:     imc.Size,
			Validity: conf.TTL,
		})
	case string(MEMCACHE):
		var mc MemcachedConfig
		if err := yaml.Unmarshal(config, &mc); err != nil {
			return nil, err
		}

		c = newMemcached(name, mc, conf.TTL)
	default:
		return nil, errors.Errorf("cache client with type %s is not supported", conf.Type)
	}

	//if err != nil {
	//	return nil, errors.Wrap(err, fmt.Sprintf("create %s client", conf.Type))
	//}
	return WithMetrics(name, c, reg), nil
}

type InMemConfig struct {
	// Size specifies number of entries in cache.
	Size int
}

type MemcachedConfig struct {
	Host           string
	Service        string
	Timeout        time.Duration
	MaxIdleConns   int
	UpdateInterval time.Duration

	BatchSize   int
	Parallelism int
}

func newMemcached(name string, mc MemcachedConfig, ttl time.Duration) cache.Cache {
	client := cache.NewMemcachedClient(cache.MemcachedClientConfig{
		Host:           mc.Host,
		Service:        mc.Service,
		Timeout:        mc.Timeout,
		MaxIdleConns:   mc.MaxIdleConns,
		UpdateInterval: mc.UpdateInterval,
	})

	return cache.NewMemcached(cache.MemcachedConfig{
		BatchSize:   mc.BatchSize,
		Expiration:  ttl,
		Parallelism: mc.Parallelism,
	}, client, name)
}

type withMetricsWrapper struct {
	wrapped cache.Cache
}

// TODO: Add metrics / contribute back.
func WithMetrics(name string, c cache.Cache, reg prometheus.Registerer) cache.Cache {
	return &withMetricsWrapper{wrapped: c}
}

func (w *withMetricsWrapper) Store(ctx context.Context, key []string, buf [][]byte) {
	w.wrapped.Store(ctx, key, buf)
}

func (w *withMetricsWrapper) Fetch(ctx context.Context, keys []string) (found []string, bufs [][]byte, missing []string) {
	return w.wrapped.Fetch(ctx, keys)
}

func (w *withMetricsWrapper) Stop() error {
	return w.wrapped.Stop()
}
