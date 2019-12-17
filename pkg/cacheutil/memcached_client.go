package cacheutil

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/tracing"
	"gopkg.in/yaml.v2"
)

const (
	opSet      = "set"
	opGetMulti = "getmulti"
)

var (
	errMemcachedAsyncBufferFull = errors.New("the async buffer is full")
	errMemcachedConfigNoAddrs   = errors.New("no memcached addrs provided")

	defaultMemcachedClientConfig = MemcachedClientConfig{
		Timeout:                     500 * time.Millisecond,
		MaxIdleConnections:          100,
		MaxAsyncConcurrency:         20,
		MaxAsyncBufferSize:          10000,
		MaxGetMultiBatchConcurrency: 20,
		MaxGetMultiBatchSize:        0,
		DNSProviderUpdateInterval:   10 * time.Second,
	}
)

// MemcachedClient is a high level client to interact with memcached.
type MemcachedClient interface {
	// GetMulti fetches multiple keys at once from memcached. In case of error,
	// an empty map is returned and the error tracked/logged.
	GetMulti(ctx context.Context, keys []string) map[string][]byte

	// SetAsync enqueues an asynchronous operation to store a key into memcached.
	SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Stop client and release underlying resources.
	Stop()
}

// memcachedClientBackend is an interface used to mock the underlying client in tests.
type memcachedClientBackend interface {
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Set(item *memcache.Item) error
}

// MemcachedClientConfig is the config accepted by MemcachedClient.
type MemcachedClientConfig struct {
	// Addrs specifies the list of memcached addresses. The addresses get
	// resolved with the DNS provider.
	Addrs []string `yaml:"addrs"`

	// Timeout specifies the socket read/write timeout.
	Timeout time.Duration `yaml:"timeout"`

	// MaxIdleConnections specifies the maximum number of idle connections that
	// will be maintained per address. For better performances, this should be
	// set to a number higher than your peak parallel requests.
	MaxIdleConnections int `yaml:"max_idle_connections"`

	// MaxAsyncConcurrency specifies the maximum number of concurrent asynchronous
	// operations can occur.
	MaxAsyncConcurrency int `yaml:"max_async_concurrency"`

	// MaxAsyncBufferSize specifies the maximum number of enqueued asynchronous
	// operations allowed.
	MaxAsyncBufferSize int `yaml:"max_async_buffer_size"`

	// MaxGetMultiBatchConcurrency specifies the maximum number of concurrent batch
	// executions by GetMulti().
	// TODO(pracucci) Should this be a global (per-client) limit or a per-single MultiGet()
	//                limit? The latter would allow us to avoid a single very large MultiGet()
	//                will slow down other requests.
	MaxGetMultiBatchConcurrency int `yaml:"max_get_multi_batch_concurrency"`

	// MaxGetMultiBatchSize specifies the maximum number of keys a single underlying
	// GetMulti() should run. If more keys are specified, internally keys are splitted
	// into multiple batches and fetched concurrently up to MaxGetMultiBatchConcurrency
	// parallelism. If set to 0, the max batch size is unlimited.
	MaxGetMultiBatchSize int `yaml:"max_get_multi_batch_size"`

	// DNSProviderUpdateInterval specifies the DNS discovery update interval.
	DNSProviderUpdateInterval time.Duration `yaml:"dns_provider_update_interval"`
}

func (c *MemcachedClientConfig) validate() error {
	if len(c.Addrs) == 0 {
		return errMemcachedConfigNoAddrs
	}

	return nil
}

// parseMemcachedClientConfig unmarshals a buffer into a MemcachedClientConfig with default values.
func parseMemcachedClientConfig(conf []byte) (MemcachedClientConfig, error) {
	config := defaultMemcachedClientConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return MemcachedClientConfig{}, err
	}

	return config, nil
}

type memcachedClient struct {
	logger   log.Logger
	config   MemcachedClientConfig
	client   memcachedClientBackend
	selector *MemcachedJumpHashSelector

	// DNS provider used to keep the memcached servers list updated.
	dnsProvider *dns.Provider

	// Channel used to notify internal goroutines when they should quit.
	stop chan struct{}

	// Channel used to enqueue async operations.
	asyncQueue chan func()

	// Channel used to enqueue get multi operations.
	getMultiQueue chan *memcachedGetMultiBatch

	// Wait group used to wait all workers on stopping.
	workers sync.WaitGroup

	// Tracked metrics.
	operations *prometheus.CounterVec
	failures   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
}

type memcachedGetMultiBatch struct {
	ctx     context.Context
	keys    []string
	results chan<- *memcachedGetMultiResult
}

type memcachedGetMultiResult struct {
	items map[string]*memcache.Item
	err   error
}

// NewMemcachedClient makes a new MemcachedClient.
func NewMemcachedClient(logger log.Logger, name string, conf []byte, reg prometheus.Registerer) (*memcachedClient, error) {
	config, err := parseMemcachedClientConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewMemcachedClientWithConfig(logger, name, config, reg)
}

// NewMemcachedClientWithConfig makes a new MemcachedClient.
func NewMemcachedClientWithConfig(logger log.Logger, name string, config MemcachedClientConfig, reg prometheus.Registerer) (*memcachedClient, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	// We use a custom servers selector in order to use a jump hash
	// for servers selection.
	selector := &MemcachedJumpHashSelector{}

	client := memcache.NewFromSelector(selector)
	client.Timeout = config.Timeout
	client.MaxIdleConns = config.MaxIdleConnections

	return newMemcachedClient(logger, name, client, selector, config, reg)
}

func newMemcachedClient(
	logger log.Logger,
	name string,
	client memcachedClientBackend,
	selector *MemcachedJumpHashSelector,
	config MemcachedClientConfig,
	reg prometheus.Registerer,
) (*memcachedClient, error) {
	dnsProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_memcached_", reg),
		dns.ResolverType(dns.GolangResolverType),
	)

	c := &memcachedClient{
		logger:        logger,
		config:        config,
		client:        client,
		selector:      selector,
		dnsProvider:   dnsProvider,
		asyncQueue:    make(chan func(), config.MaxAsyncBufferSize),
		getMultiQueue: make(chan *memcachedGetMultiBatch),
		stop:          make(chan struct{}, 1),
	}

	c.operations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "thanos_memcached_operations_total",
		Help:        "Total number of operations against memcached.",
		ConstLabels: prometheus.Labels{"name": name},
	}, []string{"operation"})

	c.failures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "thanos_memcached_operation_failures_total",
		Help:        "Total number of operations against memcached that failed.",
		ConstLabels: prometheus.Labels{"name": name},
	}, []string{"operation"})

	c.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "thanos_memcached_operation_duration_seconds",
		Help:        "Duration of operations against memcached.",
		ConstLabels: prometheus.Labels{"name": name},
		Buckets:     []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1},
	}, []string{"operation"})

	if reg != nil {
		reg.MustRegister(c.operations, c.failures, c.duration)
	}

	// As soon as the client is created it must ensure that memcached server
	// addresses are resolved, so we're going to trigger an initial addresses
	// resolution here.
	if err := c.resolveAddrs(); err != nil {
		return nil, err
	}

	c.workers.Add(1)
	go c.resolveAddrsLoop()

	// Start a number of goroutines - processing async operations - equal
	// to the max concurrency we have.
	c.workers.Add(c.config.MaxAsyncConcurrency)
	for i := 0; i < c.config.MaxAsyncConcurrency; i++ {
		go c.asyncQueueProcessLoop()
	}

	// Start a number of goroutines - processing get multi batch operations - equal
	// to the max concurrency we have.
	c.workers.Add(c.config.MaxGetMultiBatchConcurrency)
	for i := 0; i < c.config.MaxGetMultiBatchConcurrency; i++ {
		go c.getMultiQueueProcessLoop()
	}

	return c, nil
}

func (c *memcachedClient) Stop() {
	close(c.stop)

	// Wait until all workers have terminated.
	c.workers.Wait()
}

func (c *memcachedClient) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.enqueueAsync(func() {
		start := time.Now()
		c.operations.WithLabelValues(opSet).Inc()

		span, _ := tracing.StartSpan(ctx, "memcached_set")
		err := c.client.Set(&memcache.Item{
			Key:        key,
			Value:      value,
			Expiration: int32(time.Now().Add(ttl).Unix()),
		})
		span.Finish()
		if err != nil {
			c.failures.WithLabelValues(opSet).Inc()
			level.Warn(c.logger).Log("msg", fmt.Sprintf("failed to store item with key %s to memcached", key), "err", err)
			return
		}

		c.duration.WithLabelValues(opSet).Observe(time.Since(start).Seconds())
	})
}

func (c *memcachedClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	batches, err := c.getMultiBatched(ctx, keys)
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to fetch keys from memcached", "err", err)

		// In case we have both results and an error, it means some batch requests
		// failed and other succeeded. In this case we prefer to log it and move on,
		// given returning some results from the cache is better than returning
		// nothing.
		if len(batches) == 0 {
			return nil
		}
	}

	hits := map[string][]byte{}
	for _, items := range batches {
		for key, item := range items {
			hits[key] = item.Value
		}
	}

	return hits
}

func (c *memcachedClient) getMultiBatched(ctx context.Context, keys []string) ([]map[string]*memcache.Item, error) {
	// Do not batch if the input keys are less then the max batch size.
	if (c.config.MaxGetMultiBatchSize <= 0) || (len(keys) <= c.config.MaxGetMultiBatchSize) {
		items, err := c.getMultiSingle(ctx, keys)
		if err != nil {
			return nil, err
		}

		return []map[string]*memcache.Item{items}, nil
	}

	// Calculate the number of expected results.
	batchSize := c.config.MaxGetMultiBatchSize
	numResults := len(keys) / batchSize
	if len(keys)%batchSize != 0 {
		numResults++
	}

	// Split input keys into batches and schedule a job for it.
	results := make(chan *memcachedGetMultiResult, numResults)
	defer close(results)

	go func() {
		for batchStart := 0; batchStart < len(keys); batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > len(keys) {
				batchEnd = len(keys)
			}

			c.getMultiQueue <- &memcachedGetMultiBatch{
				ctx:     ctx,
				keys:    keys[batchStart:batchEnd],
				results: results,
			}
		}
	}()

	// Wait for all batch results. In case of error, we keep
	// track of the last error occurred.
	items := make([]map[string]*memcache.Item, 0, numResults)
	var lastErr error

	for i := 0; i < numResults; i++ {
		result := <-results
		if result.err != nil {
			lastErr = result.err
			continue
		}

		items = append(items, result.items)
	}

	return items, lastErr
}

func (c *memcachedClient) getMultiSingle(ctx context.Context, keys []string) (map[string]*memcache.Item, error) {
	start := time.Now()
	c.operations.WithLabelValues(opGetMulti).Inc()

	span, _ := tracing.StartSpan(ctx, "memcached_get_multi")
	items, err := c.client.GetMulti(keys)
	span.Finish()
	if err != nil {
		c.failures.WithLabelValues(opGetMulti).Inc()
	} else {
		c.duration.WithLabelValues(opGetMulti).Observe(time.Since(start).Seconds())
	}

	return items, err
}

func (c *memcachedClient) enqueueAsync(op func()) error {
	select {
	case c.asyncQueue <- op:
		return nil
	default:
		return errMemcachedAsyncBufferFull
	}
}

func (c *memcachedClient) asyncQueueProcessLoop() {
	defer c.workers.Done()

	for {
		select {
		case op := <-c.asyncQueue:
			op()
		case <-c.stop:
			return
		}
	}
}

func (c *memcachedClient) getMultiQueueProcessLoop() {
	defer c.workers.Done()

	for {
		select {
		case batch := <-c.getMultiQueue:
			res := &memcachedGetMultiResult{}
			res.items, res.err = c.getMultiSingle(batch.ctx, batch.keys)

			batch.results <- res
		case <-c.stop:
			return
		}
	}
}

func (c *memcachedClient) resolveAddrsLoop() {
	defer c.workers.Done()

	ticker := time.NewTicker(c.config.DNSProviderUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := c.resolveAddrs()
			if err != nil {
				level.Warn(c.logger).Log("msg", "failed update memcached servers list", "err", err)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *memcachedClient) resolveAddrs() error {
	// Resolve configured addresses with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c.dnsProvider.Resolve(ctx, c.config.Addrs)

	// Fail in case no server address is resolved.
	servers := c.dnsProvider.Addresses()
	if len(servers) == 0 {
		return errors.New("no server address resolved")
	}

	return c.selector.SetServers(servers...)
}
