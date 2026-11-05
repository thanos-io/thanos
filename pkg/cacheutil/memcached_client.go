// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sony/gobreaker"
	"golang.org/x/sync/singleflight"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/discovery/dns"
	memcacheDiscovery "github.com/thanos-io/thanos/pkg/discovery/memcache"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/model"
)

const (
	opSet                 = "set"
	opSetMulti            = "setmulti"
	opGetMulti            = "getmulti"
	reasonMaxItemSize     = "max-item-size"
	reasonAsyncBufferFull = "async-buffer-full"
	reasonMalformedKey    = "malformed-key"
	reasonTimeout         = "timeout"
	reasonServerError     = "server-error"
	reasonNetworkError    = "network-error"
	reasonOther           = "other"
)

var (
	errMemcachedConfigNoAddrs                  = errors.New("no memcached addresses provided")
	errMemcachedDNSUpdateIntervalNotPositive   = errors.New("DNS provider update interval must be positive")
	errMemcachedMaxAsyncConcurrencyNotPositive = errors.New("max async concurrency must be positive")

	defaultMemcachedClientConfig = MemcachedClientConfig{
		Timeout:                   500 * time.Millisecond,
		MaxIdleConnections:        100,
		MaxAsyncConcurrency:       20,
		MaxAsyncBufferSize:        10000,
		MaxItemSize:               model.Bytes(1024 * 1024),
		MaxGetMultiConcurrency:    100,
		MaxGetMultiBatchSize:      0,
		DNSProviderUpdateInterval: 10 * time.Second,
		AutoDiscovery:             false,

		SetAsyncCircuitBreaker: defaultCircuitBreakerConfig,
	}
)

var (
	_ ReadThroughRemoteCache = (*memcachedClient)(nil)
	_ ReadThroughRemoteCache = (*RedisClient)(nil)
)

// ReadThroughRemoteCache is a high level client to interact with remote cache.
type ReadThroughRemoteCache interface {
	// GetMulti fetches multiple keys at once from remoteCache. In case of error,
	// an empty map is returned and the error tracked/logged.
	GetMulti(ctx context.Context, keys []string) map[string][]byte

	// SetAsync enqueues an asynchronous operation to store a key into memcached.
	// Returns an error in case it fails to enqueue the operation. In case the
	// underlying async operation will fail, the error will be tracked/logged.
	SetAsync(key string, value []byte, ttl time.Duration) error

	// GrabKeys fetches multiple keys from the cache and returns a map containing the fetched data.
	// It ensures that the cache filling function is called only once even if there are multiple requests for the same key.
	GrabKeys(ctx context.Context, keys []string) map[string][]byte

	// Stop client and release underlying resources.
	Stop()
}

// MemcachedClient for compatible.
type MemcachedClient = ReadThroughRemoteCache

// memcachedClientBackend is an interface used to mock the underlying client in tests.
type memcachedClientBackend interface {
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Set(item *memcache.Item) error
}

// updatableServerSelector extends the interface used for picking a memcached server
// for a key to allow servers to be updated at runtime. It allows the selector used
// by the client to be mocked in tests.
type updatableServerSelector interface {
	memcache.ServerSelector

	// SetServers changes a ServerSelector's set of servers at runtime
	// and is safe for concurrent use by multiple goroutines.
	//
	// SetServers returns an error if any of the server names fail to
	// resolve. No attempt is made to connect to the server. If any
	// error occurs, no changes are made to the internal server list.
	SetServers(servers ...string) error

	// PickServerForKeys is like PickServer but returns a map of server address
	// and corresponding keys.
	PickServerForKeys(keys []string) (map[string][]string, error)
}

// MemcachedClientConfig is the config accepted by ReadThroughRemoteCache.
type MemcachedClientConfig struct {
	// Addresses specifies the list of memcached addresses. The addresses get
	// resolved with the DNS provider.
	Addresses []string `yaml:"addresses"`

	// Timeout specifies the socket read/write timeout.
	Timeout time.Duration `yaml:"timeout"`

	// MaxIdleConnections specifies the maximum number of idle connections that
	// will be maintained per address. For better performances, this should be
	// set to a number higher than your peak parallel requests.
	MaxIdleConnections int `yaml:"max_idle_connections"`

	// MaxAsyncConcurrency specifies the maximum number of SetAsync goroutines.
	MaxAsyncConcurrency int `yaml:"max_async_concurrency"`

	// MaxAsyncBufferSize specifies the queue buffer size for SetAsync operations.
	MaxAsyncBufferSize int `yaml:"max_async_buffer_size"`

	// MaxGetMultiConcurrency specifies the maximum number of concurrent GetMulti() operations.
	// If set to 0, concurrency is unlimited.
	MaxGetMultiConcurrency int `yaml:"max_get_multi_concurrency"`

	// MaxItemSize specifies the maximum size of an item stored in memcached.
	// Items bigger than MaxItemSize are skipped.
	// If set to 0, no maximum size is enforced.
	MaxItemSize model.Bytes `yaml:"max_item_size"`

	// MaxGetMultiBatchSize specifies the maximum number of keys a single underlying
	// GetMulti() should run. If more keys are specified, internally keys are split
	// into multiple batches and fetched concurrently, honoring MaxGetMultiConcurrency parallelism.
	// If set to 0, the max batch size is unlimited.
	MaxGetMultiBatchSize int `yaml:"max_get_multi_batch_size"`

	// DNSProviderUpdateInterval specifies the DNS discovery update interval.
	DNSProviderUpdateInterval time.Duration `yaml:"dns_provider_update_interval"`

	// AutoDiscovery configures memached client to perform auto-discovery instead of DNS resolution
	AutoDiscovery bool `yaml:"auto_discovery"`

	// SetAsyncCircuitBreaker configures the circuit breaker for SetAsync operations.
	SetAsyncCircuitBreaker CircuitBreakerConfig `yaml:"set_async_circuit_breaker_config"`
}

func (c *MemcachedClientConfig) validate() error {
	if len(c.Addresses) == 0 {
		return errMemcachedConfigNoAddrs
	}

	// Avoid panic in time ticker.
	if c.DNSProviderUpdateInterval <= 0 {
		return errMemcachedDNSUpdateIntervalNotPositive
	}

	// Set async only available when MaxAsyncConcurrency > 0.
	if c.MaxAsyncConcurrency <= 0 {
		return errMemcachedMaxAsyncConcurrencyNotPositive
	}

	if err := c.SetAsyncCircuitBreaker.validate(); err != nil {
		return err
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
	selector updatableServerSelector

	// Name provides an identifier for the instantiated Client
	name string

	// Address provider used to keep the memcached servers list updated.
	addressProvider AddressProvider

	// Gate used to enforce the max number of concurrent GetMulti() operations.
	getMultiGate gate.Gate

	// Tracked metrics.
	clientInfo prometheus.GaugeFunc
	operations *prometheus.CounterVec
	failures   *prometheus.CounterVec
	skipped    *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	dataSize   *prometheus.HistogramVec

	p *AsyncOperationProcessor

	setAsyncCircuitBreaker CircuitBreaker
}

// AddressProvider performs node address resolution given a list of clusters.
type AddressProvider interface {
	// Resolves the provided list of memcached cluster to the actual nodes
	Resolve(context.Context, []string, bool) error

	// Returns the nodes
	Addresses() []string
}

type memcachedGetMultiResult struct {
	items map[string]*memcache.Item
	err   error
}

// NewMemcachedClient makes a new ReadThroughRemoteCache.
func NewMemcachedClient(logger log.Logger, name string, conf []byte, reg prometheus.Registerer) (*memcachedClient, error) {
	config, err := parseMemcachedClientConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewMemcachedClientWithConfig(logger, name, config, reg)
}

// NewMemcachedClientWithConfig makes a new ReadThroughRemoteCache.
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

	if reg != nil {
		reg = prometheus.WrapRegistererWith(prometheus.Labels{"name": name}, reg)
	}
	return newMemcachedClient(logger, client, selector, config, reg, name)
}

func newMemcachedClient(
	logger log.Logger,
	client memcachedClientBackend,
	selector updatableServerSelector,
	config MemcachedClientConfig,
	reg prometheus.Registerer,
	name string,
) (*memcachedClient, error) {
	promRegisterer := extprom.WrapRegistererWithPrefix("thanos_memcached_", reg)

	var addressProvider AddressProvider
	if config.AutoDiscovery {
		addressProvider = memcacheDiscovery.NewProvider(
			logger,
			promRegisterer,
			config.Timeout,
		)
	} else {
		addressProvider = dns.NewProvider(
			logger,
			extprom.WrapRegistererWithPrefix("thanos_memcached_", reg),
			dns.MiekgdnsResolverType,
		)
	}

	c := &memcachedClient{
		logger:          log.With(logger, "name", name),
		config:          config,
		client:          client,
		selector:        selector,
		addressProvider: addressProvider,
		getMultiGate: gate.New(
			extprom.WrapRegistererWithPrefix("thanos_memcached_getmulti_", reg),
			config.MaxGetMultiConcurrency,
			gate.Gets,
		),
		p:                      NewAsyncOperationProcessor(config.MaxAsyncBufferSize, config.MaxAsyncConcurrency),
		setAsyncCircuitBreaker: newCircuitBreaker("memcached-set-async", config.SetAsyncCircuitBreaker),
	}

	c.clientInfo = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_memcached_client_info",
		Help: "A metric with a constant '1' value labeled by configuration options from which memcached client was configured.",
		ConstLabels: prometheus.Labels{
			"timeout":                      config.Timeout.String(),
			"max_idle_connections":         strconv.Itoa(config.MaxIdleConnections),
			"max_async_concurrency":        strconv.Itoa(config.MaxAsyncConcurrency),
			"max_async_buffer_size":        strconv.Itoa(config.MaxAsyncBufferSize),
			"max_item_size":                strconv.FormatUint(uint64(config.MaxItemSize), 10),
			"max_get_multi_concurrency":    strconv.Itoa(config.MaxGetMultiConcurrency),
			"max_get_multi_batch_size":     strconv.Itoa(config.MaxGetMultiBatchSize),
			"dns_provider_update_interval": config.DNSProviderUpdateInterval.String(),
		},
	},
		func() float64 { return 1 },
	)

	c.operations = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_memcached_operations_total",
		Help: "Total number of operations against memcached.",
	}, []string{"operation"})
	c.operations.WithLabelValues(opGetMulti)
	c.operations.WithLabelValues(opSet)

	c.failures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_memcached_operation_failures_total",
		Help: "Total number of operations against memcached that failed.",
	}, []string{"operation", "reason"})
	c.failures.WithLabelValues(opGetMulti, reasonTimeout)
	c.failures.WithLabelValues(opGetMulti, reasonMalformedKey)
	c.failures.WithLabelValues(opGetMulti, reasonServerError)
	c.failures.WithLabelValues(opGetMulti, reasonNetworkError)
	c.failures.WithLabelValues(opGetMulti, reasonOther)
	c.failures.WithLabelValues(opSet, reasonTimeout)
	c.failures.WithLabelValues(opSet, reasonMalformedKey)
	c.failures.WithLabelValues(opSet, reasonServerError)
	c.failures.WithLabelValues(opSet, reasonNetworkError)
	c.failures.WithLabelValues(opSet, reasonOther)

	c.skipped = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_memcached_operation_skipped_total",
		Help: "Total number of operations against memcached that have been skipped.",
	}, []string{"operation", "reason"})
	c.skipped.WithLabelValues(opGetMulti, reasonMaxItemSize)
	c.skipped.WithLabelValues(opSet, reasonMaxItemSize)
	c.skipped.WithLabelValues(opSet, reasonAsyncBufferFull)

	c.duration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_memcached_operation_duration_seconds",
		Help:    "Duration of operations against memcached.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 3, 6, 10},
	}, []string{"operation"})
	c.duration.WithLabelValues(opGetMulti)
	c.duration.WithLabelValues(opSet)

	c.dataSize = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "thanos_memcached_operation_data_size_bytes",
		Help: "Tracks the size of the data stored in and fetched from memcached.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	},
		[]string{"operation"},
	)
	c.dataSize.WithLabelValues(opGetMulti)
	c.dataSize.WithLabelValues(opSet)

	// As soon as the client is created it must ensure that memcached server
	// addresses are resolved, so we're going to trigger an initial addresses
	// resolution here.
	if err := c.resolveAddrs(); err != nil {
		return nil, err
	}

	c.p.workers.Add(1)
	go c.resolveAddrsLoop()

	return c, nil
}

func (c *memcachedClient) Stop() {
	c.p.Stop()
}

func (c *memcachedClient) SetAsync(key string, value []byte, ttl time.Duration) error {
	// Skip hitting memcached at all if the item is bigger than the max allowed size.
	if c.config.MaxItemSize > 0 && uint64(len(value)) > uint64(c.config.MaxItemSize) {
		c.skipped.WithLabelValues(opSet, reasonMaxItemSize).Inc()
		return nil
	}

	err := c.p.EnqueueAsync(func() {
		start := time.Now()
		c.operations.WithLabelValues(opSet).Inc()

		err := c.setAsyncCircuitBreaker.Execute(func() error {
			return c.client.Set(&memcache.Item{
				Key:        key,
				Value:      value,
				Expiration: int32(time.Now().Add(ttl).Unix()),
			})
		})
		if err != nil {
			if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
				level.Warn(c.logger).Log(
					"msg", "circuit breaker disallows storing item in memcached",
					"key", key,
					"err", err)
			} else {
				// If the PickServer will fail for any reason the server address will be nil
				// and so missing in the logs. We're OK with that (it's a best effort).
				serverAddr, _ := c.selector.PickServer(key)
				level.Debug(c.logger).Log(
					"msg", "failed to store item to memcached",
					"key", key,
					"sizeBytes", len(value),
					"server", serverAddr,
					"err", err,
				)
			}
			c.trackError(opSet, err)
			return
		}

		c.dataSize.WithLabelValues(opSet).Observe(float64(len(value)))
		c.duration.WithLabelValues(opSet).Observe(time.Since(start).Seconds())
	})

	if errors.Is(err, ErrAsyncBufferFull) {
		c.skipped.WithLabelValues(opSet, reasonAsyncBufferFull).Inc()
		level.Debug(c.logger).Log("msg", "failed to store item to memcached because the async buffer is full", "err", err, "size", len(c.p.asyncQueue))
		return nil
	}
	return err
}

func (c *memcachedClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
	if len(keys) == 0 {
		return nil
	}

	batches, err := c.getMultiBatched(ctx, keys)
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to fetch items from memcached", "numKeys", len(keys), "firstKey", keys[0], "err", err)

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

func (c *memcachedClient) GrabKeys(ctx context.Context, keys []string) map[string][]byte {
	var group singleflight.Group
	fetchedData := make(map[string][]byte)
	var mu sync.Mutex

	fetchData := func(key string) ([]byte, error) {
		result := c.GetMulti(ctx, []string{key})
		if data, ok := result[key]; ok {
			return data, nil
		}
		return nil, errors.New("key not found in cache")
	}

	for _, key := range keys {
		val, err, _ := group.Do(key, func() (interface{}, error) {
			return fetchData(key)
		})

		if err != nil {
			level.Warn(c.logger).Log("Failed to fetch data for key:", key, "Error:", err)
			continue
		}

		data, ok := val.([]byte)
		if !ok {
			level.Warn(c.logger).Log("Failed to convert data for key:", key, "to []byte")
			continue
		}

		mu.Lock()
		fetchedData[key] = data
		mu.Unlock()
	}

	return fetchedData
}

func (c *memcachedClient) getMultiBatched(ctx context.Context, keys []string) ([]map[string]*memcache.Item, error) {
	// Do not batch if the input keys are less than the max batch size.
	if (c.config.MaxGetMultiBatchSize <= 0) || (len(keys) <= c.config.MaxGetMultiBatchSize) {
		// Even if we're not splitting the input into batches, make sure that our single request
		// still counts against the concurrency limit.
		if c.config.MaxGetMultiConcurrency > 0 {
			if err := c.getMultiGate.Start(ctx); err != nil {
				return nil, errors.Wrapf(err, "failed to wait for turn. Instance: %s", c.name)
			}

			defer c.getMultiGate.Done()
		}

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

	// If max concurrency is disabled, use a nil gate for the doWithBatch method which will
	// not apply any limit to the number goroutines started to make batch requests in that case.
	var getMultiGate gate.Gate
	if c.config.MaxGetMultiConcurrency > 0 {
		getMultiGate = c.getMultiGate
	}

	// Sort keys based on which memcached server they will be sharded to. Sorting keys that
	// are on the same server together before splitting into batches reduces the number of
	// connections required and increases the number of "gets" per connection.
	sortedKeys := c.sortKeysByServer(keys)

	// Allocate a channel to store results for each batch request. The max concurrency will be
	// enforced by doWithBatch.
	results := make(chan *memcachedGetMultiResult, numResults)
	defer close(results)

	// Ignore the error here since it can only be returned by our provided function which
	// always returns nil. NOTE also we are using a background context here for the doWithBatch
	// method. This is to ensure that it runs the expected number of batches _even if_ our
	// context (`ctx`) is canceled since we expect a certain number of batches to be read
	// from `results` below. The wrapped `getMultiSingle` method will still check our context
	// and short-circuit if it has been canceled.
	_ = doWithBatch(context.Background(), len(keys), c.config.MaxGetMultiBatchSize, getMultiGate, func(startIndex, endIndex int) error {
		batchKeys := sortedKeys[startIndex:endIndex]

		res := &memcachedGetMultiResult{}
		res.items, res.err = c.getMultiSingle(ctx, batchKeys)

		results <- res
		return nil
	})

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

func (c *memcachedClient) getMultiSingle(ctx context.Context, keys []string) (items map[string]*memcache.Item, err error) {
	start := time.Now()
	c.operations.WithLabelValues(opGetMulti).Inc()

	select {
	case <-ctx.Done():
		// Make sure our context hasn't been canceled before fetching cache items using
		// cache client backend.
		return nil, ctx.Err()
	default:
		items, err = c.client.GetMulti(keys)
	}

	if err != nil {
		level.Debug(c.logger).Log("msg", "failed to get multiple items from memcached", "err", err)
		c.trackError(opGetMulti, err)
	} else {
		var total int
		for _, it := range items {
			total += len(it.Value)
		}
		c.dataSize.WithLabelValues(opGetMulti).Observe(float64(total))
		c.duration.WithLabelValues(opGetMulti).Observe(time.Since(start).Seconds())
	}

	return items, err
}

// sortKeysByServer sorts cache keys within a slice based on which server they are
// sharded to using a memcache.ServerSelector instance. The keys are ordered so keys
// on the same server are next to each other. Any errors encountered determining which
// server a key should be on will result in returning keys unsorted (in the same order
// they were supplied in). Note that output is not guaranteed to be any particular order
// *except* that keys sharded to the same server will be together. The order of keys
// returned may change from call to call.
func (c *memcachedClient) sortKeysByServer(keys []string) []string {
	bucketed, err := c.selector.PickServerForKeys(keys)
	// No need to pick server and sort keys if no more than 1 server.
	if err != nil || len(bucketed) <= 1 {
		return keys
	}

	out := make([]string, 0, len(keys))
	for srv := range bucketed {
		out = append(out, bucketed[srv]...)
	}

	return out
}

func (c *memcachedClient) trackError(op string, err error) {
	var connErr *memcache.ConnectTimeoutError
	var netErr net.Error
	switch {
	case errors.As(err, &connErr):
		c.failures.WithLabelValues(op, reasonTimeout).Inc()
	case errors.As(err, &netErr):
		if netErr.Timeout() {
			c.failures.WithLabelValues(op, reasonTimeout).Inc()
		} else {
			c.failures.WithLabelValues(op, reasonNetworkError).Inc()
		}
	case errors.Is(err, memcache.ErrMalformedKey):
		c.failures.WithLabelValues(op, reasonMalformedKey).Inc()
	case errors.Is(err, memcache.ErrServerError):
		c.failures.WithLabelValues(op, reasonServerError).Inc()
	default:
		c.failures.WithLabelValues(op, reasonOther).Inc()
	}
}

func (c *memcachedClient) resolveAddrsLoop() {
	defer c.p.workers.Done()

	ticker := time.NewTicker(c.config.DNSProviderUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := c.resolveAddrs()
			if err != nil {
				level.Warn(c.logger).Log("msg", "failed update memcached servers list", "err", err)
			}
		case <-c.p.stop:
			return
		}
	}
}

func (c *memcachedClient) resolveAddrs() error {
	// Resolve configured addresses with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// If some of the dns resolution fails, log the error.
	if err := c.addressProvider.Resolve(ctx, c.config.Addresses, true); err != nil {
		level.Error(c.logger).Log("msg", "failed to resolve addresses for memcached", "addresses", strings.Join(c.config.Addresses, ","), "err", err)
	}
	// Fail in case no server address is resolved.
	servers := c.addressProvider.Addresses()
	if len(servers) == 0 {
		return fmt.Errorf("no server address resolved for %s", c.name)
	}

	return c.selector.SetServers(servers...)
}
