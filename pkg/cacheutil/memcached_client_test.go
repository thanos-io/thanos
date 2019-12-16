package cacheutil

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMemcachedClientConfig_validate(t *testing.T) {
	tests := map[string]struct {
		config   MemcachedClientConfig
		expected error
	}{
		"should pass on valid config": {
			config: MemcachedClientConfig{
				Addrs: []string{"127.0.0.1:11211"},
			},
			expected: nil,
		},
		"should fail on no addrs": {
			config: MemcachedClientConfig{
				Addrs: []string{},
			},
			expected: errMemcachedConfigNoAddrs,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			testutil.Equals(t, testData.expected, testData.config.validate())
		})
	}
}

func TestMemcachedClientConfig_applyDefault(t *testing.T) {
	c := MemcachedClientConfig{}
	c.applyDefaults()

	testutil.Equals(t, defaultTimeout, c.Timeout)
	testutil.Equals(t, defaultMaxIdleConnections, c.MaxIdleConnections)
	testutil.Equals(t, defaultMaxAsyncConcurrency, c.MaxAsyncConcurrency)
	testutil.Equals(t, defaultMaxAsyncBufferSize, c.MaxAsyncBufferSize)
	testutil.Equals(t, defaultDNSProviderUpdateInterval, c.DNSProviderUpdateInterval)
	testutil.Equals(t, defaultMaxGetMultiBatchConcurrency, c.MaxGetMultiBatchConcurrency)
	testutil.Equals(t, defaultMaxGetMultiBatchSize, c.MaxGetMultiBatchSize)
}

func TestMemcachedClient_SetAsync(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx := context.Background()
	config := MemcachedClientConfig{Addrs: []string{"127.0.0.1:11211"}}
	backendMock := newMemcachedClientBackendMock()

	client, err := prepare(config, backendMock)
	testutil.Ok(t, err)
	defer client.Stop()

	testutil.Ok(t, client.SetAsync(ctx, "key-1", []byte("value-1"), time.Second))
	testutil.Ok(t, client.SetAsync(ctx, "key-2", []byte("value-2"), time.Second))
	testutil.Ok(t, backendMock.waitItems(2))

	testutil.Equals(t, 2.0, prom_testutil.ToFloat64(client.operations.WithLabelValues(opSet)))
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(client.operations.WithLabelValues(opGetMulti)))
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(client.failures.WithLabelValues(opSet)))
}

func TestMemcachedClient_GetMulti(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	tests := map[string]struct {
		maxBatchSize          int
		maxBatchConcurrency   int
		mockedGetMultiErrors  int
		initialItems          []memcache.Item
		getKeys               []string
		expectedHits          map[string][]byte
		expectedGetMultiCount int
	}{
		"should fetch keys in a single batch if the input keys is <= the max batch size": {
			maxBatchSize:        2,
			maxBatchConcurrency: 5,
			initialItems: []memcache.Item{
				{Key: "key-1", Value: []byte("value-1")},
				{Key: "key-2", Value: []byte("value-2")},
			},
			getKeys: []string{"key-1", "key-2"},
			expectedHits: map[string][]byte{
				"key-1": []byte("value-1"),
				"key-2": []byte("value-2"),
			},
			expectedGetMultiCount: 1,
		},
		"should fetch keys in a multiple batches if the input keys is > the max batch size": {
			maxBatchSize:        2,
			maxBatchConcurrency: 5,
			initialItems: []memcache.Item{
				{Key: "key-1", Value: []byte("value-1")},
				{Key: "key-2", Value: []byte("value-2")},
				{Key: "key-3", Value: []byte("value-3")},
			},
			getKeys: []string{"key-1", "key-2", "key-3"},
			expectedHits: map[string][]byte{
				"key-1": []byte("value-1"),
				"key-2": []byte("value-2"),
				"key-3": []byte("value-3"),
			},
			expectedGetMultiCount: 2,
		},
		"should fetch keys in a multiple batches on input keys exact multiple of batch size": {
			maxBatchSize:        2,
			maxBatchConcurrency: 5,
			initialItems: []memcache.Item{
				{Key: "key-1", Value: []byte("value-1")},
				{Key: "key-2", Value: []byte("value-2")},
				{Key: "key-3", Value: []byte("value-3")},
				{Key: "key-4", Value: []byte("value-4")},
			},
			getKeys: []string{"key-1", "key-2", "key-3", "key-4"},
			expectedHits: map[string][]byte{
				"key-1": []byte("value-1"),
				"key-2": []byte("value-2"),
				"key-3": []byte("value-3"),
				"key-4": []byte("value-4"),
			},
			expectedGetMultiCount: 2,
		},
		"should return no error on key misses": {
			maxBatchSize:        2,
			maxBatchConcurrency: 5,
			initialItems: []memcache.Item{
				{Key: "key-1", Value: []byte("value-1")},
				{Key: "key-2", Value: []byte("value-2")},
			},
			getKeys: []string{"key-1", "key-2", "key-3", "key-4"},
			expectedHits: map[string][]byte{
				"key-1": []byte("value-1"),
				"key-2": []byte("value-2"),
			},
			expectedGetMultiCount: 2,
		},
		"should return no error on partial errors while fetching batches": {
			maxBatchSize:         2,
			maxBatchConcurrency:  1, // No parallelism to get predictable results.
			mockedGetMultiErrors: 1,
			initialItems: []memcache.Item{
				{Key: "key-1", Value: []byte("value-1")},
				{Key: "key-2", Value: []byte("value-2")},
				{Key: "key-3", Value: []byte("value-3")},
			},
			getKeys:               []string{"key-1", "key-2", "key-3", "key-4"},
			expectedHits:          map[string][]byte{"key-3": []byte("value-3")},
			expectedGetMultiCount: 2,
		},
		"should return no error on partial errors while fetching batches and no items found": {
			maxBatchSize:         2,
			maxBatchConcurrency:  1, // No parallelism to get predictable results.
			mockedGetMultiErrors: 1,
			initialItems: []memcache.Item{
				{Key: "key-1", Value: []byte("value-1")},
				{Key: "key-2", Value: []byte("value-2")},
				{Key: "key-3", Value: []byte("value-3")},
			},
			getKeys:               []string{"key-5", "key-6", "key-7"},
			expectedHits:          map[string][]byte{},
			expectedGetMultiCount: 2,
		},
		"should return error on all errors while fetching batches": {
			maxBatchSize:         2,
			maxBatchConcurrency:  1, // No parallelism to get predictable results.
			mockedGetMultiErrors: 2,
			initialItems: []memcache.Item{
				{Key: "key-1", Value: []byte("value-1")},
				{Key: "key-2", Value: []byte("value-2")},
				{Key: "key-3", Value: []byte("value-3")},
			},
			getKeys:               []string{"key-5", "key-6", "key-7"},
			expectedHits:          nil,
			expectedGetMultiCount: 2,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			config := MemcachedClientConfig{
				Addrs:                       []string{"127.0.0.1:11211"},
				MaxGetMultiBatchSize:        testData.maxBatchSize,
				MaxGetMultiBatchConcurrency: testData.maxBatchConcurrency,
			}

			backendMock := newMemcachedClientBackendMock()
			backendMock.getMultiErrors = testData.mockedGetMultiErrors

			client, err := prepare(config, backendMock)
			testutil.Ok(t, err)
			defer client.Stop()

			// Populate memcached with the initial items.
			for _, item := range testData.initialItems {
				testutil.Ok(t, client.SetAsync(ctx, item.Key, item.Value, time.Second))
			}

			// Wait until initial items have been added.
			testutil.Ok(t, backendMock.waitItems(len(testData.initialItems)))

			// Read back the items.
			testutil.Equals(t, testData.expectedHits, client.GetMulti(ctx, testData.getKeys))

			// Ensure the client has interacted with the backend as expected.
			backendMock.lock.Lock()
			defer backendMock.lock.Unlock()
			testutil.Equals(t, testData.expectedGetMultiCount, backendMock.getMultiCount)

			// Ensure metrics are tracked.
			testutil.Equals(t, float64(testData.expectedGetMultiCount), prom_testutil.ToFloat64(client.operations.WithLabelValues(opGetMulti)))
			testutil.Equals(t, float64(testData.mockedGetMultiErrors), prom_testutil.ToFloat64(client.failures.WithLabelValues(opGetMulti)))
		})
	}
}

func prepare(config MemcachedClientConfig, backendMock *memcachedClientBackendMock) (*memcachedClient, error) {
	logger := log.NewNopLogger()
	selector := &MemcachedJumpHashSelector{}
	provider := dns.NewProvider(logger, nil, dns.GolangResolverType)
	client, err := newMemcachedClient(logger, "test", backendMock, selector, provider, config, nil)

	return client, err
}

type memcachedClientBackendMock struct {
	lock           sync.Mutex
	items          map[string]*memcache.Item
	getMultiCount  int
	getMultiErrors int
}

func newMemcachedClientBackendMock() *memcachedClientBackendMock {
	return &memcachedClientBackendMock{
		items: map[string]*memcache.Item{},
	}
}

func (c *memcachedClientBackendMock) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.getMultiCount++
	if c.getMultiCount <= c.getMultiErrors {
		return nil, errors.New("mocked GetMulti error")
	}

	items := make(map[string]*memcache.Item)
	for _, key := range keys {
		if item, ok := c.items[key]; ok {
			items[key] = item
		}
	}

	return items, nil
}

func (c *memcachedClientBackendMock) Set(item *memcache.Item) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.items[item.Key] = item

	return nil
}

func (c *memcachedClientBackendMock) waitItems(expected int) error {
	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		c.lock.Lock()
		count := len(c.items)
		c.lock.Unlock()

		if count >= expected {
			return nil
		}
	}

	return errors.New("timeout expired while waiting for items in the memcached mock")
}
