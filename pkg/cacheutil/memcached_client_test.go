// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMemcachedClientConfig_validate(t *testing.T) {
	tests := map[string]struct {
		config   MemcachedClientConfig
		expected error
	}{
		"should pass on valid config": {
			config: MemcachedClientConfig{
				Addresses:                 []string{"127.0.0.1:11211"},
				MaxAsyncConcurrency:       1,
				DNSProviderUpdateInterval: time.Second,
			},
			expected: nil,
		},
		"should fail on no addresses": {
			config: MemcachedClientConfig{
				Addresses:                 []string{},
				MaxAsyncConcurrency:       1,
				DNSProviderUpdateInterval: time.Second,
			},
			expected: errMemcachedConfigNoAddrs,
		},
		"should fail on max_async_concurrency <= 0": {
			config: MemcachedClientConfig{
				Addresses:                 []string{"127.0.0.1:11211"},
				MaxAsyncConcurrency:       0,
				DNSProviderUpdateInterval: time.Second,
			},
			expected: errMemcachedMaxAsyncConcurrencyNotPositive,
		},
		"should fail on dns_provider_update_interval <= 0": {
			config: MemcachedClientConfig{
				Addresses:           []string{"127.0.0.1:11211"},
				MaxAsyncConcurrency: 1,
			},
			expected: errMemcachedDNSUpdateIntervalNotPositive,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			testutil.Equals(t, testData.expected, testData.config.validate())
		})
	}
}

func TestNewMemcachedClient(t *testing.T) {
	// Should return error on empty YAML config.
	conf := []byte{}
	cache, err := NewMemcachedClient(log.NewNopLogger(), "test", conf, nil)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*memcachedClient)(nil), cache)

	// Should return error on invalid YAML config.
	conf = []byte("invalid")
	cache, err = NewMemcachedClient(log.NewNopLogger(), "test", conf, nil)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*memcachedClient)(nil), cache)

	// Should instance a memcached client with minimum YAML config.
	conf = []byte(`
addresses:
  - 127.0.0.1:11211
  - 127.0.0.2:11211
`)
	cache, err = NewMemcachedClient(log.NewNopLogger(), "test", conf, nil)
	testutil.Ok(t, err)
	defer cache.Stop()

	testutil.Equals(t, []string{"127.0.0.1:11211", "127.0.0.2:11211"}, cache.config.Addresses)
	testutil.Equals(t, defaultMemcachedClientConfig.Timeout, cache.config.Timeout)
	testutil.Equals(t, defaultMemcachedClientConfig.MaxIdleConnections, cache.config.MaxIdleConnections)
	testutil.Equals(t, defaultMemcachedClientConfig.MaxAsyncConcurrency, cache.config.MaxAsyncConcurrency)
	testutil.Equals(t, defaultMemcachedClientConfig.MaxAsyncBufferSize, cache.config.MaxAsyncBufferSize)
	testutil.Equals(t, defaultMemcachedClientConfig.DNSProviderUpdateInterval, cache.config.DNSProviderUpdateInterval)
	testutil.Equals(t, defaultMemcachedClientConfig.MaxGetMultiConcurrency, cache.config.MaxGetMultiConcurrency)
	testutil.Equals(t, defaultMemcachedClientConfig.MaxGetMultiBatchSize, cache.config.MaxGetMultiBatchSize)
	testutil.Equals(t, defaultMemcachedClientConfig.MaxItemSize, cache.config.MaxItemSize)

	// Should instance a memcached client with configured YAML config.
	conf = []byte(`
addresses:
  - 127.0.0.1:11211
  - 127.0.0.2:11211
timeout: 1s
max_idle_connections: 1
max_async_concurrency: 1
max_async_buffer_size: 1
max_get_multi_concurrency: 1
max_item_size: 1MiB
max_get_multi_batch_size: 1
dns_provider_update_interval: 1s
`)
	cache, err = NewMemcachedClient(log.NewNopLogger(), "test", conf, nil)
	testutil.Ok(t, err)
	defer cache.Stop()

	testutil.Equals(t, []string{"127.0.0.1:11211", "127.0.0.2:11211"}, cache.config.Addresses)
	testutil.Equals(t, 1*time.Second, cache.config.Timeout)
	testutil.Equals(t, 1, cache.config.MaxIdleConnections)
	testutil.Equals(t, 1, cache.config.MaxAsyncConcurrency)
	testutil.Equals(t, 1, cache.config.MaxAsyncBufferSize)
	testutil.Equals(t, 1*time.Second, cache.config.DNSProviderUpdateInterval)
	testutil.Equals(t, 1, cache.config.MaxGetMultiConcurrency)
	testutil.Equals(t, 1, cache.config.MaxGetMultiBatchSize)
	testutil.Equals(t, model.Bytes(1024*1024), cache.config.MaxItemSize)
}

func TestMemcachedClient_SetAsync(t *testing.T) {
	ctx := context.Background()
	config := defaultMemcachedClientConfig
	config.Addresses = []string{"127.0.0.1:11211"}
	backendMock := newMemcachedClientBackendMock()

	client, err := prepare(config, backendMock)
	testutil.Ok(t, err)
	defer client.Stop()

	testutil.Ok(t, client.SetAsync(ctx, "key-1", []byte("value-1"), time.Second))
	testutil.Ok(t, client.SetAsync(ctx, "key-2", []byte("value-2"), time.Second))
	testutil.Ok(t, backendMock.waitItems(2))

	actual, err := client.getMultiSingle(ctx, []string{"key-1", "key-2"})
	testutil.Ok(t, err)
	testutil.Equals(t, []byte("value-1"), actual["key-1"].Value)
	testutil.Equals(t, []byte("value-2"), actual["key-2"].Value)

	testutil.Equals(t, 2.0, prom_testutil.ToFloat64(client.operations.WithLabelValues(opSet)))
	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(client.operations.WithLabelValues(opGetMulti)))
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(client.failures.WithLabelValues(opSet, reasonOther)))
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(client.skipped.WithLabelValues(opSet, reasonMaxItemSize)))
}

func TestMemcachedClient_SetAsyncWithCustomMaxItemSize(t *testing.T) {
	ctx := context.Background()
	config := defaultMemcachedClientConfig
	config.Addresses = []string{"127.0.0.1:11211"}
	config.MaxItemSize = model.Bytes(10)
	backendMock := newMemcachedClientBackendMock()

	client, err := prepare(config, backendMock)
	testutil.Ok(t, err)
	defer client.Stop()

	testutil.Ok(t, client.SetAsync(ctx, "key-1", []byte("value-1"), time.Second))
	testutil.Ok(t, client.SetAsync(ctx, "key-2", []byte("value-2-too-long-to-be-stored"), time.Second))
	testutil.Ok(t, backendMock.waitItems(1))

	actual, err := client.getMultiSingle(ctx, []string{"key-1", "key-2"})
	testutil.Ok(t, err)
	testutil.Equals(t, []byte("value-1"), actual["key-1"].Value)
	testutil.Equals(t, (*memcache.Item)(nil), actual["key-2"])

	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(client.operations.WithLabelValues(opSet)))
	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(client.operations.WithLabelValues(opGetMulti)))
	testutil.Equals(t, 0.0, prom_testutil.ToFloat64(client.failures.WithLabelValues(opSet, reasonOther)))
	testutil.Equals(t, 1.0, prom_testutil.ToFloat64(client.skipped.WithLabelValues(opSet, reasonMaxItemSize)))
}

func TestMemcachedClient_GetMulti(t *testing.T) {
	tests := map[string]struct {
		maxBatchSize          int
		maxConcurrency        int
		mockedGetMultiErrors  int
		initialItems          []memcache.Item
		getKeys               []string
		expectedHits          map[string][]byte
		expectedGetMultiCount int
	}{
		"should fetch keys in a single batch if the input keys is <= the max batch size": {
			maxBatchSize:   2,
			maxConcurrency: 5,
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
		"should fetch keys in multiple batches if the input keys is > the max batch size": {
			maxBatchSize:   2,
			maxConcurrency: 5,
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
		"should fetch keys in multiple batches on input keys exact multiple of batch size": {
			maxBatchSize:   2,
			maxConcurrency: 5,
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
		"should fetch keys in multiple batches on input keys exact multiple of batch size with max concurrency disabled (0)": {
			maxBatchSize:   2,
			maxConcurrency: 0,
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
		"should fetch keys in multiple batches on input keys exact multiple of batch size with max concurrency lower than the batches": {
			maxBatchSize:   1,
			maxConcurrency: 1,
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
			expectedGetMultiCount: 4,
		},
		"should fetch keys in a single batch if max batch size is disabled (0)": {
			maxBatchSize:   0,
			maxConcurrency: 5,
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
			expectedGetMultiCount: 1,
		},
		"should fetch keys in a single batch if max batch size is disabled (0) and max concurrency is disabled (0)": {
			maxBatchSize:   0,
			maxConcurrency: 0,
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
			expectedGetMultiCount: 1,
		},
		"should return no hits on all keys missing": {
			maxBatchSize:   2,
			maxConcurrency: 5,
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
		"should return no hits on partial errors while fetching batches and no items found": {
			maxBatchSize:         2,
			maxConcurrency:       5,
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
		"should return no hits on all errors while fetching batches": {
			maxBatchSize:         2,
			maxConcurrency:       5,
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
			config := defaultMemcachedClientConfig
			config.Addresses = []string{"127.0.0.1:11211"}
			config.MaxGetMultiBatchSize = testData.maxBatchSize
			config.MaxGetMultiConcurrency = testData.maxConcurrency

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
			testutil.Equals(t, float64(testData.mockedGetMultiErrors), prom_testutil.ToFloat64(client.failures.WithLabelValues(opGetMulti, reasonOther)))
		})
	}
}

func prepare(config MemcachedClientConfig, backendMock *memcachedClientBackendMock) (*memcachedClient, error) {
	logger := log.NewNopLogger()
	selector := &MemcachedJumpHashSelector{}
	client, err := newMemcachedClient(logger, backendMock, selector, config, nil)

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

func TestMultipleClientsCanUseSameRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()

	config := defaultMemcachedClientConfig
	config.Addresses = []string{"127.0.0.1:11211"}

	client1, err := NewMemcachedClientWithConfig(log.NewNopLogger(), "a", config, reg)
	testutil.Ok(t, err)
	defer client1.Stop()

	client2, err := NewMemcachedClientWithConfig(log.NewNopLogger(), "b", config, reg)
	testutil.Ok(t, err)
	defer client2.Stop()
}
