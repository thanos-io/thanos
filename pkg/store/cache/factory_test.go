// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/cacheutil"
)

func TestIndexCacheMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	commonMetrics := NewCommonMetrics(reg)

	memcached := newMockedMemcachedClient(nil)
	_, err := NewRemoteIndexCache(log.NewNopLogger(), memcached, commonMetrics, reg, memcachedDefaultTTL)
	testutil.Ok(t, err)
	conf := []byte(`
max_size: 10MB
max_item_size: 1MB
`)
	// Make sure that the in memory cache does not register the same metrics of the remote index cache.
	// If so, we should move those metrics to the `commonMetrics`
	_, err = NewInMemoryIndexCache(log.NewNopLogger(), commonMetrics, reg, conf)
	testutil.Ok(t, err)
}

func TestMultiLevelIndexCacheConfig(t *testing.T) {
	for name, config := range map[string]string{
		"no levels":           "type: MULTILEVEL\nconfig: {caches: []}",
		"one level":           "type: MULTILEVEL\nconfig: {caches: [{type: IN-MEMORY}]}",
		"unknown field":       "type: MULTILEVEL\nconfig: {cachez: []}",
		"nested levels":       "type: MULTILEVEL\nconfig: {caches: [{type: IN-MEMORY}, {type: multilevel}]}",
		"unsupported backend": "type: MULTILEVEL\nconfig: {caches: [{type: IN-MEMORY}, {type: UNKNOWN}]}",
		"invalid filter":      "type: MULTILEVEL\nconfig: {caches: [{type: IN-MEMORY}, {type: IN-MEMORY, enabled_items: [Unknown]}]}",
		"invalid memory size": "type: MULTILEVEL\nconfig: {caches: [{type: IN-MEMORY}, {type: IN-MEMORY, config: {max_size: 1KB, max_item_size: 2KB}}]}",
		// A failed later level must stop the first level's remote client workers.
		"remote then invalid": "type: MULTILEVEL\nconfig: {caches: [{type: MEMCACHED, config: {addresses: [localhost:11211]}}, {type: UNKNOWN}]}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewIndexCache(log.NewNopLogger(), []byte(config), prometheus.NewRegistry())
			testutil.NotOk(t, err)
		})
	}

	for _, reg := range []prometheus.Registerer{nil, prometheus.NewRegistry()} {
		_, err := NewIndexCache(log.NewNopLogger(), []byte(`
type: multilevel
config:
  caches:
    - type: in-memory
    - type: IN-MEMORY
`), reg)
		testutil.Ok(t, err)
	}
}

func TestMultiLevelIndexCacheMetricsAndFilters(t *testing.T) {
	reg := prometheus.NewRegistry()
	cache, err := NewIndexCache(log.NewNopLogger(), []byte(`
type: MULTILEVEL
config:
  caches:
    - type: IN-MEMORY
      enabled_items: [Series]
    - type: IN-MEMORY
enabled_items: [Postings, Series]
`), reg)
	testutil.Ok(t, err)
	block := ulid.MustNew(1, nil)
	key := labels.Label{Name: "a", Value: "1"}
	ctx := context.Background()
	cache.StorePostings(block, key, []byte{1}, "tenant")
	for range 2 {
		hits, misses := cache.FetchMultiPostings(ctx, block, []labels.Label{key}, "tenant")
		testutil.Equals(t, []byte{1}, hits[key])
		testutil.Equals(t, 0, len(misses))
	}
	// L1 disables postings, so both reads hit L2. The overall request/hit
	// counters must count each key only once, and promotion must not be counted
	// as another application-level store.
	testutil.Ok(t, promtest.GatherAndCompare(reg, strings.NewReader(`
# HELP thanos_store_index_cache_requests_total Total number of items requests to the cache.
# TYPE thanos_store_index_cache_requests_total counter
thanos_store_index_cache_requests_total{item_type="ExpandedPostings",tenant="default-tenant"} 0
thanos_store_index_cache_requests_total{item_type="Postings",tenant="default-tenant"} 0
thanos_store_index_cache_requests_total{item_type="Postings",tenant="tenant"} 2
thanos_store_index_cache_requests_total{item_type="Series",tenant="default-tenant"} 0
# HELP thanos_store_index_cache_hits_total Total number of items requests to the cache that were a hit.
# TYPE thanos_store_index_cache_hits_total counter
thanos_store_index_cache_hits_total{item_type="ExpandedPostings",tenant="default-tenant"} 0
thanos_store_index_cache_hits_total{item_type="Postings",tenant="default-tenant"} 0
thanos_store_index_cache_hits_total{item_type="Postings",tenant="tenant"} 2
thanos_store_index_cache_hits_total{item_type="Series",tenant="default-tenant"} 0
`), "thanos_store_index_cache_requests_total", "thanos_store_index_cache_hits_total"))

	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")}
	cache.StoreExpandedPostings(block, matchers, []byte{1}, "tenant")
	_, ok := cache.FetchExpandedPostings(ctx, block, matchers, "tenant")
	testutil.Assert(t, !ok)
	metrics, err := reg.Gather()
	testutil.Ok(t, err)
	for _, family := range metrics {
		if family.GetName() == "thanos_store_index_cache_stored_data_size_bytes" {
			testutil.Equals(t, 1, len(family.Metric))
			testutil.Equals(t, uint64(1), family.Metric[0].Histogram.GetSampleCount())
		}
	}
}

func TestMultiLevelIndexCacheTTL(t *testing.T) {
	for _, backend := range []IndexCacheProvider{MEMCACHED} {
		reg := prometheus.NewRegistry()
		factory := &indexCacheFactory{logger: log.NewNopLogger()}
		t.Cleanup(func() {
			for _, client := range factory.clients {
				client.Stop()
			}
		})
		cache, err := factory.newCache(IndexCacheConfig{
			Type: MULTILEVEL,
			TTL:  3 * time.Hour,
			Config: MultiLevelIndexCacheConfig{Caches: []IndexCacheConfig{
				{Type: INMEMORY},
				{Type: backend, Config: cacheutil.MemcachedClientConfig{Addresses: []string{"localhost:11211"}, MaxAsyncConcurrency: 1, DNSProviderUpdateInterval: time.Minute}},
				{Type: backend, TTL: time.Hour, Config: cacheutil.MemcachedClientConfig{Addresses: []string{"localhost:11211"}, MaxAsyncConcurrency: 1, DNSProviderUpdateInterval: time.Minute}},
			}},
		}, NewCommonMetrics(reg), reg)
		testutil.Ok(t, err)
		levels := cache.(*TracingIndexCache).cache.(*multiLevelIndexCache).caches
		testutil.Equals(t, 3*time.Hour, levels[1].(*TracingIndexCache).cache.(*RemoteIndexCache).ttl)
		testutil.Equals(t, time.Hour, levels[2].(*TracingIndexCache).cache.(*RemoteIndexCache).ttl)
	}
}
