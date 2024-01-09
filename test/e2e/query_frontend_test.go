// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/efficientgo/e2e/monitoring/matchers"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"

	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQueryFrontend(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("query-frontend")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// Predefined Timestamp
	predefTimestamp := time.Date(2023, time.December, 22, 12, 0, 0, 0, time.UTC)

	i := e2ethanos.NewReceiveBuilder(e, "ingestor-rw").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	q := e2ethanos.NewQuerierBuilder(e, "1", i.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}

	cfg := queryfrontend.Config{}
	queryFrontend := e2ethanos.NewQueryFrontend(e, "1", "http://"+q.InternalEndpoint("http"), cfg, inMemoryCacheConfig)
	testutil.Ok(t, e2e.StartAndWaitReady(queryFrontend))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	// Writing a custom Timeseries into the receiver
	testutil.Ok(t, remoteWrite(ctx, []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "up"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "myself"},
			{Name: "prometheus", Value: "test"},
			{Name: "replica", Value: "0"},
		},
		Samples: []prompb.Sample{
			{Value: float64(1), Timestamp: timestamp.FromTime(predefTimestamp)},
		}}},
		i.Endpoint("remote-write"),
	))

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, func() time.Time { return predefTimestamp }, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"receive":    "receive-ingestor-rw",
			"replica":    "0",
			"tenant_id":  "default-tenant",
		},
	})

	vals, err := q.SumMetrics([]string{"http_requests_total"})
	e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query"))

	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(vals))
	queryTimes := vals[0]

	t.Run("query frontend works for instant query", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, func() time.Time { return predefTimestamp }, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "test",
				"receive":    "receive-ingestor-rw",
				"replica":    "0",
				"tenant_id":  "default-tenant",
			},
		})

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query")),
		))

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(queryTimes+1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query")),
		))
	})

	t.Run("query frontend works for range query and it can cache results", func(t *testing.T) {
		rangeQuery(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			e2ethanos.QueryUpWithoutInstance,
			timestamp.FromTime(predefTimestamp.Add(-time.Hour)),
			timestamp.FromTime(predefTimestamp.Add(time.Hour)),
			14,
			promclient.QueryOptions{
				Deduplicate: true,
			},
			func(res model.Matrix) error {
				if len(res) == 0 {
					return errors.Errorf("expected some results, got nothing")
				}
				return nil
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range")),
		))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_cache_fetched_keys_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(0), "cortex_cache_hits_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_misses_total"))

		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "thanos_frontend_split_queries_total"))

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range")),
		))
	})

	t.Run("same range query, cache hit.", func(t *testing.T) {
		// Run the same range query again, the result can be retrieved from cache directly.
		rangeQuery(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			e2ethanos.QueryUpWithoutInstance,
			timestamp.FromTime(predefTimestamp.Add(-time.Hour)),
			timestamp.FromTime(predefTimestamp.Add(time.Hour)),
			14,
			promclient.QueryOptions{
				Deduplicate: true,
			},
			func(res model.Matrix) error {
				if len(res) == 0 {
					return errors.Errorf("expected some results, got nothing")
				}
				return nil
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "cortex_cache_fetched_keys_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_cache_hits_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_misses_total"))

		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(2), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "query_range"))),
		)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range"))),
		)
	})

	t.Run("range query > 24h should be split", func(t *testing.T) {
		rangeQuery(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			e2ethanos.QueryUpWithoutInstance,
			timestamp.FromTime(predefTimestamp.Add(-time.Hour)),
			timestamp.FromTime(predefTimestamp.Add(24*time.Hour)),
			14,
			promclient.QueryOptions{
				Deduplicate: true,
			},
			func(res model.Matrix) error {
				if len(res) == 0 {
					return errors.Errorf("expected some results, got nothing")
				}
				return nil
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(3),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(4), "cortex_cache_fetched_keys_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "cortex_cache_hits_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(4), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_misses_total"))

		// Query is 25h so it will be split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(4), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "query_range"))),
		)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(3),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range"))),
		)
	})

	t.Run("query frontend splitting works for labels names API", func(t *testing.T) {
		// LabelNames and LabelValues API should still work via query frontend.
		labelNames(t, ctx, queryFrontend.Endpoint("http"), nil, timestamp.FromTime(predefTimestamp.Add(-time.Hour)), timestamp.FromTime(predefTimestamp.Add(time.Hour)), func(res []string) bool {
			return len(res) > 0
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_names"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_names"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(1), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)

		labelNames(t, ctx, queryFrontend.Endpoint("http"), nil, timestamp.FromTime(predefTimestamp.Add(-24*time.Hour)), timestamp.FromTime(predefTimestamp.Add(time.Hour)), func(res []string) bool {
			return len(res) > 0
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(3),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_names"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_names"))),
		)
		// Query is 25h so split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(3), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)
	})

	t.Run("query frontend splitting works for labels values API", func(t *testing.T) {
		labelValues(t, ctx, queryFrontend.Endpoint("http"), "instance", nil, timestamp.FromTime(predefTimestamp.Add(-time.Hour)), timestamp.FromTime(predefTimestamp.Add(time.Hour)), func(res []string) bool {
			return len(res) == 1 && res[0] == "localhost:9090"
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_values"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_values"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(4), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)

		labelValues(t, ctx, queryFrontend.Endpoint("http"), "instance", nil, timestamp.FromTime(predefTimestamp.Add(-24*time.Hour)), timestamp.FromTime(predefTimestamp.Add(time.Hour)), func(res []string) bool {
			return len(res) == 1 && res[0] == "localhost:9090"
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(3),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_values"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_values"))),
		)
		// Query is 25h so split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(6), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)
	})

	t.Run("query frontend splitting works for series API", func(t *testing.T) {
		series(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
			timestamp.FromTime(predefTimestamp.Add(-time.Hour)),
			timestamp.FromTime(predefTimestamp.Add(time.Hour)),
			func(res []map[string]string) bool {
				if len(res) != 1 {
					return false
				}

				return reflect.DeepEqual(res[0], map[string]string{
					"__name__":   "up",
					"instance":   "localhost:9090",
					"job":        "myself",
					"prometheus": "test",
					"receive":    "receive-ingestor-rw",
					"tenant_id":  "default-tenant",
				})
			},
		)
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "series"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "series"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(7), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)

		series(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
			timestamp.FromTime(predefTimestamp.Add(-24*time.Hour)),
			timestamp.FromTime(predefTimestamp.Add(time.Hour)),
			func(res []map[string]string) bool {
				if len(res) != 1 {
					return false
				}

				return reflect.DeepEqual(res[0], map[string]string{
					"__name__":   "up",
					"instance":   "localhost:9090",
					"job":        "myself",
					"prometheus": "test",
					"receive":    "receive-ingestor-rw",
					"tenant_id":  "default-tenant",
				})
			},
		)
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(3),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "series"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "series"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(9), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)
	})
}

func TestQueryFrontendMemcachedCache(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("qf-memcached")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// Predefined timestamp
	predefTimestamp := time.Date(2023, time.December, 22, 12, 0, 0, 0, time.UTC)

	i := e2ethanos.NewReceiveBuilder(e, "ingestor-rw").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	q := e2ethanos.NewQuerierBuilder(e, "1", i.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	memcached := e2ethanos.NewMemcached(e, "1")
	testutil.Ok(t, e2e.StartAndWaitReady(memcached))

	memCachedConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.MEMCACHED,
		Config: queryfrontend.MemcachedResponseCacheConfig{
			Memcached: cacheutil.MemcachedClientConfig{
				Addresses:                 []string{memcached.InternalEndpoint("memcached")},
				MaxIdleConnections:        100,
				MaxAsyncConcurrency:       20,
				MaxGetMultiConcurrency:    100,
				MaxGetMultiBatchSize:      0,
				Timeout:                   time.Minute,
				MaxAsyncBufferSize:        10000,
				DNSProviderUpdateInterval: 10 * time.Second,
			},
		},
	}

	cfg := queryfrontend.Config{}
	queryFrontend := e2ethanos.NewQueryFrontend(e, "1", "http://"+q.InternalEndpoint("http"), cfg, memCachedConfig)
	testutil.Ok(t, e2e.StartAndWaitReady(queryFrontend))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, remoteWrite(ctx, []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "up"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "myself"},
			{Name: "prometheus", Value: "test"},
			{Name: "replica", Value: "0"},
		},
		Samples: []prompb.Sample{
			{Value: float64(1), Timestamp: timestamp.FromTime(predefTimestamp)},
		}}},
		i.Endpoint("remote-write")))

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_memcache_client_servers"))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, func() time.Time { return predefTimestamp }, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"receive":    "receive-ingestor-rw",
			"replica":    "0",
			"tenant_id":  "default-tenant",
		},
	})

	vals, err := q.SumMetrics([]string{"http_requests_total"}, e2emon.WithLabelMatchers(
		matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query")))
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(vals))

	rangeQuery(
		t,
		ctx,
		queryFrontend.Endpoint("http"),
		e2ethanos.QueryUpWithoutInstance,
		timestamp.FromTime(predefTimestamp.Add(-time.Hour)),
		timestamp.FromTime(predefTimestamp.Add(time.Hour)),
		14,
		promclient.QueryOptions{
			Deduplicate: true,
		},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.Errorf("expected some results, got nothing")
			}
			return nil
		},
	)

	testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
		e2emon.Equals(1),
		[]string{"thanos_query_frontend_queries_total"},
		e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
	)

	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_cache_fetched_keys_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(0), "cortex_cache_hits_total"))

	// Query is only 2h so it won't be split.
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "thanos_frontend_split_queries_total"))

	// Run the same range query again, the result can be retrieved from cache directly.
	rangeQuery(
		t,
		ctx,
		queryFrontend.Endpoint("http"),
		e2ethanos.QueryUpWithoutInstance,
		timestamp.FromTime(predefTimestamp.Add(-time.Hour)),
		timestamp.FromTime(predefTimestamp.Add(time.Hour)),
		14,
		promclient.QueryOptions{
			Deduplicate: true,
		},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.Errorf("expected some results, got nothing")
			}
			return nil
		},
	)

	testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
		e2emon.Equals(2),
		[]string{"thanos_query_frontend_queries_total"},
		e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
	)

	// Query is only 2h so it won't be split.
	// If it was split this would be increase by more then 1.
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "thanos_frontend_split_queries_total"))

	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "cortex_cache_fetched_keys_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_cache_hits_total"))
}

func TestRangeQueryShardingWithRandomData(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("rq-sharding")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	i := e2ethanos.NewReceiveBuilder(e, "ingestor-rw").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	predefTimestamp := model.TimeFromUnixNano(time.Date(2023, time.December, 22, 12, 0, 0, 0, time.UTC).UnixNano())

	ctx := context.Background()
	timeSeries := []labels.Labels{
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "1"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "1"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "2"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "2"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "3"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "3"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "4"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "4"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "5"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "5"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "6"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "6"}, {Name: "handler", Value: "/metrics"}},
	}

	// Ensure labels are ordered.
	for _, ts := range timeSeries {
		sort.Slice(ts, func(i, j int) bool {
			return ts[i].Name < ts[j].Name
		})
	}

	samplespb := make([]prompb.TimeSeries, 0, len(timeSeries))
	for _, labels := range timeSeries {
		labelspb := make([]prompb.Label, 0, len(labels))
		for _, label := range labels {
			labelspb = append(labelspb, prompb.Label{
				Name:  string(label.Name),
				Value: string(label.Value),
			})
		}
		samplespb = append(samplespb, prompb.TimeSeries{
			Labels: labelspb,
			Samples: []prompb.Sample{
				{
					Value:     float64(1),
					Timestamp: timestamp.FromTime(predefTimestamp.Time()),
				},
			},
		})
	}

	testutil.Ok(t, remoteWrite(ctx, samplespb, i.Endpoint("remote-write")))

	q1 := e2ethanos.NewQuerierBuilder(e, "q1", i.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q1))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}
	config := queryfrontend.Config{
		QueryRangeConfig: queryfrontend.QueryRangeConfig{
			AlignRangeWithStep: false,
		},
		NumShards: 2,
	}
	qfe := e2ethanos.NewQueryFrontend(e, "query-frontend", "http://"+q1.InternalEndpoint("http"), config, inMemoryCacheConfig)
	testutil.Ok(t, e2e.StartAndWaitReady(qfe))

	qryFunc := func() string { return `sum by (pod) (http_requests_total)` }
	queryOpts := promclient.QueryOptions{Deduplicate: true}

	startTime := timestamp.FromTime(predefTimestamp.Time().Add(-1 * time.Hour))
	endTime := timestamp.FromTime(predefTimestamp.Time().Add(1 * time.Hour))

	var resultWithoutSharding model.Matrix
	rangeQuery(t, ctx, q1.Endpoint("http"), qryFunc, startTime, endTime, 30, queryOpts, func(res model.Matrix) error {
		resultWithoutSharding = res
		return nil
	})
	var resultWithSharding model.Matrix
	rangeQuery(t, ctx, qfe.Endpoint("http"), qryFunc, startTime, endTime, 30, queryOpts, func(res model.Matrix) error {
		resultWithSharding = res
		return nil
	})

	testutil.Equals(t, resultWithoutSharding, resultWithSharding)
}

func TestRangeQueryDynamicHorizontalSharding(t *testing.T) {
	t.Parallel()

	e, err := e2e.New(e2e.WithName("qfe-dyn-sharding"))
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	predefTimestamp := time.Date(2023, time.December, 22, 12, 0, 0, 0, time.UTC)

	i := e2ethanos.NewReceiveBuilder(e, "ingestor-rw").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	querier := e2ethanos.NewQuerierBuilder(e, "1", i.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}

	cfg := queryfrontend.Config{
		QueryRangeConfig: queryfrontend.QueryRangeConfig{
			MinQuerySplitInterval:  time.Hour,
			MaxQuerySplitInterval:  12 * time.Hour,
			HorizontalShards:       4,
			SplitQueriesByInterval: 0,
		},
	}
	queryFrontend := e2ethanos.NewQueryFrontend(e, "1", "http://"+querier.InternalEndpoint("http"), cfg, inMemoryCacheConfig)
	testutil.Ok(t, e2e.StartAndWaitReady(queryFrontend))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, remoteWrite(ctx, []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "up"},
			{Name: "instance", Value: "localhost:9090"},
			{Name: "job", Value: "myself"},
			{Name: "prometheus", Value: "test"},
			{Name: "replica", Value: "0"},
		},
		Samples: []prompb.Sample{
			{Value: float64(1), Timestamp: timestamp.FromTime(predefTimestamp)},
		}}},
		i.Endpoint("remote-write")))

	testutil.Ok(t, querier.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, func() time.Time { return predefTimestamp }, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"receive":    "receive-ingestor-rw",
			"replica":    "0",
			"tenant_id":  "default-tenant",
		},
	})

	// -- test starts here --
	rangeQuery(
		t,
		ctx,
		queryFrontend.Endpoint("http"),
		e2ethanos.QueryUpWithoutInstance,
		timestamp.FromTime(predefTimestamp.Add(-time.Hour)),
		timestamp.FromTime(predefTimestamp.Add(time.Hour)),
		14,
		promclient.QueryOptions{
			Deduplicate: true,
		},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.Errorf("expected some results, got nothing")
			}
			return nil
		},
	)

	testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
		e2emon.Equals(1),
		[]string{"thanos_query_frontend_queries_total"},
		e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range")),
	))

	// make sure that we don't break cortex cache code.
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(4), "cortex_cache_fetched_keys_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(0), "cortex_cache_hits_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_added_new_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(4), "querier_cache_added_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(4), "querier_cache_misses_total"))

	// Query interval is 2 hours, which is greater than min-slit-interval, query will be broken down into 4 parts
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(4), "thanos_frontend_split_queries_total"))

	testutil.Ok(t, querier.WaitSumMetricsWithOptions(
		e2emon.Equals(4),
		[]string{"http_requests_total"},
		e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range")),
	))
}

func TestInstantQueryShardingWithRandomData(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("query-sharding")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	i := e2ethanos.NewReceiveBuilder(e, "ingestor-rw").WithIngestionEnabled().Init()
	testutil.Ok(t, e2e.StartAndWaitReady(i))

	predefTimestamp := model.TimeFromUnixNano(time.Date(2023, time.December, 22, 12, 0, 0, 0, time.UTC).UnixNano())
	ctx := context.Background()

	timeSeries := []labels.Labels{
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "1"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "1"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "2"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "2"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "3"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "3"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "4"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "4"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "5"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "5"}, {Name: "handler", Value: "/metrics"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "6"}, {Name: "handler", Value: "/"}},
		{{Name: labels.MetricName, Value: "http_requests_total"}, {Name: "pod", Value: "6"}, {Name: "handler", Value: "/metrics"}},
	}

	// Ensure labels are ordered.
	for _, ts := range timeSeries {
		sort.Slice(ts, func(i, j int) bool {
			return ts[i].Name < ts[j].Name
		})
	}

	samplespb := make([]prompb.TimeSeries, 0, len(timeSeries))
	for _, labels := range timeSeries {
		labelspb := make([]prompb.Label, 0, len(labels))
		for _, label := range labels {
			labelspb = append(labelspb, prompb.Label{
				Name:  string(label.Name),
				Value: string(label.Value),
			})
		}
		samplespb = append(samplespb, prompb.TimeSeries{
			Labels: labelspb,
			Samples: []prompb.Sample{
				{
					Value:     float64(1),
					Timestamp: timestamp.FromTime(predefTimestamp.Time()),
				},
			},
		})
	}

	testutil.Ok(t, remoteWrite(ctx, samplespb, i.Endpoint("remote-write")))

	q1 := e2ethanos.NewQuerierBuilder(e, "q1", i.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q1))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}
	config := queryfrontend.Config{
		QueryRangeConfig: queryfrontend.QueryRangeConfig{
			AlignRangeWithStep: false,
		},
		NumShards: 2,
	}
	qfe := e2ethanos.NewQueryFrontend(e, "query-frontend", "http://"+q1.InternalEndpoint("http"), config, inMemoryCacheConfig)
	testutil.Ok(t, e2e.StartAndWaitReady(qfe))

	queryOpts := promclient.QueryOptions{Deduplicate: true}
	for _, tc := range []struct {
		name           string
		qryFunc        func() string
		expectedSeries int
	}{
		{
			name:           "aggregation",
			qryFunc:        func() string { return `sum(http_requests_total)` },
			expectedSeries: 1,
		},
		{
			name:           "outer aggregation with no grouping",
			qryFunc:        func() string { return `count(sum by (pod) (http_requests_total))` },
			expectedSeries: 1,
		},
		{
			name:           "scalar",
			qryFunc:        func() string { return `1 + 1` },
			expectedSeries: 1,
		},
		{
			name:           "binary expression",
			qryFunc:        func() string { return `http_requests_total{pod="1"} / http_requests_total` },
			expectedSeries: 2,
		},
		{
			name:           "binary expression with constant",
			qryFunc:        func() string { return `http_requests_total / 2` },
			expectedSeries: 12,
		},
		{
			name:           "vector selector",
			qryFunc:        func() string { return `http_requests_total` },
			expectedSeries: 12,
		},
		{
			name:           "aggregation with grouping",
			qryFunc:        func() string { return `sum by (pod) (http_requests_total)` },
			expectedSeries: 6,
		},
		{
			name:           "aggregate without grouping",
			qryFunc:        func() string { return `sum without (pod) (http_requests_total)` },
			expectedSeries: 2,
		},
		{
			name:           "multiple aggregations with grouping",
			qryFunc:        func() string { return `max by (handler) (sum(http_requests_total) by (pod, handler))` },
			expectedSeries: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resultWithoutSharding := instantQuery(t, ctx, q1.Endpoint("http"), tc.qryFunc, func() time.Time {
				return predefTimestamp.Time()
			}, queryOpts, tc.expectedSeries)
			resultWithSharding := instantQuery(t, ctx, qfe.Endpoint("http"), tc.qryFunc, func() time.Time {
				return predefTimestamp.Time()
			}, queryOpts, tc.expectedSeries)
			testutil.Equals(t, resultWithoutSharding, resultWithSharding)
		})
	}
}

func TestQueryFrontendTenantForward(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		customTenantHeaderName string
		tenantName             string
	}{
		{
			name:                   "default tenant header name with a tenant name",
			customTenantHeaderName: tenancy.DefaultTenantHeader,
			tenantName:             "test-tenant",
		},
		{
			name:                   "default tenant header name without a tenant name",
			customTenantHeaderName: tenancy.DefaultTenantHeader,
		},
		{
			name:                   "custom tenant header name with a tenant name",
			customTenantHeaderName: "X-Foobar-Tenant",
			tenantName:             "test-tenant",
		},
		{
			name:                   "custom tenant header name without a tenant name",
			customTenantHeaderName: "X-Foobar-Tenant",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.tenantName == "" {
				tc.tenantName = tenancy.DefaultTenant
			}
			// Use a shorthash of tc.name as e2e env name because the random name generator is having a collision for
			// some reason.
			e2ename := fmt.Sprintf("%x", sha256.Sum256([]byte(tc.name)))[:8]
			e, err := e2e.New(e2e.WithName(e2ename))
			testutil.Ok(t, err)
			t.Cleanup(e2ethanos.CleanScenario(t, e))

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
				// The tenant header present in the outgoing request should be the default tenant header.
				testutil.Equals(t, tc.tenantName, r.Header.Get(tenancy.DefaultTenantHeader))

				// In case the query frontend is configured with a custom tenant header name, verify such header
				// is not present in the outgoing request.
				if tc.customTenantHeaderName != tenancy.DefaultTenantHeader {
					testutil.Equals(t, "", r.Header.Get(tc.customTenantHeaderName))
				}

				// Verify the outgoing request will keep the X-Scope-OrgID header for compatibility with Cortex.
				testutil.Equals(t, tc.tenantName, r.Header.Get("X-Scope-OrgID"))
			}))
			t.Cleanup(ts.Close)
			tsPort := urlParse(t, ts.URL).Port()

			inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
				Type: queryfrontend.INMEMORY,
				Config: queryfrontend.InMemoryResponseCacheConfig{
					MaxSizeItems: 1000,
					Validity:     time.Hour,
				},
			}
			queryFrontendConfig := queryfrontend.Config{
				TenantHeader: tc.customTenantHeaderName,
			}
			queryFrontend := e2ethanos.NewQueryFrontend(
				e,
				"qfe",
				fmt.Sprintf("http://%s:%s", e.HostAddr(), tsPort),
				queryFrontendConfig,
				inMemoryCacheConfig,
			)
			testutil.Ok(t, e2e.StartAndWaitReady(queryFrontend))

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			t.Cleanup(cancel)

			promClient, err := api.NewClient(api.Config{
				Address: "http://" + queryFrontend.Endpoint("http"),
				RoundTripper: tenantRoundTripper{
					tenant: tc.tenantName,
					rt:     http.DefaultTransport,
				},
			})
			testutil.Ok(t, err)
			v1api := v1.NewAPI(promClient)

			r := v1.Range{
				Start: time.Now().Add(-time.Hour),
				End:   time.Now(),
				Step:  time.Minute,
			}

			_, _, _ = v1api.QueryRange(ctx, "rate(prometheus_tsdb_head_samples_appended_total[5m])", r)
			_, _, _ = v1api.Query(ctx, "rate(prometheus_tsdb_head_samples_appended_total[5m])", time.Now())
		})
	}
}

type tenantRoundTripper struct {
	tenant       string
	tenantHeader string
	rt           http.RoundTripper
}

var _ http.RoundTripper = tenantRoundTripper{}

// RoundTrip implements the http.RoundTripper interface.
func (u tenantRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if u.tenantHeader == "" {
		u.tenantHeader = tenancy.DefaultTenantHeader
	}
	r.Header.Set(u.tenantHeader, u.tenant)
	return u.rt.RoundTrip(r)
}

func TestTenantQFEHTTPMetrics(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("qfetenantmetrics")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// scrape the local prometheus, and our querier metrics
	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "alone", e2ethanos.DefaultPromConfig("prom-alone", 0, "", "", e2ethanos.LocalPrometheusTarget, "qfetenantmetrics-querier-1:8080"), "", e2ethanos.DefaultPrometheusImage(), "")

	q := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}

	cfg := queryfrontend.Config{}
	queryFrontend := e2ethanos.NewQueryFrontend(e, "1", "http://"+q.InternalEndpoint("http"), cfg, inMemoryCacheConfig)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, queryFrontend))

	// Query once with default-tenant to ensure everything is ready
	// for the following requests
	instantQuery(t, ctx, queryFrontend.Endpoint("http"), func() string {
		return "prometheus_api_remote_read_queries"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
	}, 1)
	testutil.Ok(t, err)

	// Query a few times with tenant 1
	instantQuery(t, ctx, queryFrontend.Endpoint("http"), func() string {
		return "prometheus_api_remote_read_queries"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
		HTTPHeaders: map[string][]string{"thanos-tenant": {"test-tenant-1"}},
	}, 1)
	testutil.Ok(t, err)

	instantQuery(t, ctx, queryFrontend.Endpoint("http"), func() string {
		return "go_goroutines"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
		HTTPHeaders: map[string][]string{"thanos-tenant": {"test-tenant-1"}},
	}, 2)
	testutil.Ok(t, err)

	instantQuery(t, ctx, queryFrontend.Endpoint("http"), func() string {
		return "go_memstats_frees_total"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
		HTTPHeaders: map[string][]string{"thanos-tenant": {"test-tenant-1"}},
	}, 2)
	testutil.Ok(t, err)

	// query just once with tenant-2
	instantQuery(t, ctx, queryFrontend.Endpoint("http"), func() string {
		return "go_memstats_heap_alloc_bytes"
	}, time.Now, promclient.QueryOptions{
		Deduplicate: true,
		HTTPHeaders: map[string][]string{"thanos-tenant": {"test-tenant-2"}},
	}, 2)
	testutil.Ok(t, err)

	// check that http metrics for tenant-1 matches 3 requests, both for querier and query frontend
	tenant1Matcher, err := matchers.NewMatcher(matchers.MatchEqual, "tenant", "test-tenant-1")
	testutil.Ok(t, err)
	testutil.Ok(t, q.WaitSumMetricsWithOptions(
		e2emon.GreaterOrEqual(3),
		[]string{"http_requests_total"}, e2emon.WithLabelMatchers(
			tenant1Matcher,
		),
		e2emon.WaitMissingMetrics(),
	))
	testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
		e2emon.GreaterOrEqual(3),
		[]string{"http_requests_total"}, e2emon.WithLabelMatchers(
			tenant1Matcher,
		),
		e2emon.WaitMissingMetrics(),
	))

	// check that http metrics for tenant-2 matches 1 requests, both for querier and query frontend
	tenant2Matcher, err := matchers.NewMatcher(matchers.MatchEqual, "tenant", "test-tenant-2")
	testutil.Ok(t, err)
	testutil.Ok(t, q.WaitSumMetricsWithOptions(
		e2emon.Equals(1),
		[]string{"http_requests_total"}, e2emon.WithLabelMatchers(
			tenant2Matcher,
		),
		e2emon.WaitMissingMetrics(),
	))
	testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
		e2emon.Equals(1),
		[]string{"http_requests_total"}, e2emon.WithLabelMatchers(
			tenant2Matcher,
		),
		e2emon.WaitMissingMetrics(),
	))
}
