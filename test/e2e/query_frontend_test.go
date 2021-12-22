// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	"github.com/efficientgo/e2e/matchers"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQueryFrontend(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_frontend")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	now := time.Now()

	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(e, "1", defaultPromConfig("test", 0, "", ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", sidecar.InternalEndpoint("grpc")).Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}

	queryFrontend, err := e2ethanos.NewQueryFrontend(e, "1", "http://"+q.InternalEndpoint("http"), inMemoryCacheConfig)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(queryFrontend))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics()))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), queryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"replica":    "0",
		},
	})

	vals, err := q.SumMetrics([]string{"http_requests_total"})
	e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query"))

	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(vals))
	queryTimes := vals[0]

	t.Run("query frontend works for instant query", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, queryFrontend.Endpoint("http"), queryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "test",
				"replica":    "0",
			},
		})

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query")),
		))

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(queryTimes+1),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query")),
		))
	})

	t.Run("query frontend works for range query and it can cache results", func(t *testing.T) {
		rangeQuery(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			queryUpWithoutInstance,
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
			14,
			promclient.QueryOptions{},
			func(res model.Matrix) error {
				if len(res) == 0 {
					return errors.Errorf("expected some results, got nothing")
				}
				return nil
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range")),
		))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_cache_fetched_keys"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(0), "cortex_cache_hits"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_misses_total"))

		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "thanos_frontend_split_queries_total"))

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range")),
		))
	})

	t.Run("same range query, cache hit.", func(t *testing.T) {
		// Run the same range query again, the result can be retrieved from cache directly.
		rangeQuery(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			queryUpWithoutInstance,
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
			14,
			promclient.QueryOptions{},
			func(res model.Matrix) error {
				if len(res) == 0 {
					return errors.Errorf("expected some results, got nothing")
				}
				return nil
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "cortex_cache_fetched_keys"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_cache_hits"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_misses_total"))

		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(2), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "query_range"))),
		)

		// One more request is needed in order to satisfy the req range.
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(2),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range"))),
		)
	})

	t.Run("range query > 24h should be split", func(t *testing.T) {
		rangeQuery(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			queryUpWithoutInstance,
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(24*time.Hour)),
			14,
			promclient.QueryOptions{},
			func(res model.Matrix) error {
				if len(res) == 0 {
					return errors.Errorf("expected some results, got nothing")
				}
				return nil
			},
		)

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(3),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(3), "cortex_cache_fetched_keys"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(2), "cortex_cache_hits"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(3), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(3), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "querier_cache_misses_total"))

		// Query is 25h so it will be split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(4), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "query_range"))),
		)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(4),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range"))),
		)
	})

	t.Run("query frontend splitting works for labels names API", func(t *testing.T) {
		// LabelNames and LabelValues API should still work via query frontend.
		labelNames(t, ctx, queryFrontend.Endpoint("http"), nil, timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) > 0
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_names"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_names"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)

		labelNames(t, ctx, queryFrontend.Endpoint("http"), nil, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) > 0
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(3),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_names"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_names"))),
		)
		// Query is 25h so split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(3), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)
	})

	t.Run("query frontend splitting works for labels values API", func(t *testing.T) {
		labelValues(t, ctx, queryFrontend.Endpoint("http"), "instance", nil, timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) == 1 && res[0] == "localhost:9090"
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_values"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_values"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(4), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)

		labelValues(t, ctx, queryFrontend.Endpoint("http"), "instance", nil, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
			return len(res) == 1 && res[0] == "localhost:9090"
		})
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(3),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "label_values"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "label_values"))),
		)
		// Query is 25h so split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(6), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)
	})

	t.Run("query frontend splitting works for series API", func(t *testing.T) {
		series(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
			func(res []map[string]string) bool {
				if len(res) != 1 {
					return false
				}

				return reflect.DeepEqual(res[0], map[string]string{
					"__name__":   "up",
					"instance":   "localhost:9090",
					"job":        "myself",
					"prometheus": "test",
				})
			},
		)
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "series"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "series"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(7), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)

		series(
			t,
			ctx,
			queryFrontend.Endpoint("http"),
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
			timestamp.FromTime(now.Add(-24*time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
			func(res []map[string]string) bool {
				if len(res) != 1 {
					return false
				}

				return reflect.DeepEqual(res[0], map[string]string{
					"__name__":   "up",
					"instance":   "localhost:9090",
					"job":        "myself",
					"prometheus": "test",
				})
			},
		)
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2e.Equals(3),
			[]string{"http_requests_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "series"))),
		)
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(2),
			[]string{"thanos_query_frontend_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "series"))),
		)
		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Equals(9), []string{"thanos_frontend_split_queries_total"},
			e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "labels"))),
		)
	})
}

// (Wiard) Query splitting behaviour is somewhat non-deterministric, due to the fact that we use a literal timestamp.FromTime(now.Add(-time.Hour).
// If the start and end time falls between a new day (passed 23:59), it will see this as '24h' (default --query-range.split-interval).
// My guess is that certain behavior has an underlying bug or that I'm a lost in the complexity.
// It will need to double the testing values if the hour is "1" for thanos_frontend_split_queries_total and cortex_cache_*
// However, only thanos_frontend_split_queries_total will need to double its value on hour 0. cortex_cache_*'s do not.
// This test needs to be fixed properly and possibly requires more debugging, especially on the non-logical behaviour (or more documentation is required).
// https://github.com/thanos-io/thanos/issues/3570 - however, for now it won't fail tests if ran between hour 0 and 1.
func doubleValueIfSplit(n float64, whenPartialOvernight bool) float64 {

	currentTime := time.Now()

	// Some metrics DO NOT increase if the query range is from 'now -1hour' if it the hour is "0". Then just return the expected value.
	if whenPartialOvernight && currentTime.Hour() == 0 {
		return n
	}

	// For the rest, if it is hour "0" or "1", the query is split, and we should expect double it's values.
	if currentTime.Hour() == 1 || currentTime.Hour() == 0 {
		return n * 2
	}

	return n
}

func TestQueryFrontendMemcachedCache(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_query_frontend_memcached")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	now := time.Now()

	prom, sidecar, err := e2ethanos.NewPrometheusWithSidecar(e, "1", defaultPromConfig("test", 0, "", ""), "", e2ethanos.DefaultPrometheusImage())
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q, err := e2ethanos.NewQuerierBuilder(e, "1", sidecar.InternalEndpoint("grpc")).Build()
	testutil.Ok(t, err)
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

	queryFrontend, err := e2ethanos.NewQueryFrontend(e, "1", "http://"+q.InternalEndpoint("http"), memCachedConfig)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(queryFrontend))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2e.WaitMissingMetrics()))

	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_memcache_client_servers"))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), queryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"replica":    "0",
		},
	})

	vals, err := q.SumMetrics([]string{"http_requests_total"}, e2e.WithLabelMatchers(
		matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query")))
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(vals))

	rangeQuery(
		t,
		ctx,
		queryFrontend.Endpoint("http"),
		queryUpWithoutInstance,
		timestamp.FromTime(now.Add(-time.Hour)),
		timestamp.FromTime(now.Add(time.Hour)),
		14,
		promclient.QueryOptions{},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.Errorf("expected some results, got nothing")
			}
			return nil
		},
	)

	testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
		e2e.Equals(1),
		[]string{"thanos_query_frontend_queries_total"},
		e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
	)

	// https://github.com/thanos-io/thanos/issues/3570
	// "The query splitting test case is flaky because we are using the current timestamp to send queries, which makes the splitting behavior non-deterministic"
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(doubleValueIfSplit(1, true)), "cortex_cache_fetched_keys"))

	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(0), "cortex_cache_hits"))

	// https://github.com/thanos-io/thanos/issues/3570
	// "The query splitting test case is flaky because we are using the current timestamp to send queries, which makes the splitting behavior non-deterministic"
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(doubleValueIfSplit(1, false)), "thanos_frontend_split_queries_total"))

	// Run the same range query again, the result can be retrieved from cache directly.
	rangeQuery(
		t,
		ctx,
		queryFrontend.Endpoint("http"),
		queryUpWithoutInstance,
		timestamp.FromTime(now.Add(-time.Hour)),
		timestamp.FromTime(now.Add(time.Hour)),
		14,
		promclient.QueryOptions{},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.Errorf("expected some results, got nothing")
			}
			return nil
		},
	)

	testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
		e2e.Equals(2),
		[]string{"thanos_query_frontend_queries_total"},
		e2e.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query_range"))),
	)

	// https://github.com/thanos-io/thanos/issues/3570
	// "The query splitting test case is flaky because we are using the current timestamp to send queries, which makes the splitting behavior non-deterministic"
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(doubleValueIfSplit(2, false)), "thanos_frontend_split_queries_total"))

	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(doubleValueIfSplit(2, true)), "cortex_cache_fetched_keys"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2e.Equals(doubleValueIfSplit(1, true)), "cortex_cache_hits"))
}
