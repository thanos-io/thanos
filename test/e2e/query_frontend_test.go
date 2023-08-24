// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/efficientgo/e2e/monitoring/matchers"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQFEEngineExplanation(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("qfe-opts")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "1", e2ethanos.DefaultPromConfig("test", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q := e2ethanos.NewQuerierBuilder(e, "1", sidecar.InternalEndpoint("grpc")).Init()
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

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	var queriesBeforeUp = 0
	testutil.Ok(t, runutil.RetryWithLog(e2e.NewLogger(os.Stderr), 5*time.Second, ctx.Done(), func() error {
		queriesBeforeUp++
		_, _, err := simpleInstantQuery(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{}, 1)
		return err
	}))

	t.Logf("queried %d times before prom is up", queriesBeforeUp)

	t.Run("passes query to Thanos engine", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
			Engine:      "thanos",
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "test",
				"replica":    "0",
			},
		})

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.GreaterOrEqual(1), []string{"thanos_engine_queries_total"}, e2emon.WaitMissingMetrics(), e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "fallback", "false"))))

	})

	now := time.Now()
	t.Run("passes range query to Thanos engine and returns explanation", func(t *testing.T) {
		explanation := rangeQuery(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance,
			timestamp.FromTime(now.Add(-5*time.Minute)),
			timestamp.FromTime(now), 1, promclient.QueryOptions{
				Explain:     true,
				Engine:      "thanos",
				Deduplicate: true,
			}, func(res model.Matrix) error {
				if res.Len() == 0 {
					return fmt.Errorf("expected results")
				}
				return nil
			})
		testutil.Assert(t, explanation != nil, "expected to have an explanation")
		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.GreaterOrEqual(2), []string{"thanos_engine_queries_total"}, e2emon.WaitMissingMetrics(), e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "fallback", "false"))))
	})

	t.Run("passes query to Prometheus engine", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
			Engine:      "prometheus",
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "test",
				"replica":    "0",
			},
		})

		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.GreaterOrEqual(1), []string{"thanos_engine_queries_total"}, e2emon.WaitMissingMetrics(), e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "fallback", "false"))))
		testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.GreaterOrEqual(float64(queriesBeforeUp)+2), []string{"thanos_query_concurrent_gate_queries_total"}, e2emon.WaitMissingMetrics()))

	})

	t.Run("explanation works with instant query", func(t *testing.T) {
		_, explanation := instantQuery(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: true,
			Engine:      "thanos",
			Explain:     true,
		}, 1)
		testutil.Assert(t, explanation != nil, "expected to have an explanation")
	})

	t.Run("does not return explanation if not needed", func(t *testing.T) {
		_, explanation := instantQuery(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
			Engine:      "thanos",
			Explain:     false,
		}, 1)
		testutil.Assert(t, explanation == nil, "expected to have no explanation")
	})
}

func TestQueryFrontend(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("query-frontend")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	now := time.Now()

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "1", e2ethanos.DefaultPromConfig("test", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q := e2ethanos.NewQuerierBuilder(e, "1", sidecar.InternalEndpoint("grpc")).Init()
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

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"replica":    "0",
		},
	})

	vals, err := q.SumMetrics([]string{"http_requests_total"})
	e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query"))

	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(vals))
	queryTimes := vals[0]

	t.Run("query frontend works for instant query", func(t *testing.T) {
		queryAndAssertSeries(t, ctx, queryFrontend.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
			Deduplicate: false,
		}, []model.Metric{
			{
				"job":        "myself",
				"prometheus": "test",
				"replica":    "0",
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
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
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
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(time.Hour)),
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
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_misses_total"))

		// Query is only 2h so it won't be split.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(2), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "query_range"))),
		)

		// One more request is needed in order to satisfy the req range.
		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(2),
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
			timestamp.FromTime(now.Add(-time.Hour)),
			timestamp.FromTime(now.Add(24*time.Hour)),
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
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "cortex_cache_fetched_keys_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "cortex_cache_hits_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_misses_total"))

		// Query is 25h so it will be split to 2 requests.
		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(4), []string{"thanos_frontend_split_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "tripperware", "query_range"))),
		)

		testutil.Ok(t, q.WaitSumMetricsWithOptions(
			e2emon.Equals(4),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range"))),
		)
	})

	t.Run("query frontend splitting works for labels names API", func(t *testing.T) {
		// LabelNames and LabelValues API should still work via query frontend.
		labelNames(t, ctx, queryFrontend.Endpoint("http"), nil, timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
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

		labelNames(t, ctx, queryFrontend.Endpoint("http"), nil, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
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
		labelValues(t, ctx, queryFrontend.Endpoint("http"), "instance", nil, timestamp.FromTime(now.Add(-time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
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

		labelValues(t, ctx, queryFrontend.Endpoint("http"), "instance", nil, timestamp.FromTime(now.Add(-24*time.Hour)), timestamp.FromTime(now.Add(time.Hour)), func(res []string) bool {
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

	now := time.Now()

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "1", e2ethanos.DefaultPromConfig("test", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	q := e2ethanos.NewQuerierBuilder(e, "1", sidecar.InternalEndpoint("grpc")).Init()
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

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_memcache_client_servers"))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, q.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"replica":    "0",
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
		timestamp.FromTime(now.Add(-time.Hour)),
		timestamp.FromTime(now.Add(time.Hour)),
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
		timestamp.FromTime(now.Add(-time.Hour)),
		timestamp.FromTime(now.Add(time.Hour)),
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

	promConfig := e2ethanos.DefaultPromConfig("p1", 0, "", "")
	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "p1", promConfig, "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")

	now := model.Now()
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

	startTime := now.Time().Add(-1 * time.Hour)
	endTime := now.Time().Add(1 * time.Hour)
	_, err = e2eutil.CreateBlock(ctx, prom.Dir(), timeSeries, 20, timestamp.FromTime(startTime), timestamp.FromTime(endTime), nil, 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	stores := []string{sidecar.InternalEndpoint("grpc")}
	q1 := e2ethanos.NewQuerierBuilder(e, "q1", stores...).Init()
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

	var resultWithoutSharding model.Matrix
	rangeQuery(t, ctx, q1.Endpoint("http"), qryFunc, timestamp.FromTime(startTime), timestamp.FromTime(endTime), 30, queryOpts, func(res model.Matrix) error {
		resultWithoutSharding = res
		return nil
	})
	var resultWithSharding model.Matrix
	rangeQuery(t, ctx, qfe.Endpoint("http"), qryFunc, timestamp.FromTime(startTime), timestamp.FromTime(endTime), 30, queryOpts, func(res model.Matrix) error {
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

	now := time.Now()

	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "1", e2ethanos.DefaultPromConfig("test", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	querier := e2ethanos.NewQuerierBuilder(e, "1", sidecar.InternalEndpoint("grpc")).Init()
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

	testutil.Ok(t, querier.WaitSumMetricsWithOptions(e2emon.Equals(1), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), e2ethanos.QueryUpWithoutInstance, time.Now, promclient.QueryOptions{
		Deduplicate: false,
	}, []model.Metric{
		{
			"job":        "myself",
			"prometheus": "test",
			"replica":    "0",
		},
	})

	// -- test starts here --
	rangeQuery(
		t,
		ctx,
		queryFrontend.Endpoint("http"),
		e2ethanos.QueryUpWithoutInstance,
		timestamp.FromTime(now.Add(-time.Hour)),
		timestamp.FromTime(now.Add(time.Hour)),
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
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "cortex_cache_fetched_keys_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(0), "cortex_cache_hits_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_added_new_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "querier_cache_added_total"))
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "querier_cache_misses_total"))

	// Query interval is 2 hours, which is greater than min-slit-interval, query will be broken down into 4 parts
	// + rest (of interval)
	testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(5), "thanos_frontend_split_queries_total"))

	testutil.Ok(t, querier.WaitSumMetricsWithOptions(
		e2emon.Equals(5),
		[]string{"http_requests_total"},
		e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range")),
	))
}

func TestInstantQueryShardingWithRandomData(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("query-sharding")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	promConfig := e2ethanos.DefaultPromConfig("p1", 0, "", "")
	prom, sidecar := e2ethanos.NewPrometheusWithSidecar(e, "p1", promConfig, "", e2ethanos.DefaultPrometheusImage(), "", "remote-write-receiver")

	now := model.Now()
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

	startTime := now.Time().Add(-1 * time.Hour)
	endTime := now.Time().Add(1 * time.Hour)
	_, err = e2eutil.CreateBlock(ctx, prom.Dir(), timeSeries, 20, timestamp.FromTime(startTime), timestamp.FromTime(endTime), nil, 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom, sidecar))

	stores := []string{sidecar.InternalEndpoint("grpc")}
	q1 := e2ethanos.NewQuerierBuilder(e, "q1", stores...).Init()
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
			resultWithoutSharding, _ := instantQuery(t, ctx, q1.Endpoint("http"), tc.qryFunc, func() time.Time {
				return now.Time()
			}, queryOpts, tc.expectedSeries)
			resultWithSharding, _ := instantQuery(t, ctx, qfe.Endpoint("http"), tc.qryFunc, func() time.Time {
				return now.Time()
			}, queryOpts, tc.expectedSeries)
			testutil.Equals(t, resultWithoutSharding, resultWithSharding)
		})
	}
}
