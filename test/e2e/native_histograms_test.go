// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/efficientgo/e2e/monitoring/matchers"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

const testHistogramMetricName = "fake_histogram"

func TestQueryNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "ha1", e2ethanos.DefaultPromConfig("prom-ha", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "ha2", e2ethanos.DefaultPromConfig("prom-ha", 1, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).
		WithEnabledFeatures([]string{"query-pushdown"}).
		Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL1 := "http://" + prom1.Endpoint("http") + "/api/v1/write"
	rawRemoteWriteURL2 := "http://" + prom2.Endpoint("http") + "/api/v1/write"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	histograms := tsdbutil.GenerateTestHistograms(4)
	now := time.Now()

	_, err = writeHistograms(ctx, now, testHistogramMetricName, histograms, nil, rawRemoteWriteURL1)
	testutil.Ok(t, err)
	_, err = writeHistograms(ctx, now, testHistogramMetricName, histograms, nil, rawRemoteWriteURL2)
	testutil.Ok(t, err)

	ts := func() time.Time { return now }

	// Make sure we can query histogram from both Prometheus instances.
	queryAndAssert(t, ctx, prom1.Endpoint("http"), func() string { return testHistogramMetricName }, ts, promclient.QueryOptions{}, expectedHistogramModelVector(testHistogramMetricName, histograms[len(histograms)-1], nil, nil))
	queryAndAssert(t, ctx, prom2.Endpoint("http"), func() string { return testHistogramMetricName }, ts, promclient.QueryOptions{}, expectedHistogramModelVector(testHistogramMetricName, histograms[len(histograms)-1], nil, nil))

	t.Run("query deduplicated histogram", func(t *testing.T) {
		queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return testHistogramMetricName }, ts, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(testHistogramMetricName, histograms[len(histograms)-1], nil, map[string]string{
			"prometheus": "prom-ha",
		}))
	})

	t.Run("query histogram using histogram_count fn and deduplication", func(t *testing.T) {
		queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return fmt.Sprintf("histogram_count(%v)", testHistogramMetricName) }, ts, promclient.QueryOptions{Deduplicate: true}, model.Vector{
			&model.Sample{
				Value: 34,
				Metric: model.Metric{
					"foo":        "bar",
					"prometheus": "prom-ha",
				},
			},
		})
	})

	t.Run("query histogram using group function for testing pushdown", func(t *testing.T) {
		queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return fmt.Sprintf("group(%v)", testHistogramMetricName) }, ts, promclient.QueryOptions{Deduplicate: true}, model.Vector{
			&model.Sample{
				Value:  1,
				Metric: model.Metric{},
			},
		})
	})

	t.Run("query histogram rate and compare to Prometheus result", func(t *testing.T) {
		query := func() string { return fmt.Sprintf("rate(%v[1m])", testHistogramMetricName) }
		expected, _ := instantQuery(t, ctx, prom1.Endpoint("http"), query, ts, promclient.QueryOptions{}, 1)
		expected[0].Metric["prometheus"] = "prom-ha"
		expected[0].Timestamp = 0
		queryAndAssert(t, ctx, querier.Endpoint("http"), query, ts, promclient.QueryOptions{Deduplicate: true}, expected)
	})
}

func TestWriteNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-write")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ingestor0 := e2ethanos.NewReceiveBuilder(e, "ingestor0").WithIngestionEnabled().WithNativeHistograms().Init()
	ingestor1 := e2ethanos.NewReceiveBuilder(e, "ingestor1").WithIngestionEnabled().WithNativeHistograms().Init()

	h := receive.HashringConfig{
		Endpoints: []receive.Endpoint{
			{Address: ingestor0.InternalEndpoint("grpc")},
			{Address: ingestor1.InternalEndpoint("grpc")},
		},
	}

	router0 := e2ethanos.NewReceiveBuilder(e, "router0").WithRouting(2, h).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(ingestor0, ingestor1, router0))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", ingestor0.InternalEndpoint("grpc"), ingestor1.InternalEndpoint("grpc")).WithReplicaLabels("receive").Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL := "http://" + router0.Endpoint("remote-write") + "/api/v1/receive"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	timeNow := time.Now()

	histograms := tsdbutil.GenerateTestHistograms(1)
	_, err = writeHistograms(ctx, timeNow, testHistogramMetricName, histograms, nil, rawRemoteWriteURL)
	testutil.Ok(t, err)

	testFloatHistogramMetricName := testHistogramMetricName + "_float"
	floatHistograms := tsdbutil.GenerateTestFloatHistograms(1)
	_, err = writeHistograms(ctx, timeNow, testFloatHistogramMetricName, nil, floatHistograms, rawRemoteWriteURL)
	testutil.Ok(t, err)

	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return testHistogramMetricName }, time.Now, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(testHistogramMetricName, histograms[0], nil, map[string]string{
		"tenant_id": "default-tenant",
	}))

	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return testFloatHistogramMetricName }, time.Now, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(testFloatHistogramMetricName, nil, floatHistograms[0], map[string]string{
		"tenant_id": "default-tenant",
	}))
}

func TestQueryFrontendNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-qfe")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "ha1", e2ethanos.DefaultPromConfig("prom-ha", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "ha2", e2ethanos.DefaultPromConfig("prom-ha", 1, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	inMemoryCacheConfig := queryfrontend.CacheProviderConfig{
		Type: queryfrontend.INMEMORY,
		Config: queryfrontend.InMemoryResponseCacheConfig{
			MaxSizeItems: 1000,
			Validity:     time.Hour,
		},
	}

	queryFrontend := e2ethanos.NewQueryFrontend(e, "query-frontend", "http://"+querier.InternalEndpoint("http"), queryfrontend.Config{}, inMemoryCacheConfig)
	testutil.Ok(t, e2e.StartAndWaitReady(queryFrontend))

	rawRemoteWriteURL1 := "http://" + prom1.Endpoint("http") + "/api/v1/write"
	rawRemoteWriteURL2 := "http://" + prom2.Endpoint("http") + "/api/v1/write"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	histograms := tsdbutil.GenerateTestHistograms(4)
	now := time.Now()
	_, err = writeHistograms(ctx, now, testHistogramMetricName, histograms, nil, rawRemoteWriteURL1)
	testutil.Ok(t, err)
	startTime, err := writeHistograms(ctx, now, testHistogramMetricName, histograms, nil, rawRemoteWriteURL2)
	testutil.Ok(t, err)

	// Ensure we can get the result from Querier first so that it
	// doesn't need to retry when we send queries to the frontend later.
	queryAndAssertSeries(t, ctx, querier.Endpoint("http"), func() string { return testHistogramMetricName }, time.Now, promclient.QueryOptions{Deduplicate: true}, []model.Metric{
		{
			"__name__":   testHistogramMetricName,
			"prometheus": "prom-ha",
			"foo":        "bar",
		},
	})

	vals, err := querier.SumMetrics(
		[]string{"http_requests_total"},
		e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query")),
	)

	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(vals))
	queryTimes := vals[0]

	ts := func() time.Time { return now }

	t.Run("query frontend works for instant query", func(t *testing.T) {
		queryAndAssert(t, ctx, queryFrontend.Endpoint("http"), func() string { return testHistogramMetricName }, ts, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(testHistogramMetricName, histograms[len(histograms)-1], nil, map[string]string{
			"prometheus": "prom-ha",
		}))

		testutil.Ok(t, queryFrontend.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"thanos_query_frontend_queries_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "op", "query")),
		))

		testutil.Ok(t, querier.WaitSumMetricsWithOptions(
			e2emon.Equals(queryTimes+1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query")),
		))
	})

	t.Run("query range query, all but last histogram", func(t *testing.T) {
		expectedRes := expectedHistogramModelMatrix(testHistogramMetricName, histograms[:len(histograms)-1], nil, startTime, map[string]string{
			"prometheus": "prom-ha",
		})

		// query all but last sample
		rangeQuery(t, ctx, queryFrontend.Endpoint("http"), func() string { return testHistogramMetricName },
			startTime.UnixMilli(),
			// Skip last step, so that news samples is not queried and will be queried in next step.
			now.Add(-30*time.Second).UnixMilli(),
			30, // Taken from UI.
			promclient.QueryOptions{
				Deduplicate: true,
			}, func(res model.Matrix) error {
				if !reflect.DeepEqual(res, expectedRes) {
					return fmt.Errorf("unexpected results (got %v but expected %v)", res, expectedRes)
				}
				return nil
			})

		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_cache_fetched_keys_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(0), "cortex_cache_hits_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_misses_total"))

		testutil.Ok(t, querier.WaitSumMetricsWithOptions(
			e2emon.Equals(1),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range")),
		))

	})

	t.Run("query range, all histograms", func(t *testing.T) {
		expectedRes := expectedHistogramModelMatrix(testHistogramMetricName, histograms, nil, startTime, map[string]string{
			"prometheus": "prom-ha",
		})

		rangeQuery(t, ctx, queryFrontend.Endpoint("http"), func() string { return testHistogramMetricName },
			startTime.UnixMilli(),
			now.UnixMilli(),
			30, // Taken from UI.
			promclient.QueryOptions{
				Deduplicate: true,
			}, func(res model.Matrix) error {
				if !reflect.DeepEqual(res, expectedRes) {
					return fmt.Errorf("unexpected results (got %v but expected %v)", res, expectedRes)
				}
				return nil
			})

		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "cortex_cache_fetched_keys_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "cortex_cache_hits_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_misses_total"))

		testutil.Ok(t, querier.WaitSumMetricsWithOptions(
			e2emon.Equals(2),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range")),
		))
	})

	t.Run("query range, all histograms again", func(t *testing.T) {
		expectedRes := expectedHistogramModelMatrix(testHistogramMetricName, histograms, nil, startTime, map[string]string{
			"prometheus": "prom-ha",
		})

		rangeQuery(t, ctx, queryFrontend.Endpoint("http"), func() string { return testHistogramMetricName },
			startTime.UnixMilli(),
			now.UnixMilli(),
			30, // Taken from UI.
			promclient.QueryOptions{
				Deduplicate: true,
			}, func(res model.Matrix) error {
				if !reflect.DeepEqual(res, expectedRes) {
					return fmt.Errorf("unexpected results (got %v but expected %v)", res, expectedRes)
				}
				return nil
			})

		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "cortex_cache_fetched_keys_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(2), "cortex_cache_hits_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_added_new_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "querier_cache_added_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_entries"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(3), "querier_cache_gets_total"))
		testutil.Ok(t, queryFrontend.WaitSumMetrics(e2emon.Equals(1), "querier_cache_misses_total"))

		testutil.Ok(t, querier.WaitSumMetricsWithOptions(
			e2emon.Equals(3),
			[]string{"http_requests_total"},
			e2emon.WithLabelMatchers(matchers.MustNewMatcher(matchers.MatchEqual, "handler", "query_range")),
		))

	})
}

func writeHistograms(ctx context.Context, now time.Time, name string, histograms []*histogram.Histogram, floatHistograms []*histogram.FloatHistogram, rawRemoteWriteURL string) (time.Time, error) {
	startTime := now.Add(time.Duration(len(histograms)-1) * -30 * time.Second).Truncate(30 * time.Second)
	prompbHistograms := make([]prompb.Histogram, 0, len(histograms))

	for i, fh := range floatHistograms {
		ts := startTime.Add(time.Duration(i) * 30 * time.Second).UnixMilli()
		prompbHistograms = append(prompbHistograms, remote.FloatHistogramToHistogramProto(ts, fh))
	}

	for i, h := range histograms {
		ts := startTime.Add(time.Duration(i) * 30 * time.Second).UnixMilli()
		prompbHistograms = append(prompbHistograms, remote.HistogramToHistogramProto(ts, h))
	}

	timeSeriespb := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: name},
			{Name: "foo", Value: "bar"},
		},
		Histograms: prompbHistograms,
	}

	return startTime, storeWriteRequest(ctx, rawRemoteWriteURL, &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{timeSeriespb},
	})
}

func expectedHistogramModelVector(metricName string, histogram *histogram.Histogram, floatHistogram *histogram.FloatHistogram, externalLabels map[string]string) model.Vector {
	metrics := model.Metric{
		"__name__": model.LabelValue(metricName),
		"foo":      "bar",
	}
	for labelKey, labelValue := range externalLabels {
		metrics[model.LabelName(labelKey)] = model.LabelValue(labelValue)
	}

	var sampleHistogram *model.SampleHistogram

	if histogram != nil {
		sampleHistogram = histogramToSampleHistogram(histogram)
	} else {
		sampleHistogram = floatHistogramToSampleHistogram(floatHistogram)
	}

	return model.Vector{
		&model.Sample{
			Metric:    metrics,
			Histogram: sampleHistogram,
		},
	}
}

func expectedHistogramModelMatrix(metricName string, histograms []*histogram.Histogram, floatHistograms []*histogram.FloatHistogram, startTime time.Time, externalLabels map[string]string) model.Matrix {
	metrics := model.Metric{
		"__name__": model.LabelValue(metricName),
		"foo":      "bar",
	}
	for labelKey, labelValue := range externalLabels {
		metrics[model.LabelName(labelKey)] = model.LabelValue(labelValue)
	}

	sampleHistogramPair := make([]model.SampleHistogramPair, 0, len(histograms))

	for i, h := range histograms {
		sampleHistogramPair = append(sampleHistogramPair, model.SampleHistogramPair{
			Timestamp: model.Time(startTime.Add(time.Duration(i) * 30 * time.Second).UnixMilli()),
			Histogram: histogramToSampleHistogram(h),
		})
	}

	for i, fh := range floatHistograms {
		sampleHistogramPair = append(sampleHistogramPair, model.SampleHistogramPair{
			Timestamp: model.Time(startTime.Add(time.Duration(i) * 30 * time.Second).UnixMilli()),
			Histogram: floatHistogramToSampleHistogram(fh),
		})
	}

	return model.Matrix{
		&model.SampleStream{
			Metric:     metrics,
			Histograms: sampleHistogramPair,
		},
	}
}

func histogramToSampleHistogram(h *histogram.Histogram) *model.SampleHistogram {
	var buckets []*model.HistogramBucket

	it := h.NegativeBucketIterator()
	for it.Next() {
		buckets = append([]*model.HistogramBucket{bucketToSampleHistogramBucket(it.At())}, buckets...)
	}

	buckets = append(buckets, bucketToSampleHistogramBucket(h.ZeroBucket()))

	it = h.PositiveBucketIterator()
	for it.Next() {
		buckets = append(buckets, bucketToSampleHistogramBucket(it.At()))
	}

	return &model.SampleHistogram{
		Count:   model.FloatString(h.Count),
		Sum:     model.FloatString(h.Sum),
		Buckets: buckets,
	}
}

func floatHistogramToSampleHistogram(fh *histogram.FloatHistogram) *model.SampleHistogram {
	var buckets []*model.HistogramBucket

	it := fh.NegativeBucketIterator()
	for it.Next() {
		buckets = append([]*model.HistogramBucket{bucketToSampleHistogramBucket(it.At())}, buckets...)
	}

	buckets = append(buckets, bucketToSampleHistogramBucket(fh.ZeroBucket()))

	it = fh.PositiveBucketIterator()
	for it.Next() {
		buckets = append(buckets, bucketToSampleHistogramBucket(it.At()))
	}

	return &model.SampleHistogram{
		Count:   model.FloatString(fh.Count),
		Sum:     model.FloatString(fh.Sum),
		Buckets: buckets,
	}
}

func bucketToSampleHistogramBucket[BC histogram.BucketCount](bucket histogram.Bucket[BC]) *model.HistogramBucket {
	return &model.HistogramBucket{
		Lower:      model.FloatString(bucket.Lower),
		Upper:      model.FloatString(bucket.Upper),
		Count:      model.FloatString(bucket.Count),
		Boundaries: boundaries(bucket),
	}
}

func boundaries[BC histogram.BucketCount](bucket histogram.Bucket[BC]) int32 {
	switch {
	case bucket.UpperInclusive && !bucket.LowerInclusive:
		return 0
	case !bucket.UpperInclusive && bucket.LowerInclusive:
		return 1
	case !bucket.UpperInclusive && !bucket.LowerInclusive:
		return 2
	default:
		return 3
	}
}
