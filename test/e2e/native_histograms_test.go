// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestQueryNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-query")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1 := e2ethanos.NewPrometheusWithSidecar(e, "ha1", e2ethanos.DefaultPromConfig("prom-ha", 0, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	prom2, sidecar2 := e2ethanos.NewPrometheusWithSidecar(e, "ha2", e2ethanos.DefaultPromConfig("prom-ha", 1, "", "", e2ethanos.LocalPrometheusTarget), "", e2ethanos.DefaultPrometheusImage(), "", "native-histograms", "remote-write-receiver")
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL1 := "http://" + prom1.Endpoint("http") + "/api/v1/write"
	rawRemoteWriteURL2 := "http://" + prom2.Endpoint("http") + "/api/v1/write"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, synthesizeHistogram(ctx, rawRemoteWriteURL1))
	testutil.Ok(t, synthesizeHistogram(ctx, rawRemoteWriteURL2))

	queryAndAssertSeries(t, ctx, prom1.Endpoint("http"), func() string { return "fake_histogram" }, time.Now, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__": "fake_histogram",
			"foo":      "bar",
		},
	})
	queryAndAssertSeries(t, ctx, prom2.Endpoint("http"), func() string { return "fake_histogram" }, time.Now, promclient.QueryOptions{}, []model.Metric{
		{
			"__name__": "fake_histogram",
			"foo":      "bar",
		},
	})

	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "fake_histogram" }, time.Now, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(map[string]string{
		"prometheus": "prom-ha",
	}))

	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "histogram_count(fake_histogram)" }, time.Now, promclient.QueryOptions{Deduplicate: true}, model.Vector{
		&model.Sample{
			Value: 5,
			Metric: model.Metric{
				"foo":        "bar",
				"prometheus": "prom-ha",
			},
		},
	})

	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "group(fake_histogram)" }, time.Now, promclient.QueryOptions{Deduplicate: true}, model.Vector{
		&model.Sample{
			Value:  1,
			Metric: model.Metric{},
		},
	})
}

func TestWriteNativeHistograms(t *testing.T) {
	e, err := e2e.NewDockerEnvironment("nat-hist-write")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	ingestor0 := e2ethanos.NewReceiveBuilder(e, "ingestor0").WithIngestionEnabled().WithNativeHistograms().Init()
	ingestor1 := e2ethanos.NewReceiveBuilder(e, "ingestor1").WithIngestionEnabled().WithNativeHistograms().Init()

	h := receive.HashringConfig{
		Endpoints: []string{
			ingestor0.InternalEndpoint("grpc"),
			ingestor1.InternalEndpoint("grpc"),
		},
	}

	router0 := e2ethanos.NewReceiveBuilder(e, "router0").WithRouting(2, h).Init()
	testutil.Ok(t, e2e.StartAndWaitReady(ingestor0, ingestor1, router0))

	querier := e2ethanos.NewQuerierBuilder(e, "querier", ingestor0.InternalEndpoint("grpc"), ingestor1.InternalEndpoint("grpc")).WithReplicaLabels("receive").Init()
	testutil.Ok(t, e2e.StartAndWaitReady(querier))

	rawRemoteWriteURL := "http://" + router0.Endpoint("remote-write") + "/api/v1/receive"

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	err = synthesizeHistogram(ctx, rawRemoteWriteURL)
	testutil.Ok(t, err)

	queryAndAssert(t, ctx, querier.Endpoint("http"), func() string { return "fake_histogram" }, time.Now, promclient.QueryOptions{Deduplicate: true}, expectedHistogramModelVector(map[string]string{
		"tenant_id": "default-tenant",
	}))
}

func synthesizeHistogram(ctx context.Context, rawRemoteWriteURL string) error {
	timeSeriespb := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "fake_histogram"},
			{Name: "foo", Value: "bar"},
		},
		Histograms: []prompb.Histogram{
			remote.HistogramToHistogramProto(time.Now().UnixMilli(), testHistogram()),
		},
	}

	return storeWriteRequest(ctx, rawRemoteWriteURL, &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{timeSeriespb},
	})
}

func expectedHistogramModelVector(externalLabels map[string]string) model.Vector {
	metrics := model.Metric{
		"__name__": "fake_histogram",
		"foo":      "bar",
	}
	for labelKey, labelValue := range externalLabels {
		metrics[model.LabelName(labelKey)] = model.LabelValue(labelValue)
	}

	return model.Vector{
		&model.Sample{
			Metric:    metrics,
			Histogram: histogramToSampleHistogram(testHistogram()),
		},
	}
}

func histogramToSampleHistogram(h *histogram.Histogram) *model.SampleHistogram {
	var buckets []*model.HistogramBucket

	buckets = append(buckets, bucketToSampleHistogramBucket(h.ZeroBucket()))

	it := h.PositiveBucketIterator()
	for it.Next() {
		buckets = append(buckets, bucketToSampleHistogramBucket(it.At()))
	}

	return &model.SampleHistogram{
		Count:   model.FloatString(h.Count),
		Sum:     model.FloatString(h.Sum),
		Buckets: buckets,
	}
}

func bucketToSampleHistogramBucket(bucket histogram.Bucket[uint64]) *model.HistogramBucket {
	return &model.HistogramBucket{
		Lower:      model.FloatString(bucket.Lower),
		Upper:      model.FloatString(bucket.Upper),
		Count:      model.FloatString(bucket.Count),
		Boundaries: boundaries(bucket),
	}
}

func boundaries(bucket histogram.Bucket[uint64]) int {
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

func testHistogram() *histogram.Histogram {
	return &histogram.Histogram{
		Count:         5,
		ZeroCount:     2,
		ZeroThreshold: 0.001,
		Sum:           18.4,
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []int64{1, 1, -1, 0},
	}
}
