// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestRmLabelsCornerCases(t *testing.T) {
	t.Parallel()

	testutil.Equals(t, rmLabels(labelsFromStrings("aa", "bb"), map[string]struct{}{
		"aa": {},
	}), labels.Labels{})
	testutil.Equals(t, rmLabels(labelsFromStrings(), map[string]struct{}{
		"aa": {},
	}), labels.Labels{})
}

func TestProxyResponseTreeSort(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		title string
		input []respSet
		exp   []*storepb.SeriesResponse
	}{
		{
			title: "merge sets with different series and common labels",
			input: []respSet{
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
					},
				},
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4", "e", "5")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "d", "4")),
					},
				},
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4", "e", "5")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "d", "4")),
			},
		},
		{
			title: "merge sets with different series and labels",
			input: []respSet{
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "c", "3")),
						storeSeriesResponse(t, labelsFromStrings("b", "2", "c", "3")),
						storeSeriesResponse(t, labelsFromStrings("g", "7", "h", "8", "i", "9")),
					},
				},
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("d", "4", "e", "5")),
						storeSeriesResponse(t, labelsFromStrings("d", "4", "e", "5", "f", "6")),
					},
				},
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("b", "2", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("d", "4", "e", "5")),
				storeSeriesResponse(t, labelsFromStrings("d", "4", "e", "5", "f", "6")),
				storeSeriesResponse(t, labelsFromStrings("g", "7", "h", "8", "i", "9")),
			},
		},
		{
			title: "merge repeated series in stores with different external labels",
			input: []respSet{
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
					},
					storeLabels: map[string]struct{}{"ext2": {}},
				},
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
					},
					storeLabels: map[string]struct{}{"ext1": {}, "ext2": {}},
				},
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
			},
		},
		{
			title: "merge series with external labels at beginning of series",
			input: []respSet{
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "c", "3")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "2")),
					},
					storeLabels: map[string]struct{}{"a": {}},
				},
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "1", "c", "3")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
					},
					storeLabels: map[string]struct{}{"a": {}},
				},
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "2")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
			},
		},
		{
			title: "merge series in stores with external labels not present in series (e.g. stripped during dedup)",
			input: []respSet{
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
					},
					storeLabels: map[string]struct{}{"ext2": {}, "replica": {}},
				},
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
						storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
					},
					storeLabels: map[string]struct{}{"ext1": {}, "ext2": {}, "replica": {}},
				},
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext1", "5", "ext2", "9")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "2", "ext2", "9")),
			},
		},
		{
			title: "test",
			input: []respSet{
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.13.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
						storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.5.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
						storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.6.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
					},
					storeLabels: map[string]struct{}{"receive": {}, "tenant_id": {}, "thanos_replica": {}},
				},
				&eagerRespSet{
					closeSeries: func() {},
					cl:          nopClientSendCloser{},
					wg:          &sync.WaitGroup{},
					bufferedResponses: []*storepb.SeriesResponse{
						storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.13.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
						storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.5.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
						storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.6.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
					},
					storeLabels: map[string]struct{}{"cluster": {}, "prometheus": {}, "prometheus_replica": {}, "receive": {}, "tenant_id": {}, "thanos_replica": {}, "thanos_ruler_replica": {}},
				},
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.13.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
				storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.13.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
				storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.5.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
				storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.5.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
				storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.6.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
				storeSeriesResponse(t, labelsFromStrings("cluster", "beam-platform", "instance", "10.70.6.3:15692", "prometheus", "telemetry/observe-prometheus", "receive", "true", "tenant_id", "default-tenant")),
			},
		},
	} {
		t.Run(tcase.title, func(t *testing.T) {
			h := NewProxyResponseLoserTree(tcase.input...)
			got := []*storepb.SeriesResponse{}
			for h.Next() {
				r := h.At()
				got = append(got, r)
			}
			testutil.Equals(t, tcase.exp, got)
		})
	}
}

type nopClientSendCloser struct {
	storepb.Store_SeriesClient
}

func (c nopClientSendCloser) CloseSend() error { return nil }

// TestProxyResponseTreeSortWithBatchResponses verifies that batch responses are
// properly unpacked into individual series before being merged by the loser tree.
// Without proper unpacking, the loser tree would receive batch responses and fail
// to merge series correctly.
func TestProxyResponseTreeSortWithBatchResponses(t *testing.T) {
	t.Parallel()

	// Create series that will be sent in batches from two stores.
	// Store 1 sends batch with series a=1, a=3
	// Store 2 sends batch with series a=2, a=4
	// After unpacking and merging, we expect: a=1, a=2, a=3, a=4
	series1 := storeSeriesResponse(t, labelsFromStrings("a", "1")).GetSeries()
	series2 := storeSeriesResponse(t, labelsFromStrings("a", "2")).GetSeries()
	series3 := storeSeriesResponse(t, labelsFromStrings("a", "3")).GetSeries()
	series4 := storeSeriesResponse(t, labelsFromStrings("a", "4")).GetSeries()

	mockClient1 := &mockBatchSeriesClient{
		responses: []*storepb.SeriesResponse{
			storepb.NewBatchResponse([]*storepb.Series{series1, series3}),
		},
	}
	mockClient2 := &mockBatchSeriesClient{
		responses: []*storepb.SeriesResponse{
			storepb.NewBatchResponse([]*storepb.Series{series2, series4}),
		},
	}

	// Create eagerRespSets which should unpack the batches
	var shardInfo *storepb.ShardInfo
	respSet1 := newEagerRespSet(
		noopSpan{},
		0,
		"store1",
		nil,
		func() {},
		mockClient1,
		shardInfo.Matcher(nil),
		false,
		promauto.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("%s-1", t.Name())}),
		nil,
	)
	respSet2 := newEagerRespSet(
		noopSpan{},
		0,
		"store2",
		nil,
		func() {},
		mockClient2,
		shardInfo.Matcher(nil),
		false,
		promauto.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("%s-2", t.Name())}),
		nil,
	)

	h := NewProxyResponseLoserTree(respSet1, respSet2)
	var got []*storepb.SeriesResponse
	for h.Next() {
		got = append(got, h.At())
	}

	exp := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labelsFromStrings("a", "1")),
		storeSeriesResponse(t, labelsFromStrings("a", "2")),
		storeSeriesResponse(t, labelsFromStrings("a", "3")),
		storeSeriesResponse(t, labelsFromStrings("a", "4")),
	}
	testutil.Equals(t, exp, got)
}

type mockBatchSeriesClient struct {
	storepb.Store_SeriesClient
	responses []*storepb.SeriesResponse
	i         int
}

func (c *mockBatchSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if c.i >= len(c.responses) {
		return nil, io.EOF
	}
	resp := c.responses[c.i]
	c.i++
	return resp, nil
}

func (c *mockBatchSeriesClient) CloseSend() error { return nil }

// noopSpan implements opentracing.Span for testing.
type noopSpan struct{}

func (noopSpan) Finish()                                        {}
func (noopSpan) SetTag(string, any) opentracing.Span            { return noopSpan{} }
func (noopSpan) Context() opentracing.SpanContext               { return nil }
func (noopSpan) SetOperationName(string) opentracing.Span       { return noopSpan{} }
func (noopSpan) Tracer() opentracing.Tracer                     { return nil }
func (noopSpan) SetBaggageItem(string, string) opentracing.Span { return noopSpan{} }
func (noopSpan) BaggageItem(string) string                      { return "" }
func (noopSpan) LogKV(...any)                                   {}
func (noopSpan) LogFields(...otlog.Field)                       {}
func (noopSpan) Log(opentracing.LogData)                        {}
func (noopSpan) FinishWithOptions(opentracing.FinishOptions)    {}
func (noopSpan) LogEvent(string)                                {}
func (noopSpan) LogEventWithPayload(string, interface{})        {}

// TestLazyRespSetUnpacksBatchResponses verifies that lazyRespSet properly unpacks
// batch responses into individual series before being merged by the loser tree.
func TestLazyRespSetUnpacksBatchResponses(t *testing.T) {
	t.Parallel()

	series1 := storeSeriesResponse(t, labelsFromStrings("a", "1")).GetSeries()
	series2 := storeSeriesResponse(t, labelsFromStrings("a", "2")).GetSeries()
	series3 := storeSeriesResponse(t, labelsFromStrings("a", "3")).GetSeries()
	series4 := storeSeriesResponse(t, labelsFromStrings("a", "4")).GetSeries()

	mockClient1 := &mockBatchSeriesClient{
		responses: []*storepb.SeriesResponse{
			storepb.NewBatchResponse([]*storepb.Series{series1, series3}),
		},
	}
	mockClient2 := &mockBatchSeriesClient{
		responses: []*storepb.SeriesResponse{
			storepb.NewBatchResponse([]*storepb.Series{series2, series4}),
		},
	}

	var shardInfo *storepb.ShardInfo
	respSet1 := newLazyRespSet(
		noopSpan{},
		0,
		"store1",
		nil,
		func() {},
		mockClient1,
		shardInfo.Matcher(nil),
		false,
		promauto.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("%s-1", t.Name())}),
		10,
	)
	respSet2 := newLazyRespSet(
		noopSpan{},
		0,
		"store2",
		nil,
		func() {},
		mockClient2,
		shardInfo.Matcher(nil),
		false,
		promauto.NewCounter(prometheus.CounterOpts{Name: fmt.Sprintf("%s-2", t.Name())}),
		10,
	)

	h := NewProxyResponseLoserTree(respSet1, respSet2)
	var got []*storepb.SeriesResponse
	for h.Next() {
		got = append(got, h.At())
	}

	exp := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labelsFromStrings("a", "1")),
		storeSeriesResponse(t, labelsFromStrings("a", "2")),
		storeSeriesResponse(t, labelsFromStrings("a", "3")),
		storeSeriesResponse(t, labelsFromStrings("a", "4")),
	}
	testutil.Equals(t, exp, got)
}

func TestSortWithoutLabels(t *testing.T) {
	t.Parallel()

	for _, tcase := range []struct {
		input       []*storepb.SeriesResponse
		exp         []*storepb.SeriesResponse
		dedupLabels map[string]struct{}
	}{
		// Single deduplication label.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-2", "c", "3")),
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4")),
			},
			dedupLabels: map[string]struct{}{"b": {}},
		},
		// Multi deduplication labels.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-1", "c", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "b1", "replica-2", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-2", "c", "3")),
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4")),
			},
			dedupLabels: map[string]struct{}{"b": {}, "b1": {}},
		},
		// Non series responses mixed.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "3", "d", "4")),
				storepb.NewWarnSeriesResponse(errors.Newf("yolo")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-1", "c", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "b", "replica-2", "c", "3")),
			},
			exp: []*storepb.SeriesResponse{
				storepb.NewWarnSeriesResponse(errors.Newf("yolo")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "3", "d", "4")),
				storeSeriesResponse(t, labelsFromStrings("a", "1", "c", "4")),
			},
			dedupLabels: map[string]struct{}{"b": {}},
		},
		// Longer series.
		{
			input: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labels.FromStrings(
					"__name__", "gitlab_transaction_cache_read_hit_count_total", "action", "widget.json", "controller", "Projects::MergeRequests::ContentController", "env", "gprd", "environment",
					"gprd", "fqdn", "web-08-sv-gprd.c.gitlab-production.internal", "instance", "web-08-sv-gprd.c.gitlab-production.internal:8083", "job", "gitlab-rails", "monitor", "app", "provider",
					"gcp", "region", "us-east", "replica", "01", "shard", "default", "stage", "main", "tier", "sv", "type", "web",
				)),
			},
			exp: []*storepb.SeriesResponse{
				storeSeriesResponse(t, labels.FromStrings(
					// No replica label anymore.
					"__name__", "gitlab_transaction_cache_read_hit_count_total", "action", "widget.json", "controller", "Projects::MergeRequests::ContentController", "env", "gprd", "environment",
					"gprd", "fqdn", "web-08-sv-gprd.c.gitlab-production.internal", "instance", "web-08-sv-gprd.c.gitlab-production.internal:8083", "job", "gitlab-rails", "monitor", "app", "provider",
					"gcp", "region", "us-east", "shard", "default", "stage", "main", "tier", "sv", "type", "web",
				)),
			},
			dedupLabels: map[string]struct{}{"replica": {}},
		},
	} {
		t.Run("", func(t *testing.T) {
			sortWithoutLabels(tcase.input, tcase.dedupLabels)
			testutil.Equals(t, tcase.exp, tcase.input)
		})
	}
}

// labelsFromStrings is like labels.FromString, but it does not sort the input.
func labelsFromStrings(ss ...string) labels.Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}

	b := labels.NewScratchBuilder(len(ss) / 2)
	for i := 0; i < len(ss); i += 2 {
		b.Add(ss[i], ss[i+1])
	}
	return b.Labels()
}

func BenchmarkSortWithoutLabels(b *testing.B) {
	resps := make([]*storepb.SeriesResponse, 1e4)
	labelsToRemove := map[string]struct{}{
		"a": {}, "b": {},
	}

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		for i := range int(1e4) {
			resps[i] = storeSeriesResponse(b, labels.FromStrings("a", "1", "b", "replica-1", "c", "replica-1", "d", "1"))
		}
		b.StartTimer()
		sortWithoutLabels(resps, labelsToRemove)
	}
}

func BenchmarkKWayMerge(b *testing.B) {
	for b.Loop() {
		respSets := []respSet{}
		for j := range 1000 {
			respSets = append(respSets, &eagerRespSet{
				closeSeries: func() {},
				cl:          nopClientSendCloser{},
				wg:          &sync.WaitGroup{},
				bufferedResponses: []*storepb.SeriesResponse{
					storeSeriesResponse(b, labelsFromStrings("a", "1", "b", fmt.Sprintf("replica-%d", j), "c", "3")),
					storeSeriesResponse(b, labelsFromStrings("a", "1", "b", fmt.Sprintf("replica-%d", j), "c", "3", "d", "4")),
					storeSeriesResponse(b, labelsFromStrings("a", "1", "b", fmt.Sprintf("replica-%d", j), "c", "4")),
				},
			})
		}
		lt := NewProxyResponseLoserTree(respSets...)

		got := []*storepb.SeriesResponse{}
		for lt.Next() {
			r := lt.At()
			got = append(got, r)
		}

		var _ = got
	}
}
