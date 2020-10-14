// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	cortexcache "github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	cortexvalidation "github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/user"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const (
	seconds = 1e3 // 1e3 milliseconds per second.
	hour    = 3600 * seconds
	day     = 24 * time.Hour
)

var defaultLimits = &cortexvalidation.Limits{
	MaxQueryLength:      7 * 24 * time.Hour,
	MaxQueryParallelism: 14,
	MaxCacheFreshness:   time.Minute,
}

// fakeRoundTripper implements the RoundTripper interface.
type fakeRoundTripper struct {
	*httptest.Server
	host string
}

func newFakeRoundTripper() (*fakeRoundTripper, error) {
	s := httptest.NewServer(nil)
	u, err := url.Parse(s.URL)
	if err != nil {
		return nil, err
	}
	return &fakeRoundTripper{
		Server: s,
		host:   u.Host,
	}, nil
}

// setHandler is used for mocking.
func (r *fakeRoundTripper) setHandler(h http.Handler) {
	r.Config.Handler = h
}

func (r *fakeRoundTripper) RoundTrip(h *http.Request) (*http.Response, error) {
	h.URL.Scheme = "http"
	h.URL.Host = r.host
	return http.DefaultTransport.RoundTrip(h)
}

// TestRoundTripRetryMiddleware tests the retry middleware.
func TestRoundTripRetryMiddleware(t *testing.T) {
	testRequest := &ThanosQueryRangeRequest{
		Path:  "/api/v1/query_range",
		Start: 0,
		End:   2 * hour,
		Step:  10 * seconds,
	}

	testLabelsRequest := &ThanosLabelsRequest{Path: "/api/v1/labels", Start: 0, End: 2 * hour}
	testSeriesRequest := &ThanosSeriesRequest{
		Path:     "/api/v1/series",
		Start:    0,
		End:      2 * hour,
		Matchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
	}

	queryRangeCodec := NewThanosQueryRangeCodec(true)
	labelsCodec := NewThanosLabelsCodec(true, 2*time.Hour)

	for _, tc := range []struct {
		name        string
		maxRetries  int
		req         queryrange.Request
		codec       queryrange.Codec
		handlerFunc func(fail bool) (*int, http.Handler)
		fail        bool
		expected    int
	}{
		{
			name:       "not query range, retry won't be triggered 1.",
			maxRetries: 100,
			req: &ThanosQueryRangeRequest{
				Path:  "/api/v1/query",
				Start: 0,
				End:   2 * hour,
				Step:  10 * seconds,
			},
			codec:       queryRangeCodec,
			handlerFunc: promqlResults,
			expected:    1,
		},
		{
			name:        "no retry, get counter value 1",
			maxRetries:  0,
			req:         testRequest,
			codec:       queryRangeCodec,
			handlerFunc: promqlResults,
			fail:        true,
			expected:    1,
		},
		{
			name:        "retry set to 1",
			maxRetries:  1,
			req:         testRequest,
			codec:       queryRangeCodec,
			handlerFunc: promqlResults,
			fail:        true,
			expected:    1,
		},
		{
			name:        "retry set to 3",
			maxRetries:  3,
			fail:        true,
			req:         testRequest,
			codec:       queryRangeCodec,
			handlerFunc: promqlResults,
			expected:    3,
		},
		{
			name:        "labels requests: no retry, get counter value 1",
			maxRetries:  0,
			req:         testLabelsRequest,
			codec:       labelsCodec,
			handlerFunc: labelsResults,
			fail:        true,
			expected:    1,
		},
		{
			name:        "labels requests: retry set to 1",
			maxRetries:  1,
			req:         testLabelsRequest,
			codec:       labelsCodec,
			handlerFunc: labelsResults,
			fail:        true,
			expected:    1,
		},
		{
			name:        "labels requests: retry set to 3",
			maxRetries:  3,
			fail:        true,
			req:         testLabelsRequest,
			codec:       labelsCodec,
			handlerFunc: labelsResults,
			expected:    3,
		},
		{
			name:        "series requests: no retry, get counter value 1",
			maxRetries:  0,
			req:         testSeriesRequest,
			codec:       labelsCodec,
			handlerFunc: seriesResults,
			fail:        true,
			expected:    1,
		},
		{
			name:        "series requests: retry set to 1",
			maxRetries:  1,
			req:         testSeriesRequest,
			codec:       labelsCodec,
			handlerFunc: seriesResults,
			fail:        true,
			expected:    1,
		},
		{
			name:        "series requests: retry set to 3",
			maxRetries:  3,
			fail:        true,
			req:         testSeriesRequest,
			codec:       labelsCodec,
			handlerFunc: seriesResults,
			expected:    3,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {
			tpw, err := NewTripperware(
				Config{
					QueryRangeConfig: QueryRangeConfig{
						MaxRetries:             tc.maxRetries,
						Limits:                 defaultLimits,
						SplitQueriesByInterval: day,
					},
					LabelsConfig: LabelsConfig{
						MaxRetries:             tc.maxRetries,
						Limits:                 defaultLimits,
						SplitQueriesByInterval: day,
					},
				}, nil, log.NewNopLogger(),
			)
			testutil.Ok(t, err)

			rt, err := newFakeRoundTripper()
			testutil.Ok(t, err)
			res, handler := tc.handlerFunc(tc.fail)
			rt.setHandler(handler)

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := tc.codec.EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Equals(t, tc.fail, err != nil)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}

// TestRoundTripSplitIntervalMiddleware tests the split interval middleware.
func TestRoundTripSplitIntervalMiddleware(t *testing.T) {
	testRequest := &ThanosQueryRangeRequest{
		Path:  "/api/v1/query_range",
		Start: 0,
		End:   2 * hour,
		Step:  10 * seconds,
	}

	testLabelsRequest := &ThanosLabelsRequest{
		Path:  "/api/v1/labels",
		Start: 0,
		End:   2 * hour,
	}

	testSeriesRequest := &ThanosSeriesRequest{
		Path:     "/api/v1/series",
		Start:    0,
		End:      2 * hour,
		Matchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
	}

	queryRangeCodec := NewThanosQueryRangeCodec(true)
	labelsCodec := NewThanosLabelsCodec(true, 2*time.Hour)

	for _, tc := range []struct {
		name          string
		splitInterval time.Duration
		req           queryrange.Request
		codec         queryrange.Codec
		handlerFunc   func(bool) (*int, http.Handler)
		expected      int
	}{
		{
			name: "non query range request won't be split 1",
			req: &ThanosQueryRangeRequest{
				Path:  "/api/v1/query",
				Start: 0,
				End:   2 * hour,
				Step:  10 * seconds,
			},
			handlerFunc:   promqlResults,
			codec:         queryRangeCodec,
			splitInterval: time.Hour,
			expected:      1,
		},
		{
			name:          "split interval == 0, disable split",
			req:           testRequest,
			handlerFunc:   promqlResults,
			codec:         queryRangeCodec,
			splitInterval: 0,
			expected:      1,
		},
		{
			name:          "won't be split. Interval == time range",
			req:           testRequest,
			handlerFunc:   promqlResults,
			codec:         queryRangeCodec,
			splitInterval: 2 * time.Hour,
			expected:      1,
		},
		{
			name:          "won't be split. Interval > time range",
			req:           testRequest,
			handlerFunc:   promqlResults,
			codec:         queryRangeCodec,
			splitInterval: day,
			expected:      1,
		},
		{
			name:          "split to 2 requests",
			req:           testRequest,
			handlerFunc:   promqlResults,
			codec:         queryRangeCodec,
			splitInterval: 1 * time.Hour,
			expected:      2,
		},
		{
			name:          "labels request won't be split",
			req:           testLabelsRequest,
			handlerFunc:   labelsResults,
			codec:         labelsCodec,
			splitInterval: day,
			expected:      1,
		},
		{
			name:          "labels request split to 2",
			req:           testLabelsRequest,
			handlerFunc:   labelsResults,
			codec:         labelsCodec,
			splitInterval: 1 * time.Hour,
			expected:      2,
		},
		{
			name:          "series request won't be split",
			req:           testSeriesRequest,
			handlerFunc:   seriesResults,
			codec:         labelsCodec,
			splitInterval: day,
			expected:      1,
		},
		{
			name:          "series request split to 2",
			req:           testSeriesRequest,
			handlerFunc:   seriesResults,
			codec:         labelsCodec,
			splitInterval: 1 * time.Hour,
			expected:      2,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {
			tpw, err := NewTripperware(
				Config{
					QueryRangeConfig: QueryRangeConfig{
						Limits:                 defaultLimits,
						SplitQueriesByInterval: tc.splitInterval,
					},
					LabelsConfig: LabelsConfig{
						Limits:                 defaultLimits,
						SplitQueriesByInterval: tc.splitInterval,
					},
				}, nil, log.NewNopLogger(),
			)
			testutil.Ok(t, err)

			rt, err := newFakeRoundTripper()
			testutil.Ok(t, err)
			defer rt.Close()
			res, handler := tc.handlerFunc(false)
			rt.setHandler(handler)

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := tc.codec.EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expected, *res)
		})
	}
}

// TestRoundTripQueryRangeCacheMiddleware tests the cache middleware.
func TestRoundTripQueryRangeCacheMiddleware(t *testing.T) {
	testRequest := &ThanosQueryRangeRequest{
		Path:                "/api/v1/query_range",
		Start:               0,
		End:                 2 * hour,
		Step:                10 * seconds,
		MaxSourceResolution: 1 * seconds,
	}

	// Non query range request, won't be cached.
	testRequestInstant := &ThanosQueryRangeRequest{
		Path:  "/api/v1/query",
		Start: 0,
		End:   2 * hour,
		Step:  10 * seconds,
	}

	// Same query params as testRequest, different maxSourceResolution
	// but still in the same downsampling level, so it will be cached in this case.
	testRequestSameLevelDownsampling := &ThanosQueryRangeRequest{
		Path:                "/api/v1/query_range",
		Start:               0,
		End:                 2 * hour,
		Step:                10 * seconds,
		MaxSourceResolution: 10 * seconds,
	}

	// Same query params as testRequest, different maxSourceResolution
	// and downsampling level so it won't be cached in this case.
	testRequestHigherLevelDownsampling := &ThanosQueryRangeRequest{
		Path:                "/api/v1/query_range",
		Start:               0,
		End:                 2 * hour,
		Step:                10 * seconds,
		MaxSourceResolution: 1 * hour,
	}

	// Same query params as testRequest, but with storeMatchers
	testRequestWithStoreMatchers := &ThanosQueryRangeRequest{
		Path:                "/api/v1/query_range",
		Start:               0,
		End:                 2 * hour,
		Step:                10 * seconds,
		MaxSourceResolution: 1 * seconds,
		StoreMatchers:       [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
	}

	cacheConf := &queryrange.ResultsCacheConfig{
		CacheConfig: cortexcache.Config{
			EnableFifoCache: true,
			Fifocache: cortexcache.FifoCacheConfig{
				MaxSizeBytes: "1MiB",
				MaxSizeItems: 1000,
				Validity:     time.Hour,
			},
		},
	}

	now := time.Now()
	tpw, err := NewTripperware(
		Config{
			QueryRangeConfig: QueryRangeConfig{
				Limits:                 defaultLimits,
				ResultsCacheConfig:     cacheConf,
				SplitQueriesByInterval: day,
			},
		}, nil, log.NewNopLogger(),
	)
	testutil.Ok(t, err)

	rt, err := newFakeRoundTripper()
	testutil.Ok(t, err)
	defer rt.Close()
	res, handler := promqlResults(false)
	rt.setHandler(handler)

	for _, tc := range []struct {
		name             string
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expected         int
	}{
		{name: "first request", req: testRequest, expected: 1},
		{name: "same request as the first one, directly use cache", req: testRequest, expected: 1},
		{name: "non query range request won't be cached", req: testRequestInstant, expected: 2},
		{name: "do it again", req: testRequestInstant, expected: 3},
		{name: "different max source resolution but still same level", req: testRequestSameLevelDownsampling, expected: 3},
		{name: "different max source resolution and different level", req: testRequestHigherLevelDownsampling, expected: 4},
		{name: "storeMatchers requests won't go to cache", req: testRequestWithStoreMatchers, expected: 5},
		{
			name: "request but will be partitioned",
			req: &ThanosQueryRangeRequest{
				Path:  "/api/v1/query_range",
				Start: timestamp.FromTime(now.Add(-time.Hour)),
				End:   timestamp.FromTime(now.Add(time.Hour)),
				Step:  10 * seconds,
			},
			expected: 6,
		},
		{
			name: "same query as the previous one",
			req: &ThanosQueryRangeRequest{
				Path:  "/api/v1/query_range",
				Start: timestamp.FromTime(now.Add(-time.Hour)),
				End:   timestamp.FromTime(now.Add(time.Hour)),
				Step:  10 * seconds,
			},
			expected: 7,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := NewThanosQueryRangeCodec(true).EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}

// TestRoundTripLabelsCacheMiddleware tests the cache middleware for labels requests.
func TestRoundTripLabelsCacheMiddleware(t *testing.T) {
	testRequest := &ThanosLabelsRequest{
		Path:  "/api/v1/labels",
		Start: 0,
		End:   2 * hour,
	}

	// Same query params as testRequest, but with storeMatchers
	testRequestWithStoreMatchers := &ThanosLabelsRequest{
		Path:          "/api/v1/labels",
		Start:         0,
		End:           2 * hour,
		StoreMatchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
	}

	testLabelValuesRequestFoo := &ThanosLabelsRequest{
		Path:  "/api/v1/label/foo/values",
		Start: 0,
		End:   2 * hour,
		Label: "foo",
	}

	testLabelValuesRequestBar := &ThanosLabelsRequest{
		Path:  "/api/v1/label/bar/values",
		Start: 0,
		End:   2 * hour,
		Label: "bar",
	}

	cacheConf := &queryrange.ResultsCacheConfig{
		CacheConfig: cortexcache.Config{
			EnableFifoCache: true,
			Fifocache: cortexcache.FifoCacheConfig{
				MaxSizeBytes: "1MiB",
				MaxSizeItems: 1000,
				Validity:     time.Hour,
			},
		},
	}

	now := time.Now()
	tpw, err := NewTripperware(
		Config{
			LabelsConfig: LabelsConfig{
				Limits:                 defaultLimits,
				ResultsCacheConfig:     cacheConf,
				SplitQueriesByInterval: day,
			},
		}, nil, log.NewNopLogger(),
	)
	testutil.Ok(t, err)

	rt, err := newFakeRoundTripper()
	testutil.Ok(t, err)
	defer rt.Close()
	res, handler := labelsResults(false)
	rt.setHandler(handler)

	for _, tc := range []struct {
		name             string
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expected         int
	}{
		{name: "first request", req: testRequest, expected: 1},
		{name: "same request as the first one, directly use cache", req: testRequest, expected: 1},
		{name: "storeMatchers requests won't go to cache", req: testRequestWithStoreMatchers, expected: 2},
		{name: "label values request label name foo", req: testLabelValuesRequestFoo, expected: 3},
		{name: "same label values query, use cache", req: testLabelValuesRequestFoo, expected: 3},
		{name: "label values request different label", req: testLabelValuesRequestBar, expected: 4},
		{
			name: "request but will be partitioned",
			req: &ThanosLabelsRequest{
				Path:  "/api/v1/labels",
				Start: timestamp.FromTime(now.Add(-time.Hour)),
				End:   timestamp.FromTime(now.Add(time.Hour)),
			},
			expected: 5,
		},
		{
			name: "same query as the previous one",
			req: &ThanosLabelsRequest{
				Path:  "/api/v1/labels",
				Start: timestamp.FromTime(now.Add(-time.Hour)),
				End:   timestamp.FromTime(now.Add(time.Hour)),
			},
			expected: 6,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := NewThanosLabelsCodec(true, 24*time.Hour).EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}

// TestRoundTripSeriesCacheMiddleware tests the cache middleware for series requests.
func TestRoundTripSeriesCacheMiddleware(t *testing.T) {
	testRequest := &ThanosSeriesRequest{
		Path:     "/api/v1/series",
		Start:    0,
		End:      2 * hour,
		Matchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
	}

	// Different matchers set with the first request.
	testRequest2 := &ThanosSeriesRequest{
		Path:     "/api/v1/series",
		Start:    0,
		End:      2 * hour,
		Matchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "baz")}},
	}

	// Same query params as testRequest, but with storeMatchers
	testRequestWithStoreMatchers := &ThanosLabelsRequest{
		Path:          "/api/v1/series",
		Start:         0,
		End:           2 * hour,
		StoreMatchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
	}

	cacheConf := &queryrange.ResultsCacheConfig{
		CacheConfig: cortexcache.Config{
			EnableFifoCache: true,
			Fifocache: cortexcache.FifoCacheConfig{
				MaxSizeBytes: "1MiB",
				MaxSizeItems: 1000,
				Validity:     time.Hour,
			},
		},
	}

	tpw, err := NewTripperware(
		Config{
			LabelsConfig: LabelsConfig{
				Limits:                 defaultLimits,
				ResultsCacheConfig:     cacheConf,
				SplitQueriesByInterval: day,
			},
		}, nil, log.NewNopLogger(),
	)
	testutil.Ok(t, err)

	rt, err := newFakeRoundTripper()
	testutil.Ok(t, err)
	defer rt.Close()
	res, handler := seriesResults(false)
	rt.setHandler(handler)

	for _, tc := range []struct {
		name             string
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expected         int
	}{
		{name: "first request", req: testRequest, expected: 1},
		{name: "same request as the first one, directly use cache", req: testRequest, expected: 1},
		{name: "different series request, not use cache", req: testRequest2, expected: 2},
		{name: "storeMatchers requests won't go to cache", req: testRequestWithStoreMatchers, expected: 3},
	} {

		t.Run(tc.name, func(t *testing.T) {

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := NewThanosLabelsCodec(true, 24*time.Hour).EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}

// promqlResults is a mock handler used to test split and cache middleware.
// Modified from Loki https://github.com/grafana/loki/blob/master/pkg/querier/queryrange/roundtrip_test.go#L547.
func promqlResults(fail bool) (*int, http.Handler) {
	count := 0
	var lock sync.Mutex
	q := queryrange.PrometheusResponse{
		Status: "success",
		Data: queryrange.PrometheusData{
			ResultType: string(parser.ValueTypeMatrix),
			Result: []queryrange.SampleStream{
				{
					Labels: []client.LabelAdapter{},
					Samples: []client.Sample{
						{Value: 0, TimestampMs: 0},
						{Value: 1, TimestampMs: 1},
					},
				},
			},
		},
	}

	return &count, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()

		// Set fail in the response code to test retry.
		if fail {
			w.WriteHeader(500)
		}
		if err := json.NewEncoder(w).Encode(q); err != nil {
			panic(err)
		}
		count++
	})
}

//labelsResults is a mock handler used to test split and cache middleware for label names and label values requests.
func labelsResults(fail bool) (*int, http.Handler) {
	count := 0
	var lock sync.Mutex
	q := ThanosLabelsResponse{
		Status: "success",
		Data:   []string{"__name__", "job"},
	}

	return &count, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()

		// Set fail in the response code to test retry.
		if fail {
			w.WriteHeader(500)
		}
		if err := json.NewEncoder(w).Encode(q); err != nil {
			panic(err)
		}
		count++
	})
}

// seriesResults is a mock handler used to test split and cache middleware for series requests.
func seriesResults(fail bool) (*int, http.Handler) {
	count := 0
	var lock sync.Mutex
	q := ThanosSeriesResponse{
		Status: "success",
		Data:   []labelpb.LabelSet{{Labels: []labelpb.Label{{Name: "__name__", Value: "up"}, {Name: "foo", Value: "bar"}}}},
	}

	return &count, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()

		// Set fail in the response code to test retry.
		if fail {
			w.WriteHeader(500)
		}
		if err := json.NewEncoder(w).Encode(q); err != nil {
			panic(err)
		}
		count++
	})
}
