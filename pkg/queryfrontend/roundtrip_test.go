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
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/user"

	"github.com/thanos-io/thanos/pkg/testutil"
)

const (
	seconds = 1e3 // 1e3 milliseconds per second.
	hour    = 3600 * seconds
	day     = 24 * time.Hour
)

// fakeLimits implements the Cortex queryrange.Limits interface.
type fakeLimits struct{}

func (l *fakeLimits) MaxQueryLength(_ string) time.Duration {
	return 7 * 24 * time.Hour
}

func (l *fakeLimits) MaxQueryParallelism(_ string) int {
	return 14
}

func (l *fakeLimits) MaxCacheFreshness(_ string) time.Duration {
	return time.Minute
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
	testRequest := &ThanosRequest{
		Path:  "/api/v1/query_range",
		Start: 0,
		End:   2 * hour,
		Step:  10 * seconds,
	}

	codec := NewThanosCodec(true)

	for _, tc := range []struct {
		name             string
		maxRetries       int
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expectedError    bool
		expected         int
	}{
		{
			name:       "not query range, retry won't be triggered 1.",
			maxRetries: 100,
			req: &ThanosRequest{
				Path:  "/api/v1/query",
				Start: 0,
				End:   2 * hour,
				Step:  10 * seconds,
			},
			handlerAndResult: counter,
			// Not go through tripperware so no error.
			expectedError: false,
			expected:      1,
		},
		{
			name:       "not query range, retry won't be triggered 2.",
			maxRetries: 100,
			req: &ThanosRequest{
				Path:  "/api/v1/labels",
				Start: 0,
				End:   2 * hour,
				Step:  10 * seconds,
			},
			handlerAndResult: counter,
			// Not go through tripperware so no error.
			expectedError: false,
			expected:      1,
		},
		{
			name:             "no retry, get counter value 1",
			maxRetries:       0,
			req:              testRequest,
			handlerAndResult: counter,
			expectedError:    true,
			expected:         1,
		},
		{
			name:             "retry set to 1",
			maxRetries:       1,
			req:              testRequest,
			handlerAndResult: counter,
			expectedError:    true,
			expected:         1,
		},
		{
			name:             "retry set to 3",
			maxRetries:       3,
			req:              testRequest,
			handlerAndResult: counter,
			expectedError:    true,
			expected:         3,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {
			tpw, err := NewTripperWare(&fakeLimits{}, nil, codec, nil,
				day, tc.maxRetries, nil, log.NewNopLogger())
			testutil.Ok(t, err)

			rt, err := newFakeRoundTripper()
			testutil.Ok(t, err)
			res, handler := tc.handlerAndResult()
			rt.setHandler(handler)

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := codec.EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Equals(t, tc.expectedError, err != nil)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}

// TestRoundTripSplitIntervalMiddleware tests the split interval middleware.
func TestRoundTripSplitIntervalMiddleware(t *testing.T) {
	testRequest := &ThanosRequest{
		Path:  "/api/v1/query_range",
		Start: 0,
		End:   2 * hour,
		Step:  10 * seconds,
	}

	codec := NewThanosCodec(true)

	for _, tc := range []struct {
		name             string
		splitInterval    time.Duration
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expectError      bool
		expected         int
	}{
		{
			name: "non query range request won't be split 1",
			req: &ThanosRequest{
				Path:  "/api/v1/query",
				Start: 0,
				End:   2 * hour,
				Step:  10 * seconds,
			},
			splitInterval:    time.Hour,
			handlerAndResult: counter,
			expectError:      false,
			expected:         1,
		},
		{
			name: "non query range request won't be split 2",
			req: &ThanosRequest{
				Path:  "/api/v1/labels",
				Start: 0,
				End:   2 * hour,
				Step:  10 * seconds,
			},
			splitInterval:    time.Hour,
			handlerAndResult: counter,
			expectError:      false,
			expected:         1,
		},
		{
			name:             "split interval == 0, disable split",
			req:              testRequest,
			splitInterval:    0,
			handlerAndResult: counter,
			expectError:      true,
			expected:         1,
		},
		{
			name:             "won't be split. Interval == time range",
			req:              testRequest,
			splitInterval:    2 * time.Hour,
			handlerAndResult: counter,
			expectError:      true,
			expected:         1,
		},
		{
			name:             "won't be split. Interval > time range",
			req:              testRequest,
			splitInterval:    day,
			handlerAndResult: counter,
			expectError:      true,
			expected:         1,
		},
		{
			name:             "split to 2 requests",
			req:              testRequest,
			splitInterval:    1 * time.Hour,
			handlerAndResult: counter,
			expectError:      true,
			expected:         2,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {
			tpw, err := NewTripperWare(&fakeLimits{}, nil, codec, nil,
				tc.splitInterval, 0, nil, log.NewNopLogger())
			testutil.Ok(t, err)

			rt, err := newFakeRoundTripper()
			testutil.Ok(t, err)
			res, handler := tc.handlerAndResult()
			rt.setHandler(handler)

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := codec.EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Equals(t, tc.expectError, err != nil)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}

// TestRoundTripCacheMiddleware tests the cache middleware.
func TestRoundTripCacheMiddleware(t *testing.T) {
	testRequest := &ThanosRequest{
		Path:                "/api/v1/query_range",
		Start:               0,
		End:                 2 * hour,
		Step:                10 * seconds,
		MaxSourceResolution: 1 * seconds,
	}

	// Non query range request, won't be cached.
	testRequest2 := &ThanosRequest{
		Path:  "/api/v1/query",
		Start: 0,
		End:   2 * hour,
		Step:  10 * seconds,
	}

	// Same query params as testRequest, different maxSourceResolution
	// but still in the same downsampling level, so it will be cached in this case.
	testRequest3 := &ThanosRequest{
		Path:                "/api/v1/query_range",
		Start:               0,
		End:                 2 * hour,
		Step:                10 * seconds,
		MaxSourceResolution: 10 * seconds,
	}

	// Same query params as testRequest, different maxSourceResolution
	// and downsampling level so it won't be cached in this case.
	testRequest4 := &ThanosRequest{
		Path:                "/api/v1/query_range",
		Start:               0,
		End:                 2 * hour,
		Step:                10 * seconds,
		MaxSourceResolution: 1 * hour,
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

	codec := NewThanosCodec(true)

	now := time.Now()
	tpw, err := NewTripperWare(&fakeLimits{}, cacheConf, codec, queryrange.PrometheusResponseExtractor{},
		day, 0, nil, log.NewNopLogger())
	testutil.Ok(t, err)

	rt, err := newFakeRoundTripper()
	testutil.Ok(t, err)
	res, handler := promqlResults()
	rt.setHandler(handler)

	for _, tc := range []struct {
		name             string
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expected         int
	}{
		{name: "first request", req: testRequest, expected: 1},
		{name: "same request as the first one, directly use cache", req: testRequest, expected: 1},
		{name: "non query range request won't be cached", req: testRequest2, expected: 2},
		{name: "do it again", req: testRequest2, expected: 3},
		{name: "different max source resolution but still same level", req: testRequest3, expected: 3},
		{name: "different max source resolution and different level", req: testRequest4, expected: 4},
		{
			name: "request but will be partitioned",
			req: &ThanosRequest{
				Path:  "/api/v1/query_range",
				Start: timestamp.FromTime(now.Add(-time.Hour)),
				End:   timestamp.FromTime(now.Add(time.Hour)),
				Step:  10 * seconds,
			},
			expected: 5,
		},
		{
			name: "same query as the previous one",
			req: &ThanosRequest{
				Path:  "/api/v1/query_range",
				Start: timestamp.FromTime(now.Add(-time.Hour)),
				End:   timestamp.FromTime(now.Add(time.Hour)),
				Step:  10 * seconds,
			},
			expected: 6,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := codec.EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}

// counter is a mock handler used to test retry and split.
// Copied from Loki https://github.com/grafana/loki/blob/master/pkg/querier/queryrange/roundtrip_test.go#L526.
func counter() (*int, http.Handler) {
	count := 0
	var lock sync.Mutex
	return &count, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		count++
	})
}

// promqlResults is a mock handler used to test cache middleware.
// Modified from Loki https://github.com/grafana/loki/blob/master/pkg/querier/queryrange/roundtrip_test.go#L547.
func promqlResults() (*int, http.Handler) {
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
		if err := json.NewEncoder(w).Encode(q); err != nil {
			panic(err)
		}
		count++
	})
}
