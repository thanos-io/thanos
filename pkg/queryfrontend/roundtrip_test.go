// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/weaveworks/common/user"

	"github.com/thanos-io/thanos/pkg/testutil"
)

// fakeLimits implements the Cortex queryrange.Limits interface.
type fakeLimits struct{}

func (l *fakeLimits) MaxQueryLength(_ string) time.Duration {
	return 24 * time.Hour
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
	testRequest := &queryrange.PrometheusRequest{
		Path:  "/api/v1/query_range",
		Start: 100,
		End:   1100,
		Step:  10,
	}

	for _, tc := range []struct {
		name             string
		maxRetries       int
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expected         int
	}{
		{
			name:             "no retry, get counter value 1",
			maxRetries:       0,
			req:              testRequest,
			handlerAndResult: counter,
			expected:         1,
		},
		{
			name:             "retry set to 1",
			maxRetries:       1,
			req:              testRequest,
			handlerAndResult: counter,
			expected:         1,
		},
		{
			name:             "retry set to 3",
			maxRetries:       3,
			req:              testRequest,
			handlerAndResult: counter,
			expected:         3,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {
			cache := NewFifoCacheConfig("1MB", 1000, time.Minute)
			tpw, err := NewTripperWare(&fakeLimits{}, cache, queryrange.PrometheusCodec, nil, false,
				time.Hour, tc.maxRetries, nil, log.NewNopLogger())
			testutil.Ok(t, err)

			rt, err := newFakeRoundTripper()
			testutil.Ok(t, err)
			res, handler := tc.handlerAndResult()
			rt.setHandler(handler)

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := queryrange.PrometheusCodec.EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.NotOk(t, err)

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

// TestRoundTripRetryMiddleware tests the split interval middleware.
func TestRoundTripSplitIntervalMiddleware(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name             string
		splitInterval    time.Duration
		req              queryrange.Request
		handlerAndResult func() (*int, http.Handler)
		expected         int
	}{
		{
			name: "disable split",
			req: &queryrange.PrometheusRequest{
				Path:  "/api/v1/query_range",
				Start: 0,
				End:   1000,
				Step:  10,
			},
			splitInterval:    0,
			handlerAndResult: counter,
			expected:         1,
		},
		{
			name: "won't be split",
			req: &queryrange.PrometheusRequest{
				Path:  "/api/v1/query_range",
				Start: timestamp.FromTime(now.Add(-1 * time.Hour)),
				End:   timestamp.FromTime(now),
				Step:  1400000,
			},
			splitInterval:    2 * time.Hour,
			handlerAndResult: counter,
			expected:         1,
		},
		{
			name: "split to 2 requests",
			req: &queryrange.PrometheusRequest{
				Path:  "/api/v1/query_range",
				Start: timestamp.FromTime(now.Add(-3 * time.Hour)),
				End:   timestamp.FromTime(now),
				Step:  1400000,
			},
			splitInterval:    2 * time.Hour,
			handlerAndResult: counter,
			expected:         2,
		},
	} {

		t.Run(tc.name, func(t *testing.T) {
			cache := NewFifoCacheConfig("1MB", 1000, time.Minute)
			tpw, err := NewTripperWare(&fakeLimits{}, cache, queryrange.PrometheusCodec, nil, false,
				tc.splitInterval, 0, nil, log.NewNopLogger())
			testutil.Ok(t, err)

			rt, err := newFakeRoundTripper()
			testutil.Ok(t, err)
			res, handler := tc.handlerAndResult()
			rt.setHandler(handler)

			ctx := user.InjectOrgID(context.Background(), "1")
			httpReq, err := queryrange.PrometheusCodec.EncodeRequest(ctx, tc.req)
			testutil.Ok(t, err)

			_, err = tpw(rt).RoundTrip(httpReq)
			testutil.NotOk(t, err)

			testutil.Equals(t, tc.expected, *res)
		})

	}
}
