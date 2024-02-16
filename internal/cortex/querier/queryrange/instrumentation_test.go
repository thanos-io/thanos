// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"context"
	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockRequest struct {
	start int64
	end   int64
}

func (m mockRequest) Reset() {
}

func (m mockRequest) String() string {
	return "mock"
}

func (m mockRequest) ProtoMessage() {
}

func (m mockRequest) GetStart() int64 {
	return m.start
}

func (m mockRequest) GetEnd() int64 {
	return m.end
}

func (m mockRequest) GetStep() int64 {
	return 0
}

func (m mockRequest) GetQuery() string {
	return ""
}

func (m mockRequest) GetCachingOptions() CachingOptions {
	return CachingOptions{}
}

func (m mockRequest) WithStartEnd(startTime int64, endTime int64) Request {
	return mockRequest{start: startTime, end: endTime}
}

func (m mockRequest) WithQuery(query string) Request {
	return mockRequest{}
}

func (m mockRequest) LogToSpan(span opentracing.Span) {}

func (m mockRequest) GetStats() string {
	return ""
}

func (m mockRequest) WithStats(stats string) Request {
	return mockRequest{}
}

func TestGetRangeBucket(t *testing.T) {
	cases := []struct {
		start    int64
		end      int64
		expected string
	}{
		{0, -1, "Invalid"},
		{1, 60 * 60 * 1000, "1h"},
		{1, 6 * 60 * 60 * 1000, "6h"},
		{1, 12 * 60 * 60 * 1000, "12h"},
		{1, 24 * 60 * 60 * 1000, "1d"},
		{1, 48 * 60 * 60 * 1000, "2d"},
		{1, 7 * 24 * 60 * 60 * 1000, "7d"},
		{1, 30 * 24 * 60 * 60 * 1000, "30d"},
		{1, 31 * 24 * 60 * 60 * 1000, "+INF"},
	}

	for _, c := range cases {
		req := mockRequest{start: c.start, end: c.end}
		bucket := getRangeBucket(req)
		if bucket != c.expected {
			t.Errorf("getRangeBucket(%v) returned %v, expected %v", req, bucket, c.expected)
		}
	}
}

func TestInstrumentMiddleware(t *testing.T) {
	registry := prometheus.DefaultRegisterer

	metrics := NewInstrumentMiddlewareMetrics(registry)

	logger := log.NewNopLogger()

	middleware := InstrumentMiddleware("step_align", metrics, logger)

	// Create a new dummy Request object with a duration of 6 hours.
	req := mockRequest{1, 6 * 60 * 60 * 1000}

	// Create a dummy Handler object that just returns a Response object.
	handler := HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
		return Response(nil), nil
	})

	_, err := middleware.Wrap(handler).Do(context.Background(), req)
	assert.NoError(t, err)

	_, error := testutil.CollectAndLint(metrics.duration, "cortex_frontend_query_range_duration_seconds")
	assert.NoError(t, error)
}
