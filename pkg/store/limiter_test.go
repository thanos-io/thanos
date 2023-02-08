// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestLimiter(t *testing.T) {
	c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	l := NewLimiter(10, c)

	testutil.Ok(t, l.Reserve(5))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.Ok(t, l.Reserve(5))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l.Reserve(1))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l.Reserve(2))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))
}

func TestRateLimitedServer(t *testing.T) {
	series := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labels.FromStrings("series", "1"), []sample{{t: 0, v: 0}}),
		storeSeriesResponse(t, labels.FromStrings("series", "2"), []sample{{t: 0, v: 0}}),
		storeSeriesResponse(t, labels.FromStrings("series", "3"), []sample{{t: 0, v: 0}}),
	}
	tests := []struct {
		name   string
		limits RateLimits
		series []*storepb.SeriesResponse
		err    string
	}{
		{
			name: "no limits",
			limits: RateLimits{
				SeriesPerRequest: 0,
				ChunksPerRequest: 0,
			},
			series: series,
		},
		{
			name: "series bellow limit",
			limits: RateLimits{
				SeriesPerRequest: 3,
				ChunksPerRequest: 0,
			},
			series: series,
		},
		{
			name: "series over limit",
			limits: RateLimits{
				SeriesPerRequest: 2,
				ChunksPerRequest: 0,
			},
			series: series,
			err:    "failed to send series: limit 2 violated (got 3)",
		},
		{
			name: "chunks bellow limit",
			limits: RateLimits{
				SeriesPerRequest: 0,
				ChunksPerRequest: 3,
			},
			series: series,
		},
		{
			name: "chunks over limit",
			limits: RateLimits{
				SeriesPerRequest: 0,
				ChunksPerRequest: 2,
			},
			series: series,
			err:    "failed to send chunks: limit 2 violated (got 3)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			store := NewRateLimitedStoreServer(newStoreServerStub(test.series), prometheus.NewRegistry(), test.limits)
			seriesServer := storepb.NewInProcessStream(ctx, 10)
			err := store.Series(&storepb.SeriesRequest{}, seriesServer)
			if test.err == "" {
				testutil.Ok(t, err)
			} else {
				testutil.NotOk(t, err)
				testutil.Assert(t, test.err == err.Error(), "want %s, got %s", test.err, err.Error())
			}
		})
	}
}

type testStoreServer struct {
	storepb.StoreServer
	responses []*storepb.SeriesResponse
}

func newStoreServerStub(responses []*storepb.SeriesResponse) *testStoreServer {
	return &testStoreServer{responses: responses}
}

func (m *testStoreServer) Series(_ *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	for _, r := range m.responses {
		if err := server.Send(r); err != nil {
			return err
		}
	}
	return nil
}
