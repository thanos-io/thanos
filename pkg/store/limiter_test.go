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
	numSamples := 60
	series := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labels.FromStrings("series", "1"), makeSamples(numSamples)),
		storeSeriesResponse(t, labels.FromStrings("series", "2"), makeSamples(numSamples)),
		storeSeriesResponse(t, labels.FromStrings("series", "3"), makeSamples(numSamples)),
	}
	tests := []struct {
		name   string
		limits SeriesSelectLimits
		series []*storepb.SeriesResponse
		err    string
	}{
		{
			name: "no limits",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: 0,
			},
			series: series,
		},
		{
			name: "series bellow limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  3,
				SamplesPerRequest: 0,
			},
			series: series,
		},
		{
			name: "series over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  2,
				SamplesPerRequest: 0,
			},
			series: series,
			err:    "failed to send series: limit 2 violated (got 3)",
		},
		{
			name: "chunks bellow limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: uint64(3 * numSamples * MaxSamplesPerChunk),
			},
			series: series,
		},
		{
			name: "chunks over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: 50,
			},
			series: series,
			err:    "failed to send samples: limit 50 violated (got 120)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			store := NewLimitedStoreServer(newStoreServerStub(test.series), prometheus.NewRegistry(), test.limits)
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

func makeSamples(numSamples int) []sample {
	samples := make([]sample, numSamples)
	for i := range samples {
		samples[i] = sample{t: int64(i), v: float64(i)}
	}
	return samples
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
