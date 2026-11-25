// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestLimiter(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	numSamples := 60
	series := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labels.FromStrings("series", "1"), makeSamples(numSamples)),
		storeSeriesResponse(t, labels.FromStrings("series", "2"), makeSamples(numSamples)),
		storeSeriesResponse(t, labels.FromStrings("series", "3"), makeSamples(numSamples)),
	}
	batchedSeries := []*storepb.SeriesResponse{
		storepb.NewBatchResponse([]*storepb.Series{
			series[0].GetSeries(),
			nil,
			series[1].GetSeries(),
			series[2].GetSeries(),
		}),
	}
	nonSeriesResponses := []*storepb.SeriesResponse{
		storepb.NewWarnSeriesResponse(errors.New("warning")),
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
			name: "series below limit",
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
			name: "batched series below limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  3,
				SamplesPerRequest: uint64(3 * MaxSamplesPerChunk),
			},
			series: batchedSeries,
		},
		{
			name: "non-series responses bypass limits",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  1,
				SamplesPerRequest: 1,
			},
			series: nonSeriesResponses,
		},
		{
			name: "batched series over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  2,
				SamplesPerRequest: 0,
			},
			series: batchedSeries,
			err:    "failed to send series: limit 2 violated (got 3)",
		},
		{
			name: "chunks below limit",
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
		{
			name: "batched chunks over limit",
			limits: SeriesSelectLimits{
				SeriesPerRequest:  0,
				SamplesPerRequest: uint64(2 * MaxSamplesPerChunk),
			},
			series: batchedSeries,
			err:    "failed to send samples: limit 240 violated (got 360)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			store := NewLimitedStoreServer(newStoreServerStub(test.series), prometheus.NewRegistry(), test.limits)
			client := storepb.ServerAsClient(store, atomic.Bool{})
			seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{})
			testutil.Ok(t, err)
			for {
				_, err = seriesClient.Recv()
				if err == io.EOF {
					err = nil
					break
				}
				if err != nil {
					break
				}
			}
			if test.err == "" {
				testutil.Ok(t, err)
			} else {
				testutil.NotOk(t, err)
				testutil.Assert(t, test.err == err.Error(), "want %s, got %s", test.err, err.Error())
			}
		})
	}
}

func TestLimitedStoreServerMetadataSeriesLimitPushdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		noPushdown  bool
		seriesLimit uint64
		skipChunks  bool
		reqLimit    int64
		available   int
		wantLimit   int64
		wantSent    int
		wantErr     string
	}{
		{
			name:        "no limit in the request",
			seriesLimit: 10, skipChunks: true, available: 100,
			wantLimit: 11, wantSent: 10, wantErr: "failed to send series: limit 10 violated (got 11)",
		},
		{
			name:        "request limit above the series limit",
			seriesLimit: 10, skipChunks: true, reqLimit: 500, available: 100,
			wantLimit: 11, wantSent: 10, wantErr: "failed to send series: limit 10 violated (got 11)",
		},
		{
			name:        "request limit below the series limit",
			seriesLimit: 10, skipChunks: true, reqLimit: 5, available: 100,
			wantLimit: 5, wantSent: 5,
		},
		{
			name:        "exactly the series limit",
			seriesLimit: 10, skipChunks: true, available: 10,
			wantLimit: 11, wantSent: 10,
		},
		{
			name:        "no series limit",
			seriesLimit: 0, skipChunks: true, available: 20,
			wantLimit: 0, wantSent: 20,
		},
		{
			name:        "request with chunks",
			seriesLimit: 10, skipChunks: false, available: 100,
			wantLimit: 0, wantSent: 10, wantErr: "failed to send series: limit 10 violated (got 11)",
		},
		{
			name:       "pushdown not enabled",
			noPushdown: true, seriesLimit: 10, skipChunks: true, available: 100,
			wantLimit: 0, wantSent: 10, wantErr: "failed to send series: limit 10 violated (got 11)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			backend := &limitHonouringStoreServer{available: test.available}
			limits := SeriesSelectLimits{SeriesPerRequest: test.seriesLimit}
			var opts []LimitedStoreServerOption
			if !test.noPushdown {
				opts = append(opts, WithMetadataSeriesLimitPushdown(limits))
			}
			store := NewLimitedStoreServer(backend, prometheus.NewRegistry(), limits, opts...)
			client := storepb.ServerAsClient(store, atomic.Bool{})

			req := &storepb.SeriesRequest{SkipChunks: test.skipChunks, Limit: test.reqLimit}
			seriesClient, err := client.Series(ctx, req)
			testutil.Ok(t, err)

			sent, gotErr := 0, ""
			for {
				resp, err := seriesClient.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					gotErr = err.Error()
					break
				}
				if resp.GetSeries() != nil {
					sent++
				}
			}
			testutil.Equals(t, test.wantErr, gotErr)
			testutil.Equals(t, test.wantLimit, backend.gotLimit)
			testutil.Equals(t, test.wantSent, sent)
			testutil.Equals(t, test.reqLimit, req.Limit)
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

// limitHonouringStoreServer sends at most Limit series, like Prometheus does.
type limitHonouringStoreServer struct {
	storepb.StoreServer
	available int
	gotLimit  int64
}

func (m *limitHonouringStoreServer) Series(r *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	m.gotLimit = r.Limit
	n := m.available
	if r.Limit > 0 && int(r.Limit) < n {
		n = int(r.Limit)
	}
	for i := 0; i < n; i++ {
		lset := labelpb.ZLabelsFromPromLabels(labels.FromStrings("series", strconv.Itoa(i)))
		if err := server.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: lset})); err != nil {
			return err
		}
	}
	return nil
}
