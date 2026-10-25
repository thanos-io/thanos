// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestInstrumentedServer(t *testing.T) {
	t.Parallel()

	series := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labels.FromStrings("series", "1"), makeSamples(60)),
		storeSeriesResponse(t, labels.FromStrings("series", "2"), makeSamples(60), makeSamples(60)),
		storeSeriesResponse(t, labels.FromStrings("series", "3"), makeSamples(30)),
	}
	batchedSeries := []*storepb.SeriesResponse{
		storepb.NewBatchResponse([]*storepb.Series{
			series[0].GetSeries(),
			series[1].GetSeries(),
		}),
		storepb.NewBatchResponse([]*storepb.Series{
			series[2].GetSeries(),
		}),
	}
	for _, test := range []struct {
		name      string
		responses []*storepb.SeriesResponse
	}{
		{name: "series", responses: series},
		{name: "batched series", responses: batchedSeries},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			reg := prometheus.NewRegistry()
			store := NewInstrumentedStoreServer(reg, newStoreServerStub(test.responses))
			client := storepb.ServerAsClient(store, atomic.Bool{})
			seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{})
			testutil.Ok(t, err)
			for {
				_, err = seriesClient.Recv()
				if err == io.EOF {
					break
				}
				testutil.Ok(t, err)
			}

			testutil.Equals(t, 3.0, histogramSum(t, reg, "thanos_store_server_series_requested"))
			testutil.Equals(t, 4.0, histogramSum(t, reg, "thanos_store_server_chunks_requested"))
		})
	}
}

func histogramSum(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	testutil.Ok(t, err)
	for _, family := range families {
		if family.GetName() == name {
			return family.GetMetric()[0].GetHistogram().GetSampleSum()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
