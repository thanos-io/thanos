// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"io"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestInstrumentedStoreServer(t *testing.T) {
	t.Parallel()

	// Series 1-5 have 1, 2, 3, 4, 5 chunks respectively.
	responses := []*storepb.SeriesResponse{
		storeSeriesResponse(t, labels.FromStrings("series", "1"), makeSamples(2)),
		storeSeriesResponse(t, labels.FromStrings("series", "2"), makeSamples(2), makeSamples(2)),
		// Batched response, as produced by batchableServer when ResponseBatchSize > 1.
		storepb.NewBatchResponse([]*storepb.Series{
			storeSeriesResponse(t, labels.FromStrings("series", "3"), makeSamples(2), makeSamples(2), makeSamples(2)).GetSeries(),
			storeSeriesResponse(t, labels.FromStrings("series", "4"), makeSamples(2), makeSamples(2), makeSamples(2), makeSamples(2)).GetSeries(),
			storeSeriesResponse(t, labels.FromStrings("series", "5"), makeSamples(2), makeSamples(2), makeSamples(2), makeSamples(2), makeSamples(2)).GetSeries(),
		}),
	}

	reg := prometheus.NewRegistry()
	store := NewInstrumentedStoreServer(reg, newStoreServerStub(responses))

	client := storepb.ServerAsClient(store, atomic.Bool{})
	seriesClient, err := client.Series(context.Background(), &storepb.SeriesRequest{})
	testutil.Ok(t, err)
	for {
		if _, err = seriesClient.Recv(); err == io.EOF {
			break
		}
		testutil.Ok(t, err)
	}

	// 5 series total: 2 single, 3 batched. Chunks: 1+2+3+4+5 = 15.
	mfs, err := reg.Gather()
	testutil.Ok(t, err)
	sums := map[string]float64{}
	for _, mf := range mfs {
		if h := mf.GetMetric()[0].GetHistogram(); h != nil {
			testutil.Equals(t, uint64(1), h.GetSampleCount())
			sums[mf.GetName()] = h.GetSampleSum()
		}
	}
	testutil.Equals(t, map[string]float64{
		"thanos_store_server_series_requested": 5,
		"thanos_store_server_chunks_requested": 15,
	}, sums)
}
