// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func newBatchableServer(
	upstream storepb.Store_SeriesServer,
	batchSize int,
) storepb.Store_SeriesServer {
	switch batchSize {
	case 0:
		return &passthroughServer{Store_SeriesServer: upstream}
	case 1:
		return &passthroughServer{Store_SeriesServer: upstream}
	default:
		return &batchableServer{
			Store_SeriesServer: upstream,
			batchSize:          batchSize,
			series:             make([]*storepb.Series, 0, batchSize),
		}
	}
}

// batchableServer is a flushableServer that allows sending a batch of Series per message.
type batchableServer struct {
	storepb.Store_SeriesServer
	batchSize int
	series    []*storepb.Series
}

func (b *batchableServer) Flush() error {
	if len(b.series) != 0 {
		if err := b.Store_SeriesServer.Send(storepb.NewBatchResponse(b.series)); err != nil {
			return err
		}
		b.series = make([]*storepb.Series, 0, b.batchSize)
	}

	return nil
}

func (b *batchableServer) Send(response *storepb.SeriesResponse) error {
	series := response.GetSeries()
	if series == nil {
		if len(b.series) > 0 {
			if err := b.Store_SeriesServer.Send(storepb.NewBatchResponse(b.series)); err != nil {
				return err
			}
			b.series = make([]*storepb.Series, 0, b.batchSize)
		}
		return b.Store_SeriesServer.Send(response)
	}

	b.series = append(b.series, series)

	if len(b.series) >= b.batchSize {
		if err := b.Store_SeriesServer.Send(storepb.NewBatchResponse(b.series)); err != nil {
			return err
		}
		b.series = make([]*storepb.Series, 0, b.batchSize)
	}

	return nil
}

// DefaultResponseBatchSize is the default number of timeseries to batch per gRPC response message.
// This value provides a good balance between reducing per-message overhead and keeping message sizes reasonable.
const DefaultResponseBatchSize = 64
