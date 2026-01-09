// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package querypb

import (
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// NewQueryBatchResponse creates a QueryResponse with a batch of timeseries.
func NewQueryBatchResponse(series []*prompb.TimeSeries) *QueryResponse {
	return &QueryResponse{
		Result: &QueryResponse_TimeseriesBatch{
			TimeseriesBatch: &TimeSeriesBatch{
				Series: series,
			},
		},
	}
}

// NewQueryRangeBatchResponse creates a QueryRangeResponse with a batch of timeseries.
func NewQueryRangeBatchResponse(series []*prompb.TimeSeries) *QueryRangeResponse {
	return &QueryRangeResponse{
		Result: &QueryRangeResponse_TimeseriesBatch{
			TimeseriesBatch: &TimeSeriesBatch{
				Series: series,
			},
		},
	}
}
