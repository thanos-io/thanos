// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/querier/stats"
)

func Test_statsMiddleware_AddsHeaderWithStats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		forceStats   bool
		peakSamples  int32
		totalSamples int64
	}{
		{
			name:         "With forceStats true",
			forceStats:   true,
			peakSamples:  100,
			totalSamples: 1000,
		},
		{
			name:         "With forceStats false",
			forceStats:   false,
			peakSamples:  200,
			totalSamples: 2000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fakeHandler := &fakeHandler{
				response: &PrometheusResponse{
					Status: "success",
					Data: PrometheusData{
						ResultType: "vector",
						Result:     []SampleStream{},
						Stats: &PrometheusResponseStats{
							Samples: &PrometheusResponseSamplesStats{
								TotalQueryableSamples: tt.totalSamples,
								PeakSamples:           tt.peakSamples,
							},
						},
					},
				},
			}

			middleware := NewStatsMiddleware(tt.forceStats)
			wrappedHandler := middleware.Wrap(fakeHandler)

			origCtx := context.Background()
			qryStats, ctx := stats.ContextWithEmptyStats(origCtx)

			resp, err := wrappedHandler.Do(ctx, &PrometheusRequest{
				Path:  "/api/v1/query_range",
				Start: 1536673680 * 1e3,
				End:   1536716898 * 1e3,
				Step:  120 * 1e3,
				Query: "sum(container_memory_rss) by (namespace)",
				Headers: []*PrometheusRequestHeader{
					{
						Name:   "Accept",
						Values: []string{"application/json"},
					},
				},
			})
			require.NoError(t, err)

			if tt.forceStats {
				require.Equal(t, fakeHandler.request.GetStats(), "all")
			}

			promResp, ok := resp.(*PrometheusResponse)
			require.True(t, ok)

			assert.Equal(t, qryStats.LoadPeakSamples(), tt.peakSamples)
			assert.Equal(t, qryStats.LoadTotalSamples(), tt.totalSamples)
			assert.Equal(t, promResp.Data.Stats.Samples.PeakSamples, tt.peakSamples)
			assert.Equal(t, promResp.Data.Stats.Samples.TotalQueryableSamples, tt.totalSamples)
		})
	}
}

type fakeHandler struct {
	request  Request
	response Response
}

func (f *fakeHandler) Do(ctx context.Context, r Request) (Response, error) {
	f.request = r
	return f.response, nil
}
