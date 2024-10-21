package queryrange

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_statsMiddleware_AddsHeaderWithStats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		forceStats    bool
		peakSamples   int
		totalSamples  int64
		expectedStats bool
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
								PeakSamples:           int32(tt.peakSamples),
							},
						},
					},
				},
			}

			middleware := NewStatsMiddleware(tt.forceStats)
			wrappedHandler := middleware.Wrap(fakeHandler)

			resp, err := wrappedHandler.Do(context.Background(), &PrometheusRequest{
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

			assert.Contains(t, promResp.Headers, &PrometheusResponseHeader{
				Name:   StatsTotalSamplesHeader,
				Values: []string{strconv.FormatInt(tt.totalSamples, 10)},
			})
			assert.Contains(t, promResp.Headers, &PrometheusResponseHeader{
				Name:   StatsPeakSamplesHeader,
				Values: []string{strconv.FormatInt(int64(tt.peakSamples), 10)},
			})

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
