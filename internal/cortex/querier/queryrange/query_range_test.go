// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"

	"github.com/prometheus/common/model"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
)

func TestRequest(t *testing.T) {
	// Create a Copy parsedRequest to assign the expected headers to the request without affecting other tests using the global.
	// The test below adds a Test-Header header to the request and expects it back once the encode/decode of request is done via PrometheusCodec
	parsedRequestWithHeaders := *parsedRequest
	parsedRequestWithHeaders.Headers = reqHeaders
	for _, tc := range []struct {
		url         string
		expected    Request
		expectedErr error
	}{
		{
			url:      query,
			expected: &parsedRequestWithHeaders,
		},
		{
			url:         "api/v1/query_range?start=foo&stats=all",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "invalid parameter \"start\"; cannot parse \"foo\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=bar",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "invalid parameter \"end\"; cannot parse \"bar\" to a valid timestamp"),
		},
		{
			url:         "api/v1/query_range?start=123&end=0",
			expectedErr: errEndBeforeStart,
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=baz",
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, "invalid parameter \"step\"; cannot parse \"baz\" to a valid duration"),
		},
		{
			url:         "api/v1/query_range?start=123&end=456&step=-1",
			expectedErr: errNegativeStep,
		},
		{
			url:         "api/v1/query_range?start=0&end=11001&step=1",
			expectedErr: errStepTooSmall,
		},
	} {
		t.Run(tc.url, func(t *testing.T) {
			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)
			r.Header.Add("Test-Header", "test")

			ctx := user.InjectOrgID(context.Background(), "1")

			// Get a deep copy of the request with Context changed to ctx
			r = r.Clone(ctx)

			req, err := PrometheusCodec.DecodeRequest(ctx, r, []string{"Test-Header"})
			if err != nil {
				require.EqualValues(t, tc.expectedErr, err)
				return
			}
			require.EqualValues(t, tc.expected, req)

			rdash, err := PrometheusCodec.EncodeRequest(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.url, rdash.RequestURI)
		})
	}
}

func TestResponse(t *testing.T) {
	for i, tc := range []struct {
		body     string
		expected *PrometheusResponse
	}{
		{
			body:     responseBody,
			expected: withHeaders(parsedResponse, respHeaders),
		},
		{
			body:     histogramResponseBody,
			expected: withHeaders(parsedHistogramResponse, respHeaders),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       ioutil.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			resp, err := PrometheusCodec.DecodeResponse(context.Background(), response, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, resp)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          ioutil.NopCloser(bytes.NewBuffer([]byte(tc.body))),
				ContentLength: int64(len(tc.body)),
			}
			resp2, err := PrometheusCodec.EncodeResponse(context.Background(), resp)
			require.NoError(t, err)
			assert.Equal(t, response, resp2)
		})
	}
}

func TestResponseWithStats(t *testing.T) {
	for i, tc := range []struct {
		body     string
		expected *PrometheusResponse
	}{
		{
			body: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"foo":"bar"},"values":[[1536673680,"137"],[1536673780,"137"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1536673680,5],[1536673780,5]]}}}}`,
			expected: &PrometheusResponse{
				Status: "success",
				Data: PrometheusData{
					ResultType: model.ValMatrix.String(),
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{
								{Name: "foo", Value: "bar"},
							},
							Samples: []cortexpb.Sample{
								{Value: 137, TimestampMs: 1536673680000},
								{Value: 137, TimestampMs: 1536673780000},
							},
						},
					},
					Stats: &PrometheusResponseStats{
						Samples: &PrometheusResponseSamplesStats{
							TotalQueryableSamples: 10,
							TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
								{Value: 5, TimestampMs: 1536673680000},
								{Value: 5, TimestampMs: 1536673780000},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			tc.expected.Headers = respHeaders
			response := &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       ioutil.NopCloser(bytes.NewBuffer([]byte(tc.body))),
			}
			resp, err := PrometheusCodec.DecodeResponse(context.Background(), response, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, resp)

			// Reset response, as the above call will have consumed the body reader.
			response = &http.Response{
				StatusCode:    200,
				Header:        http.Header{"Content-Type": []string{"application/json"}},
				Body:          ioutil.NopCloser(bytes.NewBuffer([]byte(tc.body))),
				ContentLength: int64(len(tc.body)),
			}
			resp2, err := PrometheusCodec.EncodeResponse(context.Background(), resp)
			require.NoError(t, err)
			assert.Equal(t, response, resp2)
		})
	}
}

func TestMergeAPIResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    []Response
		expected Response
	}{
		{
			name:  "No responses shouldn't panic and return a non-null result and result type.",
			input: []Response{},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "A single empty response shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "Multiple empty responses shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "Basic merging of two responses.",
			input: []Response{
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{},
								Samples: []cortexpb.Sample{
									{Value: 0, TimestampMs: 0},
									{Value: 1, TimestampMs: 1},
								},
							},
						},
					},
				},
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{},
								Samples: []cortexpb.Sample{
									{Value: 2, TimestampMs: 2},
									{Value: 3, TimestampMs: 3},
								},
							},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{},
							Samples: []cortexpb.Sample{
								{Value: 0, TimestampMs: 0},
								{Value: 1, TimestampMs: 1},
								{Value: 2, TimestampMs: 2},
								{Value: 3, TimestampMs: 3},
							},
						},
					},
				},
			},
		},

		{
			name: "Merging of responses when labels are in different order.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[0,"0"],[1,"1"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 0, TimestampMs: 0},
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
				},
			},
		},

		{
			name: "Merging of samples where there is single overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is multiple partial overlaps.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is complete overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
				},
			},
		},
		{
			name: "[stats] A single empty response shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
						Stats:      &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
					Stats:      &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}},
				},
			},
		},

		{
			name: "[stats] Multiple empty responses shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
						Stats:      &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}},
					},
				},
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
						Stats:      &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
					Stats:      &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{}},
				},
			},
		},

		{
			name: "[stats] Basic merging of two responses.",
			input: []Response{
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{},
								Samples: []cortexpb.Sample{
									{Value: 0, TimestampMs: 0},
									{Value: 1, TimestampMs: 1},
								},
							},
						},
						Stats: &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{
							TotalQueryableSamples: 20,
							TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
								{Value: 5, TimestampMs: 0},
								{Value: 15, TimestampMs: 1},
							},
						}},
					},
				},
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []cortexpb.LabelAdapter{},
								Samples: []cortexpb.Sample{
									{Value: 2, TimestampMs: 2},
									{Value: 3, TimestampMs: 3},
								},
							},
						},
						Stats: &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{
							TotalQueryableSamples: 10,
							TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
								{Value: 5, TimestampMs: 2},
								{Value: 5, TimestampMs: 3},
							},
						}},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{},
							Samples: []cortexpb.Sample{
								{Value: 0, TimestampMs: 0},
								{Value: 1, TimestampMs: 1},
								{Value: 2, TimestampMs: 2},
								{Value: 3, TimestampMs: 3},
							},
						},
					},
					Stats: &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{
						TotalQueryableSamples: 30,
						TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 5, TimestampMs: 0},
							{Value: 15, TimestampMs: 1},
							{Value: 5, TimestampMs: 2},
							{Value: 5, TimestampMs: 3},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of samples where there is single overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"]]}],"stats":{"samples":{"totalQueryableSamples":10,"totalQueryableSamplesPerStep":[[1,5],[2,5]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,5],[3,15]]}}}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
					Stats: &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{
						TotalQueryableSamples: 25,
						TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 5, TimestampMs: 1000},
							{Value: 5, TimestampMs: 2000},
							{Value: 15, TimestampMs: 3000},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of multiple responses with some overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[3,"3"],[4,"4"],[5,"5"]]}],"stats":{"samples":{"totalQueryableSamples":12,"totalQueryableSamplesPerStep":[[3,3],[4,4],[5,5]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"],[4,"4"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3],[4,4]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[5,"5"],[6,"6"],[7,"7"]]}],"stats":{"samples":{"totalQueryableSamples":18,"totalQueryableSamplesPerStep":[[5,5],[6,6],[7,7]]}}}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
								{Value: 6, TimestampMs: 6000},
								{Value: 7, TimestampMs: 7000},
							},
						},
					},
					Stats: &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{
						TotalQueryableSamples: 28,
						TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 1, TimestampMs: 1000},
							{Value: 2, TimestampMs: 2000},
							{Value: 3, TimestampMs: 3000},
							{Value: 4, TimestampMs: 4000},
							{Value: 5, TimestampMs: 5000},
							{Value: 6, TimestampMs: 6000},
							{Value: 7, TimestampMs: 7000},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of samples where there is multiple partial overlaps.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":6,"totalQueryableSamplesPerStep":[[1,1],[2,2],[3,3]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3],[4,4],[5,5]]}}}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
					Stats: &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{
						TotalQueryableSamples: 15,
						TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 1, TimestampMs: 1000},
							{Value: 2, TimestampMs: 2000},
							{Value: 3, TimestampMs: 3000},
							{Value: 4, TimestampMs: 4000},
							{Value: 5, TimestampMs: 5000},
						},
					}},
				},
			},
		},
		{
			name: "[stats] Merging of samples where there is complete overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3]]}}}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}],"stats":{"samples":{"totalQueryableSamples":20,"totalQueryableSamplesPerStep":[[2,2],[3,3],[4,4],[5,5]]}}}}`),
			},
			expected: &PrometheusResponse{
				Status: StatusSuccess,
				Data: PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []cortexpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []cortexpb.Sample{
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
					Stats: &PrometheusResponseStats{Samples: &PrometheusResponseSamplesStats{
						TotalQueryableSamples: 14,
						TotalQueryableSamplesPerStep: []*PrometheusResponseQueryableSamplesStatsPerStep{
							{Value: 2, TimestampMs: 2000},
							{Value: 3, TimestampMs: 3000},
							{Value: 4, TimestampMs: 4000},
							{Value: 5, TimestampMs: 5000},
						},
					}},
				},
			},
		}} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := PrometheusCodec.MergeResponse(nil, tc.input...)
			require.NoError(t, err)
			require.Equal(t, tc.expected, output)
		})
	}
}

func mustParse(t *testing.T, response string) Response {
	var resp PrometheusResponse
	// Needed as goimports automatically add a json import otherwise.
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	require.NoError(t, json.Unmarshal([]byte(response), &resp))
	return &resp
}

func withHeaders(response *PrometheusResponse, headers []*PrometheusResponseHeader) *PrometheusResponse {
	r := *response
	r.Headers = headers
	return &r
}
