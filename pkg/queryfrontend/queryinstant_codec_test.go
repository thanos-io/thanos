// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/compact"
)

func TestQueryInstantCodec_DecodeRequest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		url             string
		partialResponse bool
		expectedError   error
		expectedRequest *ThanosQueryInstantRequest
	}{
		{
			name:            "cannot parse time",
			url:             "/api/v1/query?time=foo",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "foo" to a valid timestamp`),
		},
		{
			name:            "parse time",
			url:             "/api/v1/query?time=123",
			partialResponse: false,
			expectedRequest: &ThanosQueryInstantRequest{
				Path:          "/api/v1/query",
				Time:          123000,
				Dedup:         true,
				StoreMatchers: [][]*labels.Matcher{},
			},
		},
		{
			name:            "parse query",
			url:             "/api/v1/query?time=123&query=up",
			partialResponse: false,
			expectedRequest: &ThanosQueryInstantRequest{
				Path:          "/api/v1/query",
				Query:         "up",
				Time:          123000,
				Dedup:         true,
				StoreMatchers: [][]*labels.Matcher{},
			},
		},
		{
			name:            "cannot parse dedup",
			url:             "/api/v1/query?dedup=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse parameter dedup"),
		},
		{
			name:            "cannot parse downsampling resolution",
			url:             "/api/v1/query?max_source_resolution=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse parameter max_source_resolution"),
		},
		{
			name:            "negative downsampling resolution",
			url:             "/api/v1/query?max_source_resolution=-1",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "negative max_source_resolution is not accepted. Try a positive integer"),
		},
		{
			name: "auto downsampling enabled",
			url:  "/api/v1/query?max_source_resolution=auto",
			expectedRequest: &ThanosQueryInstantRequest{
				Path:             "/api/v1/query",
				AutoDownsampling: true,
				Dedup:            true,
				StoreMatchers:    [][]*labels.Matcher{},
			},
		},
		{
			name:            "cannot parse partial_response",
			url:             "/api/v1/query?partial_response=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse parameter partial_response"),
		},
		{
			name:            "partial_response default to true",
			url:             "/api/v1/query",
			partialResponse: true,
			expectedRequest: &ThanosQueryInstantRequest{
				Path:            "/api/v1/query",
				Dedup:           true,
				PartialResponse: true,
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "partial_response default to false, but set to true in query",
			url:             "/api/v1/query?partial_response=true",
			partialResponse: false,
			expectedRequest: &ThanosQueryInstantRequest{
				Path:            "/api/v1/query",
				Dedup:           true,
				PartialResponse: true,
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "replicaLabels",
			url:             "/api/v1/query?replicaLabels[]=foo&replicaLabels[]=bar",
			partialResponse: false,
			expectedRequest: &ThanosQueryInstantRequest{
				Path:          "/api/v1/query",
				Dedup:         true,
				ReplicaLabels: []string{"foo", "bar"},
				StoreMatchers: [][]*labels.Matcher{},
			},
		},
		{
			name:            "storeMatchers",
			url:             `/api/v1/query?storeMatch[]={__address__="localhost:10901", cluster="test"}`,
			partialResponse: false,
			expectedRequest: &ThanosQueryInstantRequest{
				Path:  "/api/v1/query",
				Dedup: true,
				StoreMatchers: [][]*labels.Matcher{
					{
						labels.MustNewMatcher(labels.MatchEqual, "__address__", "localhost:10901"),
						labels.MustNewMatcher(labels.MatchEqual, "cluster", "test"),
					},
				},
			},
		},
		{
			name:            "lookback_delta",
			url:             "/api/v1/query?lookback_delta=1000",
			partialResponse: false,
			expectedRequest: &ThanosQueryInstantRequest{
				Path:          "/api/v1/query",
				Dedup:         true,
				LookbackDelta: 1000000,
				StoreMatchers: [][]*labels.Matcher{},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, tc.url, nil)
			testutil.Ok(t, err)

			codec := NewThanosQueryInstantCodec(tc.partialResponse)
			req, err := codec.DecodeRequest(context.Background(), r, nil)
			if tc.expectedError != nil {
				testutil.Equals(t, err, tc.expectedError)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, req, tc.expectedRequest)
			}
		})
	}
}

func TestQueryInstantCodec_EncodeRequest(t *testing.T) {
	for _, tc := range []struct {
		name          string
		expectedError error
		checkFunc     func(r *http.Request) bool
		req           queryrange.Request
	}{
		{
			name:          "prometheus request, invalid format",
			req:           &queryrange.PrometheusRequest{},
			expectedError: httpgrpc.Errorf(http.StatusBadRequest, "invalid request format"),
		},
		{
			name: "empty thanos request",
			req:  &ThanosQueryInstantRequest{},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue("time") == "" && r.FormValue("query") == ""
			},
		},
		{
			name: "query set",
			req:  &ThanosQueryInstantRequest{Query: "up"},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue("query") == "up"
			},
		},
		{
			name: "time set",
			req:  &ThanosQueryInstantRequest{Time: 123000},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue("time") == "123"
			},
		},
		{
			name: "query and time set",
			req:  &ThanosQueryInstantRequest{Time: 123000, Query: "foo"},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue("time") == "123" && r.FormValue("query") == "foo"
			},
		},
		{
			name: "Dedup disabled",
			req: &ThanosQueryInstantRequest{
				Dedup: false,
			},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue(queryv1.DedupParam) == "false"
			},
		},
		{
			name: "Partial response set to true",
			req: &ThanosQueryInstantRequest{
				PartialResponse: true,
			},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue(queryv1.PartialResponseParam) == "true"
			},
		},
		{
			name: "Downsampling resolution set to 5m",
			req: &ThanosQueryInstantRequest{
				MaxSourceResolution: int64(compact.ResolutionLevel5m),
			},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue(queryv1.MaxSourceResolutionParam) == "300"
			},
		},
		{
			name: "Downsampling resolution set to 1h",
			req: &ThanosQueryInstantRequest{
				MaxSourceResolution: int64(compact.ResolutionLevel1h),
			},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue(queryv1.MaxSourceResolutionParam) == "3600"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Default partial response value doesn't matter when encoding requests.
			codec := NewThanosQueryInstantCodec(false)
			r, err := codec.EncodeRequest(context.TODO(), tc.req)
			if tc.expectedError != nil {
				testutil.Equals(t, err, tc.expectedError)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.checkFunc(r), true)
			}
		})
	}
}

func TestMergeResponse(t *testing.T) {
	codec := NewThanosQueryInstantCodec(false)
	for _, tc := range []struct {
		name         string
		resps        []queryrange.Response
		expectedResp queryrange.Response
		expectedErr  error
	}{
		{
			name:  "empty response",
			resps: []queryrange.Response{},
			expectedResp: &queryrange.PrometheusInstantQueryResponse{
				Status: queryrange.StatusSuccess,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Vector{},
					},
				},
			},
		},
		{
			name: "one response",
			resps: []queryrange.Response{
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Vector{
								Vector: &queryrange.Vector{
									Samples: []*queryrange.Sample{
										{
											Sample: cortexpb.Sample{TimestampMs: 0, Value: 1},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
											})),
										},
									},
								},
							},
						},
					},
				},
			},
			expectedResp: &queryrange.PrometheusInstantQueryResponse{
				Status: queryrange.StatusSuccess,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Vector{
							Vector: &queryrange.Vector{
								Samples: []*queryrange.Sample{
									{
										Sample: cortexpb.Sample{TimestampMs: 0, Value: 1},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
										})),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "merge two responses",
			resps: []queryrange.Response{
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Vector{
								Vector: &queryrange.Vector{
									Samples: []*queryrange.Sample{
										{
											Sample: cortexpb.Sample{TimestampMs: 0, Value: 1},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "foo",
											})),
										},
									},
								},
							},
						},
					},
				},
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Vector{
								Vector: &queryrange.Vector{
									Samples: []*queryrange.Sample{
										{
											Sample: cortexpb.Sample{TimestampMs: 0, Value: 2},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "bar",
											})),
										},
									},
								},
							},
						},
					},
				},
			},
			expectedResp: &queryrange.PrometheusInstantQueryResponse{
				Status: queryrange.StatusSuccess,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Vector{
							Vector: &queryrange.Vector{
								Samples: []*queryrange.Sample{
									{
										Sample: cortexpb.Sample{TimestampMs: 0, Value: 2},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"job":      "bar",
										})),
									},
									{
										Sample: cortexpb.Sample{TimestampMs: 0, Value: 1},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"job":      "foo",
										})),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "merge multiple responses with same label sets, won't happen if sharding is enabled on downstream querier",
			resps: []queryrange.Response{
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Vector{
								Vector: &queryrange.Vector{
									Samples: []*queryrange.Sample{
										{
											Sample: cortexpb.Sample{TimestampMs: 0, Value: 1},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "foo",
											})),
										},
									},
								},
							},
						},
					},
				},
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValVector.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Vector{
								Vector: &queryrange.Vector{
									Samples: []*queryrange.Sample{
										{
											Sample: cortexpb.Sample{TimestampMs: 1, Value: 2},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "foo",
											})),
										},
									},
								},
							},
						},
					},
				},
			},
			expectedResp: &queryrange.PrometheusInstantQueryResponse{
				Status: queryrange.StatusSuccess,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Vector{
							Vector: &queryrange.Vector{
								Samples: []*queryrange.Sample{
									{
										Sample: cortexpb.Sample{TimestampMs: 1, Value: 2},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"job":      "foo",
										})),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "responses don't contain vector, return empty vector",
			resps: []queryrange.Response{
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValScalar.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Scalar{
								Scalar: &cortexpb.Sample{
									TimestampMs: 0,
									Value:       1,
								},
							},
						},
					},
				},
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValScalar.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Scalar{
								Scalar: &cortexpb.Sample{
									TimestampMs: 0,
									Value:       2,
								},
							},
						},
					},
				},
			},
			expectedResp: &queryrange.PrometheusInstantQueryResponse{
				Status: queryrange.StatusSuccess,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Vector{
							Vector: &queryrange.Vector{
								Samples: []*queryrange.Sample{},
							},
						},
					},
				},
			},
		},
		{
			name: "merge two matrix responses with non-duplicate samples",
			resps: []queryrange.Response{
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Matrix{
								Matrix: &queryrange.Matrix{
									SampleStreams: []*queryrange.SampleStream{
										{
											Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "bar",
											})),
										},
										{
											Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "foo",
											})),
										},
									},
								},
							},
						},
					},
				},
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Matrix{
								Matrix: &queryrange.Matrix{
									SampleStreams: []*queryrange.SampleStream{
										{
											Samples: []cortexpb.Sample{{TimestampMs: 2, Value: 3}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "bar",
											})),
										},
										{
											Samples: []cortexpb.Sample{{TimestampMs: 2, Value: 3}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "foo",
											})),
										},
									},
								},
							},
						},
					},
				},
			},
			expectedResp: &queryrange.PrometheusInstantQueryResponse{
				Status: queryrange.StatusSuccess,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Matrix{
							Matrix: &queryrange.Matrix{
								SampleStreams: []*queryrange.SampleStream{
									{
										Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}, {TimestampMs: 2, Value: 3}},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"job":      "bar",
										})),
									},
									{
										Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}, {TimestampMs: 2, Value: 3}},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"job":      "foo",
										})),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "merge two matrix responses with duplicate samples",
			resps: []queryrange.Response{
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Matrix{
								Matrix: &queryrange.Matrix{
									SampleStreams: []*queryrange.SampleStream{
										{
											Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "bar",
											})),
										},
										{
											Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "foo",
											})),
										},
									},
								},
							},
						},
					},
				},
				&queryrange.PrometheusInstantQueryResponse{
					Status: queryrange.StatusSuccess,
					Data: queryrange.PrometheusInstantQueryData{
						ResultType: model.ValMatrix.String(),
						Result: queryrange.PrometheusInstantQueryResult{
							Result: &queryrange.PrometheusInstantQueryResult_Matrix{
								Matrix: &queryrange.Matrix{
									SampleStreams: []*queryrange.SampleStream{
										{
											Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "bar",
											})),
										},
										{
											Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
											Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
												"__name__": "up",
												"job":      "foo",
											})),
										},
									},
								},
							},
						},
					},
				},
			},
			expectedResp: &queryrange.PrometheusInstantQueryResponse{
				Status: queryrange.StatusSuccess,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Matrix{
							Matrix: &queryrange.Matrix{
								SampleStreams: []*queryrange.SampleStream{
									{
										Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"job":      "bar",
										})),
									},
									{
										Samples: []cortexpb.Sample{{TimestampMs: 1, Value: 2}},
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"job":      "foo",
										})),
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := codec.MergeResponse(tc.resps...)
			testutil.Equals(t, err, tc.expectedErr)
			testutil.Equals(t, resp, tc.expectedResp)
		})
	}
}

func TestDecodeResponse(t *testing.T) {
	codec := NewThanosQueryInstantCodec(false)
	headers := []*queryrange.PrometheusResponseHeader{
		{Name: "Content-Type", Values: []string{"application/json"}},
	}
	for _, tc := range []struct {
		name             string
		body             string
		expectedResponse queryrange.Response
		expectedErr      error
	}{
		{
			name: "empty vector",
			body: `{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [

    ]
  }
}`,
			expectedResponse: &queryrange.PrometheusInstantQueryResponse{
				Status:  queryrange.StatusSuccess,
				Headers: headers,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Vector{
							Vector: &queryrange.Vector{
								Samples: []*queryrange.Sample{},
							},
						},
					},
				},
			},
		},
		{
			name: "vector",
			body: `{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "up",
          "instance": "localhost:9090",
          "job": "prometheus"
        },
        "value": [
          1661020672.043,
          "1"
        ]
      }
    ]
  }
}`,
			expectedResponse: &queryrange.PrometheusInstantQueryResponse{
				Status:  queryrange.StatusSuccess,
				Headers: headers,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValVector.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Vector{
							Vector: &queryrange.Vector{
								Samples: []*queryrange.Sample{
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"instance": "localhost:9090",
											"job":      "prometheus",
										})),
										Sample: cortexpb.Sample{TimestampMs: 1661020672043, Value: 1},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "scalar",
			body: `{
  "status": "success",
  "data": {
    "resultType": "scalar",
    "result": [
      1661020145.547,
      "1"
    ]
  }
}`,
			expectedResponse: &queryrange.PrometheusInstantQueryResponse{
				Status:  queryrange.StatusSuccess,
				Headers: headers,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValScalar.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Scalar{
							Scalar: &cortexpb.Sample{TimestampMs: 1661020145547, Value: 1},
						},
					},
				},
			},
		},
		{
			name: "string",
			body: `{
  "status": "success",
  "data": {
    "resultType": "string",
    "result": [
      1661020232.424,
      "test"
    ]
  }
}`,
			expectedResponse: &queryrange.PrometheusInstantQueryResponse{
				Status:  queryrange.StatusSuccess,
				Headers: headers,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValString.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_StringSample{
							StringSample: &queryrange.StringSample{TimestampMs: 1661020232424, Value: "test"},
						},
					},
				},
			},
		},
		{
			name: "empty matrix",
			body: `{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [

    ]
  }
}`,
			expectedResponse: &queryrange.PrometheusInstantQueryResponse{
				Status:  queryrange.StatusSuccess,
				Headers: headers,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Matrix{
							Matrix: &queryrange.Matrix{
								SampleStreams: []*queryrange.SampleStream{},
							},
						},
					},
				},
			},
		},
		{
			name: "matrix",
			body: `{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "up",
          "instance": "localhost:9090",
          "job": "prometheus"
        },
        "values": [
          [
            1661020250.310,
            "1"
          ],
          [
            1661020265.309,
            "1"
          ],
          [
            1661020280.309,
            "1"
          ],
          [
            1661020295.310,
            "1"
          ]
        ]
      }
    ]
  }
}`,
			expectedResponse: &queryrange.PrometheusInstantQueryResponse{
				Status:  queryrange.StatusSuccess,
				Headers: headers,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Matrix{
							Matrix: &queryrange.Matrix{
								SampleStreams: []*queryrange.SampleStream{
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "up",
											"instance": "localhost:9090",
											"job":      "prometheus",
										})),
										Samples: []cortexpb.Sample{
											{TimestampMs: 1661020250310, Value: 1},
											{TimestampMs: 1661020265309, Value: 1},
											{TimestampMs: 1661020280309, Value: 1},
											{TimestampMs: 1661020295310, Value: 1},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "matrix with multiple metrics",
			body: `{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "prometheus_http_requests_total",
          "code": "200",
          "handler": "/favicon.ico",
          "instance": "localhost:9090",
          "job": "prometheus"
        },
        "values": [
          [
            1661020430.311,
            "1"
          ],
          [
            1661020445.312,
            "1"
          ],
          [
            1661020460.313,
            "1"
          ],
          [
            1661020475.313,
            "1"
          ]
        ]
      },
      {
        "metric": {
          "__name__": "prometheus_http_requests_total",
          "code": "200",
          "handler": "/metrics",
          "instance": "localhost:9090",
          "job": "prometheus"
        },
        "values": [
          [
            1661020430.311,
            "33"
          ],
          [
            1661020445.312,
            "34"
          ],
          [
            1661020460.313,
            "35"
          ],
          [
            1661020475.313,
            "36"
          ]
        ]
      }
    ]
  }
}`,
			expectedResponse: &queryrange.PrometheusInstantQueryResponse{
				Status:  queryrange.StatusSuccess,
				Headers: headers,
				Data: queryrange.PrometheusInstantQueryData{
					ResultType: model.ValMatrix.String(),
					Result: queryrange.PrometheusInstantQueryResult{
						Result: &queryrange.PrometheusInstantQueryResult_Matrix{
							Matrix: &queryrange.Matrix{
								SampleStreams: []*queryrange.SampleStream{
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "prometheus_http_requests_total",
											"code":     "200",
											"handler":  "/favicon.ico",
											"instance": "localhost:9090",
											"job":      "prometheus",
										})),
										Samples: []cortexpb.Sample{
											{TimestampMs: 1661020430311, Value: 1},
											{TimestampMs: 1661020445312, Value: 1},
											{TimestampMs: 1661020460313, Value: 1},
											{TimestampMs: 1661020475313, Value: 1},
										},
									},
									{
										Labels: cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{
											"__name__": "prometheus_http_requests_total",
											"code":     "200",
											"handler":  "/metrics",
											"instance": "localhost:9090",
											"job":      "prometheus",
										})),
										Samples: []cortexpb.Sample{
											{TimestampMs: 1661020430311, Value: 33},
											{TimestampMs: 1661020445312, Value: 34},
											{TimestampMs: 1661020460313, Value: 35},
											{TimestampMs: 1661020475313, Value: 36},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	} {
		resp := &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewBuffer([]byte(tc.body))),
		}
		gotResponse, err := codec.DecodeResponse(context.Background(), resp, nil)
		testutil.Equals(t, tc.expectedErr, err)
		testutil.Equals(t, tc.expectedResponse, gotResponse)
	}
}
