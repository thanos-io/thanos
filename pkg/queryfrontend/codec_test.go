// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//nolint:goconst
package queryfrontend

import (
	"context"
	"net/http"
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/weaveworks/common/httpgrpc"
)

func TestCodec_DecodeRequest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		url             string
		partialResponse bool
		expectedError   error
		expectedRequest *ThanosRequest
	}{
		{
			name:            "instant query, no params set",
			url:             "/api/v1/query",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "" to a valid timestamp`),
		},
		{
			name:            "cannot parse start",
			url:             "/api/v1/query_range?start=foo",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "foo" to a valid timestamp`),
		},
		{
			name:            "cannot parse end",
			url:             "/api/v1/query_range?start=123&end=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "bar" to a valid timestamp`),
		},
		{
			name:            "end before start",
			url:             "/api/v1/query_range?start=123&end=0",
			partialResponse: false,
			expectedError:   errEndBeforeStart,
		},
		{
			name:            "cannot parse step",
			url:             "/api/v1/query_range?start=123&end=456&step=baz",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse \"baz\" to a valid duration"),
		},
		{
			name:            "step == 0",
			url:             "/api/v1/query_range?start=123&end=456&step=0",
			partialResponse: false,
			expectedError:   errNegativeStep,
		},
		{
			name:            "step too small",
			url:             "/api/v1/query_range?start=0&end=11001&step=1",
			partialResponse: false,
			expectedError:   errStepTooSmall,
		},
		{
			name:            "cannot parse dedup",
			url:             "/api/v1/query_range?start=123&end=456&step=1&dedup=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse parameter dedup"),
		},
		{
			name:            "cannot parse downsampling resolution",
			url:             "/api/v1/query_range?start=123&end=456&step=1&max_source_resolution=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse parameter max_source_resolution"),
		},
		{
			name:            "negative downsampling resolution",
			url:             "/api/v1/query_range?start=123&end=456&step=1&max_source_resolution=-1",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "negative max_source_resolution is not accepted. Try a positive integer"),
		},
		{
			name: "auto downsampling enabled",
			url:  "/api/v1/query_range?start=123&end=456&step=10&max_source_resolution=auto",
			expectedRequest: &ThanosRequest{
				Path:                "/api/v1/query_range",
				Start:               123000,
				End:                 456000,
				Step:                10000,
				MaxSourceResolution: 2000,
				AutoDownsampling:    true,
				Dedup:               true,
				StoreMatchers:       [][]storepb.LabelMatcher{},
			},
		},
		{
			name:            "cannot parse partial_response",
			url:             "/api/v1/query_range?start=123&end=456&step=1&partial_response=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse parameter partial_response"),
		},
		{
			name:            "partial_response default to true",
			url:             "/api/v1/query_range?start=123&end=456&step=1",
			partialResponse: true,
			expectedRequest: &ThanosRequest{
				Path:            "/api/v1/query_range",
				Start:           123000,
				End:             456000,
				Step:            1000,
				Dedup:           true,
				PartialResponse: true,
				StoreMatchers:   [][]storepb.LabelMatcher{},
			},
		},
		{
			name:            "partial_response default to false, but set to true in query",
			url:             "/api/v1/query_range?start=123&end=456&step=1&partial_response=true",
			partialResponse: false,
			expectedRequest: &ThanosRequest{
				Path:            "/api/v1/query_range",
				Start:           123000,
				End:             456000,
				Step:            1000,
				Dedup:           true,
				PartialResponse: true,
				StoreMatchers:   [][]storepb.LabelMatcher{},
			},
		},
		{
			name:            "replicaLabels",
			url:             "/api/v1/query_range?start=123&end=456&step=1&replicaLabels[]=foo&replicaLabels[]=bar",
			partialResponse: false,
			expectedRequest: &ThanosRequest{
				Path:          "/api/v1/query_range",
				Start:         123000,
				End:           456000,
				Step:          1000,
				Dedup:         true,
				ReplicaLabels: []string{"foo", "bar"},
				StoreMatchers: [][]storepb.LabelMatcher{},
			},
		},
		{
			name:            "storeMatchers",
			url:             `/api/v1/query_range?start=123&end=456&step=1&storeMatch[]={__address__="localhost:10901", cluster="test"}`,
			partialResponse: false,
			expectedRequest: &ThanosRequest{
				Path:  "/api/v1/query_range",
				Start: 123000,
				End:   456000,
				Step:  1000,
				Dedup: true,
				StoreMatchers: [][]storepb.LabelMatcher{
					{
						storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: "__address__", Value: "localhost:10901"},
						storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: "cluster", Value: "test"},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, tc.url, nil)
			testutil.Ok(t, err)

			codec := NewThanosCodec(tc.partialResponse)
			req, err := codec.DecodeRequest(context.Background(), r)
			if tc.expectedError != nil {
				testutil.Equals(t, err, tc.expectedError)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, req, tc.expectedRequest)
			}
		})
	}
}

func TestCodec_EncodeRequest(t *testing.T) {
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
			name: "normal thanos request",
			req: &ThanosRequest{
				Start: 123000,
				End:   456000,
				Step:  1000,
			},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get("start") == "123" &&
					r.URL.Query().Get("end") == "456" &&
					r.URL.Query().Get("step") == "1"
			},
		},
		{
			name: "Dedup enabled",
			req: &ThanosRequest{
				Start: 123000,
				End:   456000,
				Step:  1000,
				Dedup: true,
			},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get("start") == "123" &&
					r.URL.Query().Get("end") == "456" &&
					r.URL.Query().Get("step") == "1" &&
					r.URL.Query().Get("dedup") == "true"
			},
		},
		{
			name: "Partial response set to true",
			req: &ThanosRequest{
				Start:           123000,
				End:             456000,
				Step:            1000,
				PartialResponse: true,
			},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get("start") == "123" &&
					r.URL.Query().Get("end") == "456" &&
					r.URL.Query().Get("step") == "1" &&
					r.URL.Query().Get("partial_response") == "true"
			},
		},
		{
			name: "Downsampling resolution set to 5m",
			req: &ThanosRequest{
				Start:               123000,
				End:                 456000,
				Step:                1000,
				MaxSourceResolution: int64(compact.ResolutionLevel5m),
			},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get("start") == "123" &&
					r.URL.Query().Get("end") == "456" &&
					r.URL.Query().Get("step") == "1" &&
					r.URL.Query().Get("max_source_resolution") == "300"
			},
		},
		{
			name: "Downsampling resolution set to 1h",
			req: &ThanosRequest{
				Start:               123000,
				End:                 456000,
				Step:                1000,
				MaxSourceResolution: int64(compact.ResolutionLevel1h),
			},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get("start") == "123" &&
					r.URL.Query().Get("end") == "456" &&
					r.URL.Query().Get("step") == "1" &&
					r.URL.Query().Get("max_source_resolution") == "3600"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Default partial response value doesn't matter when encoding requests.
			codec := NewThanosCodec(false)
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
