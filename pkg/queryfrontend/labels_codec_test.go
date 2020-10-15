// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/weaveworks/common/httpgrpc"

	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestLabelsCodec_DecodeRequest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		url             string
		partialResponse bool
		expectedError   error
		expectedRequest ThanosRequest
	}{
		{
			name:            "label_names cannot parse start",
			url:             "/api/v1/labels?start=foo",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "foo" to a valid timestamp`),
		},
		{
			name:            "label_values cannot parse start",
			url:             "/api/v1/label/__name__/values?start=foo",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "foo" to a valid timestamp`),
		},
		{
			name:            "series cannot parse start",
			url:             "/api/v1/series?start=foo",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "foo" to a valid timestamp`),
		},
		{
			name:            "label_names cannot parse end",
			url:             "/api/v1/labels?start=123&end=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "bar" to a valid timestamp`),
		},
		{
			name:            "label_values cannot parse end",
			url:             "/api/v1/label/__name__/values?start=123&end=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "bar" to a valid timestamp`),
		},
		{
			name:            "series cannot parse end",
			url:             "/api/v1/series?start=123&end=bar",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, `cannot parse "bar" to a valid timestamp`),
		},
		{
			name:            "label_names end before start",
			url:             "/api/v1/labels?start=123&end=0",
			partialResponse: false,
			expectedError:   errEndBeforeStart,
		},
		{
			name:            "label_values end before start",
			url:             "/api/v1/label/__name__/values?start=123&end=0",
			partialResponse: false,
			expectedError:   errEndBeforeStart,
		},
		{
			name:            "series end before start",
			url:             "/api/v1/series?start=123&end=0",
			partialResponse: false,
			expectedError:   errEndBeforeStart,
		},
		{
			name:            "cannot parse partial_response",
			url:             "/api/v1/labels?start=123&end=456&partial_response=boo",
			partialResponse: false,
			expectedError:   httpgrpc.Errorf(http.StatusBadRequest, "cannot parse parameter partial_response"),
		},
		{
			name:            "label_names partial_response default to true",
			url:             "/api/v1/labels?start=123&end=456",
			partialResponse: true,
			expectedRequest: &ThanosLabelsRequest{
				Path:            "/api/v1/labels",
				Start:           123000,
				End:             456000,
				PartialResponse: true,
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "label_values partial_response default to true",
			url:             "/api/v1/label/__name__/values?start=123&end=456",
			partialResponse: true,
			expectedRequest: &ThanosLabelsRequest{
				Path:            "/api/v1/label/__name__/values",
				Start:           123000,
				End:             456000,
				PartialResponse: true,
				Label:           "__name__",
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "series partial_response default to true",
			url:             `/api/v1/series?start=123&end=456&match[]={foo="bar"}`,
			partialResponse: true,
			expectedRequest: &ThanosSeriesRequest{
				Path:            "/api/v1/series",
				Start:           123000,
				End:             456000,
				PartialResponse: true,
				Dedup:           true,
				Matchers:        [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "partial_response default to false, but set to true in query",
			url:             "/api/v1/labels?start=123&end=456&partial_response=true",
			partialResponse: false,
			expectedRequest: &ThanosLabelsRequest{
				Path:            "/api/v1/labels",
				Start:           123000,
				End:             456000,
				PartialResponse: true,
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "storeMatchers",
			url:             `/api/v1/labels?start=123&end=456&storeMatch[]={__address__="localhost:10901", cluster="test"}`,
			partialResponse: false,
			expectedRequest: &ThanosLabelsRequest{
				Path:  "/api/v1/labels",
				Start: 123000,
				End:   456000,
				StoreMatchers: [][]*labels.Matcher{
					{
						labels.MustNewMatcher(labels.MatchEqual, "__address__", "localhost:10901"),
						labels.MustNewMatcher(labels.MatchEqual, "cluster", "test"),
					},
				},
			},
		},
		{
			name:            "series dedup set to false",
			url:             `/api/v1/series?start=123&dedup=false&end=456&match[]={foo="bar"}`,
			partialResponse: false,
			expectedRequest: &ThanosSeriesRequest{
				Path:          "/api/v1/series",
				Start:         123000,
				End:           456000,
				Dedup:         false,
				Matchers:      [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
				StoreMatchers: [][]*labels.Matcher{},
			},
		},
		{
			name:            "series replicaLabels",
			url:             "/api/v1/series?start=123&end=456&replicaLabels[]=foo&replicaLabels[]=bar",
			partialResponse: false,
			expectedRequest: &ThanosSeriesRequest{
				Path:          "/api/v1/series",
				Start:         123000,
				End:           456000,
				Dedup:         true,
				ReplicaLabels: []string{"foo", "bar"},
				Matchers:      [][]*labels.Matcher{},
				StoreMatchers: [][]*labels.Matcher{},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, tc.url, nil)
			testutil.Ok(t, err)

			codec := NewThanosLabelsCodec(tc.partialResponse, 2*time.Hour)
			req, err := codec.DecodeRequest(context.Background(), r)
			if tc.expectedError != nil {
				testutil.Equals(t, tc.expectedError, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.expectedRequest, req)
			}
		})
	}
}

func TestLabelsCodec_EncodeRequest(t *testing.T) {
	const (
		start     = "start"
		end       = "end"
		startTime = "123"
		endTime   = "456"
	)
	for _, tc := range []struct {
		name          string
		expectedError error
		checkFunc     func(r *http.Request) bool
		req           queryrange.Request
	}{
		{
			name:          "prometheus request, invalid format",
			req:           &queryrange.PrometheusRequest{},
			expectedError: httpgrpc.Errorf(http.StatusInternalServerError, "invalid request format"),
		},
		{
			name:          "thanos query range request, invalid format",
			req:           &ThanosQueryRangeRequest{},
			expectedError: httpgrpc.Errorf(http.StatusInternalServerError, "invalid request format"),
		},
		{
			name: "thanos labels names request",
			req:  &ThanosLabelsRequest{Start: 123000, End: 456000, Path: "/api/v1/labels"},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Path == "/api/v1/labels"
			},
		},
		{
			name: "thanos labels values request",
			req:  &ThanosLabelsRequest{Start: 123000, End: 456000, Path: "/api/v1/label/__name__/values"},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Path == "/api/v1/label/__name__/values"
			},
		},
		{
			name: "thanos labels values request, partial response set to true",
			req:  &ThanosLabelsRequest{Start: 123000, End: 456000, Path: "/api/v1/label/__name__/values", PartialResponse: true},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Path == "/api/v1/label/__name__/values" &&
					r.URL.Query().Get(queryv1.PartialResponseParam) == "true"
			},
		},
		{
			name: "thanos series request with empty matchers",
			req:  &ThanosSeriesRequest{Start: 123000, End: 456000, Path: "/api/v1/series"},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Path == "/api/v1/series"
			},
		},
		{
			name: "thanos series request",
			req: &ThanosSeriesRequest{
				Start:    123000,
				End:      456000,
				Path:     "/api/v1/series",
				Matchers: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "cluster", "test")}},
			},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Query().Get(queryv1.MatcherParam) == `{cluster="test"}` &&
					r.URL.Path == "/api/v1/series"
			},
		},
		{
			name: "thanos series request, dedup to true",
			req: &ThanosSeriesRequest{
				Start: 123000,
				End:   456000,
				Path:  "/api/v1/series",
				Dedup: true,
			},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Query().Get(queryv1.DedupParam) == "true" &&
					r.URL.Path == "/api/v1/series"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Default partial response value doesn't matter when encoding requests.
			codec := NewThanosLabelsCodec(false, time.Hour*2)
			r, err := codec.EncodeRequest(context.TODO(), tc.req)
			if tc.expectedError != nil {
				testutil.Equals(t, tc.expectedError, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, true, tc.checkFunc(r))
			}
		})
	}
}

func TestLabelsCodec_DecodeResponse(t *testing.T) {
	labelResponse := &ThanosLabelsResponse{
		Status: "success",
		Data:   []string{"__name__"},
	}
	labelsData, err := json.Marshal(labelResponse)
	testutil.Ok(t, err)

	seriesResponse := &ThanosSeriesResponse{
		Status: "success",
		Data:   []labelpb.LabelSet{{Labels: []labelpb.Label{{Name: "foo", Value: "bar"}}}},
	}
	seriesData, err := json.Marshal(seriesResponse)
	testutil.Ok(t, err)
	for _, tc := range []struct {
		name             string
		expectedError    error
		res              http.Response
		req              queryrange.Request
		expectedResponse queryrange.Response
	}{
		{
			name:          "prometheus request, invalid for labelsCodec",
			req:           &queryrange.PrometheusRequest{},
			res:           http.Response{StatusCode: 200, Body: ioutil.NopCloser(bytes.NewBuffer([]byte("foo")))},
			expectedError: httpgrpc.Errorf(http.StatusInternalServerError, "invalid request type"),
		},
		{
			name:          "thanos query range request, invalid for labelsCodec",
			req:           &ThanosQueryRangeRequest{},
			res:           http.Response{StatusCode: 200, Body: ioutil.NopCloser(bytes.NewBuffer([]byte("foo")))},
			expectedError: httpgrpc.Errorf(http.StatusInternalServerError, "invalid request type"),
		},
		{
			name:             "thanos labels request",
			req:              &ThanosLabelsRequest{},
			res:              http.Response{StatusCode: 200, Body: ioutil.NopCloser(bytes.NewBuffer(labelsData))},
			expectedResponse: labelResponse,
		},
		{
			name:             "thanos series request",
			req:              &ThanosSeriesRequest{},
			res:              http.Response{StatusCode: 200, Body: ioutil.NopCloser(bytes.NewBuffer(seriesData))},
			expectedResponse: seriesResponse,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Default partial response value doesn't matter when encoding requests.
			codec := NewThanosLabelsCodec(false, time.Hour*2)
			r, err := codec.DecodeResponse(context.TODO(), &tc.res, tc.req)
			if tc.expectedError != nil {
				testutil.Equals(t, err, tc.expectedError)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.expectedResponse, r)
			}
		})
	}
}
