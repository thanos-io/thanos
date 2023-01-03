// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	io "io"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func TestLabelsCodec_DecodeRequest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		url             string
		partialResponse bool
		expectedError   error
		expectedRequest ThanosRequestStoreMatcherGetter
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
			url:             `/api/v1/labels?start=123&end=456&match[]={foo="bar"}`,
			partialResponse: true,
			expectedRequest: &ThanosLabelsRequest{
				Path:            "/api/v1/labels",
				Start:           123000,
				End:             456000,
				PartialResponse: true,
				Matchers:        [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "label_values partial_response default to true",
			url:             `/api/v1/label/__name__/values?start=123&end=456&match[]={foo="bar"}`,
			partialResponse: true,
			expectedRequest: &ThanosLabelsRequest{
				Path:            "/api/v1/label/__name__/values",
				Start:           123000,
				End:             456000,
				PartialResponse: true,
				Label:           "__name__",
				Matchers:        [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
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
			url:             `/api/v1/labels?start=123&end=456&partial_response=true&match[]={foo="bar"}`,
			partialResponse: false,
			expectedRequest: &ThanosLabelsRequest{
				Path:            "/api/v1/labels",
				Start:           123000,
				End:             456000,
				PartialResponse: true,
				Matchers:        [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
				StoreMatchers:   [][]*labels.Matcher{},
			},
		},
		{
			name:            "storeMatchers",
			url:             `/api/v1/labels?start=123&end=456&storeMatch[]={__address__="localhost:10901", cluster="test"}`,
			partialResponse: false,
			expectedRequest: &ThanosLabelsRequest{
				Path:     "/api/v1/labels",
				Start:    123000,
				End:      456000,
				Matchers: [][]*labels.Matcher{},
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
			req, err := codec.DecodeRequest(context.Background(), r, nil)
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
		trueStr   = "true"
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
				return r.FormValue(start) == startTime &&
					r.FormValue(end) == endTime &&
					r.URL.Path == "/api/v1/labels"
			},
		},
		{
			name: "thanos labels values request",
			req:  &ThanosLabelsRequest{Start: 123000, End: 456000, Path: "/api/v1/label/__name__/values", Label: "__name__"},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Path == "/api/v1/label/__name__/values"
			},
		},
		{
			name: "thanos labels values request, partial response set to true",
			req:  &ThanosLabelsRequest{Start: 123000, End: 456000, Path: "/api/v1/label/__name__/values", Label: "__name__", PartialResponse: true},
			checkFunc: func(r *http.Request) bool {
				return r.URL.Query().Get(start) == startTime &&
					r.URL.Query().Get(end) == endTime &&
					r.URL.Path == "/api/v1/label/__name__/values" &&
					r.URL.Query().Get(queryv1.PartialResponseParam) == trueStr
			},
		},
		{
			name: "thanos series request with empty matchers",
			req:  &ThanosSeriesRequest{Start: 123000, End: 456000, Path: "/api/v1/series"},
			checkFunc: func(r *http.Request) bool {
				return r.FormValue(start) == startTime &&
					r.FormValue(end) == endTime &&
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
				return r.FormValue(start) == startTime &&
					r.FormValue(end) == endTime &&
					r.FormValue(queryv1.MatcherParam) == `{cluster="test"}` &&
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
				return r.FormValue(start) == startTime &&
					r.FormValue(end) == endTime &&
					r.FormValue(queryv1.DedupParam) == trueStr &&
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

	labelResponseWithHeaders := &ThanosLabelsResponse{
		Status:  "success",
		Data:    []string{"__name__"},
		Headers: []*ResponseHeader{{Name: cacheControlHeader, Values: []string{noStoreValue}}},
	}
	labelsDataWithHeaders, err := json.Marshal(labelResponseWithHeaders)
	testutil.Ok(t, err)

	seriesResponse := &ThanosSeriesResponse{
		Status: "success",
		Data:   []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}},
	}
	seriesData, err := json.Marshal(seriesResponse)
	testutil.Ok(t, err)

	seriesResponseWithHeaders := &ThanosSeriesResponse{
		Status:  "success",
		Data:    []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}},
		Headers: []*ResponseHeader{{Name: cacheControlHeader, Values: []string{noStoreValue}}},
	}
	seriesDataWithHeaders, err := json.Marshal(seriesResponseWithHeaders)
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
			res:           http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBuffer([]byte("foo")))},
			expectedError: httpgrpc.Errorf(http.StatusInternalServerError, "invalid request type"),
		},
		{
			name:          "thanos query range request, invalid for labelsCodec",
			req:           &ThanosQueryRangeRequest{},
			res:           http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBuffer([]byte("foo")))},
			expectedError: httpgrpc.Errorf(http.StatusInternalServerError, "invalid request type"),
		},
		{
			name:             "thanos labels request",
			req:              &ThanosLabelsRequest{},
			res:              http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBuffer(labelsData))},
			expectedResponse: labelResponse,
		},
		{
			name: "thanos labels request with HTTP headers",
			req:  &ThanosLabelsRequest{},
			res: http.Response{
				StatusCode: 200, Body: io.NopCloser(bytes.NewBuffer(labelsDataWithHeaders)),
				Header: map[string][]string{
					cacheControlHeader: {noStoreValue},
				},
			},
			expectedResponse: labelResponseWithHeaders,
		},
		{
			name:             "thanos series request",
			req:              &ThanosSeriesRequest{},
			res:              http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBuffer(seriesData))},
			expectedResponse: seriesResponse,
		},
		{
			name: "thanos series request with HTTP headers",
			req:  &ThanosSeriesRequest{},
			res: http.Response{
				StatusCode: 200, Body: io.NopCloser(bytes.NewBuffer(seriesDataWithHeaders)),
				Header: map[string][]string{
					cacheControlHeader: {noStoreValue},
				},
			},
			expectedResponse: seriesResponseWithHeaders,
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

func TestLabelsCodec_MergeResponse(t *testing.T) {
	for _, tc := range []struct {
		name             string
		expectedError    error
		responses        []queryrange.Response
		expectedResponse queryrange.Response
	}{
		{
			name: "Prometheus range query response format, not valid",
			responses: []queryrange.Response{
				&queryrange.PrometheusResponse{Status: "success"},
			},
			expectedError: httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format"),
		},
		{
			name:             "Empty response",
			responses:        nil,
			expectedResponse: &ThanosLabelsResponse{Status: queryrange.StatusSuccess, Data: []string{}},
		},
		{
			name: "One label response",
			responses: []queryrange.Response{
				&ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9090", "localhost:9091"}},
			},
			expectedResponse: &ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9090", "localhost:9091"}},
		},
		{
			name: "One label response and two empty responses",
			responses: []queryrange.Response{
				&ThanosLabelsResponse{Status: queryrange.StatusSuccess, Data: []string{}},
				&ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9090", "localhost:9091"}},
				&ThanosLabelsResponse{Status: queryrange.StatusSuccess, Data: []string{}},
			},
			expectedResponse: &ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9090", "localhost:9091"}},
		},
		{
			name: "Multiple duplicate label responses",
			responses: []queryrange.Response{
				&ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9090", "localhost:9091"}},
				&ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9091", "localhost:9092"}},
				&ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9092", "localhost:9093"}},
			},
			expectedResponse: &ThanosLabelsResponse{Status: "success",
				Data: []string{"localhost:9090", "localhost:9091", "localhost:9092", "localhost:9093"}},
		},
		// This case shouldn't happen because the responses from Querier are sorted.
		{
			name: "Multiple unordered label responses",
			responses: []queryrange.Response{
				&ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9093", "localhost:9092"}},
				&ThanosLabelsResponse{Status: "success", Data: []string{"localhost:9091", "localhost:9090"}},
			},
			expectedResponse: &ThanosLabelsResponse{Status: "success",
				Data: []string{"localhost:9090", "localhost:9091", "localhost:9092", "localhost:9093"}},
		},
		{
			name: "One series response",
			responses: []queryrange.Response{
				&ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
			},
			expectedResponse: &ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
		},
		{
			name: "One series response and two empty responses",
			responses: []queryrange.Response{
				&ThanosSeriesResponse{Status: queryrange.StatusSuccess},
				&ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
				&ThanosSeriesResponse{Status: queryrange.StatusSuccess},
			},
			expectedResponse: &ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
		},
		{
			name: "Multiple duplicate series responses",
			responses: []queryrange.Response{
				&ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
				&ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
				&ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
			},
			expectedResponse: &ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}}},
		},
		{
			name: "Multiple unordered series responses",
			responses: []queryrange.Response{
				&ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{
					{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}},
					{Labels: []labelpb.ZLabel{{Name: "test", Value: "aaa"}, {Name: "instance", Value: "localhost:9090"}}},
				}},
				&ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{
					{Labels: []labelpb.ZLabel{{Name: "foo", Value: "aaa"}}},
					{Labels: []labelpb.ZLabel{{Name: "test", Value: "bbb"}, {Name: "instance", Value: "localhost:9091"}}},
				}},
			},
			expectedResponse: &ThanosSeriesResponse{Status: "success", Data: []labelpb.ZLabelSet{
				{Labels: []labelpb.ZLabel{{Name: "foo", Value: "aaa"}}},
				{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}},
				{Labels: []labelpb.ZLabel{{Name: "test", Value: "aaa"}, {Name: "instance", Value: "localhost:9090"}}},
				{Labels: []labelpb.ZLabel{{Name: "test", Value: "bbb"}, {Name: "instance", Value: "localhost:9091"}}},
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Default partial response value doesn't matter when encoding requests.
			codec := NewThanosLabelsCodec(false, time.Hour*2)
			r, err := codec.MergeResponse(tc.responses...)
			if tc.expectedError != nil {
				testutil.Equals(t, err, tc.expectedError)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.expectedResponse, r)
			}
		})
	}
}

func BenchmarkLabelsCodecEncodeAndDecodeRequest(b *testing.B) {
	codec := NewThanosLabelsCodec(false, time.Hour*2)
	ctx := context.TODO()

	b.Run("SeriesRequest", func(b *testing.B) {
		req := &ThanosSeriesRequest{
			Start: 123000,
			End:   456000,
			Path:  "/api/v1/series",
			Dedup: true,
		}

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			reqEnc, err := codec.EncodeRequest(ctx, req)
			testutil.Ok(b, err)
			_, err = codec.DecodeRequest(ctx, reqEnc, nil)
			testutil.Ok(b, err)
		}
	})

	b.Run("LabelsRequest", func(b *testing.B) {
		req := &ThanosLabelsRequest{
			Path:            "/api/v1/labels",
			Start:           123000,
			End:             456000,
			PartialResponse: true,
			Matchers:        [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}},
			StoreMatchers:   [][]*labels.Matcher{},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			reqEnc, err := codec.EncodeRequest(ctx, req)
			testutil.Ok(b, err)
			_, err = codec.DecodeRequest(ctx, reqEnc, nil)
			testutil.Ok(b, err)
		}
	})
}

func BenchmarkLabelsCodecDecodeResponse(b *testing.B) {
	codec := NewThanosLabelsCodec(false, time.Hour*2)
	ctx := context.TODO()

	b.Run("SeriesResponse", func(b *testing.B) {
		seriesData, err := json.Marshal(&ThanosSeriesResponse{
			Status: "success",
			Data:   []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}},
		})
		testutil.Ok(b, err)

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			_, err := codec.DecodeResponse(
				ctx,
				makeResponse(seriesData, false),
				&ThanosSeriesRequest{})
			testutil.Ok(b, err)
		}
	})

	b.Run("SeriesResponseWithHeaders", func(b *testing.B) {
		seriesDataWithHeaders, err := json.Marshal(&ThanosSeriesResponse{
			Status:  "success",
			Data:    []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: "foo", Value: "bar"}}}},
			Headers: []*ResponseHeader{{Name: cacheControlHeader, Values: []string{noStoreValue}}},
		})
		testutil.Ok(b, err)

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			_, err := codec.DecodeResponse(
				ctx,
				makeResponse(seriesDataWithHeaders, true),
				&ThanosSeriesRequest{})
			testutil.Ok(b, err)
		}
	})

	b.Run("LabelsResponse", func(b *testing.B) {
		labelsData, err := json.Marshal(&ThanosLabelsResponse{
			Status: "success",
			Data:   []string{"__name__"},
		})
		testutil.Ok(b, err)

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			_, err := codec.DecodeResponse(
				ctx,
				makeResponse(labelsData, false),
				&ThanosLabelsRequest{})
			testutil.Ok(b, err)
		}
	})

	b.Run("LabelsResponseWithHeaders", func(b *testing.B) {
		labelsDataWithHeaders, err := json.Marshal(&ThanosLabelsResponse{
			Status:  "success",
			Data:    []string{"__name__"},
			Headers: []*ResponseHeader{{Name: cacheControlHeader, Values: []string{noStoreValue}}},
		})
		testutil.Ok(b, err)

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			_, err := codec.DecodeResponse(
				ctx,
				makeResponse(labelsDataWithHeaders, true),
				&ThanosLabelsRequest{})
			testutil.Ok(b, err)
		}
	})
}

func BenchmarkLabelsCodecMergeResponses_1(b *testing.B) {
	benchmarkMergeResponses(b, 1)
}

func BenchmarkLabelsCodecMergeResponses_10(b *testing.B) {
	benchmarkMergeResponses(b, 10)
}

func BenchmarkLabelsCodecMergeResponses_100(b *testing.B) {
	benchmarkMergeResponses(b, 100)
}

func BenchmarkLabelsCodecMergeResponses_1000(b *testing.B) {
	benchmarkMergeResponses(b, 1000)
}

func benchmarkMergeResponses(b *testing.B, size int) {
	codec := NewThanosLabelsCodec(false, time.Hour*2)
	queryResLabel, queryResSeries := makeQueryRangeResponses(size)

	b.Run("SeriesResponses", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = codec.MergeResponse(queryResSeries...)
		}
	})

	b.Run("LabelsResponses", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, _ = codec.MergeResponse(queryResLabel...)
		}
	})

}

func makeQueryRangeResponses(size int) ([]queryrange.Response, []queryrange.Response) {
	labelResp := make([]queryrange.Response, 0, size)
	seriesResp := make([]queryrange.Response, 0, size*2)

	// Generate with some duplicated values.
	for i := 0; i < size; i++ {
		labelResp = append(labelResp, &ThanosLabelsResponse{
			Status: "success",
			Data:   []string{fmt.Sprintf("data-%d", i), fmt.Sprintf("data-%d", i+1)},
		})

		seriesResp = append(
			seriesResp,
			&ThanosSeriesResponse{
				Status: "success",
				Data:   []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: fmt.Sprintf("foo-%d", i), Value: fmt.Sprintf("bar-%d", i)}}}},
			},
			&ThanosSeriesResponse{
				Status: "success",
				Data:   []labelpb.ZLabelSet{{Labels: []labelpb.ZLabel{{Name: fmt.Sprintf("foo-%d", i+1), Value: fmt.Sprintf("bar-%d", i+1)}}}},
			},
		)
	}

	return labelResp, seriesResp
}

func makeResponse(data []byte, withHeader bool) *http.Response {
	r := &http.Response{
		StatusCode: 200, Body: io.NopCloser(bytes.NewBuffer(data)),
	}

	if withHeader {
		r.Header = map[string][]string{
			cacheControlHeader: {noStoreValue},
		}
	}

	return r
}
