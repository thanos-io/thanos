// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	cortexutil "github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/weaveworks/common/httpgrpc"

	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

var (
	infMinTime = time.Unix(math.MinInt64/1000+62135596801, 0)
	infMaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999)
)

// labelsCodec is used to encode/decode Thanos labels and series requests and responses.
type labelsCodec struct {
	queryrange.Codec
	partialResponse          bool
	defaultMetadataTimeRange time.Duration
}

// NewThanosLabelsCodec initializes a labelsCodec.
func NewThanosLabelsCodec(partialResponse bool, defaultMetadataTimeRange time.Duration) *labelsCodec {
	return &labelsCodec{
		Codec:                    queryrange.PrometheusCodec,
		partialResponse:          partialResponse,
		defaultMetadataTimeRange: defaultMetadataTimeRange,
	}
}

func (c labelsCodec) MergeResponse(responses ...queryrange.Response) (queryrange.Response, error) {
	if len(responses) == 0 {
		return &ThanosLabelsResponse{
			Status: queryrange.StatusSuccess,
			Data:   []string{},
		}, nil
	}

	if len(responses) == 1 {
		return responses[0], nil
	}

	switch responses[0].(type) {
	case *ThanosLabelsResponse:
		set := make(map[string]struct{})

		for _, res := range responses {
			for _, value := range res.(*ThanosLabelsResponse).Data {
				if _, ok := set[value]; !ok {
					set[value] = struct{}{}
				}
			}
		}
		lbls := make([]string, 0, len(set))
		for label := range set {
			lbls = append(lbls, label)
		}

		sort.Strings(lbls)
		return &ThanosLabelsResponse{
			Status: queryrange.StatusSuccess,
			Data:   lbls,
		}, nil
	case *ThanosSeriesResponse:
		seriesData := make([]labelpb.LabelSet, 0)

		// seriesString is used in soring so we don't have to calculate the string of label sets again.
		seriesString := make([]string, 0)
		uniqueSeries := make(map[string]struct{})
		for _, res := range responses {
			for _, series := range res.(*ThanosSeriesResponse).Data {
				s := labelpb.LabelsToPromLabels(series.Labels).String()
				if _, ok := uniqueSeries[s]; !ok {
					seriesData = append(seriesData, series)
					seriesString = append(seriesString, s)
					uniqueSeries[s] = struct{}{}
				}
			}
		}

		sort.Slice(seriesData, func(i, j int) bool {
			return seriesString[i] < seriesString[j]
		})
		return &ThanosSeriesResponse{
			Status: queryrange.StatusSuccess,
			Data:   seriesData,
		}, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}
}

func (c labelsCodec) DecodeRequest(_ context.Context, r *http.Request) (queryrange.Request, error) {
	if err := r.ParseForm(); err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	var (
		req queryrange.Request
		err error
	)
	switch op := getOperation(r); op {
	case labelNamesOp, labelValuesOp:
		req, err = c.parseLabelsRequest(r, op)
	case seriesOp:
		req, err = c.parseSeriesRequest(r)
	}
	if err != nil {
		return nil, err
	}

	return req, nil
}

func (c labelsCodec) EncodeRequest(ctx context.Context, r queryrange.Request) (*http.Request, error) {
	var u *url.URL
	switch thanosReq := r.(type) {
	case *ThanosLabelsRequest:
		var params = url.Values{
			"start":                      []string{encodeTime(thanosReq.Start)},
			"end":                        []string{encodeTime(thanosReq.End)},
			queryv1.PartialResponseParam: []string{strconv.FormatBool(thanosReq.PartialResponse)},
		}
		if len(thanosReq.StoreMatchers) > 0 {
			params[queryv1.StoreMatcherParam] = matchersToStringSlice(thanosReq.StoreMatchers)
		}
		u = &url.URL{
			Path:     thanosReq.Path,
			RawQuery: params.Encode(),
		}
	case *ThanosSeriesRequest:
		var params = url.Values{
			"start":                      []string{encodeTime(thanosReq.Start)},
			"end":                        []string{encodeTime(thanosReq.End)},
			queryv1.DedupParam:           []string{strconv.FormatBool(thanosReq.Dedup)},
			queryv1.PartialResponseParam: []string{strconv.FormatBool(thanosReq.PartialResponse)},
			queryv1.ReplicaLabelsParam:   thanosReq.ReplicaLabels,
		}
		if len(thanosReq.Matchers) > 0 {
			params[queryv1.MatcherParam] = matchersToStringSlice(thanosReq.Matchers)
		}
		if len(thanosReq.StoreMatchers) > 0 {
			params[queryv1.StoreMatcherParam] = matchersToStringSlice(thanosReq.StoreMatchers)
		}
		u = &url.URL{
			Path:     thanosReq.Path,
			RawQuery: params.Encode(),
		}
	default:
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid request format")
	}

	req := &http.Request{
		Method:     "GET",
		RequestURI: u.String(), // This is what the httpgrpc code looks at.
		URL:        u,
		Body:       http.NoBody,
		Header:     http.Header{},
	}

	return req.WithContext(ctx), nil
}

func (c labelsCodec) DecodeResponse(ctx context.Context, r *http.Response, req queryrange.Request) (queryrange.Response, error) {
	if r.StatusCode/100 != 2 {
		body, _ := ioutil.ReadAll(r.Body)
		return nil, httpgrpc.Errorf(r.StatusCode, string(body))
	}
	log, ctx := spanlogger.New(ctx, "ParseQueryResponse") //nolint:ineffassign,staticcheck
	defer log.Finish()

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err) //nolint:errcheck
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	log.LogFields(otlog.Int("bytes", len(buf)))

	switch req.(type) {
	case *ThanosLabelsRequest:
		var resp ThanosLabelsResponse
		if err := json.Unmarshal(buf, &resp); err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
		}
		for h, hv := range r.Header {
			resp.Headers = append(resp.Headers, &ResponseHeader{Name: h, Values: hv})
		}
		return &resp, nil
	case *ThanosSeriesRequest:
		var resp ThanosSeriesResponse
		if err := json.Unmarshal(buf, &resp); err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
		}
		for h, hv := range r.Header {
			resp.Headers = append(resp.Headers, &ResponseHeader{Name: h, Values: hv})
		}
		return &resp, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid request type")
	}
}

func (c labelsCodec) EncodeResponse(ctx context.Context, res queryrange.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	var (
		b   []byte
		err error
	)
	switch resp := res.(type) {
	case *ThanosLabelsResponse:
		sp.LogFields(otlog.Int("labels", len(resp.Data)))
		b, err = json.Marshal(resp)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
		}
	case *ThanosSeriesResponse:
		sp.LogFields(otlog.Int("series", len(resp.Data)))
		b, err = json.Marshal(resp)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
		}
	default:
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}

	sp.LogFields(otlog.Int("bytes", len(b)))
	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:       ioutil.NopCloser(bytes.NewBuffer(b)),
		StatusCode: http.StatusOK,
	}
	return &resp, nil
}

func (c labelsCodec) parseLabelsRequest(r *http.Request, op string) (queryrange.Request, error) {
	var (
		result ThanosLabelsRequest
		err    error
	)
	result.Start, result.End, err = parseMetadataTimeRange(r, c.defaultMetadataTimeRange)
	if err != nil {
		return nil, err
	}

	result.PartialResponse, err = parsePartialResponseParam(r.FormValue(queryv1.PartialResponseParam), c.partialResponse)
	if err != nil {
		return nil, err
	}

	result.StoreMatchers, err = parseMatchersParam(r.Form[queryv1.StoreMatcherParam])
	if err != nil {
		return nil, err
	}

	result.Path = r.URL.Path

	if op == labelValuesOp {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) > 1 {
			result.Label = parts[len(parts)-2]
		}
	}

	for _, value := range r.Header.Values(cacheControlHeader) {
		if strings.Contains(value, noStoreValue) {
			result.CachingOptions.Disabled = true
			break
		}
	}

	return &result, nil
}

func (c labelsCodec) parseSeriesRequest(r *http.Request) (queryrange.Request, error) {
	var (
		result ThanosSeriesRequest
		err    error
	)
	result.Start, result.End, err = parseMetadataTimeRange(r, c.defaultMetadataTimeRange)
	if err != nil {
		return nil, err
	}

	result.Matchers, err = parseMatchersParam(r.Form[queryv1.MatcherParam])
	if err != nil {
		return nil, err
	}

	result.Dedup, err = parseEnableDedupParam(r.FormValue(queryv1.DedupParam))
	if err != nil {
		return nil, err
	}

	result.PartialResponse, err = parsePartialResponseParam(r.FormValue(queryv1.PartialResponseParam), c.partialResponse)
	if err != nil {
		return nil, err
	}

	if len(r.Form[queryv1.ReplicaLabelsParam]) > 0 {
		result.ReplicaLabels = r.Form[queryv1.ReplicaLabelsParam]
	}

	result.StoreMatchers, err = parseMatchersParam(r.Form[queryv1.StoreMatcherParam])
	if err != nil {
		return nil, err
	}

	result.Path = r.URL.Path

	for _, value := range r.Header.Values(cacheControlHeader) {
		if strings.Contains(value, noStoreValue) {
			result.CachingOptions.Disabled = true
			break
		}
	}

	return &result, nil
}

func parseMetadataTimeRange(r *http.Request, defaultMetadataTimeRange time.Duration) (int64, int64, error) {
	// If start and end time not specified as query parameter, we get the range from the beginning of time by default.
	var defaultStartTime, defaultEndTime time.Time
	if defaultMetadataTimeRange == 0 {
		defaultStartTime = infMinTime
		defaultEndTime = infMaxTime
	} else {
		now := time.Now()
		defaultStartTime = now.Add(-defaultMetadataTimeRange)
		defaultEndTime = now
	}

	start, err := parseTimeParam(r, "start", defaultStartTime)
	if err != nil {
		return 0, 0, err
	}
	end, err := parseTimeParam(r, "end", defaultEndTime)
	if err != nil {
		return 0, 0, err
	}
	if end < start {
		return 0, 0, errEndBeforeStart
	}

	return start, end, nil
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (int64, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return timestamp.FromTime(defaultValue), nil
	}
	result, err := cortexutil.ParseTime(val)
	if err != nil {
		return 0, err
	}
	return result, nil
}
