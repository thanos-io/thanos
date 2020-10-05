package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	cortexutil "github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/weaveworks/common/httpgrpc"

	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
)

type metadataCodec struct {
	queryrange.Codec
	partialResponse bool
}

func NewThanosMetadataCodec(partialResponse bool) *metadataCodec {
	return &metadataCodec{
		Codec:           queryrange.PrometheusCodec,
		partialResponse: partialResponse,
	}
}

func (c metadataCodec) MergeResponse(responses ...queryrange.Response) (queryrange.Response, error) {
	if len(responses) == 0 {
		return &ThanosLabelsResponse{
			Status: queryrange.StatusSuccess,
			Data:   []string{},
		}, nil
	}

	switch responses[0].(type) {
	case *ThanosLabelsResponse:
		set := make(map[string]struct{})

		for _, res := range responses {
			for _, value := range res.(*ThanosLabelsResponse).Data {
				set[value] = struct{}{}
			}
		}
		labels := make([]string, 0, len(set))
		for label := range set {
			labels = append(labels, label)
		}

		sort.Strings(labels)
		return &ThanosLabelsResponse{
			Status: queryrange.StatusSuccess,
			Data:   labels,
		}, nil
	case *ThanosSeriesResponse:
		metadataResponses := make([]*ThanosSeriesResponse, 0, len(responses))

		for _, res := range responses {
			metadataResponses = append(metadataResponses, res.(*ThanosSeriesResponse))
		}

		return &ThanosSeriesResponse{
			Status: queryrange.StatusSuccess,
			Data:   metadataResponses[0].Data,
		}, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid response format")
	}
}

func (c metadataCodec) DecodeRequest(_ context.Context, r *http.Request) (queryrange.Request, error) {
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

func (c metadataCodec) EncodeRequest(ctx context.Context, r queryrange.Request) (*http.Request, error) {
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

func (c metadataCodec) DecodeResponse(ctx context.Context, r *http.Response, req queryrange.Request) (queryrange.Response, error) {
	if r.StatusCode/100 != 2 {
		body, _ := ioutil.ReadAll(r.Body)
		return nil, httpgrpc.Errorf(r.StatusCode, string(body))
	}
	log, ctx := spanlogger.New(ctx, "ParseQueryResponse") //nolint:ineffassign,staticcheck
	defer log.Finish()

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error(err)
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	log.LogFields(otlog.Int("bytes", len(buf)))

	switch req.(type) {
	case *ThanosLabelsRequest:
		var resp ThanosLabelsResponse
		if err := json.Unmarshal(buf, &resp); err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
		}
		return &resp, nil
	case *ThanosSeriesRequest:
		var resp ThanosSeriesResponse
		if err := json.Unmarshal(buf, &resp); err != nil {
			return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
		}
		return &resp, nil
	default:
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid response format")
	}
}

func (c metadataCodec) EncodeResponse(ctx context.Context, res queryrange.Response) (*http.Response, error) {
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
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid response format")
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

func (c metadataCodec) parseLabelsRequest(r *http.Request, op string) (queryrange.Request, error) {
	var (
		result ThanosLabelsRequest
		err    error
	)
	result.Start, err = cortexutil.ParseTime(r.FormValue("start"))
	if err != nil {
		return nil, err
	}

	result.End, err = cortexutil.ParseTime(r.FormValue("end"))
	if err != nil {
		return nil, err
	}

	if result.End < result.Start {
		return nil, errEndBeforeStart
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

	if op == seriesOp {
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

func (c metadataCodec) parseSeriesRequest(r *http.Request) (queryrange.Request, error) {
	var (
		result ThanosSeriesRequest
		err    error
	)
	result.Start, err = cortexutil.ParseTime(r.FormValue("start"))
	if err != nil {
		return nil, err
	}

	result.End, err = cortexutil.ParseTime(r.FormValue("end"))
	if err != nil {
		return nil, err
	}

	if result.End < result.Start {
		return nil, errEndBeforeStart
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
