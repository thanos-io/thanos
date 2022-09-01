// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	cortexutil "github.com/thanos-io/thanos/internal/cortex/util"
	"github.com/thanos-io/thanos/internal/cortex/util/spanlogger"
	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
)

// queryInstantCodec is used to encode/decode Thanos instant query requests and responses.
type queryInstantCodec struct {
	partialResponse bool
}

// NewThanosQueryInstantCodec initializes a queryInstantCodec.
func NewThanosQueryInstantCodec(partialResponse bool) *queryInstantCodec {
	return &queryInstantCodec{
		partialResponse: partialResponse,
	}
}

// MergeResponse merges multiple responses into a single response.
// For instant query only vector responses will be merged because other types of queries
// are not shardable like number literal, string literal, scalar, etc.
func (c queryInstantCodec) MergeResponse(responses ...queryrange.Response) (queryrange.Response, error) {
	if len(responses) == 0 {
		return queryrange.NewEmptyPrometheusInstantQueryResponse(), nil
	} else if len(responses) == 1 {
		return responses[0], nil
	}

	promResponses := make([]*queryrange.PrometheusInstantQueryResponse, 0, len(responses))
	for _, resp := range responses {
		promResponses = append(promResponses, resp.(*queryrange.PrometheusInstantQueryResponse))
	}
	res := &queryrange.PrometheusInstantQueryResponse{
		Status: queryrange.StatusSuccess,
		Data: queryrange.PrometheusInstantQueryData{
			ResultType: model.ValVector.String(),
			Result: queryrange.PrometheusInstantQueryResult{
				Result: &queryrange.PrometheusInstantQueryResult_Vector{
					Vector: vectorMerge(promResponses),
				},
			},
			Stats: queryrange.StatsMerge(responses),
		},
	}
	return res, nil
}

func (c queryInstantCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (queryrange.Request, error) {
	var (
		result ThanosQueryInstantRequest
		err    error
	)
	if len(r.FormValue("time")) > 0 {
		result.Time, err = cortexutil.ParseTime(r.FormValue("time"))
		if err != nil {
			return nil, err
		}
	}

	result.Dedup, err = parseEnableDedupParam(r.FormValue(queryv1.DedupParam))
	if err != nil {
		return nil, err
	}

	if r.FormValue(queryv1.MaxSourceResolutionParam) == "auto" {
		result.AutoDownsampling = true
	} else {
		result.MaxSourceResolution, err = parseDownsamplingParamMillis(r.FormValue(queryv1.MaxSourceResolutionParam))
		if err != nil {
			return nil, err
		}
	}

	result.PartialResponse, err = parsePartialResponseParam(r.FormValue(queryv1.PartialResponseParam), c.partialResponse)
	if err != nil {
		return nil, err
	}

	if len(r.Form[queryv1.ReplicaLabelsParam]) > 0 {
		result.ReplicaLabels = r.Form[queryv1.ReplicaLabelsParam]
	}

	result.StoreMatchers, err = parseMatchersParam(r.Form, queryv1.StoreMatcherParam)
	if err != nil {
		return nil, err
	}

	result.ShardInfo, err = parseShardInfo(r.Form, queryv1.ShardInfoParam)
	if err != nil {
		return nil, err
	}

	result.Query = r.FormValue("query")
	result.Path = r.URL.Path

	for _, header := range forwardHeaders {
		for h, hv := range r.Header {
			if strings.EqualFold(h, header) {
				result.Headers = append(result.Headers, &RequestHeader{Name: h, Values: hv})
				break
			}
		}
	}
	return &result, nil
}

func (c queryInstantCodec) EncodeRequest(ctx context.Context, r queryrange.Request) (*http.Request, error) {
	thanosReq, ok := r.(*ThanosQueryInstantRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}
	params := url.Values{
		"query":                      []string{thanosReq.Query},
		queryv1.DedupParam:           []string{strconv.FormatBool(thanosReq.Dedup)},
		queryv1.PartialResponseParam: []string{strconv.FormatBool(thanosReq.PartialResponse)},
		queryv1.ReplicaLabelsParam:   thanosReq.ReplicaLabels,
	}

	if thanosReq.Time > 0 {
		params["time"] = []string{encodeTime(thanosReq.Time)}
	}
	if thanosReq.AutoDownsampling {
		params[queryv1.MaxSourceResolutionParam] = []string{"auto"}
	} else if thanosReq.MaxSourceResolution != 0 {
		// Add this param only if it is set. Set to 0 will impact
		// auto-downsampling in the querier.
		params[queryv1.MaxSourceResolutionParam] = []string{encodeDurationMillis(thanosReq.MaxSourceResolution)}
	}

	if len(thanosReq.StoreMatchers) > 0 {
		params[queryv1.StoreMatcherParam] = matchersToStringSlice(thanosReq.StoreMatchers)
	}

	if thanosReq.ShardInfo != nil {
		data, err := encodeShardInfo(thanosReq.ShardInfo)
		if err != nil {
			return nil, err
		}
		params[queryv1.ShardInfoParam] = []string{data}
	}

	req, err := http.NewRequest(http.MethodPost, thanosReq.Path, bytes.NewBufferString(params.Encode()))
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "error creating request: %s", err.Error())
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, hv := range thanosReq.Headers {
		for _, v := range hv.Values {
			req.Header.Add(hv.Name, v)
		}
	}
	return req.WithContext(ctx), nil
}

func (c queryInstantCodec) EncodeResponse(ctx context.Context, res queryrange.Response) (*http.Response, error) {
	sp, _ := opentracing.StartSpanFromContext(ctx, "APIResponse.ToHTTPResponse")
	defer sp.Finish()

	a, ok := res.(*queryrange.PrometheusInstantQueryResponse)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "invalid response format")
	}

	b, err := json.Marshal(a)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error encoding response: %v", err)
	}

	sp.LogFields(otlog.Int("bytes", len(b)))

	resp := http.Response{
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:          io.NopCloser(bytes.NewBuffer(b)),
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(b)),
	}
	return &resp, nil
}

func (c queryInstantCodec) DecodeResponse(ctx context.Context, r *http.Response, _ queryrange.Request) (queryrange.Response, error) {
	if r.StatusCode/100 != 2 {
		body, _ := io.ReadAll(r.Body)
		return nil, httpgrpc.Errorf(r.StatusCode, string(body))
	}
	log, ctx := spanlogger.New(ctx, "ParseQueryInstantResponse") //nolint:ineffassign,staticcheck
	defer log.Finish()

	buf, err := queryrange.BodyBuffer(r)
	if err != nil {
		log.Error(err) //nolint:errcheck
		return nil, err
	}
	log.LogFields(otlog.Int("bytes", len(buf)))

	var resp queryrange.PrometheusInstantQueryResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, httpgrpc.Errorf(http.StatusInternalServerError, "error decoding response: %v", err)
	}

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &queryrange.PrometheusResponseHeader{Name: h, Values: hv})
	}
	return &resp, nil
}

func vectorMerge(resps []*queryrange.PrometheusInstantQueryResponse) *queryrange.Vector {
	output := map[string]*queryrange.Sample{}
	for _, resp := range resps {
		if resp == nil {
			continue
		}
		// Merge vector result samples only. Skip other types such as
		// string, scalar as those are not sharable.
		if resp.Data.Result.GetVector() == nil {
			continue
		}
		for _, sample := range resp.Data.Result.GetVector().Samples {
			s := sample
			if s == nil {
				continue
			}
			metric := cortexpb.FromLabelAdaptersToLabels(sample.Labels).String()
			if existingSample, ok := output[metric]; !ok {
				output[metric] = s
			} else if existingSample.GetSample().TimestampMs < s.GetSample().TimestampMs {
				// Choose the latest sample if we see overlap.
				output[metric] = s
			}
		}
	}

	if len(output) == 0 {
		return &queryrange.Vector{
			Samples: make([]*queryrange.Sample, 0),
		}
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := &queryrange.Vector{
		Samples: make([]*queryrange.Sample, 0, len(output)),
	}
	for _, key := range keys {
		result.Samples = append(result.Samples, output[key])
	}
	return result
}
