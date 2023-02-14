// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	promqlparser "github.com/prometheus/prometheus/promql/parser"
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

// MergeResponse merges multiple responses into a single response. For instant query
// only vector and matrix responses will be merged because other types of queries
// are not shardable like number literal, string literal, scalar, etc.
func (c queryInstantCodec) MergeResponse(responses ...queryrange.Response) (queryrange.Response, error) {
	if len(responses) == 0 {
		return queryrange.NewEmptyPrometheusInstantQueryResponse(), nil
	} else if len(responses) == 1 {
		resp := responses[0].(*queryrange.PrometheusInstantQueryResponse)
		return &queryrange.PrometheusInstantQueryResponse{
			Status:    resp.Status,
			Data:      resp.Data,
			ErrorType: resp.ErrorType,
			Error:     resp.Error,
			Headers:   resp.Headers,
		}, nil
	}

	promResponses := make([]*queryrange.PrometheusInstantQueryResponse, 0, len(responses))
	for _, resp := range responses {
		promResponses = append(promResponses, resp.(*queryrange.PrometheusInstantQueryResponse))
	}
	var res queryrange.Response
	switch promResponses[0].Data.ResultType {
	case model.ValMatrix.String():
		res = &queryrange.PrometheusInstantQueryResponse{
			Status: queryrange.StatusSuccess,
			Data: queryrange.PrometheusInstantQueryData{
				ResultType: model.ValMatrix.String(),
				Result: queryrange.PrometheusInstantQueryResult{
					Result: &queryrange.PrometheusInstantQueryResult_Matrix{
						Matrix: matrixMerge(promResponses),
					},
				},
				Stats: queryrange.StatsMerge(responses),
			},
		}
	default:
		v, err := vectorMerge(promResponses)
		if err != nil {
			return nil, err
		}
		res = &queryrange.PrometheusInstantQueryResponse{
			Status: queryrange.StatusSuccess,
			Data: queryrange.PrometheusInstantQueryData{
				ResultType: model.ValVector.String(),
				Result: queryrange.PrometheusInstantQueryResult{
					Result: &queryrange.PrometheusInstantQueryResult_Vector{
						Vector: v,
					},
				},
				Stats: queryrange.StatsMerge(responses),
			},
		}
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

	result.LookbackDelta, err = parseLookbackDelta(r.Form, queryv1.LookbackDeltaParam)
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

	if thanosReq.LookbackDelta > 0 {
		params[queryv1.LookbackDeltaParam] = []string{encodeDurationMillis(thanosReq.LookbackDelta)}
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

func (c queryInstantCodec) DecodeResponse(ctx context.Context, r *http.Response, req queryrange.Request) (queryrange.Response, error) {
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
	// The query will be used later for merging responses.
	resp.Query = req.GetQuery()

	for h, hv := range r.Header {
		resp.Headers = append(resp.Headers, &queryrange.PrometheusResponseHeader{Name: h, Values: hv})
	}
	return &resp, nil
}

func vectorMerge(resps []*queryrange.PrometheusInstantQueryResponse) (*queryrange.Vector, error) {
	output := map[string]*queryrange.Sample{}
	sortAsc, sortDesc, err := parseQueryForSort(resps[0].Query)
	if err != nil {
		return nil, err
	}
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
		}, nil
	}

	var ss []*queryrange.Sample
	for _, v := range output {
		ss = append(ss, v)
	}

	sort.Slice(ss, func(i, j int) bool {
		// Order is determined by the sortFn in the query.
		if sortAsc {
			return ss[i].Sample.Value < ss[j].Sample.Value
		} else if sortDesc {
			return ss[i].Sample.Value > ss[j].Sample.Value
		} else {
			// Fallback on sorting by series
			m1 := cortexpb.FromLabelAdaptersToLabels(ss[i].Labels).String()
			m2 := cortexpb.FromLabelAdaptersToLabels(ss[j].Labels).String()
			return m1 < m2
		}
	})
	result := &queryrange.Vector{
		Samples: make([]*queryrange.Sample, 0, len(output)),
	}
	result.Samples = append(result.Samples, ss...)
	return result, nil
}

func parseQueryForSort(q string) (bool, bool, error) {
	expr, err := promqlparser.ParseExpr(q)
	var sortAsc bool = false
	var sortDesc bool = false
	promqlparser.Inspect(expr, func(n promqlparser.Node, _ []promqlparser.Node) error {
		if call, ok := n.(*promqlparser.Call); ok {
			sortFn := promqlparser.Functions["sort"]
			sortDescFn := promqlparser.Functions["sort_desc"]
			if call.Func.Name == sortFn.Name {
				sortAsc = true
				return errors.New("done")
			}
			if call.Func.Name == sortDescFn.Name {
				sortDesc = true
				return errors.New("done")
			}
		}
		return nil
	})

	return sortAsc, sortDesc, err
}

func matrixMerge(resps []*queryrange.PrometheusInstantQueryResponse) *queryrange.Matrix {
	output := map[string]*queryrange.SampleStream{}
	for _, resp := range resps {
		if resp == nil {
			continue
		}
		// Merge matrix result samples only. Skip other types such as
		// string, scalar as those are not sharable.
		if resp.Data.Result.GetMatrix() == nil {
			continue
		}
		for _, stream := range resp.Data.Result.GetMatrix().SampleStreams {
			metric := cortexpb.FromLabelAdaptersToLabels(stream.Labels).String()
			existing, ok := output[metric]
			if !ok {
				existing = &queryrange.SampleStream{
					Labels: stream.Labels,
				}
			}
			// We need to make sure we don't repeat samples. This causes some visualizations to be broken in Grafana.
			// The prometheus API is inclusive of start and end timestamps.
			if len(existing.Samples) > 0 && len(stream.Samples) > 0 {
				existingEndTs := existing.Samples[len(existing.Samples)-1].TimestampMs
				if existingEndTs == stream.Samples[0].TimestampMs {
					// Typically this the cases where only 1 sample point overlap,
					// so optimize with simple code.
					stream.Samples = stream.Samples[1:]
				} else if existingEndTs > stream.Samples[0].TimestampMs {
					// Overlap might be big, use heavier algorithm to remove overlap.
					stream.Samples = queryrange.SliceSamples(stream.Samples, existingEndTs)
				} // else there is no overlap, yay!
			}
			existing.Samples = append(existing.Samples, stream.Samples...)
			output[metric] = existing
		}
	}

	keys := make([]string, 0, len(output))
	for key := range output {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := &queryrange.Matrix{
		SampleStreams: make([]*queryrange.SampleStream, 0, len(output)),
	}
	for _, key := range keys {
		result.SampleStreams = append(result.SampleStreams, output[key])
	}

	return result
}
