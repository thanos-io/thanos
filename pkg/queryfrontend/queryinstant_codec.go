// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/weaveworks/common/httpgrpc"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	cortexutil "github.com/thanos-io/thanos/internal/cortex/util"
	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
)

// queryInstantCodec is used to encode/decode Thanos instant query requests and responses.
type queryInstantCodec struct {
	queryrange.Codec
	partialResponse bool
}

// NewThanosQueryInstantCodec initializes a queryInstantCodec.
func NewThanosQueryInstantCodec(partialResponse bool) *queryInstantCodec {
	return &queryInstantCodec{
		Codec:           queryrange.PrometheusCodec,
		partialResponse: partialResponse,
	}
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
