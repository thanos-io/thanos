// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	cortexutil "github.com/thanos-io/thanos/internal/cortex/util"

	queryv1 "github.com/thanos-io/thanos/pkg/api/query"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	// Name of the cache control header.
	cacheControlHeader = "Cache-Control"

	// Value that cacheControlHeader has if the response indicates that the results should not be cached.
	noStoreValue = "no-store"
)

var (
	errEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "end timestamp must not be before start time")
	errNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "zero or negative query resolution step widths are not accepted. Try a positive integer")
	errStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
	errCannotParse    = "cannot parse parameter %s"
)

// queryRangeCodec is used to encode/decode Thanos query range requests and responses.
type queryRangeCodec struct {
	queryrange.Codec
	partialResponse bool
}

// NewThanosQueryRangeCodec initializes a queryRangeCodec.
func NewThanosQueryRangeCodec(partialResponse bool) *queryRangeCodec {
	return &queryRangeCodec{
		Codec:           queryrange.PrometheusCodec,
		partialResponse: partialResponse,
	}
}

func (c queryRangeCodec) DecodeRequest(_ context.Context, r *http.Request, forwardHeaders []string) (queryrange.Request, error) {
	var (
		result ThanosQueryRangeRequest
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

	result.Step, err = parseDurationMillis(r.FormValue("step"))
	if err != nil {
		return nil, err
	}

	if result.Step <= 0 {
		return nil, errNegativeStep
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if (result.End-result.Start)/result.Step > 11000 {
		return nil, errStepTooSmall
	}

	result.Dedup, err = parseEnableDedupParam(r.FormValue(queryv1.DedupParam))
	if err != nil {
		return nil, err
	}

	if r.FormValue(queryv1.MaxSourceResolutionParam) == "auto" {
		result.AutoDownsampling = true
		result.MaxSourceResolution = result.Step / 5
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
	result.Analyze = r.FormValue(queryv1.QueryAnalyzeParam)
	result.Engine = r.FormValue(queryv1.EngineParam)
	result.Path = r.URL.Path

	for _, value := range r.Header.Values(cacheControlHeader) {
		if strings.Contains(value, noStoreValue) {
			result.CachingOptions.Disabled = true
			break
		}
	}

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

func (c queryRangeCodec) EncodeRequest(ctx context.Context, r queryrange.Request) (*http.Request, error) {
	thanosReq, ok := r.(*ThanosQueryRangeRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}
	params := url.Values{
		"start":                      []string{encodeTime(thanosReq.Start)},
		"end":                        []string{encodeTime(thanosReq.End)},
		"step":                       []string{encodeDurationMillis(thanosReq.Step)},
		"query":                      []string{thanosReq.Query},
		queryv1.QueryAnalyzeParam:    []string{thanosReq.Analyze},
		queryv1.EngineParam:          []string{thanosReq.Engine},
		queryv1.DedupParam:           []string{strconv.FormatBool(thanosReq.Dedup)},
		queryv1.PartialResponseParam: []string{strconv.FormatBool(thanosReq.PartialResponse)},
		queryv1.ReplicaLabelsParam:   thanosReq.ReplicaLabels,
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

func parseDurationMillis(s string) (int64, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second/time.Millisecond)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration. It overflows int64", s)
		}
		return int64(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return int64(d) / int64(time.Millisecond/time.Nanosecond), nil
	}
	return 0, httpgrpc.Errorf(http.StatusBadRequest, "cannot parse %q to a valid duration", s)
}

func parseEnableDedupParam(s string) (bool, error) {
	enableDeduplication := true // Deduplication is enabled by default.
	if s != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(s)
		if err != nil {
			return enableDeduplication, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, queryv1.DedupParam)
		}
	}

	return enableDeduplication, nil
}

func parseDownsamplingParamMillis(s string) (int64, error) {
	var maxSourceResolution int64
	if s != "" {
		var err error
		maxSourceResolution, err = parseDurationMillis(s)
		if err != nil {
			return maxSourceResolution, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, queryv1.MaxSourceResolutionParam)
		}
	}

	if maxSourceResolution < 0 {
		return 0, httpgrpc.Errorf(http.StatusBadRequest, "negative max_source_resolution is not accepted. Try a positive integer")
	}

	return maxSourceResolution, nil
}

func parsePartialResponseParam(s string, defaultEnablePartialResponse bool) (bool, error) {
	if s != "" {
		var err error
		defaultEnablePartialResponse, err = strconv.ParseBool(s)
		if err != nil {
			return defaultEnablePartialResponse, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, queryv1.PartialResponseParam)
		}
	}

	return defaultEnablePartialResponse, nil
}

func parseMatchersParam(ss url.Values, matcherParam string) ([][]*labels.Matcher, error) {
	matchers := make([][]*labels.Matcher, 0, len(ss[matcherParam]))
	for _, s := range ss[matcherParam] {
		ms, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, matcherParam)
		}
		matchers = append(matchers, ms)
	}
	return matchers, nil
}

func parseLookbackDelta(ss url.Values, key string) (int64, error) {
	data, ok := ss[key]
	if !ok || len(data) == 0 {
		return 0, nil
	}

	return parseDurationMillis(data[0])
}

func parseShardInfo(ss url.Values, key string) (*storepb.ShardInfo, error) {
	data, ok := ss[key]
	if !ok || len(data) == 0 {
		return nil, nil
	}

	var info storepb.ShardInfo
	if err := json.Unmarshal([]byte(data[0]), &info); err != nil {
		return nil, err
	}

	return &info, nil
}

func encodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func encodeDurationMillis(d int64) string {
	return strconv.FormatFloat(float64(d)/float64(time.Second/time.Millisecond), 'f', -1, 64)
}

// matchersToStringSlice converts storeMatchers to string slice.
func matchersToStringSlice(storeMatchers [][]*labels.Matcher) []string {
	res := make([]string, 0, len(storeMatchers))
	for _, storeMatcher := range storeMatchers {
		res = append(res, storepb.PromMatchersToString(storeMatcher...))
	}
	return res
}

func encodeShardInfo(info *storepb.ShardInfo) (string, error) {
	if info == nil {
		return "", nil
	}

	data, err := json.Marshal(info)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
