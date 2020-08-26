// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	cortexutil "github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

var (
	errEndBeforeStart = httpgrpc.Errorf(http.StatusBadRequest, "end timestamp must not be before start time")
	errNegativeStep   = httpgrpc.Errorf(http.StatusBadRequest, "zero or negative query resolution step widths are not accepted. Try a positive integer")
	errStepTooSmall   = httpgrpc.Errorf(http.StatusBadRequest, "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
	errCannotParse    = "cannot parse parameter %s"
)

type codec struct {
	queryrange.Codec
	partialResponse bool
}

func NewThanosCodec(partialResponse bool) *codec {
	return &codec{
		Codec:           queryrange.PrometheusCodec,
		partialResponse: partialResponse,
	}
}

func (c codec) DecodeRequest(_ context.Context, r *http.Request) (queryrange.Request, error) {
	var (
		result ThanosRequest
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

	result.Dedup, err = parseEnableDedupParam(r.FormValue("dedup"))
	if err != nil {
		return nil, err
	}

	if r.FormValue("max_source_resolution") == "auto" {
		result.AutoDownsampling = true
		result.MaxSourceResolution = result.Step / 5
	} else {
		result.MaxSourceResolution, err = parseDownsamplingParamMillis(r.FormValue("max_source_resolution"))
		if err != nil {
			return nil, err
		}
	}

	result.PartialResponse, err = parsePartialResponseParam(r.FormValue("partial_response"), c.partialResponse)
	if err != nil {
		return nil, err
	}

	if len(r.Form["replicaLabels[]"]) > 0 {
		result.ReplicaLabels = r.Form["replicaLabels[]"]
	}

	result.StoreMatchers, err = parseStoreMatchersParam(r.Form["storeMatch[]"])
	if err != nil {
		return nil, err
	}

	result.Query = r.FormValue("query")
	result.Path = r.URL.Path
	return &result, nil
}

func (c codec) EncodeRequest(ctx context.Context, r queryrange.Request) (*http.Request, error) {
	thanosReq, ok := r.(*ThanosRequest)
	if !ok {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
	}
	params := url.Values{
		"start":            []string{encodeTime(thanosReq.Start)},
		"end":              []string{encodeTime(thanosReq.End)},
		"step":             []string{encodeDurationMillis(thanosReq.Step)},
		"query":            []string{thanosReq.Query},
		"dedup":            []string{strconv.FormatBool(thanosReq.Dedup)},
		"partial_response": []string{strconv.FormatBool(thanosReq.PartialResponse)},
		"replicaLabels[]":  thanosReq.ReplicaLabels,
	}

	if thanosReq.AutoDownsampling {
		params["max_source_resolution"] = []string{"auto"}
	} else if thanosReq.MaxSourceResolution != 0 {
		// Add this param only if it is set. Set to 0 will impact
		// auto-downsampling in the querier.
		params["max_source_resolution"] = []string{encodeDurationMillis(thanosReq.MaxSourceResolution)}
	}

	if len(thanosReq.StoreMatchers) > 0 {
		storeMatchers, err := matchersToStringSlice(thanosReq.StoreMatchers)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, "invalid request format")
		}
		params["storeMatch[]"] = storeMatchers
	}

	u := &url.URL{
		Path:     thanosReq.Path,
		RawQuery: params.Encode(),
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
	enableDeduplication := true
	if s != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(s)
		if err != nil {
			return enableDeduplication, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, "dedup")
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
			return maxSourceResolution, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, "max_source_resolution")
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
			return defaultEnablePartialResponse, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, "partial_response")
		}
	}

	return defaultEnablePartialResponse, nil
}

func parseStoreMatchersParam(ss []string) ([][]storepb.LabelMatcher, error) {
	storeMatchers := make([][]storepb.LabelMatcher, 0, len(ss))
	for _, s := range ss {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, errCannotParse, "storeMatch[]")
		}
		stm, err := storepb.TranslatePromMatchers(matchers...)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, "storeMatch[]")
		}
		storeMatchers = append(storeMatchers, stm)
	}
	return storeMatchers, nil
}

func encodeTime(t int64) string {
	f := float64(t) / 1.0e3
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func encodeDurationMillis(d int64) string {
	return strconv.FormatFloat(float64(d)/float64(time.Second/time.Millisecond), 'f', -1, 64)
}

// matchersToStringSlice converts storeMatchers to string slice.
func matchersToStringSlice(storeMatchers [][]storepb.LabelMatcher) ([]string, error) {
	res := make([]string, 0, len(storeMatchers))
	for _, storeMatcher := range storeMatchers {
		matcher, err := promclient.MatchersToString(storeMatcher)
		if err != nil {
			return nil, err
		}

		res = append(res, matcher)
	}

	return res, nil
}
