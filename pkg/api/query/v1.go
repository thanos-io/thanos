// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This package is a modified copy from
// github.com/prometheus/prometheus/web/api/v1@2121b4628baa7d9d9406aa468712a6a332e77aff.

package v1

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// QueryAPI is an API used by Thanos Query.
type QueryAPI struct {
	baseAPI         *api.BaseAPI
	logger          log.Logger
	reg             prometheus.Registerer
	gate            gate.Gate
	queryableCreate query.QueryableCreator
	queryEngine     *promql.Engine
	ruleGroups      rules.UnaryClient

	enableAutodownsampling     bool
	enableQueryPartialResponse bool
	enableRulePartialResponse  bool
	replicaLabels              []string

	storeSet                               *query.StoreSet
	defaultInstantQueryMaxSourceResolution time.Duration
}

// NewQueryAPI returns an initialized QueryAPI type.
func NewQueryAPI(
	logger log.Logger,
	reg *prometheus.Registry,
	storeSet *query.StoreSet,
	qe *promql.Engine,
	c query.QueryableCreator,
	ruleGroups rules.UnaryClient,
	enableAutodownsampling bool,
	enableQueryPartialResponse bool,
	enableRulePartialResponse bool,
	replicaLabels []string,
	flagsMap map[string]string,
	defaultInstantQueryMaxSourceResolution time.Duration,
	maxConcurrentQueries int,
) *QueryAPI {
	return &QueryAPI{
		baseAPI:         api.NewBaseAPI(logger, flagsMap),
		logger:          logger,
		reg:             reg,
		queryEngine:     qe,
		queryableCreate: c,
		gate:            gate.NewKeeper(extprom.WrapRegistererWithPrefix("thanos_query_concurrent_", reg)).NewGate(maxConcurrentQueries),
		ruleGroups:      ruleGroups,

		enableAutodownsampling:                 enableAutodownsampling,
		enableQueryPartialResponse:             enableQueryPartialResponse,
		enableRulePartialResponse:              enableRulePartialResponse,
		replicaLabels:                          replicaLabels,
		storeSet:                               storeSet,
		defaultInstantQueryMaxSourceResolution: defaultInstantQueryMaxSourceResolution,
	}
}

// Register the API's endpoints in the given router.
func (qapi *QueryAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	qapi.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware)

	r.Get("/query", instr("query", qapi.query))
	r.Post("/query", instr("query", qapi.query))

	r.Get("/query_range", instr("query_range", qapi.queryRange))
	r.Post("/query_range", instr("query_range", qapi.queryRange))

	r.Get("/label/:name/values", instr("label_values", qapi.labelValues))

	r.Get("/series", instr("series", qapi.series))
	r.Post("/series", instr("series", qapi.series))

	r.Get("/labels", instr("label_names", qapi.labelNames))
	r.Post("/labels", instr("label_names", qapi.labelNames))

	r.Get("/stores", instr("stores", qapi.stores))

	r.Get("/rules", instr("rules", NewRulesHandler(qapi.ruleGroups, qapi.enableRulePartialResponse)))
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`

	// Additional Thanos Response field.
	Warnings []error `json:"warnings,omitempty"`
}

func (qapi *QueryAPI) parseEnableDedupParam(r *http.Request) (enableDeduplication bool, _ *api.ApiError) {
	const dedupParam = "dedup"
	enableDeduplication = true

	if val := r.FormValue(dedupParam); val != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(val)
		if err != nil {
			return false, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", dedupParam)}
		}
	}
	return enableDeduplication, nil
}

func (qapi *QueryAPI) parseReplicaLabelsParam(r *http.Request) (replicaLabels []string, _ *api.ApiError) {
	const replicaLabelsParam = "replicaLabels[]"
	if err := r.ParseForm(); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	replicaLabels = qapi.replicaLabels
	// Overwrite the cli flag when provided as a query parameter.
	if len(r.Form[replicaLabelsParam]) > 0 {
		replicaLabels = r.Form[replicaLabelsParam]
	}

	return replicaLabels, nil
}

func (qapi *QueryAPI) parseStoreMatchersParam(r *http.Request) (storeMatchers [][]storepb.LabelMatcher, _ *api.ApiError) {
	const storeMatcherParam = "storeMatch[]"
	if err := r.ParseForm(); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	for _, s := range r.Form[storeMatcherParam] {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
		}
		stm, err := storepb.TranslatePromMatchers(matchers...)
		if err != nil {
			return nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrap(err, "convert store matchers")}
		}
		storeMatchers = append(storeMatchers, stm)
	}

	return storeMatchers, nil
}

func (qapi *QueryAPI) parseDownsamplingParamMillis(r *http.Request, defaultVal time.Duration) (maxResolutionMillis int64, _ *api.ApiError) {
	const maxSourceResolutionParam = "max_source_resolution"
	maxSourceResolution := 0 * time.Second

	val := r.FormValue(maxSourceResolutionParam)
	if qapi.enableAutodownsampling || (val == "auto") {
		maxSourceResolution = defaultVal
	}
	if val != "" && val != "auto" {
		var err error
		maxSourceResolution, err = parseDuration(val)
		if err != nil {
			return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", maxSourceResolutionParam)}
		}
	}

	if maxSourceResolution < 0 {
		return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("negative '%s' is not accepted. Try a positive integer", maxSourceResolutionParam)}
	}

	return int64(maxSourceResolution / time.Millisecond), nil
}

func (qapi *QueryAPI) parsePartialResponseParam(r *http.Request, defaultEnablePartialResponse bool) (enablePartialResponse bool, _ *api.ApiError) {
	const partialResponseParam = "partial_response"

	// Overwrite the cli flag when provided as a query parameter.
	if val := r.FormValue(partialResponseParam); val != "" {
		var err error
		defaultEnablePartialResponse, err = strconv.ParseBool(val)
		if err != nil {
			return false, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", partialResponseParam)}
		}
	}
	return defaultEnablePartialResponse, nil
}

func (qapi *QueryAPI) query(r *http.Request) (interface{}, []error, *api.ApiError) {
	ts, err := parseTimeParam(r, "time", qapi.baseAPI.Now())
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	storeMatchers, apiErr := qapi.parseStoreMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	maxSourceResolution, apiErr := qapi.parseDownsamplingParamMillis(r, qapi.defaultInstantQueryMaxSourceResolution)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	// We are starting promQL tracing span here, because we have no control over promQL code.
	span, ctx := tracing.StartSpan(ctx, "promql_instant_query")
	defer span.Finish()

	qry, err := qapi.queryEngine.NewInstantQuery(qapi.queryableCreate(enableDedup, replicaLabels, storeMatchers, maxSourceResolution, enablePartialResponse, false), r.FormValue("query"), ts)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	tracing.DoInSpan(ctx, "query_gate_ismyturn", func(ctx context.Context) {
		err = qapi.gate.Start(ctx)
	})
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}
	}
	defer qapi.gate.Done()

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, nil, &api.ApiError{Typ: api.ErrorCanceled, Err: res.Err}
		case promql.ErrQueryTimeout:
			return nil, nil, &api.ApiError{Typ: api.ErrorTimeout, Err: res.Err}
		case promql.ErrStorage:
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: res.Err}
		}
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: res.Err}
	}

	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, res.Warnings, nil
}

func (qapi *QueryAPI) queryRange(r *http.Request) (interface{}, []error, *api.ApiError) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrap(err, "param step")}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	storeMatchers, apiErr := qapi.parseStoreMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	// If no max_source_resolution is specified fit at least 5 samples between steps.
	maxSourceResolution, apiErr := qapi.parseDownsamplingParamMillis(r, step/5)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	// We are starting promQL tracing span here, because we have no control over promQL code.
	span, ctx := tracing.StartSpan(ctx, "promql_range_query")
	defer span.Finish()

	qry, err := qapi.queryEngine.NewRangeQuery(
		qapi.queryableCreate(enableDedup, replicaLabels, storeMatchers, maxSourceResolution, enablePartialResponse, false),
		r.FormValue("query"),
		start,
		end,
		step,
	)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	tracing.DoInSpan(ctx, "query_gate_ismyturn", func(ctx context.Context) {
		err = qapi.gate.Start(ctx)
	})
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}
	}
	defer qapi.gate.Done()

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, nil, &api.ApiError{Typ: api.ErrorCanceled, Err: res.Err}
		case promql.ErrQueryTimeout:
			return nil, nil, &api.ApiError{Typ: api.ErrorTimeout, Err: res.Err}
		}
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: res.Err}
	}

	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, res.Warnings, nil
}

func (qapi *QueryAPI) labelValues(r *http.Request) (interface{}, []error, *api.ApiError) {
	ctx := r.Context()
	name := route.Param(ctx, "name")

	if !model.LabelNameRE.MatchString(name) {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("invalid label name: %q", name)}
	}

	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	q, err := qapi.queryableCreate(true, nil, nil, 0, enablePartialResponse, false).
		Querier(ctx, timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "queryable labelValues")

	// TODO(fabxc): add back request context.

	vals, warnings, err := q.LabelValues(name)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}
	}

	if vals == nil {
		vals = make([]string, 0)
	}

	return vals, warnings, nil
}

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0)
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999)
)

func (qapi *QueryAPI) series(r *http.Request) (interface{}, []error, *api.ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	if len(r.Form["match[]"]) == 0 {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.New("no match[] parameter provided")}
	}

	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	var matcherSets [][]*labels.Matcher
	for _, s := range r.Form["match[]"] {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
		}
		matcherSets = append(matcherSets, matchers)
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	storeMatchers, apiErr := qapi.parseStoreMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	q, err := qapi.queryableCreate(enableDedup, replicaLabels, storeMatchers, math.MaxInt64, enablePartialResponse, true).
		Querier(r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "queryable series")

	var (
		metrics = []labels.Labels{}
		sets    []storage.SeriesSet
	)
	for _, mset := range matcherSets {
		sets = append(sets, q.Select(false, nil, mset...))
	}

	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: set.Err()}
	}
	return metrics, set.Warnings(), nil
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}

func (qapi *QueryAPI) labelNames(r *http.Request) (interface{}, []error, *api.ApiError) {
	ctx := r.Context()

	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	storeMatchers, apiErr := qapi.parseStoreMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	q, err := qapi.queryableCreate(true, nil, storeMatchers, 0, enablePartialResponse, false).Querier(ctx, timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "queryable labelNames")

	names, warnings, err := q.LabelNames()
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}
	}

	return names, warnings, nil
}

func (qapi *QueryAPI) stores(r *http.Request) (interface{}, []error, *api.ApiError) {
	statuses := make(map[string][]query.StoreStatus)
	for _, status := range qapi.storeSet.GetStoreStatus() {
		statuses[status.StoreType.String()] = append(statuses[status.StoreType.String()], status)
	}
	return statuses, nil, nil
}

// NewRulesHandler created handler compatible with HTTP /api/v1/rules https://prometheus.io/docs/prometheus/latest/querying/api/#rules
// which uses gRPC Unary Rules API.
func NewRulesHandler(client rules.UnaryClient, enablePartialResponse bool) func(*http.Request) (interface{}, []error, *api.ApiError) {
	ps := storepb.PartialResponseStrategy_ABORT
	if enablePartialResponse {
		ps = storepb.PartialResponseStrategy_WARN
	}

	return func(r *http.Request) (interface{}, []error, *api.ApiError) {
		typeParam := r.URL.Query().Get("type")
		typ, ok := rulespb.RulesRequest_Type_value[strings.ToUpper(typeParam)]
		if !ok {
			if typeParam != "" {
				return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("invalid rules parameter type='%v'", typeParam)}
			}
			typ = int32(rulespb.RulesRequest_ALL)
		}

		// TODO(bwplotka): Allow exactly the same functionality as query API: passing replica, dedup and partial response as HTTP params as well.
		req := &rulespb.RulesRequest{
			Type:                    rulespb.RulesRequest_Type(typ),
			PartialResponseStrategy: ps,
		}
		groups, warnings, err := client.Rules(r.Context(), req)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Errorf("error retrieving rules: %v", err)}
		}
		return groups, warnings, nil
	}
}
