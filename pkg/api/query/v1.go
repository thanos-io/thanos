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
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/stats"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	promqlapi "github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/engine"

	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/metadata"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/targets"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	DedupParam               = "dedup"
	PartialResponseParam     = "partial_response"
	MaxSourceResolutionParam = "max_source_resolution"
	ReplicaLabelsParam       = "replicaLabels[]"
	MatcherParam             = "match[]"
	StoreMatcherParam        = "storeMatch[]"
	Step                     = "step"
	Stats                    = "stats"
	ShardInfoParam           = "shard_info"
	LookbackDeltaParam       = "lookback_delta"
	EngineParam              = "engine"
	QueryAnalyzeParam        = "analyze"
)

type PromqlEngineType string

const (
	PromqlEnginePrometheus PromqlEngineType = "prometheus"
	PromqlEngineThanos     PromqlEngineType = "thanos"
)

type QueryEngineFactory struct {
	engineOpts            promql.EngineOpts
	remoteEngineEndpoints promqlapi.RemoteEndpoints

	createPrometheusEngine sync.Once
	prometheusEngine       v1.QueryEngine

	createThanosEngine sync.Once
	thanosEngine       v1.QueryEngine
	enableXFunctions   bool
}

func (f *QueryEngineFactory) GetPrometheusEngine() v1.QueryEngine {
	f.createPrometheusEngine.Do(func() {
		if f.prometheusEngine != nil {
			return
		}
		f.prometheusEngine = promql.NewEngine(f.engineOpts)
	})

	return f.prometheusEngine
}

func (f *QueryEngineFactory) GetThanosEngine() v1.QueryEngine {
	f.createThanosEngine.Do(func() {
		if f.thanosEngine != nil {
			return
		}
		if f.remoteEngineEndpoints == nil {
			f.thanosEngine = engine.New(engine.Opts{EngineOpts: f.engineOpts, Engine: f.GetPrometheusEngine(), EnableAnalysis: true, EnableXFunctions: f.enableXFunctions})
		} else {
			f.thanosEngine = engine.NewDistributedEngine(engine.Opts{EngineOpts: f.engineOpts, Engine: f.GetPrometheusEngine(), EnableAnalysis: true}, f.remoteEngineEndpoints)
		}
	})

	return f.thanosEngine
}

func NewQueryEngineFactory(engineOpts promql.EngineOpts, remoteEngineEndpoints promqlapi.RemoteEndpoints, enableExtendedFunctions bool) *QueryEngineFactory {
	return &QueryEngineFactory{
		engineOpts:            engineOpts,
		remoteEngineEndpoints: remoteEngineEndpoints,
		enableXFunctions:      enableExtendedFunctions,
	}
}

// QueryAPI is an API used by Thanos Querier.
type QueryAPI struct {
	baseAPI         *api.BaseAPI
	logger          log.Logger
	gate            gate.Gate
	queryableCreate query.QueryableCreator
	// queryEngine returns appropriate promql.Engine for a query with a given step.
	engineFactory       *QueryEngineFactory
	defaultEngine       PromqlEngineType
	lookbackDeltaCreate func(int64) time.Duration
	ruleGroups          rules.UnaryClient
	targets             targets.UnaryClient
	metadatas           metadata.UnaryClient
	exemplars           exemplars.UnaryClient

	enableAutodownsampling              bool
	enableQueryPartialResponse          bool
	enableRulePartialResponse           bool
	enableTargetPartialResponse         bool
	enableMetricMetadataPartialResponse bool
	enableExemplarPartialResponse       bool
	enableQueryPushdown                 bool
	disableCORS                         bool

	replicaLabels  []string
	endpointStatus func() []query.EndpointStatus

	defaultRangeQueryStep                  time.Duration
	defaultInstantQueryMaxSourceResolution time.Duration
	defaultMetadataTimeRange               time.Duration

	queryRangeHist prometheus.Histogram

	seriesStatsAggregatorFactory store.SeriesQueryPerformanceMetricsAggregatorFactory

	tenantHeader    string
	defaultTenant   string
	tenantCertField string
	enforceTenancy  bool
	tenantLabel     string
}

// NewQueryAPI returns an initialized QueryAPI type.
func NewQueryAPI(
	logger log.Logger,
	endpointStatus func() []query.EndpointStatus,
	engineFactory *QueryEngineFactory,
	defaultEngine PromqlEngineType,
	lookbackDeltaCreate func(int64) time.Duration,
	c query.QueryableCreator,
	ruleGroups rules.UnaryClient,
	targets targets.UnaryClient,
	metadatas metadata.UnaryClient,
	exemplars exemplars.UnaryClient,
	enableAutodownsampling bool,
	enableQueryPartialResponse bool,
	enableRulePartialResponse bool,
	enableTargetPartialResponse bool,
	enableMetricMetadataPartialResponse bool,
	enableExemplarPartialResponse bool,
	enableQueryPushdown bool,
	replicaLabels []string,
	flagsMap map[string]string,
	defaultRangeQueryStep time.Duration,
	defaultInstantQueryMaxSourceResolution time.Duration,
	defaultMetadataTimeRange time.Duration,
	disableCORS bool,
	gate gate.Gate,
	statsAggregatorFactory store.SeriesQueryPerformanceMetricsAggregatorFactory,
	reg *prometheus.Registry,
	tenantHeader string,
	defaultTenant string,
	tenantCertField string,
	enforceTenancy bool,
	tenantLabel string,
) *QueryAPI {
	if statsAggregatorFactory == nil {
		statsAggregatorFactory = &store.NoopSeriesStatsAggregatorFactory{}
	}
	return &QueryAPI{
		baseAPI:                                api.NewBaseAPI(logger, disableCORS, flagsMap),
		logger:                                 logger,
		engineFactory:                          engineFactory,
		defaultEngine:                          defaultEngine,
		lookbackDeltaCreate:                    lookbackDeltaCreate,
		queryableCreate:                        c,
		gate:                                   gate,
		ruleGroups:                             ruleGroups,
		targets:                                targets,
		metadatas:                              metadatas,
		exemplars:                              exemplars,
		enableAutodownsampling:                 enableAutodownsampling,
		enableQueryPartialResponse:             enableQueryPartialResponse,
		enableRulePartialResponse:              enableRulePartialResponse,
		enableTargetPartialResponse:            enableTargetPartialResponse,
		enableMetricMetadataPartialResponse:    enableMetricMetadataPartialResponse,
		enableExemplarPartialResponse:          enableExemplarPartialResponse,
		enableQueryPushdown:                    enableQueryPushdown,
		replicaLabels:                          replicaLabels,
		endpointStatus:                         endpointStatus,
		defaultRangeQueryStep:                  defaultRangeQueryStep,
		defaultInstantQueryMaxSourceResolution: defaultInstantQueryMaxSourceResolution,
		defaultMetadataTimeRange:               defaultMetadataTimeRange,
		disableCORS:                            disableCORS,
		seriesStatsAggregatorFactory:           statsAggregatorFactory,
		tenantHeader:                           tenantHeader,
		defaultTenant:                          defaultTenant,
		tenantCertField:                        tenantCertField,
		enforceTenancy:                         enforceTenancy,
		tenantLabel:                            tenantLabel,

		queryRangeHist: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "thanos_query_range_requested_timespan_duration_seconds",
			Help:    "A histogram of the query range window in seconds",
			Buckets: prometheus.ExponentialBuckets(15*60, 2, 12),
		}),
	}
}

// Register the API's endpoints in the given router.
func (qapi *QueryAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	qapi.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware, qapi.disableCORS)

	r.Get("/query", instr("query", qapi.query))
	r.Post("/query", instr("query", qapi.query))

	r.Get("/query_explain", instr("query", qapi.queryExplain))
	r.Post("/query_explain", instr("query", qapi.queryExplain))

	r.Get("/query_range", instr("query_range", qapi.queryRange))
	r.Post("/query_range", instr("query_range", qapi.queryRange))

	r.Get("/query_range_explain", instr("query", qapi.queryRangeExplain))
	r.Post("/query_range_explain", instr("query", qapi.queryRangeExplain))

	r.Get("/label/:name/values", instr("label_values", qapi.labelValues))

	r.Get("/series", instr("series", qapi.series))
	r.Post("/series", instr("series", qapi.series))

	r.Get("/labels", instr("label_names", qapi.labelNames))
	r.Post("/labels", instr("label_names", qapi.labelNames))

	r.Get("/stores", instr("stores", qapi.stores))

	r.Get("/alerts", instr("alerts", NewAlertsHandler(qapi.ruleGroups, qapi.enableRulePartialResponse)))
	r.Get("/rules", instr("rules", NewRulesHandler(qapi.ruleGroups, qapi.enableRulePartialResponse)))

	r.Get("/targets", instr("targets", NewTargetsHandler(qapi.targets, qapi.enableTargetPartialResponse)))

	r.Get("/metadata", instr("metadata", NewMetricMetadataHandler(qapi.metadatas, qapi.enableMetricMetadataPartialResponse)))

	r.Get("/query_exemplars", instr("exemplars", NewExemplarsHandler(qapi.exemplars, qapi.enableExemplarPartialResponse)))
	r.Post("/query_exemplars", instr("exemplars", NewExemplarsHandler(qapi.exemplars, qapi.enableExemplarPartialResponse)))
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
	Stats      stats.QueryStats `json:"stats,omitempty"`
	// Additional Thanos Response field.
	QueryAnalysis queryTelemetry `json:"analysis,omitempty"`
	Warnings      []error        `json:"warnings,omitempty"`
}

type queryTelemetry struct {
	// TODO(saswatamcode): Replace with engine.TrackedTelemetry once it has exported fields.
	// TODO(saswatamcode): Add aggregate fields to enrich data.
	OperatorName string           `json:"name,omitempty"`
	Execution    string           `json:"executionTime,omitempty"`
	Children     []queryTelemetry `json:"children,omitempty"`
}

func (qapi *QueryAPI) parseEnableDedupParam(r *http.Request) (enableDeduplication bool, _ *api.ApiError) {
	enableDeduplication = true

	if val := r.FormValue(DedupParam); val != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(val)
		if err != nil {
			return false, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", DedupParam)}
		}
	}
	return enableDeduplication, nil
}

func (qapi *QueryAPI) parseEngineParam(r *http.Request) (queryEngine v1.QueryEngine, e PromqlEngineType, _ *api.ApiError) {
	var engine v1.QueryEngine

	param := PromqlEngineType(r.FormValue("engine"))
	if param == "" {
		param = qapi.defaultEngine
	}

	switch param {
	case PromqlEnginePrometheus:
		engine = qapi.engineFactory.GetPrometheusEngine()
	case PromqlEngineThanos:
		engine = qapi.engineFactory.GetThanosEngine()
	default:
		return nil, param, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("'%s' bad engine", param)}
	}

	return engine, param, nil
}

func (qapi *QueryAPI) parseReplicaLabelsParam(r *http.Request) (replicaLabels []string, _ *api.ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	replicaLabels = qapi.replicaLabels
	// Overwrite the cli flag when provided as a query parameter.
	if len(r.Form[ReplicaLabelsParam]) > 0 {
		replicaLabels = r.Form[ReplicaLabelsParam]
	}
	return replicaLabels, nil
}

func (qapi *QueryAPI) parseStoreDebugMatchersParam(r *http.Request) (storeMatchers [][]*labels.Matcher, _ *api.ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}
	}

	for _, s := range r.Form[StoreMatcherParam] {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}
		}
		storeMatchers = append(storeMatchers, matchers)
	}

	return storeMatchers, nil
}

func (qapi *QueryAPI) parseLookbackDeltaParam(r *http.Request) (time.Duration, *api.ApiError) {
	// Overwrite the cli flag when provided as a query parameter.
	if val := r.FormValue(LookbackDeltaParam); val != "" {
		var err error
		lookbackDelta, err := parseDuration(val)
		if err != nil {
			return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", LookbackDeltaParam)}
		}
		return lookbackDelta, nil
	}
	// If duration 0 is returned, lookback delta is taken from engine config.
	return time.Duration(0), nil
}

func (qapi *QueryAPI) parseDownsamplingParamMillis(r *http.Request, defaultVal time.Duration) (maxResolutionMillis int64, _ *api.ApiError) {
	maxSourceResolution := 0 * time.Second

	val := r.FormValue(MaxSourceResolutionParam)
	if qapi.enableAutodownsampling || (val == "auto") {
		maxSourceResolution = defaultVal
	}
	if val != "" && val != "auto" {
		var err error
		maxSourceResolution, err = parseDuration(val)
		if err != nil {
			return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", MaxSourceResolutionParam)}
		}
	}

	if maxSourceResolution < 0 {
		return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("negative '%s' is not accepted. Try a positive integer", MaxSourceResolutionParam)}
	}

	return int64(maxSourceResolution / time.Millisecond), nil
}

func (qapi *QueryAPI) parsePartialResponseParam(r *http.Request, defaultEnablePartialResponse bool) (enablePartialResponse bool, _ *api.ApiError) {
	// Overwrite the cli flag when provided as a query parameter.
	if val := r.FormValue(PartialResponseParam); val != "" {
		var err error
		defaultEnablePartialResponse, err = strconv.ParseBool(val)
		if err != nil {
			return false, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", PartialResponseParam)}
		}
	}
	return defaultEnablePartialResponse, nil
}

func (qapi *QueryAPI) parseStep(r *http.Request, defaultRangeQueryStep time.Duration, rangeSeconds int64) (time.Duration, *api.ApiError) {
	// Overwrite the cli flag when provided as a query parameter.
	if val := r.FormValue(Step); val != "" {
		var err error
		defaultRangeQueryStep, err = parseDuration(val)
		if err != nil {
			return 0, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "'%s' parameter", Step)}
		}
		return defaultRangeQueryStep, nil
	}
	// Default step is used this way to make it consistent with UI.
	d := time.Duration(math.Max(float64(rangeSeconds/250), float64(defaultRangeQueryStep/time.Second))) * time.Second
	return d, nil
}

func (qapi *QueryAPI) parseShardInfo(r *http.Request) (*storepb.ShardInfo, *api.ApiError) {
	data := r.FormValue(ShardInfoParam)
	if data == "" {
		return nil, nil
	}

	if len(data) == 0 {
		return nil, nil
	}

	var info storepb.ShardInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Wrapf(err, "could not unmarshal parameter %s", ShardInfoParam)}
	}

	return &info, nil
}

func (qapi *QueryAPI) getQueryExplain(query promql.Query) (*engine.ExplainOutputNode, *api.ApiError) {
	if eq, ok := query.(engine.ExplainableQuery); ok {
		return eq.Explain(), nil
	}
	return nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("Query not explainable")}

}

func (qapi *QueryAPI) parseQueryAnalyzeParam(r *http.Request, query promql.Query) (queryTelemetry, error) {
	if r.FormValue(QueryAnalyzeParam) == "true" || r.FormValue(QueryAnalyzeParam) == "1" {
		if eq, ok := query.(engine.ExplainableQuery); ok {
			return processAnalysis(eq.Analyze()), nil
		}
		return queryTelemetry{}, errors.Errorf("Query not analyzable; change engine to 'thanos'")
	}
	return queryTelemetry{}, nil
}

func processAnalysis(a *engine.AnalyzeOutputNode) queryTelemetry {
	var analysis queryTelemetry
	analysis.OperatorName = a.OperatorTelemetry.Name()
	analysis.Execution = a.OperatorTelemetry.ExecutionTimeTaken().String()
	for _, c := range a.Children {
		analysis.Children = append(analysis.Children, processAnalysis(&c))
	}
	return analysis
}

func (qapi *QueryAPI) queryExplain(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	engine, engineParam, apiErr := qapi.parseEngineParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	if engineParam != PromqlEngineThanos {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.New("engine type must be 'thanos'")}, func() {}
	}

	ts, err := parseTimeParam(r, "time", qapi.baseAPI.Now())
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	maxSourceResolution, apiErr := qapi.parseDownsamplingParamMillis(r, qapi.defaultInstantQueryMaxSourceResolution)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	shardInfo, apiErr := qapi.parseShardInfo(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	lookbackDelta := qapi.lookbackDeltaCreate(maxSourceResolution)
	// Get custom lookback delta from request.
	lookbackDeltaFromReq, apiErr := qapi.parseLookbackDeltaParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}
	if lookbackDeltaFromReq > 0 {
		lookbackDelta = lookbackDeltaFromReq
	}

	tenant, err := tenancy.GetTenantFromHTTP(r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField)
	if err != nil {
		apiErr = &api.ApiError{Typ: api.ErrorBadData, Err: err}
		return nil, nil, apiErr, func() {}
	}
	ctx = context.WithValue(ctx, tenancy.TenantKey, tenant)

	var seriesStats []storepb.SeriesStatsCounter
	qry, err := engine.NewInstantQuery(
		ctx,
		qapi.queryableCreate(
			enableDedup,
			replicaLabels,
			storeDebugMatchers,
			maxSourceResolution,
			enablePartialResponse,
			qapi.enableQueryPushdown,
			false,
			shardInfo,
			query.NewAggregateStatsReporter(&seriesStats),
		),
		promql.NewPrometheusQueryOpts(false, lookbackDelta),
		r.FormValue("query"),
		ts,
	)

	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	explanation, apiErr := qapi.getQueryExplain(qry)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	return explanation, nil, nil, func() {}
}

func (qapi *QueryAPI) query(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	ts, err := parseTimeParam(r, "time", qapi.baseAPI.Now())
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	maxSourceResolution, apiErr := qapi.parseDownsamplingParamMillis(r, qapi.defaultInstantQueryMaxSourceResolution)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	shardInfo, apiErr := qapi.parseShardInfo(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	engine, _, apiErr := qapi.parseEngineParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	lookbackDelta := qapi.lookbackDeltaCreate(maxSourceResolution)
	// Get custom lookback delta from request.
	lookbackDeltaFromReq, apiErr := qapi.parseLookbackDeltaParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}
	if lookbackDeltaFromReq > 0 {
		lookbackDelta = lookbackDeltaFromReq
	}

	queryStr, tenant, ctx, err := tenancy.RewritePromQL(ctx, r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField, qapi.enforceTenancy, qapi.tenantLabel, r.FormValue("query"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	// We are starting promQL tracing span here, because we have no control over promQL code.
	span, ctx := tracing.StartSpan(ctx, "promql_instant_query")
	defer span.Finish()

	var seriesStats []storepb.SeriesStatsCounter
	qry, err := engine.NewInstantQuery(
		ctx,
		qapi.queryableCreate(
			enableDedup,
			replicaLabels,
			storeDebugMatchers,
			maxSourceResolution,
			enablePartialResponse,
			qapi.enableQueryPushdown,
			false,
			shardInfo,
			query.NewAggregateStatsReporter(&seriesStats),
		),
		promql.NewPrometheusQueryOpts(false, lookbackDelta),
		queryStr,
		ts,
	)

	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}
	res := qry.Exec(ctx)
	analysis, err := qapi.parseQueryAnalyzeParam(r, qry)
	if err != nil {
		return nil, nil, apiErr, func() {}
	}

	tracing.DoInSpan(ctx, "query_gate_ismyturn", func(ctx context.Context) {
		err = qapi.gate.Start(ctx)
	})
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, qry.Close
	}
	defer qapi.gate.Done()
	beforeRange := time.Now()
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, nil, &api.ApiError{Typ: api.ErrorCanceled, Err: res.Err}, qry.Close
		case promql.ErrQueryTimeout:
			return nil, nil, &api.ApiError{Typ: api.ErrorTimeout, Err: res.Err}, qry.Close
		case promql.ErrStorage:
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: res.Err}, qry.Close
		}
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: res.Err}, qry.Close
	}

	aggregator := qapi.seriesStatsAggregatorFactory.NewAggregator(tenant)
	for i := range seriesStats {
		aggregator.Aggregate(seriesStats[i])
	}
	aggregator.Observe(time.Since(beforeRange).Seconds())

	// Optional stats field in response if parameter "stats" is not empty.
	var qs stats.QueryStats
	if r.FormValue(Stats) != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}
	return &queryData{
		ResultType:    res.Value.Type(),
		Result:        res.Value,
		Stats:         qs,
		QueryAnalysis: analysis,
	}, res.Warnings.AsErrors(), nil, qry.Close
}

func (qapi *QueryAPI) queryRangeExplain(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	engine, engineParam, apiErr := qapi.parseEngineParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	if engineParam != PromqlEngineThanos {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.New("engine type must be 'thanos'")}, func() {}
	}

	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	step, apiErr := qapi.parseStep(r, qapi.defaultRangeQueryStep, int64(end.Sub(start)/time.Second))
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	// If no max_source_resolution is specified fit at least 5 samples between steps.
	maxSourceResolution, apiErr := qapi.parseDownsamplingParamMillis(r, step/5)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	shardInfo, apiErr := qapi.parseShardInfo(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	lookbackDelta := qapi.lookbackDeltaCreate(maxSourceResolution)
	// Get custom lookback delta from request.
	lookbackDeltaFromReq, apiErr := qapi.parseLookbackDeltaParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}
	if lookbackDeltaFromReq > 0 {
		lookbackDelta = lookbackDeltaFromReq
	}

	tenant, err := tenancy.GetTenantFromHTTP(r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField)
	if err != nil {
		apiErr = &api.ApiError{Typ: api.ErrorBadData, Err: err}
		return nil, nil, apiErr, func() {}
	}
	ctx = context.WithValue(ctx, tenancy.TenantKey, tenant)

	var seriesStats []storepb.SeriesStatsCounter
	qry, err := engine.NewRangeQuery(
		ctx,
		qapi.queryableCreate(
			enableDedup,
			replicaLabels,
			storeDebugMatchers,
			maxSourceResolution,
			enablePartialResponse,
			qapi.enableQueryPushdown,
			false,
			shardInfo,
			query.NewAggregateStatsReporter(&seriesStats),
		),
		promql.NewPrometheusQueryOpts(false, lookbackDelta),
		r.FormValue("query"),
		start,
		end,
		step,
	)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	explanation, apiErr := qapi.getQueryExplain(qry)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	return explanation, nil, nil, func() {}
}

func (qapi *QueryAPI) queryRange(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}
	step, apiErr := qapi.parseStep(r, qapi.defaultRangeQueryStep, int64(end.Sub(start)/time.Second))

	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	// If no max_source_resolution is specified fit at least 5 samples between steps.
	maxSourceResolution, apiErr := qapi.parseDownsamplingParamMillis(r, step/5)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	shardInfo, apiErr := qapi.parseShardInfo(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	engine, _, apiErr := qapi.parseEngineParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	lookbackDelta := qapi.lookbackDeltaCreate(maxSourceResolution)
	// Get custom lookback delta from request.
	lookbackDeltaFromReq, apiErr := qapi.parseLookbackDeltaParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}
	if lookbackDeltaFromReq > 0 {
		lookbackDelta = lookbackDeltaFromReq
	}

	queryStr, tenant, ctx, err := tenancy.RewritePromQL(ctx, r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField, qapi.enforceTenancy, qapi.tenantLabel, r.FormValue("query"))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	// Record the query range requested.
	qapi.queryRangeHist.Observe(end.Sub(start).Seconds())

	// We are starting promQL tracing span here, because we have no control over promQL code.
	span, ctx := tracing.StartSpan(ctx, "promql_range_query")
	defer span.Finish()

	var seriesStats []storepb.SeriesStatsCounter
	qry, err := engine.NewRangeQuery(
		ctx,
		qapi.queryableCreate(
			enableDedup,
			replicaLabels,
			storeDebugMatchers,
			maxSourceResolution,
			enablePartialResponse,
			qapi.enableQueryPushdown,
			false,
			shardInfo,
			query.NewAggregateStatsReporter(&seriesStats),
		),
		promql.NewPrometheusQueryOpts(false, lookbackDelta),
		queryStr,
		start,
		end,
		step,
	)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	res := qry.Exec(ctx)

	analysis, err := qapi.parseQueryAnalyzeParam(r, qry)
	if err != nil {
		return nil, nil, apiErr, func() {}
	}

	tracing.DoInSpan(ctx, "query_gate_ismyturn", func(ctx context.Context) {
		err = qapi.gate.Start(ctx)
	})
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, qry.Close
	}
	defer qapi.gate.Done()

	beforeRange := time.Now()
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, nil, &api.ApiError{Typ: api.ErrorCanceled, Err: res.Err}, qry.Close
		case promql.ErrQueryTimeout:
			return nil, nil, &api.ApiError{Typ: api.ErrorTimeout, Err: res.Err}, qry.Close
		}
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: res.Err}, qry.Close
	}
	aggregator := qapi.seriesStatsAggregatorFactory.NewAggregator(tenant)
	for i := range seriesStats {
		aggregator.Aggregate(seriesStats[i])
	}
	aggregator.Observe(time.Since(beforeRange).Seconds())

	// Optional stats field in response if parameter "stats" is not empty.
	var qs stats.QueryStats
	if r.FormValue(Stats) != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}
	return &queryData{
		ResultType:    res.Value.Type(),
		Result:        res.Value,
		Stats:         qs,
		QueryAnalysis: analysis,
	}, res.Warnings.AsErrors(), nil, qry.Close
}

func (qapi *QueryAPI) labelValues(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	ctx := r.Context()
	name := route.Param(ctx, "name")

	if !model.LabelNameRE.MatchString(name) {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("invalid label name: %q", name)}, func() {}
	}

	start, end, err := parseMetadataTimeRange(r, qapi.defaultMetadataTimeRange)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	matcherSets, ctx, err := tenancy.RewriteLabelMatchers(ctx, r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField, qapi.enforceTenancy, qapi.tenantLabel, r.Form[MatcherParam])
	if err != nil {
		apiErr = &api.ApiError{Typ: api.ErrorBadData, Err: err}
		return nil, nil, apiErr, func() {}
	}

	q, err := qapi.queryableCreate(
		true,
		nil,
		storeDebugMatchers,
		0,
		enablePartialResponse,
		qapi.enableQueryPushdown,
		true,
		nil,
		query.NoopSeriesStatsReporter,
	).Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, func() {}
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "queryable labelValues")

	var (
		vals     []string
		warnings annotations.Annotations
	)
	if len(matcherSets) > 0 {
		var callWarnings annotations.Annotations
		labelValuesSet := make(map[string]struct{})
		for _, matchers := range matcherSets {
			vals, callWarnings, err = q.LabelValues(ctx, name, matchers...)
			if err != nil {
				return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, func() {}
			}
			warnings.Merge(callWarnings)
			for _, val := range vals {
				labelValuesSet[val] = struct{}{}
			}
		}

		vals = make([]string, 0, len(labelValuesSet))
		for val := range labelValuesSet {
			vals = append(vals, val)
		}
		sort.Strings(vals)
	} else {
		vals, warnings, err = q.LabelValues(ctx, name)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, func() {}
		}
	}

	if vals == nil {
		vals = make([]string, 0)
	}

	return vals, warnings.AsErrors(), nil, func() {}
}

func (qapi *QueryAPI) series(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "parse form")}, func() {}
	}

	if len(r.Form[MatcherParam]) == 0 {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.New("no match[] parameter provided")}, func() {}
	}

	start, end, err := parseMetadataTimeRange(r, qapi.defaultMetadataTimeRange)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	matcherSets, ctx, err := tenancy.RewriteLabelMatchers(r.Context(), r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField, qapi.enforceTenancy, qapi.tenantLabel, r.Form[MatcherParam])
	if err != nil {
		apiErr := &api.ApiError{Typ: api.ErrorBadData, Err: err}
		return nil, nil, apiErr, func() {}
	}

	enableDedup, apiErr := qapi.parseEnableDedupParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	replicaLabels, apiErr := qapi.parseReplicaLabelsParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	q, err := qapi.queryableCreate(
		enableDedup,
		replicaLabels,
		storeDebugMatchers,
		math.MaxInt64,
		enablePartialResponse,
		qapi.enableQueryPushdown,
		true,
		nil,
		query.NoopSeriesStatsReporter,
	).Querier(timestamp.FromTime(start), timestamp.FromTime(end))

	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, func() {}
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "queryable series")

	var (
		metrics = []labels.Labels{}
		sets    []storage.SeriesSet
	)
	for _, mset := range matcherSets {
		sets = append(sets, q.Select(ctx, false, nil, mset...))
	}

	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: set.Err()}, func() {}
	}
	return metrics, set.Warnings().AsErrors(), nil, func() {}
}

func (qapi *QueryAPI) labelNames(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
	start, end, err := parseMetadataTimeRange(r, qapi.defaultMetadataTimeRange)
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
	}

	enablePartialResponse, apiErr := qapi.parsePartialResponseParam(r, qapi.enableQueryPartialResponse)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	storeDebugMatchers, apiErr := qapi.parseStoreDebugMatchersParam(r)
	if apiErr != nil {
		return nil, nil, apiErr, func() {}
	}

	matcherSets, ctx, err := tenancy.RewriteLabelMatchers(r.Context(), r, qapi.tenantHeader, qapi.defaultTenant, qapi.tenantCertField, qapi.enforceTenancy, qapi.tenantLabel, r.Form[MatcherParam])
	if err != nil {
		apiErr := &api.ApiError{Typ: api.ErrorBadData, Err: err}
		return nil, nil, apiErr, func() {}
	}

	q, err := qapi.queryableCreate(
		true,
		nil,
		storeDebugMatchers,
		0,
		enablePartialResponse,
		qapi.enableQueryPushdown,
		true,
		nil,
		query.NoopSeriesStatsReporter,
	).Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, func() {}
	}
	defer runutil.CloseWithLogOnErr(qapi.logger, q, "queryable labelNames")

	var (
		names    []string
		warnings annotations.Annotations
	)

	if len(matcherSets) > 0 {
		var callWarnings annotations.Annotations
		labelNamesSet := make(map[string]struct{})
		for _, matchers := range matcherSets {
			names, callWarnings, err = q.LabelNames(ctx, matchers...)
			if err != nil {
				return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, func() {}
			}
			warnings.Merge(callWarnings)
			for _, val := range names {
				labelNamesSet[val] = struct{}{}
			}
		}

		names = make([]string, 0, len(labelNamesSet))
		for name := range labelNamesSet {
			names = append(names, name)
		}
		sort.Strings(names)
	} else {
		names, warnings, err = q.LabelNames(ctx)
	}

	if err != nil {
		return nil, nil, &api.ApiError{Typ: api.ErrorExec, Err: err}, func() {}
	}
	if names == nil {
		names = make([]string, 0)
	}

	return names, warnings.AsErrors(), nil, func() {}
}

func (qapi *QueryAPI) stores(_ *http.Request) (interface{}, []error, *api.ApiError, func()) {
	statuses := make(map[string][]query.EndpointStatus)
	for _, status := range qapi.endpointStatus() {
		// Don't consider an endpoint if we cannot retrieve component type.
		if status.ComponentType == nil {
			continue
		}
		statuses[status.ComponentType.String()] = append(statuses[status.ComponentType.String()], status)
	}
	return statuses, nil, nil, func() {}
}

// NewTargetsHandler created handler compatible with HTTP /api/v1/targets https://prometheus.io/docs/prometheus/latest/querying/api/#targets
// which uses gRPC Unary Targets API.
func NewTargetsHandler(client targets.UnaryClient, enablePartialResponse bool) func(*http.Request) (interface{}, []error, *api.ApiError, func()) {
	ps := storepb.PartialResponseStrategy_ABORT
	if enablePartialResponse {
		ps = storepb.PartialResponseStrategy_WARN
	}

	return func(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
		stateParam := r.URL.Query().Get("state")
		state, ok := targetspb.TargetsRequest_State_value[strings.ToUpper(stateParam)]
		if !ok {
			if stateParam != "" {
				return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("invalid targets parameter state='%v'", stateParam)}, func() {}
			}
			state = int32(targetspb.TargetsRequest_ANY)
		}

		req := &targetspb.TargetsRequest{
			State:                   targetspb.TargetsRequest_State(state),
			PartialResponseStrategy: ps,
		}

		t, warnings, err := client.Targets(r.Context(), req)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "retrieving targets")}, func() {}
		}

		return t, warnings.AsErrors(), nil, func() {}
	}
}

// NewAlertsHandler created handler compatible with HTTP /api/v1/alerts https://prometheus.io/docs/prometheus/latest/querying/api/#alerts
// which uses gRPC Unary Rules API (Rules API works for both /alerts and /rules).
func NewAlertsHandler(client rules.UnaryClient, enablePartialResponse bool) func(*http.Request) (interface{}, []error, *api.ApiError, func()) {
	ps := storepb.PartialResponseStrategy_ABORT
	if enablePartialResponse {
		ps = storepb.PartialResponseStrategy_WARN
	}

	return func(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
		span, ctx := tracing.StartSpan(r.Context(), "receive_http_request")
		defer span.Finish()

		var (
			groups   *rulespb.RuleGroups
			warnings annotations.Annotations
			err      error
		)

		// TODO(bwplotka): Allow exactly the same functionality as query API: passing replica, dedup and partial response as HTTP params as well.
		req := &rulespb.RulesRequest{
			Type:                    rulespb.RulesRequest_ALERT,
			PartialResponseStrategy: ps,
		}
		tracing.DoInSpan(ctx, "retrieve_rules", func(ctx context.Context) {
			groups, warnings, err = client.Rules(ctx, req)
		})
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Errorf("error retrieving rules: %v", err)}, func() {}
		}

		var resp struct {
			Alerts []*rulespb.AlertInstance `json:"alerts"`
		}
		for _, g := range groups.Groups {
			for _, r := range g.Rules {
				a := r.GetAlert()
				if a == nil {
					continue
				}
				resp.Alerts = append(resp.Alerts, a.Alerts...)
			}
		}
		return resp, warnings.AsErrors(), nil, func() {}
	}
}

// NewRulesHandler created handler compatible with HTTP /api/v1/rules https://prometheus.io/docs/prometheus/latest/querying/api/#rules
// which uses gRPC Unary Rules API.
func NewRulesHandler(client rules.UnaryClient, enablePartialResponse bool) func(*http.Request) (interface{}, []error, *api.ApiError, func()) {
	ps := storepb.PartialResponseStrategy_ABORT
	if enablePartialResponse {
		ps = storepb.PartialResponseStrategy_WARN
	}

	return func(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
		span, ctx := tracing.StartSpan(r.Context(), "receive_http_request")
		defer span.Finish()

		var (
			groups   *rulespb.RuleGroups
			warnings annotations.Annotations
			err      error
		)

		typeParam := r.URL.Query().Get("type")
		typ, ok := rulespb.RulesRequest_Type_value[strings.ToUpper(typeParam)]
		if !ok {
			if typeParam != "" {
				return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("invalid rules parameter type='%v'", typeParam)}, func() {}
			}
			typ = int32(rulespb.RulesRequest_ALL)
		}

		if err := r.ParseForm(); err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Errorf("error parsing request form='%v'", MatcherParam)}, func() {}
		}

		// TODO(bwplotka): Allow exactly the same functionality as query API: passing replica, dedup and partial response as HTTP params as well.
		req := &rulespb.RulesRequest{
			Type:                    rulespb.RulesRequest_Type(typ),
			PartialResponseStrategy: ps,
			MatcherString:           r.Form[MatcherParam],
		}
		tracing.DoInSpan(ctx, "retrieve_rules", func(ctx context.Context) {
			groups, warnings, err = client.Rules(ctx, req)
		})
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Errorf("error retrieving rules: %v", err)}, func() {}
		}
		return groups, warnings.AsErrors(), nil, func() {}
	}
}

// NewExemplarsHandler creates handler compatible with HTTP /api/v1/query_exemplars https://prometheus.io/docs/prometheus/latest/querying/api/#querying-exemplars
// which uses gRPC Unary Exemplars API.
func NewExemplarsHandler(client exemplars.UnaryClient, enablePartialResponse bool) func(*http.Request) (interface{}, []error, *api.ApiError, func()) {
	ps := storepb.PartialResponseStrategy_ABORT
	if enablePartialResponse {
		ps = storepb.PartialResponseStrategy_WARN
	}

	return func(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
		span, ctx := tracing.StartSpan(r.Context(), "exemplar_query_request")
		defer span.Finish()

		var (
			data     []*exemplarspb.ExemplarData
			warnings annotations.Annotations
			err      error
		)

		start, err := parseTimeParam(r, "start", infMinTime)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}
		end, err := parseTimeParam(r, "end", infMaxTime)
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: err}, func() {}
		}

		req := &exemplarspb.ExemplarsRequest{
			Start:                   timestamp.FromTime(start),
			End:                     timestamp.FromTime(end),
			Query:                   r.FormValue("query"),
			PartialResponseStrategy: ps,
		}

		tracing.DoInSpan(ctx, "retrieve_exemplars", func(ctx context.Context) {
			data, warnings, err = client.Exemplars(ctx, req)
		})

		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "retrieving exemplars")}, func() {}
		}
		return data, warnings.AsErrors(), nil, func() {}
	}
}

var (
	infMinTime = time.Unix(math.MinInt64/1000+62135596801, 0)
	infMaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999)
)

func parseMetadataTimeRange(r *http.Request, defaultMetadataTimeRange time.Duration) (time.Time, time.Time, error) {
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
		return time.Time{}, time.Time{}, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	end, err := parseTimeParam(r, "end", defaultEndTime)
	if err != nil {
		return time.Time{}, time.Time{}, &api.ApiError{Typ: api.ErrorBadData, Err: err}
	}
	if end.Before(start) {
		return time.Time{}, time.Time{}, &api.ApiError{
			Typ: api.ErrorBadData,
			Err: errors.New("end timestamp must not be before start time"),
		}
	}

	return start, end, nil
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

// NewMetricMetadataHandler creates handler compatible with HTTP /api/v1/metadata https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata
// which uses gRPC Unary Metadata API.
func NewMetricMetadataHandler(client metadata.UnaryClient, enablePartialResponse bool) func(*http.Request) (interface{}, []error, *api.ApiError, func()) {
	ps := storepb.PartialResponseStrategy_ABORT
	if enablePartialResponse {
		ps = storepb.PartialResponseStrategy_WARN
	}

	return func(r *http.Request) (interface{}, []error, *api.ApiError, func()) {
		span, ctx := tracing.StartSpan(r.Context(), "metadata_http_request")
		defer span.Finish()

		var (
			t        map[string][]metadatapb.Meta
			warnings annotations.Annotations
			err      error
		)

		req := &metadatapb.MetricMetadataRequest{
			// By default we use -1, which means no limit.
			Limit:                   -1,
			Metric:                  r.URL.Query().Get("metric"),
			PartialResponseStrategy: ps,
		}

		limitStr := r.URL.Query().Get("limit")
		if limitStr != "" {
			limit, err := strconv.ParseInt(limitStr, 10, 32)
			if err != nil {
				return nil, nil, &api.ApiError{Typ: api.ErrorBadData, Err: errors.Errorf("invalid metric metadata limit='%v'", limit)}, func() {}
			}
			req.Limit = int32(limit)
		}

		tracing.DoInSpan(ctx, "retrieve_metadata", func(ctx context.Context) {
			t, warnings, err = client.MetricMetadata(ctx, req)
		})
		if err != nil {
			return nil, nil, &api.ApiError{Typ: api.ErrorInternal, Err: errors.Wrap(err, "retrieving metadata")}, func() {}
		}

		return t, warnings.AsErrors(), nil, func() {}
	}
}
