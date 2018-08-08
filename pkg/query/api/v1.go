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
// github.com/prometheus/prometheus/web/api/v1@2121b4628baa7d9d9406aa468712a6a332e77aff

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/NYTimes/gziphandler"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type status string

const (
	statusSuccess status = "success"
	statusError          = "error"
)

type errorType string

const (
	errorNone     errorType = ""
	errorTimeout            = "timeout"
	errorCanceled           = "canceled"
	errorExec               = "execution"
	errorBadData            = "bad_data"
	errorInternal           = "internal"
)

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Accept-Encoding, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

// Enables cross-site script calls.
func setCORS(w http.ResponseWriter) {
	for h, v := range corsHeaders {
		w.Header().Set(h, v)
	}
}

type apiFunc func(r *http.Request) (interface{}, []error, *apiError)

// API can register a set of endpoints in a router and handle
// them using the provided storage and query engine.
type API struct {
	logger          log.Logger
	queryableCreate query.QueryableCreator
	queryEngine     *promql.Engine

	instantQueryDuration   prometheus.Histogram
	rangeQueryDuration     prometheus.Histogram
	enableAutodownsampling bool
	now                    func() time.Time
}

// NewAPI returns an initialized API type.
func NewAPI(
	logger log.Logger,
	reg *prometheus.Registry,
	qe *promql.Engine,
	c query.QueryableCreator,
	enableAutodownsampling bool,
) *API {
	instantQueryDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_query_api_instant_query_duration_seconds",
		Help: "Time it takes to perform instant query on promEngine backed up with thanos querier.",
		Buckets: []float64{
			0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 20,
		},
	})
	rangeQueryDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_query_api_range_query_duration_seconds",
		Help: "Time it takes to perform range query on promEngine backed up with thanos querier.",
		Buckets: []float64{
			0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 20,
		},
	})

	reg.MustRegister(
		instantQueryDuration,
		rangeQueryDuration,
	)
	return &API{
		logger:                 logger,
		queryEngine:            qe,
		queryableCreate:        c,
		instantQueryDuration:   instantQueryDuration,
		rangeQueryDuration:     rangeQueryDuration,
		enableAutodownsampling: enableAutodownsampling,
		now: time.Now,
	}
}

// Register the API's endpoints in the given router.
func (api *API) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger) {
	instr := func(name string, f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			setCORS(w)
			if data, warnings, err := f(r); err != nil {
				respondError(w, err, data)
			} else if data != nil {
				respond(w, data, warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return prometheus.InstrumentHandler(name, tracing.HTTPMiddleware(tracer, name, logger, gziphandler.GzipHandler(hf)))
	}

	r.Options("/*path", instr("options", api.options))

	r.Get("/query", instr("query", api.query))
	r.Get("/query_range", instr("query_range", api.queryRange))

	r.Get("/label/:name/values", instr("label_values", api.labelValues))

	r.Get("/series", instr("series", api.series))
}

type queryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
	Warnings   []error          `json:"warnings,omitempty"`
}

func (api *API) options(r *http.Request) (interface{}, []error, *apiError) {
	return nil, nil, nil
}

func (api *API) query(r *http.Request) (interface{}, []error, *apiError) {
	var ts time.Time
	if t := r.FormValue("time"); t != "" {
		var err error
		ts, err = parseTime(t)
		if err != nil {
			return nil, nil, &apiError{errorBadData, err}
		}
	} else {
		ts = api.now()
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &apiError{errorBadData, err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var (
		warnmtx             sync.Mutex
		warnings            []error
		enableDeduplication = true
	)
	partialErrReporter := func(err error) {
		warnmtx.Lock()
		warnings = append(warnings, err)
		warnmtx.Unlock()
	}

	// Allow disabling deduplication on demand.
	if dedup := r.FormValue("dedup"); dedup != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(dedup)
		if err != nil {
			return nil, nil, &apiError{errorBadData, errors.Wrap(err, "'dedup' parameter")}
		}
	}

	// We are starting promQL tracing span here, because we have no control over promQL code.
	span, ctx := tracing.StartSpan(r.Context(), "promql_instant_query")
	defer span.Finish()

	begin := api.now()
	qry, err := api.queryEngine.NewInstantQuery(api.queryableCreate(enableDeduplication, 0, partialErrReporter), r.FormValue("query"), ts)
	if err != nil {
		return nil, nil, &apiError{errorBadData, err}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, nil, &apiError{errorCanceled, res.Err}
		case promql.ErrQueryTimeout:
			return nil, nil, &apiError{errorTimeout, res.Err}
		case promql.ErrStorage:
			return nil, nil, &apiError{errorInternal, res.Err}
		}
		return nil, nil, &apiError{errorExec, res.Err}
	}
	api.instantQueryDuration.Observe(time.Since(begin).Seconds())

	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, warnings, nil
}

func (api *API) queryRange(r *http.Request) (interface{}, []error, *apiError) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, nil, &apiError{errorBadData, err}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, nil, &apiError{errorBadData, err}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, nil, &apiError{errorBadData, err}
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return nil, nil, &apiError{errorBadData, errors.Wrap(err, "param step")}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return nil, nil, &apiError{errorBadData, err}
	}

	maxSourceResolution := 0 * time.Second
	if api.enableAutodownsampling {
		// If no max_source_resolution is specified fit at least 5 samples between steps.
		maxSourceResolution = step / 5
	}
	if val := r.FormValue("max_source_resolution"); val != "" {
		maxSourceResolution, err = parseDuration(val)
		if err != nil {
			return nil, nil, &apiError{errorBadData, errors.Wrap(err, "param max_source_resolution")}
		}
	}

	if maxSourceResolution < 0 {
		err := errors.New("negative query max source resolution is not accepted. Try a positive integer")
		return nil, nil, &apiError{errorBadData, err}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return nil, nil, &apiError{errorBadData, err}
	}
	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &apiError{errorBadData, err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var (
		warnmtx             sync.Mutex
		warnings            []error
		enableDeduplication = true
	)
	partialErrReporter := func(err error) {
		warnmtx.Lock()
		warnings = append(warnings, err)
		warnmtx.Unlock()
	}

	// Allow disabling deduplication on demand.
	if dedup := r.FormValue("dedup"); dedup != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(dedup)
		if err != nil {
			return nil, nil, &apiError{errorBadData, errors.Wrap(err, "'dedup' parameter")}
		}
	}

	// We are starting promQL tracing span here, because we have no control over promQL code.
	span, ctx := tracing.StartSpan(r.Context(), "promql_range_query")
	defer span.Finish()

	begin := api.now()
	qry, err := api.queryEngine.NewRangeQuery(
		api.queryableCreate(enableDeduplication, maxSourceResolution, partialErrReporter),
		r.FormValue("query"),
		start,
		end,
		step,
	)
	if err != nil {
		return nil, nil, &apiError{errorBadData, err}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, nil, &apiError{errorCanceled, res.Err}
		case promql.ErrQueryTimeout:
			return nil, nil, &apiError{errorTimeout, res.Err}
		}
		return nil, nil, &apiError{errorExec, res.Err}
	}
	api.rangeQueryDuration.Observe(time.Since(begin).Seconds())

	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, warnings, nil
}

func (api *API) labelValues(r *http.Request) (interface{}, []error, *apiError) {
	ctx := r.Context()
	name := route.Param(ctx, "name")

	if !model.LabelNameRE.MatchString(name) {
		return nil, nil, &apiError{errorBadData, fmt.Errorf("invalid label name: %q", name)}
	}

	var (
		warnmtx  sync.Mutex
		warnings []error
	)
	partialErrReporter := func(err error) {
		warnmtx.Lock()
		warnings = append(warnings, err)
		warnmtx.Unlock()
	}

	q, err := api.queryableCreate(true, 0, partialErrReporter).Querier(ctx, math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, nil, &apiError{errorExec, err}
	}
	defer runutil.CloseWithLogOnErr(api.logger, q, "queryable labelValues")

	// TODO(fabxc): add back request context.

	vals, err := q.LabelValues(name)
	if err != nil {
		return nil, nil, &apiError{errorExec, err}
	}

	return vals, warnings, nil
}

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0)
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999)
)

func (api *API) series(r *http.Request) (interface{}, []error, *apiError) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &apiError{errorInternal, errors.Wrap(err, "parse form")}
	}

	if len(r.Form["match[]"]) == 0 {
		return nil, nil, &apiError{errorBadData, fmt.Errorf("no match[] parameter provided")}
	}

	var start time.Time
	if t := r.FormValue("start"); t != "" {
		var err error
		start, err = parseTime(t)
		if err != nil {
			return nil, nil, &apiError{errorBadData, err}
		}
	} else {
		start = minTime
	}

	var end time.Time
	if t := r.FormValue("end"); t != "" {
		var err error
		end, err = parseTime(t)
		if err != nil {
			return nil, nil, &apiError{errorBadData, err}
		}
	} else {
		end = maxTime
	}

	var matcherSets [][]*labels.Matcher
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return nil, nil, &apiError{errorBadData, err}
		}
		matcherSets = append(matcherSets, matchers)
	}

	var (
		warnmtx             sync.Mutex
		warnings            []error
		enableDeduplication = true
	)
	partialErrReporter := func(err error) {
		warnmtx.Lock()
		warnings = append(warnings, err)
		warnmtx.Unlock()
	}

	// Allow disabling deduplication on demand.
	if dedup := r.FormValue("dedup"); dedup != "" {
		var err error
		enableDeduplication, err = strconv.ParseBool(dedup)
		if err != nil {
			return nil, nil, &apiError{errorBadData, errors.Wrap(err, "'dedup' parameter")}
		}
	}

	q, err := api.queryableCreate(enableDeduplication, 0, partialErrReporter).Querier(r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return nil, nil, &apiError{errorExec, err}
	}
	defer runutil.CloseWithLogOnErr(api.logger, q, "queryable series")

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		s, err := q.Select(&storage.SelectParams{}, mset...)
		if err != nil {
			return nil, nil, &apiError{errorExec, err}
		}
		sets = append(sets, s)
	}

	set := storage.NewMergeSeriesSet(sets)
	metrics := []labels.Labels{}
	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		return nil, nil, &apiError{errorExec, set.Err()}
	}

	return metrics, warnings, nil
}

func respond(w http.ResponseWriter, data interface{}, warnings []error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := &response{
		Status: statusSuccess,
		Data:   data,
	}
	for _, warn := range warnings {
		resp.Warnings = append(resp.Warnings, warn.Error())
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
