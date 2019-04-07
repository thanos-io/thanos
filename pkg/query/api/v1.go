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
	"github.com/improbable-eng/thanos/pkg/query/cacheclient"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/frontend/queryrange"

	"github.com/NYTimes/gziphandler"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
)

type status string

const (
	statusSuccess status = "success"
	statusError          = "error"
)

type ErrorType string

const (
	errorNone     ErrorType = ""
	errorTimeout            = "timeout"
	errorCanceled           = "canceled"
	errorExec               = "execution"
	errorBadData            = "bad_data"
	ErrorInternal           = "internal"
)

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Accept-Encoding, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

type ApiError struct {
	Typ ErrorType
	Err error
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("%s: %s", e.Typ, e.Err)
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType ErrorType   `json:"ErrorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

// Enables cross-site script calls.
func SetCORS(w http.ResponseWriter) {
	for h, v := range corsHeaders {
		w.Header().Set(h, v)
	}
}

type ApiFunc func(r *http.Request) (interface{}, []error, *ApiError)

// API can register a set of endpoints in a router and handle
// them using the provided storage and query engine.
type API struct {
	logger log.Logger

	instantQueryAPI        query.Instant
	rangeQueryAPI          query.Range
	nonChunkStoreAPI       storepb.StoreServer
	defOpts                query.Options
	enableAutodownsampling bool

	instantQueryDuration prometheus.Histogram
	rangeQueryDuration   prometheus.Histogram
	now                  func() time.Time

	queryRangeRT http.RoundTripper
}

// NewAPI returns an initialized API type.
func NewAPI(
	logger log.Logger,
	reg *prometheus.Registry,
	instantQueryAPI query.Instant,
	rangeQueryAPI query.Range,
	nonChunkStoreAPI storepb.StoreServer,
	defOpts query.Options,
	enableAutodownsampling bool,
	extraQueryRangeMiddlewares ...queryrange.Middleware,
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

	api := &API{
		logger: logger,

		instantQueryAPI:        instantQueryAPI,
		rangeQueryAPI:          rangeQueryAPI,
		nonChunkStoreAPI:       nonChunkStoreAPI,
		defOpts:                defOpts,
		enableAutodownsampling: enableAutodownsampling,

		instantQueryDuration: instantQueryDuration,
		rangeQueryDuration:   rangeQueryDuration,

		now: time.Now,
	}

	ms []queryrange.Middleware
	var limits queryrange.Limits // TODO: implement.
	api.queryRangeRT = queryrange.NewRoundTripper(
		nil, // We don't expect different path.
		queryrange.MergeMiddlewares(append(ms, extraQueryRangeMiddlewares...)...).Wrap(queryrange.HandlerFunc(api.queryRangeDo)),
		limits,
	)

	return api
}

// Register the API's endpoints in the given router.
func (api *API) Register(r *route.Router, tracer opentracing.Tracer) {
	instr := func(name string, f ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			SetCORS(w)
			if data, warnings, err := f(r); err != nil {
				RespondError(w, err, data)
			} else if data != nil {
				Respond(w, data, warnings)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return prometheus.InstrumentHandler(name, tracing.HTTPMiddleware(tracer, name, api.logger, gziphandler.GzipHandler(hf)))
	}

	r.Options("/*path", instr("options", api.options))

	r.Get("/query", instr("query", api.query))
	r.Post("/query", instr("query", api.query))

	r.Get("/label/:name/values", instr("label_values", api.labelValues))

	r.Get("/series", instr("series", api.series))

	r.Get("/query_range", instr("query_range", api.queryRange))
	r.Post("/query_range", instr("query_range", api.queryRange))
}

type queryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`

	// Additional Thanos Response field.
	Warnings []error `json:"warnings,omitempty"`
}

func (api *API) options(r *http.Request) (interface{}, []error, *ApiError) {
	return nil, nil, nil
}

func (api *API) query(r *http.Request) (interface{}, []error, *ApiError) {
	var ts time.Time
	if t := r.FormValue("time"); t != "" {
		var err error
		ts, err = parseTime(t)
		if err != nil {
			return nil, nil, &ApiError{errorBadData, err}
		}
	} else {
		ts = api.now()
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, &ApiError{errorBadData, err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	opts, err := query.NewOptionsFromForm(r.Form, api.defOpts)
	if err != nil {
		return nil, nil, &ApiError{errorBadData, err}
	}

	// TODO: promql_instant_query
	span, ctx := tracing.StartSpan(r.Context(), "querier_instant_query")
	defer span.Finish()

	begin := api.now()
	val, warnings, err := api.instantQueryAPI.QueryInstant(ctx, r.FormValue("query"), ts, opts)
	if err != nil {
		switch err.(type) {
		case promql.ErrQueryCanceled:
			return nil, nil, &ApiError{errorCanceled, err}
		case promql.ErrQueryTimeout:
			return nil, nil, &ApiError{errorTimeout, err}
		case promql.ErrStorage:
			return nil, nil, &ApiError{ErrorInternal, err}
		}
		return nil, nil, &ApiError{errorExec, err}
	}
	api.instantQueryDuration.Observe(time.Since(begin).Seconds())

	return &queryData{
		ResultType: val.Type(),
		Result:     val,
	}, warnings, nil
}

func (api *API) queryRange(r *http.Request) (interface{}, []error, *ApiError) {

}

func (api *API) queryRangeDo(ctx context.Context, r *queryrange.Request) (*queryrange.APIResponse, error) {

}

//	start, err := parseTime(r.FormValue("start"))
//	if err != nil {
//		return nil, nil, &ApiError{errorBadData, err}
//	}
//	end, err := parseTime(r.FormValue("end"))
//	if err != nil {
//		return nil, nil, &ApiError{errorBadData, err}
//	}
//	if end.Before(start) {
//		err := errors.New("end timestamp must not be before start time")
//		return nil, nil, &ApiError{errorBadData, err}
//	}
//
//	step, err := parseDuration(r.FormValue("step"))
//	if err != nil {
//		return nil, nil, &ApiError{errorBadData, errors.Wrap(err, "param step")}
//	}
//
//	if step <= 0 {
//		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
//		return nil, nil, &ApiError{errorBadData, err}
//	}
//
//	// For safety, limit the number of returned points per timeseries.
//	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
//	if end.Sub(start)/step > 11000 {
//		err := errors.Errorf("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
//		return nil, nil, &ApiError{errorBadData, err}
//	}
//
//	ctx := r.Context()
//	if to := r.FormValue("timeout"); to != "" {
//		var cancel context.CancelFunc
//		timeout, err := parseDuration(to)
//		if err != nil {
//			return nil, nil, &ApiError{errorBadData, err}
//		}
//
//		ctx, cancel = context.WithTimeout(ctx, timeout)
//		defer cancel()
//	}
//
//	opts, err := query.NewOptionsFromForm(r.Form, api.defOpts)
//	if err != nil {
//		return nil, nil, &ApiError{errorBadData, err}
//	}
//
//	if opts.Resolution == nil {
//		if api.enableAutodownsampling {
//			// If no max_source_resolution is specified fit at least 5 samples between steps.
//			opts.Resolution = &storepb.Resolution{
//				Window:   int64((step / 5) / time.Millisecond),
//				Strategy: storepb.Resolution_MAX,
//			}
//		}
//	}
//
//	span, ctx := tracing.StartSpan(r.Context(), "querier_range_query")
//	defer span.Finish()
//
//	begin := api.now()
//	val, warnings, err := api.rangeQueryAPI.QueryRange(ctx, r.FormValue("query"), start, end, step, opts)
//	if err != nil {
//		switch err.(type) {
//		case promql.ErrQueryCanceled:
//			return nil, nil, &ApiError{errorCanceled, err}
//		case promql.ErrQueryTimeout:
//			return nil, nil, &ApiError{errorTimeout, err}
//		case promql.ErrStorage:
//			return nil, nil, &ApiError{ErrorInternal, err}
//		}
//		return nil, nil, &ApiError{errorExec, err}
//	}
//	api.rangeQueryDuration.Observe(time.Since(begin).Seconds())
//
//	return &queryData{
//		ResultType: val.Type(),
//		Result:     val,
//	}, warnings, nil
//}

func toErrs(strs []string) []error {
	errs := make([]error, len(strs))
	for i, str := range strs {
		errs[i] = errors.New(str)
	}
	return errs
}

func (api *API) labelValues(r *http.Request) (interface{}, []error, *ApiError) {
	ctx := r.Context()
	name := route.Param(ctx, "name")

	if !model.LabelNameRE.MatchString(name) {
		return nil, nil, &ApiError{errorBadData, fmt.Errorf("invalid label name: %q", name)}
	}

	_ = r.ParseForm()
	partialResponseStrategy, err := query.NewPartialResponseStrategyFromForm(r.Form, api.defOpts.PartialResponseStrategy == storepb.PartialResponseStrategy_WARN)
	if err != nil {
		return nil, nil, &ApiError{errorBadData, err}
	}

	span, ctx := tracing.StartSpan(r.Context(), "querier_label_values")
	defer span.Finish()

	res, err := api.nonChunkStoreAPI.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label:                   name,
		PartialResponseDisabled: partialResponseStrategy == storepb.PartialResponseStrategy_ABORT,
	})
	if err != nil {
		return nil, nil, &ApiError{errorExec, err}
	}

	// TODO(fabxc): add back request context.

	return res.Values, toErrs(res.Warnings), nil
}

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0)
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999)
)

func (api *API) series(r *http.Request) (interface{}, []error, *ApiError) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, &ApiError{ErrorInternal, errors.Wrap(err, "parse form")}
	}

	if len(r.Form["match[]"]) == 0 {
		return nil, nil, &ApiError{errorBadData, fmt.Errorf("no match[] parameter provided")}
	}

	var start time.Time
	if t := r.FormValue("start"); t != "" {
		var err error
		start, err = parseTime(t)
		if err != nil {
			return nil, nil, &ApiError{errorBadData, err}
		}
	} else {
		start = minTime
	}

	var end time.Time
	if t := r.FormValue("end"); t != "" {
		var err error
		end, err = parseTime(t)
		if err != nil {
			return nil, nil, &ApiError{errorBadData, err}
		}
	} else {
		end = maxTime
	}

	var matcherSets [][]storepb.LabelMatcher
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return nil, nil, &ApiError{errorBadData, err}
		}

		pbmatchers, err := query.TranslateMatchers(matchers...)
		if err != nil {
			return nil, nil, &ApiError{errorBadData, err}
		}
		matcherSets = append(matcherSets, pbmatchers)
	}

	opts, err := query.NewOptionsFromForm(r.Form, api.defOpts)
	if err != nil {
		return nil, nil, &ApiError{errorBadData, err}
	}

	if opts.Resolution != nil {
		return nil, nil, &ApiError{errorBadData, errors.New("downsampling options are not supported on series")}
	}

	span, ctx := tracing.StartSpan(r.Context(), "querier_series")
	defer span.Finish()

	var (
		series   []storepb.Series
		warnings []string
	)

	// TODO(bwplotka): Consider doing it concurrently at some point.
	for _, mset := range matcherSets {
		resp := query.NewSeriesServer(ctx)
		if err := api.nonChunkStoreAPI.Series(&storepb.SeriesRequest{
			MinTime:  timestamp.FromTime(start),
			MaxTime:  timestamp.FromTime(end),
			Matchers: mset,

			// TODO(bwplotka): Technically we can use whatever block as series as same for both downsampled and raw blocks.
			// MaxInt64 then?
			MaxResolutionWindow:     0,
			PartialResponseDisabled: opts.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT,
			PartialResponseStrategy: opts.PartialResponseStrategy,
		}, resp); err != nil {
			return nil, nil, &ApiError{errorExec, err}
		}

		s, warns := resp.Response()
		warnings = append(warnings, warns...)
		series = append(series, s...)
	}

	set := query.NewStoreSeriesSet(series)

	var metrics [][]storepb.Label
	for set.Next() {
		lbls, _ := set.At()
		metrics = append(metrics, lbls)
	}
	if set.Err() != nil {
		return nil, nil, &ApiError{errorExec, set.Err()}
	}

	return metrics, toErrs(warnings), nil
}

func Respond(w http.ResponseWriter, data interface{}, warnings []error) {
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

func RespondError(w http.ResponseWriter, apiErr *ApiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	var code int
	switch apiErr.Typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case ErrorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(&response{
		Status:    statusError,
		ErrorType: apiErr.Typ,
		Error:     apiErr.Err.Error(),
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
