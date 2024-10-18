// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/util/stats"
	"io"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"

	"github.com/thanos-io/thanos/internal/cortex/util"
	util_log "github.com/thanos-io/thanos/internal/cortex/util/log"
)

const (
	// StatusClientClosedRequest is the status code for when a client request cancellation of an http request
	StatusClientClosedRequest = 499
	ServiceTimingHeaderName   = "Server-Timing"
)

var (
	errCanceled              = httpgrpc.Errorf(StatusClientClosedRequest, context.Canceled.Error())
	errDeadlineExceeded      = httpgrpc.Errorf(http.StatusGatewayTimeout, context.DeadlineExceeded.Error())
	errRequestEntityTooLarge = httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "http: request body too large")
)

// Config for a Handler.
type HandlerConfig struct {
	LogQueriesLongerThan time.Duration `yaml:"log_queries_longer_than"`
	MaxBodySize          int64         `yaml:"max_body_size"`
	QueryStatsEnabled    bool          `yaml:"query_stats_enabled"`
}

// Handler accepts queries and forwards them to RoundTripper. It can log slow queries,
// but all other logic is inside the RoundTripper.
type Handler struct {
	cfg          HandlerConfig
	log          log.Logger
	roundTripper http.RoundTripper

	// Metrics.
	querySeconds      *prometheus.HistogramVec
	querySamplesTotal *prometheus.HistogramVec
	activeUsers       *util.ActiveUsersCleanupService
}

// NewHandler creates a new frontend handler.
func NewHandler(cfg HandlerConfig, roundTripper http.RoundTripper, log log.Logger, reg prometheus.Registerer) http.Handler {
	h := &Handler{
		cfg:          cfg,
		log:          log,
		roundTripper: roundTripper,
	}

	if cfg.QueryStatsEnabled {
		h.querySeconds = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_query_frontend_query_seconds",
			Help:    "Total amount of wall clock time spend processing queries.",
			Buckets: []float64{0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 360},
		}, []string{"user"})

		h.querySamplesTotal = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_query_frontend_query_total_fetched_samples",
			Help:    "Number of samples touched to execute a query.",
			Buckets: []float64{1, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000},
		}, []string{"user"})

		h.activeUsers = util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
			h.querySeconds.DeleteLabelValues(user)
			h.querySamplesTotal.DeleteLabelValues(user)
		})
		// If cleaner stops or fail, we will simply not clean the metrics for inactive users.
		_ = h.activeUsers.StartAsync(context.Background())
	}

	return h
}

type ResponseDataWithStats struct {
	Stats *stats.BuiltinStats `json:"stats"`
}
type ResponseWithStats struct {
	Data ResponseDataWithStats `json:"data"`
}

func (f *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		queryString url.Values
	)

	defer func() {
		_ = r.Body.Close()
	}()

	// Buffer the request body for later use to track slow queries.
	var buf bytes.Buffer
	r.Body = http.MaxBytesReader(w, r.Body, f.cfg.MaxBodySize)
	r.Body = io.NopCloser(io.TeeReader(r.Body, &buf))

	startTime := time.Now()
	resp, err := f.roundTripper.RoundTrip(r)
	queryResponseTime := time.Since(startTime)

	if err != nil {
		writeError(w, err)
		return
	}

	hs := w.Header()
	for h, vs := range resp.Header {
		hs[h] = vs
	}

	w.WriteHeader(resp.StatusCode)

	var respBuf bytes.Buffer
	if f.cfg.QueryStatsEnabled {
		// Buffer the response body for query stat tracking later
		resp.Body = io.NopCloser(io.TeeReader(resp.Body, &respBuf))
	}

	// log copy response body error so that we will know even though success response code returned
	bytesCopied, err := io.Copy(w, resp.Body)
	if err != nil && !errors.Is(err, syscall.EPIPE) {
		level.Error(util_log.WithContext(r.Context(), f.log)).Log("msg", "write response body error", "bytesCopied", bytesCopied, "err", err)
	}

	if f.cfg.QueryStatsEnabled {
		// Parse the stats field out of the response body
		var statsResponse ResponseWithStats
		if err := json.Unmarshal(respBuf.Bytes(), &statsResponse); err == nil {
			if statsResponse.Data.Stats != nil {
				queryString = f.parseRequestQueryString(r, buf)
				f.reportQueryStats(r, queryString, queryResponseTime, statsResponse.Data.Stats)
			} else {
				// Don't fail the request if the stats are nil, just log a warning
				level.Warn(util_log.WithContext(r.Context(), f.log)).Log("msg", "error parsing query stats", "err", errors.New("stats are nil"))
			}
		} else {
			// Don't fail the request if the stats are nil, just log a warning
			level.Warn(util_log.WithContext(r.Context(), f.log)).Log("msg", "error parsing query stats", "err", err)
		}
	}

	// Check whether we should parse the query string.
	shouldReportSlowQuery := f.cfg.LogQueriesLongerThan != 0 && queryResponseTime > f.cfg.LogQueriesLongerThan
	if shouldReportSlowQuery || f.cfg.QueryStatsEnabled {
		queryString = f.parseRequestQueryString(r, buf)
	}

	if shouldReportSlowQuery {
		f.reportSlowQuery(r, hs, queryString, queryResponseTime)
	}
}

// reportSlowQuery reports slow queries.
func (f *Handler) reportSlowQuery(r *http.Request, responseHeaders http.Header, queryString url.Values, queryResponseTime time.Duration) {
	// NOTE(GiedriusS): see https://github.com/grafana/grafana/pull/60301 for more info.
	grafanaDashboardUID := "-"
	if dashboardUID := r.Header.Get("X-Dashboard-Uid"); dashboardUID != "" {
		grafanaDashboardUID = dashboardUID
	}
	grafanaPanelID := "-"
	if panelID := r.Header.Get("X-Panel-Id"); panelID != "" {
		grafanaPanelID = panelID
	}
	thanosTraceID := "-"
	if traceID := responseHeaders.Get("X-Thanos-Trace-Id"); traceID != "" {
		thanosTraceID = traceID
	}

	remoteUser, _, _ := r.BasicAuth()

	logMessage := append([]interface{}{
		"msg", "slow query detected",
		"method", r.Method,
		"host", r.Host,
		"path", r.URL.Path,
		"remote_user", remoteUser,
		"remote_addr", r.RemoteAddr,
		"time_taken", queryResponseTime.String(),
		"grafana_dashboard_uid", grafanaDashboardUID,
		"grafana_panel_id", grafanaPanelID,
		"trace_id", thanosTraceID,
	}, formatQueryString(queryString)...)

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)
}

func (f *Handler) reportQueryStats(r *http.Request, queryString url.Values, queryResponseTime time.Duration, stats *stats.BuiltinStats) {
	remoteUser, _, _ := r.BasicAuth()

	// Log stats.
	logMessage := []interface{}{
		"msg", "query stats",
		"component", "query-frontend",
		"method", r.Method,
		"path", r.URL.Path,
		"remote_user", remoteUser,
		"remote_addr", r.RemoteAddr,
		"response_time", queryResponseTime,
		"query_timings_preparation_time", stats.Timings.QueryPreparationTime,
		"query_timings_eval_total_time", stats.Timings.EvalTotalTime,
		"query_timings_exec_total_time", stats.Timings.ExecTotalTime,
		"query_timings_exec_queue_time", stats.Timings.ExecQueueTime,
		"query_timings_inner_eval_time", stats.Timings.InnerEvalTime,
		"query_timings_result_sort_time", stats.Timings.ResultSortTime,
	}
	if stats.Samples != nil {
		samples := stats.Samples

		logMessage = append(logMessage, []interface{}{
			"total_queryable_samples", samples.TotalQueryableSamples,
			"peak_samples", samples.PeakSamples,
		}...)
	}

	logMessage = append(logMessage, formatQueryString(queryString)...)

	level.Info(util_log.WithContext(r.Context(), f.log)).Log(logMessage...)

	// Record metrics.
	if f.querySeconds != nil {
		f.querySeconds.WithLabelValues(remoteUser).Observe(queryResponseTime.Seconds())
	}
	if f.querySamplesTotal != nil && stats.Samples != nil {
		f.querySamplesTotal.WithLabelValues(remoteUser).Observe(float64(stats.Samples.TotalQueryableSamples))
	}
}

func (f *Handler) parseRequestQueryString(r *http.Request, bodyBuf bytes.Buffer) url.Values {
	// Use previously buffered body.
	r.Body = io.NopCloser(&bodyBuf)

	// Ensure the form has been parsed so all the parameters are present
	err := r.ParseForm()
	if err != nil {
		level.Warn(util_log.WithContext(r.Context(), f.log)).Log("msg", "unable to parse request form", "err", err)
		return nil
	}

	return r.Form
}

func formatQueryString(queryString url.Values) (fields []interface{}) {
	for k, v := range queryString {
		fields = append(fields, fmt.Sprintf("param_%s", k), strings.Join(v, ","))
	}
	return fields
}

func writeError(w http.ResponseWriter, err error) {
	switch err {
	case context.Canceled:
		err = errCanceled
	case context.DeadlineExceeded:
		err = errDeadlineExceeded
	default:
		if util.IsRequestBodyTooLarge(err) {
			err = errRequestEntityTooLarge
		}
	}
	server.WriteError(w, err)
}
