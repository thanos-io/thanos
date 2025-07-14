// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

// MetricsRangeQueryLogging represents the logging information for a range query
type MetricsRangeQueryLogging struct {
	TimestampMs   int64  `json:"timestampMs"`
	Source        string `json:"source"`
	QueryExpr     string `json:"queryExpr"`
	Success       bool   `json:"success"`
	BytesFetched  int64  `json:"bytesFetched"`
	EvalLatencyMs int64  `json:"evalLatencyMs"`
}

type rangeQueryLoggingMiddleware struct {
	next                    queryrange.Handler
	logger                  log.Logger
	logFile                 *os.File
	queriesLogged           prometheus.Counter
	successfulQueriesLogged prometheus.Counter
	failedQueriesLogged     prometheus.Counter
}

// NewRangeQueryLoggingMiddleware creates a new middleware that logs range query information
func NewRangeQueryLoggingMiddleware(logger log.Logger, reg prometheus.Registerer) queryrange.Middleware {
	// Create the /databricks/logs directory if it doesn't exist
	logDir := "/databricks/logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		level.Error(logger).Log("msg", "failed to create log directory", "dir", logDir, "err", err)
	}

	// Open the log file for range query logging
	logFile, err := os.OpenFile(filepath.Join(logDir, "rangequerylogging.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		level.Error(logger).Log("msg", "failed to open range query logging file", "err", err)
		logFile = nil
	}

	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return &rangeQueryLoggingMiddleware{
			next:    next,
			logger:  logger,
			logFile: logFile,
			queriesLogged: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "thanos_frontend_rangequerylogging_logged_total",
				Help: "Total number of range queries logged",
			}),
			successfulQueriesLogged: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "thanos_frontend_rangequerylogging_successful_total",
				Help: "Total number of successful range queries logged",
			}),
			failedQueriesLogged: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Name: "thanos_frontend_rangequerylogging_failed_total",
				Help: "Total number of failed range queries logged",
			}),
		}
	})
}

func (m *rangeQueryLoggingMiddleware) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	// Only log for range queries
	rangeReq, ok := r.(*ThanosQueryRangeRequest)
	if !ok {
		return m.next.Do(ctx, r)
	}

	startTime := time.Now()

	// Execute the query
	resp, err := m.next.Do(ctx, r)

	// Calculate latency
	latencyMs := time.Since(startTime).Milliseconds()

	// Log the range query
	m.logRangeQuery(rangeReq, resp, err, latencyMs)

	return resp, err
}

func (m *rangeQueryLoggingMiddleware) logRangeQuery(req *ThanosQueryRangeRequest, resp queryrange.Response, err error, latencyMs int64) {
	// Increment total queries logged
	m.queriesLogged.Inc()

	// Determine success
	success := err == nil

	// Determine source from headers
	source := m.extractSource(req)

	// Calculate bytes fetched (only for successful queries)
	var bytesFetched int64
	if success && resp != nil {
		bytesFetched = m.calculateBytesFetched(resp)
	}

	// Create the range query log entry
	rangeQueryLog := MetricsRangeQueryLogging{
		TimestampMs:   time.Now().UnixMilli(),
		Source:        source,
		QueryExpr:     req.Query,
		Success:       success,
		BytesFetched:  bytesFetched,
		EvalLatencyMs: latencyMs,
	}

	// Update metrics
	if success {
		m.successfulQueriesLogged.Inc()
	} else {
		m.failedQueriesLogged.Inc()
	}

	// Log to file if available
	if m.logFile != nil {
		m.writeToLogFile(rangeQueryLog)
	}

	// Also log to regular logger at debug level
	level.Debug(m.logger).Log(
		"msg", "rangequerylogging",
		"timestampMs", rangeQueryLog.TimestampMs,
		"source", rangeQueryLog.Source,
		"queryExpr", rangeQueryLog.QueryExpr,
		"success", rangeQueryLog.Success,
		"bytesFetched", rangeQueryLog.BytesFetched,
		"evalLatencyMs", rangeQueryLog.EvalLatencyMs,
	)
}

func (m *rangeQueryLoggingMiddleware) extractSource(req *ThanosQueryRangeRequest) string {
	// Check User-Agent header to determine source
	for _, header := range req.Headers {
		if strings.ToLower(header.Name) == "user-agent" && len(header.Values) > 0 {
			userAgent := strings.ToLower(header.Values[0])
			if strings.Contains(userAgent, "grafana") {
				return "Grafana"
			}
			if strings.Contains(userAgent, "bronson") {
				return "Bronson"
			}
			if strings.Contains(userAgent, "pandora") {
				return "Pandora"
			}
			// Return the first part of the user agent if no specific match
			parts := strings.Split(userAgent, " ")
			if len(parts) > 0 {
				return parts[0]
			}
		}
	}

	// Check X-Source header as fallback
	for _, header := range req.Headers {
		if strings.ToLower(header.Name) == "x-source" && len(header.Values) > 0 {
			return header.Values[0]
		}
	}

	return "unknown"
}

func (m *rangeQueryLoggingMiddleware) calculateBytesFetched(resp queryrange.Response) int64 {
	if resp == nil {
		return 0
	}

	// Try to estimate bytes from response
	// This is a rough approximation based on the response structure
	switch r := resp.(type) {
	case *queryrange.PrometheusResponse:
		if len(r.Data.Result) > 0 {
			// Rough estimation: each sample is approximately 24 bytes (timestamp + value)
			// plus some overhead for labels
			totalSamples := int64(0)
			totalLabelBytes := int64(0)

			for _, series := range r.Data.Result {
				totalSamples += int64(len(series.Samples))
				totalSamples += int64(len(series.Histograms))

				// Estimate label bytes
				for _, label := range series.Labels {
					totalLabelBytes += int64(len(label.Name) + len(label.Value))
				}
			}

			// Rough estimation: 24 bytes per sample + 2 bytes per label character
			return totalSamples*24 + totalLabelBytes*2
		}
	}

	return 0
}

func (m *rangeQueryLoggingMiddleware) writeToLogFile(rangeQueryLog MetricsRangeQueryLogging) {
	if m.logFile == nil {
		return
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(rangeQueryLog)
	if err != nil {
		level.Error(m.logger).Log("msg", "failed to marshal range query log to JSON", "err", err)
		return
	}

	// Write to file with newline
	if _, err := fmt.Fprintf(m.logFile, "%s\n", jsonData); err != nil {
		level.Error(m.logger).Log("msg", "failed to write range query log to file", "err", err)
	}
}

// Close should be called when the middleware is no longer needed
func (m *rangeQueryLoggingMiddleware) Close() error {
	if m.logFile != nil {
		return m.logFile.Close()
	}
	return nil
}
