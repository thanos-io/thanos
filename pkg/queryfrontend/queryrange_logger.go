// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"gopkg.in/natefinch/lumberjack.v2"
)

// MetricsRangeQueryLogging represents the logging information for a range query.
type MetricsRangeQueryLogging struct {
	TimestampMs       int64  `json:"timestampMs"`
	Source            string `json:"source"`
	QueryExpr         string `json:"queryExpr"`
	Success           bool   `json:"success"`
	BytesFetched      int64  `json:"bytesFetched"`
	TimeseriesFetched int64  `json:"timeseriesFetched"`
	Chunks            int64  `json:"chunks"`
	Samples           int64  `json:"samples"`
	EvalLatencyMs     int64  `json:"evalLatencyMs"`
	// User identification fields
	GrafanaDashboardUid string `json:"grafanaDashboardUid"`
	GrafanaPanelId      string `json:"grafanaPanelId"`
	RequestId           string `json:"requestId"`
	Tenant              string `json:"tenant"`
	ForwardedFor        string `json:"forwardedFor"`
	UserAgent           string `json:"userAgent"`
	EmailId             string `json:"emailId"`
	Groups              string `json:"groups"`
	// Query-related fields
	StartTimestampMs      int64    `json:"startTimestampMs"`
	EndTimestampMs        int64    `json:"endTimestampMs"`
	StepMs                int64    `json:"stepMs"`
	Path                  string   `json:"path"`
	Dedup                 bool     `json:"dedup"`                 // Whether deduplication is enabled
	PartialResponse       bool     `json:"partialResponse"`       // Whether partial responses are allowed
	AutoDownsampling      bool     `json:"autoDownsampling"`      // Whether automatic downsampling is enabled
	MaxSourceResolutionMs int64    `json:"maxSourceResolutionMs"` // Maximum source resolution in milliseconds
	ReplicaLabels         []string `json:"replicaLabels"`         // Labels used for replica deduplication
	StoreMatchersCount    int      `json:"storeMatchersCount"`    // Number of store matcher sets
	LookbackDeltaMs       int64    `json:"lookbackDeltaMs"`       // Lookback delta in milliseconds
	Analyze               bool     `json:"analyze"`               // Whether query analysis is enabled
	Engine                string   `json:"engine"`                // Query engine being used
	SplitIntervalMs       int64    `json:"splitIntervalMs"`       // Query splitting interval in milliseconds
	Stats                 string   `json:"stats"`                 // Query statistics information
	MetricNames           []string `json:"metricNames"`           // Unique metric names (__name__ labels) in response
	Shard                 string   `json:"shard"`                 // Pantheon shard name
	// Store-matcher details
	StoreMatchers []StoreMatcherSet `json:"storeMatchers"`
}

// RangeQueryLogConfig holds configuration for range query logging.
type RangeQueryLogConfig = QueryLogConfig

// DefaultRangeQueryLogConfig returns the default configuration for range query logging.
func DefaultRangeQueryLogConfig() RangeQueryLogConfig {
	return RangeQueryLogConfig{
		LogDir:     "/databricks/logs/pantheon-range-query-frontend",
		MaxSizeMB:  2048, // 2GB per file
		MaxAge:     7,    // Keep logs for 7 days
		MaxBackups: 5,    // Keep 5 backup files
		Compress:   true,
	}
}

type rangeQueryLoggingMiddleware struct {
	next   queryrange.Handler
	logger log.Logger
	writer io.WriteCloser
}

// NewRangeQueryLoggingMiddleware creates a new middleware that logs range query information.
func NewRangeQueryLoggingMiddleware(logger log.Logger, reg prometheus.Registerer) queryrange.Middleware {
	return NewRangeQueryLoggingMiddlewareWithConfig(logger, reg, DefaultRangeQueryLogConfig())
}

// NewRangeQueryLoggingMiddlewareWithConfig creates a new middleware with custom configuration.
func NewRangeQueryLoggingMiddlewareWithConfig(logger log.Logger, reg prometheus.Registerer, config RangeQueryLogConfig) queryrange.Middleware {
	// Create the log directory if it doesn't exist.
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		level.Error(logger).Log("msg", "failed to create log directory", "dir", config.LogDir, "err", err)
	}

	// Create the rotating file logger.
	var writer io.WriteCloser
	logFilePath := filepath.Join(config.LogDir, "PantheonRangeQueryLog.json")

	rotatingLogger := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    config.MaxSizeMB,
		MaxAge:     config.MaxAge,
		MaxBackups: config.MaxBackups,
		Compress:   config.Compress,
	}

	writer = rotatingLogger

	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return &rangeQueryLoggingMiddleware{
			next:   next,
			logger: logger,
			writer: writer,
		}
	})
}

func (m *rangeQueryLoggingMiddleware) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	// Only log for range queries.
	rangeReq, ok := r.(*ThanosQueryRangeRequest)
	if !ok {
		return m.next.Do(ctx, r)
	}

	startTime := time.Now()

	// Execute the query.
	resp, err := m.next.Do(ctx, r)

	// Calculate latency.
	latencyMs := time.Since(startTime).Milliseconds()

	// Log the range query.
	m.logRangeQuery(rangeReq, resp, err, latencyMs)

	return resp, err
}

func (m *rangeQueryLoggingMiddleware) logRangeQuery(req *ThanosQueryRangeRequest, resp queryrange.Response, err error, latencyMs int64) {
	success := err == nil
	userInfo := ExtractUserInfoFromHeaders(req.Headers)

	// Calculate stats (only for successful queries).
	var stats ResponseStats
	var metricNames []string
	if success && resp != nil {
		stats = GetResponseStats(resp)
		metricNames = ExtractMetricNames(resp)
	}

	// Create the range query log entry.
	rangeQueryLog := MetricsRangeQueryLogging{
		TimestampMs:       time.Now().UnixMilli(),
		Source:            userInfo.Source,
		QueryExpr:         req.Query,
		Success:           success,
		BytesFetched:      stats.BytesFetched,
		TimeseriesFetched: stats.TimeseriesFetched,
		Chunks:            stats.Chunks,
		Samples:           stats.Samples,
		EvalLatencyMs:     latencyMs,
		// User identification fields
		GrafanaDashboardUid: userInfo.GrafanaDashboardUid,
		GrafanaPanelId:      userInfo.GrafanaPanelId,
		RequestId:           userInfo.RequestId,
		Tenant:              userInfo.Tenant,
		ForwardedFor:        userInfo.ForwardedFor,
		UserAgent:           userInfo.UserAgent,
		EmailId:             userInfo.Email,
		Groups:              userInfo.Groups,
		// Query-related fields
		StartTimestampMs:      req.Start,
		EndTimestampMs:        req.End,
		StepMs:                req.Step,
		Path:                  req.Path,
		Dedup:                 req.Dedup,
		PartialResponse:       req.PartialResponse,
		AutoDownsampling:      req.AutoDownsampling,
		MaxSourceResolutionMs: req.MaxSourceResolution,
		ReplicaLabels:         req.ReplicaLabels,
		StoreMatchersCount:    len(req.StoreMatchers),
		LookbackDeltaMs:       req.LookbackDelta,
		Analyze:               req.Analyze,
		Engine:                req.Engine,
		SplitIntervalMs:       req.SplitInterval.Milliseconds(),
		Stats:                 req.Stats,
		MetricNames:           metricNames,
		Shard:                 os.Getenv("PANTHEON_SHARDNAME"),
		// Store-matcher details
		StoreMatchers: ConvertStoreMatchers(req.StoreMatchers),
	}

	// Log to file if available.
	if m.writer != nil {
		m.writeToLogFile(rangeQueryLog)
	}

}

func (m *rangeQueryLoggingMiddleware) writeToLogFile(rangeQueryLog MetricsRangeQueryLogging) {
	err := WriteJSONLogToFile(m.logger, m.writer, rangeQueryLog, "range")
	if err != nil {
		level.Error(m.logger).Log("msg", "failed to write range query log to file", "err", err)
	}
}

// Close should be called when the middleware is no longer needed.
func (m *rangeQueryLoggingMiddleware) Close() error {
	if m.writer != nil {
		return m.writer.Close()
	}
	return nil
}
