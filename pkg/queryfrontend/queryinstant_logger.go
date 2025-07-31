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

// MetricsInstantQueryLogging represents the logging information for an instant query.
type MetricsInstantQueryLogging struct {
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
	EmailId             string `json:"emailID"`
	// Query-related fields (instant query specific)
	QueryTimestampMs      int64    `json:"queryTimestampMs"` // Query timestamp for instant queries
	Path                  string   `json:"path"`
	Dedup                 bool     `json:"dedup"`                 // Whether deduplication is enabled
	PartialResponse       bool     `json:"partialResponse"`       // Whether partial responses are allowed
	AutoDownsampling      bool     `json:"autoDownsampling"`      // Whether automatic downsampling is enabled
	MaxSourceResolutionMs int64    `json:"maxSourceResolutionMs"` // Maximum source resolution in milliseconds
	ReplicaLabels         []string `json:"replicaLabels"`
	StoreMatchersCount    int      `json:"storeMatchersCount"` // Number of store matcher sets
	LookbackDeltaMs       int64    `json:"lookbackDeltaMs"`    // Lookback delta in milliseconds
	Analyze               bool     `json:"analyze"`            // Whether query analysis is enabled
	Engine                string   `json:"engine"`             // Query engine being used
	Stats                 string   `json:"stats"`              // Query statistics information
	// Store-matcher details
	StoreMatchers []StoreMatcherSet `json:"storeMatchers"`
}

// InstantQueryLogConfig holds configuration for instant query logging.
type InstantQueryLogConfig = QueryLogConfig

// DefaultInstantQueryLogConfig returns the default configuration for instant query logging.
func DefaultInstantQueryLogConfig() InstantQueryLogConfig {
	return InstantQueryLogConfig{
		LogDir:     "/databricks/logs/pantheon-instant-query-frontend",
		MaxSizeMB:  2048, // 2GB per file
		MaxAge:     7,    // Keep logs for 7 days
		MaxBackups: 5,    // Keep 5 backup files
		Compress:   true,
	}
}

type instantQueryLoggingMiddleware struct {
	next   queryrange.Handler
	logger log.Logger
	writer io.WriteCloser
}

// NewInstantQueryLoggingMiddleware creates a new middleware that logs instant query information.
func NewInstantQueryLoggingMiddleware(logger log.Logger, reg prometheus.Registerer) queryrange.Middleware {
	return NewInstantQueryLoggingMiddlewareWithConfig(logger, reg, DefaultInstantQueryLogConfig())
}

// NewInstantQueryLoggingMiddlewareWithConfig creates a new middleware with custom configuration.
func NewInstantQueryLoggingMiddlewareWithConfig(logger log.Logger, reg prometheus.Registerer, config InstantQueryLogConfig) queryrange.Middleware {
	// Create the log directory if it doesn't exist.
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		level.Error(logger).Log("msg", "failed to create log directory", "dir", config.LogDir, "err", err)
	}

	// Create the rotating file logger.
	var writer io.WriteCloser
	logFilePath := filepath.Join(config.LogDir, "PantheonInstantQueryLog.json")

	rotatingLogger := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    config.MaxSizeMB,
		MaxAge:     config.MaxAge,
		MaxBackups: config.MaxBackups,
		Compress:   config.Compress,
	}

	writer = rotatingLogger

	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return &instantQueryLoggingMiddleware{
			next:   next,
			logger: logger,
			writer: writer,
		}
	})
}

func (m *instantQueryLoggingMiddleware) Do(ctx context.Context, r queryrange.Request) (queryrange.Response, error) {
	// Only log for instant queries.
	instantReq, ok := r.(*ThanosQueryInstantRequest)
	if !ok {
		return m.next.Do(ctx, r)
	}

	startTime := time.Now()

	// Execute the query.
	resp, err := m.next.Do(ctx, r)

	// Calculate latency.
	latencyMs := time.Since(startTime).Milliseconds()

	// Log the instant query.
	m.logInstantQuery(instantReq, resp, err, latencyMs)

	return resp, err
}

func (m *instantQueryLoggingMiddleware) logInstantQuery(req *ThanosQueryInstantRequest, resp queryrange.Response, err error, latencyMs int64) {
	success := err == nil
	userInfo := ExtractUserInfoFromHeaders(req.Headers)

	// Extract email from response headers
	email := ExtractEmailFromResponse(resp)

	// If email is empty, don't log the query.
	// This is to avoid logging queries that come from rule manager.
	if email == "" {
		return
	}

	// Calculate stats (only for successful queries).
	var stats ResponseStats
	if success && resp != nil {
		stats = GetResponseStats(resp)
	}

	// Create the instant query log entry.
	instantQueryLog := MetricsInstantQueryLogging{
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
		EmailId:             email,
		// Query-related fields (instant query specific)
		QueryTimestampMs:      req.Time,
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
		Stats:                 req.Stats,
		// Store-matcher details
		StoreMatchers: ConvertStoreMatchers(req.StoreMatchers),
	}

	// Log to file if available.
	if m.writer != nil {
		m.writeToLogFile(instantQueryLog)
	}
}

func (m *instantQueryLoggingMiddleware) writeToLogFile(instantQueryLog MetricsInstantQueryLogging) {
	err := WriteJSONLogToFile(m.logger, m.writer, instantQueryLog, "instant")
	if err != nil {
		level.Error(m.logger).Log("msg", "failed to write instant query log to file", "err", err)
	}
}

// Close should be called when the middleware is no longer needed.
func (m *instantQueryLoggingMiddleware) Close() error {
	if m.writer != nil {
		return m.writer.Close()
	}
	return nil
}
