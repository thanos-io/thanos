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
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

// MetricsRangeQueryLogging represents the logging information for a range query.
type MetricsRangeQueryLogging struct {
	TimestampMs   int64  `json:"timestampMs"`
	Source        string `json:"source"`
	QueryExpr     string `json:"queryExpr"`
	Success       bool   `json:"success"`
	BytesFetched  int64  `json:"bytesFetched"`
	EvalLatencyMs int64  `json:"evalLatencyMs"`
	// User identification fields
	GrafanaDashboardUid string `json:"grafanaDashboardUid"`
	GrafanaPanelId      string `json:"grafanaPanelId"`
	RequestId           string `json:"requestId"`
	Tenant              string `json:"tenant"`
	ForwardedFor        string `json:"forwardedFor"`
	UserAgent           string `json:"userAgent"`
	// Query-related fields
	StartTimestampMs      int64    `json:"startTimestampMs"`
	EndTimestampMs        int64    `json:"endTimestampMs"`
	StepMs                int64    `json:"stepMs"`
	Path                  string   `json:"path"`
	Dedup                 bool     `json:"dedup"`
	PartialResponse       bool     `json:"partialResponse"`
	AutoDownsampling      bool     `json:"autoDownsampling"`
	MaxSourceResolutionMs int64    `json:"maxSourceResolutionMs"`
	ReplicaLabels         []string `json:"replicaLabels"`
	StoreMatchersCount    int      `json:"storeMatchersCount"`
	LookbackDeltaMs       int64    `json:"lookbackDeltaMs"`
	Analyze               bool     `json:"analyze"`
	Engine                string   `json:"engine,omitempty"`
	SplitIntervalMs       int64    `json:"splitIntervalMs"`
	Stats                 string   `json:"stats,omitempty"`
	// Store-matcher details
	StoreMatchers []StoreMatcherSet `json:"storeMatchers"`
}

// StoreMatcherSet represents a set of label matchers for store filtering.
type StoreMatcherSet struct {
	Matchers []LabelMatcher `json:"matchers"`
}

// LabelMatcher represents a single label matcher.
type LabelMatcher struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type"` // EQ, NEQ, RE, NRE
}

// UserInfo holds user identification information extracted from request headers.
type UserInfo struct {
	Source              string
	GrafanaDashboardUid string
	GrafanaPanelId      string
	RequestId           string
	Tenant              string
	ForwardedFor        string
	UserAgent           string
}

type rangeQueryLoggingMiddleware struct {
	next    queryrange.Handler
	logger  log.Logger
	logFile *os.File
}

// NewRangeQueryLoggingMiddleware creates a new middleware that logs range query information.
func NewRangeQueryLoggingMiddleware(logger log.Logger, reg prometheus.Registerer) queryrange.Middleware {
	// Create the /databricks/logs directory if it doesn't exist.
	logDir := "/databricks/logs/pantheon-range-query-frontend"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		level.Error(logger).Log("msg", "failed to create log directory", "dir", logDir, "err", err)
	}

	// Open the log file for range query logging.
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
	userInfo := m.extractUserInfo(req)

	// Calculate bytes fetched (only for successful queries).
	var bytesFetched int64 = 0
	if success && resp != nil {
		bytesFetched = m.calculateBytesFetched(resp)
	}

	// Create the range query log entry.
	rangeQueryLog := MetricsRangeQueryLogging{
		TimestampMs:   time.Now().UnixMilli(),
		Source:        userInfo.Source,
		QueryExpr:     req.Query,
		Success:       success,
		BytesFetched:  bytesFetched,
		EvalLatencyMs: latencyMs,
		// User identification fields
		GrafanaDashboardUid: userInfo.GrafanaDashboardUid,
		GrafanaPanelId:      userInfo.GrafanaPanelId,
		RequestId:           userInfo.RequestId,
		Tenant:              userInfo.Tenant,
		ForwardedFor:        userInfo.ForwardedFor,
		UserAgent:           userInfo.UserAgent,
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
		// Store-matcher details
		StoreMatchers: m.convertStoreMatchers(req.StoreMatchers),
	}

	// Log to file if available.
	if m.logFile != nil {
		m.writeToLogFile(rangeQueryLog)
	}

}

func (m *rangeQueryLoggingMiddleware) extractUserInfo(req *ThanosQueryRangeRequest) UserInfo {
	userInfo := UserInfo{}

	for _, header := range req.Headers {
		headerName := strings.ToLower(header.Name)
		if len(header.Values) == 0 {
			continue
		}
		headerValue := header.Values[0]

		switch headerName {
		case "user-agent":
			userInfo.UserAgent = headerValue
			// Determine source from User-Agent if not already set.
			if userInfo.Source == "" {
				userAgentLower := strings.ToLower(headerValue)
				if strings.Contains(userAgentLower, "grafana") {
					userInfo.Source = "Grafana"
				} else if strings.Contains(userAgentLower, "bronson") {
					userInfo.Source = "Bronson"
				} else if strings.Contains(userAgentLower, "pandora") {
					userInfo.Source = "Pandora"
				} else {
					// Return the first part of the user agent if no specific match.
					parts := strings.Split(userAgentLower, " ")
					if len(parts) > 0 {
						userInfo.Source = parts[0]
					}
				}
			}
		case "x-dashboard-uid":
			userInfo.GrafanaDashboardUid = headerValue
		case "x-panel-id":
			userInfo.GrafanaPanelId = headerValue
		case "x-request-id":
			userInfo.RequestId = headerValue
		case "thanos-tenant":
			userInfo.Tenant = headerValue
		case "x-forwarded-for":
			userInfo.ForwardedFor = headerValue
		case "x-source":
			// X-Source header as fallback for source.
			if userInfo.Source == "" {
				userInfo.Source = headerValue
			}
		}
	}

	// Set default source if still empty.
	if userInfo.Source == "" {
		userInfo.Source = "unknown"
	}

	return userInfo
}

// convertStoreMatchers converts internal store matchers to logging format.
func (m *rangeQueryLoggingMiddleware) convertStoreMatchers(storeMatchers [][]*labels.Matcher) []StoreMatcherSet {
	if len(storeMatchers) == 0 {
		return nil
	}

	result := make([]StoreMatcherSet, len(storeMatchers))
	for i, matcherSet := range storeMatchers {
		matchers := make([]LabelMatcher, len(matcherSet))
		for j, matcher := range matcherSet {
			matchers[j] = LabelMatcher{
				Name:  matcher.Name,
				Value: matcher.Value,
				Type:  matcher.Type.String(),
			}
		}
		result[i] = StoreMatcherSet{
			Matchers: matchers,
		}
	}
	return result
}

func (m *rangeQueryLoggingMiddleware) calculateBytesFetched(resp queryrange.Response) int64 {
	if resp == nil {
		return 0
	}

	// Use SeriesStatsCounter.Bytes for range queries only.
	if r, ok := resp.(*queryrange.PrometheusResponse); ok {
		if r.Data.SeriesStatsCounter != nil {
			return r.Data.SeriesStatsCounter.Bytes
		}
	}

	return 0
}

func (m *rangeQueryLoggingMiddleware) writeToLogFile(rangeQueryLog MetricsRangeQueryLogging) {
	if m.logFile == nil {
		return
	}

	// Marshal to JSON.
	jsonData, err := json.Marshal(rangeQueryLog)
	if err != nil {
		level.Error(m.logger).Log("msg", "failed to marshal range query log to JSON", "err", err)
		return
	}

	// Write to file with newline.
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
