// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

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
	Groups              string
	Email               string
}

// ResponseStats holds statistics extracted from query response.
type ResponseStats struct {
	BytesFetched      int64
	TimeseriesFetched int64
	Chunks            int64
	Samples           int64
}

// QueryLogConfig holds configuration for query logging.
type QueryLogConfig struct {
	LogDir     string // Directory to store log files.
	MaxSizeMB  int    // Maximum size in megabytes before rotation.
	MaxAge     int    // Maximum number of days to retain old log files.
	MaxBackups int    // Maximum number of old log files to retain.
	Compress   bool   // Whether to compress rotated files.
}

// ExtractUserInfoFromHeaders extracts user info from request headers (works for both range and instant queries).
func ExtractUserInfoFromHeaders(headers []*RequestHeader) UserInfo {
	userInfo := UserInfo{}

	for _, header := range headers {
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
		case "x-auth-request-groups":
			userInfo.Groups = headerValue
		case "x-auth-request-email":
			userInfo.Email = headerValue
		}
	}

	// Set default source if still empty.
	if userInfo.Source == "" {
		userInfo.Source = "unknown"
	}

	return userInfo
}

// ConvertStoreMatchers converts internal store matchers to logging format.
func ConvertStoreMatchers(storeMatchers [][]*labels.Matcher) []StoreMatcherSet {
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

// GetResponseStats calculates stats from query response (works for both range and instant queries).
func GetResponseStats(resp queryrange.Response) ResponseStats {
	stats := ResponseStats{}

	if resp == nil {
		return stats
	}

	// Use SeriesStatsCounter for both range and instant queries using OR condition
	var seriesStatsCounter *queryrange.SeriesStatsCounter
	if r, ok := resp.(*queryrange.PrometheusResponse); ok && r.Data.SeriesStatsCounter != nil {
		seriesStatsCounter = r.Data.SeriesStatsCounter
	} else if r, ok := resp.(*queryrange.PrometheusInstantQueryResponse); ok && r.Data.SeriesStatsCounter != nil {
		seriesStatsCounter = r.Data.SeriesStatsCounter
	}

	if seriesStatsCounter != nil {
		stats.BytesFetched = seriesStatsCounter.Bytes
		stats.TimeseriesFetched = seriesStatsCounter.Series
		stats.Chunks = seriesStatsCounter.Chunks
		stats.Samples = seriesStatsCounter.Samples
	}

	return stats
}

// ExtractMetricNames extracts all unique __name__ labels from query response (works for both range and instant queries).
func ExtractMetricNames(resp queryrange.Response) []string {
	if resp == nil {
		return nil
	}

	metricNamesMap := make(map[string]struct{})

	// Handle range query response (resultType: matrix)
	if r, ok := resp.(*queryrange.PrometheusResponse); ok {
		for _, stream := range r.Data.Result {
			for _, label := range stream.Labels {
				if label.Name == "__name__" {
					metricNamesMap[label.Value] = struct{}{}
					break
				}
			}
		}
	} else if r, ok := resp.(*queryrange.PrometheusInstantQueryResponse); ok {
		// Handle instant query response - check all result types
		if vector := r.Data.Result.GetVector(); vector != nil {
			// resultType: vector
			for _, sample := range vector.Samples {
				for _, label := range sample.Labels {
					if label.Name == "__name__" {
						metricNamesMap[label.Value] = struct{}{}
						break
					}
				}
			}
		} else if matrix := r.Data.Result.GetMatrix(); matrix != nil {
			// resultType: matrix (subqueries in instant queries)
			for _, stream := range matrix.SampleStreams {
				for _, label := range stream.Labels {
					if label.Name == "__name__" {
						metricNamesMap[label.Value] = struct{}{}
						break
					}
				}
			}
		}
		// Scalar and StringSample don't have __name__ labels
	}

	// Convert map to slice
	metricNames := make([]string, 0, len(metricNamesMap))
	for name := range metricNamesMap {
		metricNames = append(metricNames, name)
	}

	return metricNames
}

// WriteJSONLogToFile writes query logs to file in JSON format.
func WriteJSONLogToFile(logger log.Logger, writer interface{}, queryLog interface{}, queryType string) error {
	if writer == nil {
		return nil
	}

	// Marshal to JSON.
	jsonData, err := json.Marshal(queryLog)
	if err != nil {
		level.Error(logger).Log("msg", "failed to marshal "+queryType+" query log to JSON", "err", err)
		return err
	}

	// Write to file with newline.
	jsonData = append(jsonData, '\n')
	if w, ok := writer.(io.Writer); ok {
		if _, err := w.Write(jsonData); err != nil {
			level.Error(logger).Log("msg", "failed to write "+queryType+" query log to file", "err", err)
			return err
		}
	}

	return nil
}
