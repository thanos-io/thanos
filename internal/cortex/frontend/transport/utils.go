// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package transport

// Monitoring platform team resource to cache failed queries

import (
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-kit/log/level"
	util_log "github.com/thanos-io/thanos/internal/cortex/util/log"
)

var (
	cacheableResponseCodes = []int{http.StatusRequestTimeout, http.StatusGatewayTimeout, http.StatusBadRequest}
	regex                  = regexp.MustCompile(`[\s\n\t]+`)
	errorExtract           = regexp.MustCompile(`Code\((\d+)\)`)
)

func (f *Handler) updateFailedQueryCache(err error, queryExpressionNormalized string, queryExpressionRangeLength int, r *http.Request) {
	// Extracting error code from error string.
	codeExtract := errorExtract.FindStringSubmatch(err.Error())

	// Checking if error code extracted successfully.
	if codeExtract == nil || len(codeExtract) < 2 {
		level.Error(util_log.WithContext(r.Context(), f.log)).Log(
			"msg", "Error string regex conversion error",
			"normalized_query", queryExpressionNormalized,
			"range_seconds", queryExpressionRangeLength,
			"error", err)
		return
	}

	// Converting error code to int.
	errCode, strConvError := strconv.Atoi(codeExtract[1])

	// Checking if error code extracted properly from string.
	if strConvError != nil {
		level.Error(util_log.WithContext(r.Context(), f.log)).Log(
			"msg", "String to int conversion error",
			"normalized_query", queryExpressionNormalized,
			"range_seconds", queryExpressionRangeLength,
			"error", err)
		return
	}

	// If error should be cached, store it in cache.
	if !isCacheableError(errCode) {
		level.Debug(util_log.WithContext(r.Context(), f.log)).Log(
			"msg", "Query not cached due to non-cacheable error code",
			"normalized_query", queryExpressionNormalized,
			"range_seconds", queryExpressionRangeLength,
			"error", err,
		)
		return
	}

	// Checks if queryExpression is already in cache, and updates time range length value to min of stored and new value.
	if contains, _ := f.lruCache.ContainsOrAdd(queryExpressionNormalized, queryExpressionRangeLength); contains {
		if oldValue, ok := f.lruCache.Get(queryExpressionNormalized); ok {
			queryExpressionRangeLength = min(queryExpressionRangeLength, oldValue.(int))
		}
		f.lruCache.Add(queryExpressionNormalized, queryExpressionRangeLength)
	}

	level.Debug(util_log.WithContext(r.Context(), f.log)).Log(
		"msg", "Cached a failed query",
		"normalized_query", queryExpressionNormalized,
		"range_seconds", queryExpressionRangeLength,
		"error", err,
	)

}

// isCacheableError Returns true if response code is in pre-defined cacheable errors list, else returns false.
func isCacheableError(statusCode int) bool {
	for _, errStatusCode := range cacheableResponseCodes {
		if errStatusCode == statusCode {
			return true
		}
	}
	return false
}

// Time range length for queries, if either of "start" or "end" are not present, return 0.
func getQueryRangeSeconds(r *http.Request) int {
	start, err := strconv.Atoi(r.URL.Query().Get("start"))
	if err != nil {
		return 0
	}
	end, err := strconv.Atoi(r.URL.Query().Get("end"))
	if err != nil {
		return 0
	}
	return end - start
}
