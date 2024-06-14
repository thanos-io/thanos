// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

// Package utils Monitoring platform team helper resources for frontend
package utils

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	lru "github.com/hashicorp/golang-lru"
)

var (
	cacheableResponseCodes = []int{http.StatusRequestTimeout, http.StatusGatewayTimeout, http.StatusBadRequest}
)

// FailedQueryCache Handler holds an instance of FailedQueryCache and calls its methods
type FailedQueryCache struct {
	regex        *regexp.Regexp
	errorExtract *regexp.Regexp
	lruCache     *lru.Cache
}

func NewFailedQueryCache(capacity int) (*FailedQueryCache, error) {
	regex := regexp.MustCompile(`[\s\n\t]+`)
	errorExtract := regexp.MustCompile(`Code\((\d+)\)`)
	lruCache, err := lru.New(capacity)
	if err != nil {
		lruCache = nil
		err = fmt.Errorf("Failed to create lru cache: %s", err)
		return nil, err
	}
	return &FailedQueryCache{
		regex:        regex,
		errorExtract: errorExtract,
		lruCache:     lruCache}, err
}

// UpdateFailedQueryCache returns true if query is cached so that callsite can increase counter, returns message as a string for callsite to log outcome
func updateFailedQueryCache(err error, queryExpressionNormalized string, queryExpressionRangeLength int, lruCache *lru.Cache) (bool, string) {
	// Extracting error code from error string.
	codeExtract := f.errorExtract.FindStringSubmatch(err.Error())

	// Checking if error code extracted successfully.
	if codeExtract == nil || len(codeExtract) < 2 {
		message := fmt.Sprintf(
			`%s: %s, %s: %s, %s: %d, %s: %s`,
			"msg", "String regex conversion error",
			"normalized query", queryExpressionNormalized,
			"query range seconds", queryExpressionRangeLength,
			"updating cache for error", err,
		)
		return false, message
	}

	// Converting error code to int.
	errCode, strConvError := strconv.Atoi(codeExtract[1])

	// Checking if error code extracted properly from string.
	if strConvError != nil {
		message := fmt.Sprintf(
			`%s: %s, %s: %s, %s: %d, %s: %s`,
			"msg", "String to int conversion error",
			"normalized query", queryExpressionNormalized,
			"query range seconds", queryExpressionRangeLength,
			"updating cache for error", err,
		)
		return false, message
	}

	// If error should be cached, store it in cache.
	if !isCacheableError(errCode) {
		message := fmt.Sprintf(
			`%s: %s, %s: %s, %s: %d, %s: %s`,
			"msg", "Query not cached due to non-cacheable error code",
			"normalized query", queryExpressionNormalized,
			"query range seconds", queryExpressionRangeLength,
			"updating cache for error", err,
		)
		return false, message
	}

	// Checks if queryExpression is already in cache, and updates time range length value to min of stored and new value.
	if contains, _ := lruCache.ContainsOrAdd(queryExpressionNormalized, queryExpressionRangeLength); contains {
		if oldValue, ok := lruCache.Get(queryExpressionNormalized); ok {
			queryExpressionRangeLength = min(queryExpressionRangeLength, oldValue.(int))
		}
		lruCache.Add(queryExpressionNormalized, queryExpressionRangeLength)
	}

	message := fmt.Sprintf(
		`%s: %s, %s: %s, %s: %d, %s: %s`,
		"msg", "Cached a failed query",
		"normalized query", queryExpressionNormalized,
		"range seconds", queryExpressionRangeLength,
		"updating cache for error", err,
	)
	return true, message
}

// QueryHitCache checks if the lru cache is hit and returns whether to increment counter for cache hits along with appropriate message.
func queryHitCache(queryExpressionNormalized string, queryExpressionRangeLength int, lruCache *lru.Cache) (bool, string) {
	if value, ok := lruCache.Get(queryExpressionNormalized); ok && value.(int) >= queryExpressionRangeLength {
		message := fmt.Sprintf(
			`%s: %s, %s: %s, %s: %d`, "msg", "Retrieved query from cache",
			"normalized query", queryExpressionNormalized,
			"range seconds", queryExpressionRangeLength)

		return true, message

	}
	return false, ""
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

// GetQueryRangeSeconds Time range length for queries, if either of "start" or "end" are not present, return 0.
func getQueryRangeSeconds(query url.Values) int {
	start, err := strconv.Atoi(query.Get("start"))
	if err != nil {
		return 0
	}
	end, err := strconv.Atoi(query.Get("end"))
	if err != nil {
		return 0
	}
	return end - start
}

func normalizeQueryString(query url.Values) string {
	return f.regex.ReplaceAllString(query.Get("query"), " ")
}

func (f *FailedQueryCache) CallUpdateFailedQueryCache(err error, query url.Values) (bool, string) {
	queryExpressionNormalized := normalizeQueryString(query)
	queryExpressionRangeLength := getQueryRangeSeconds(query)
	success, message := updateFailedQueryCache(err, queryExpressionNormalized, queryExpressionRangeLength, f.lruCache)
	return success, message
}

func (f *FailedQueryCache) CallQueryHitCache(query url.Values) (bool, string) {
	queryExpressionNormalized := normalizeQueryString(query)
	queryExpressionRangeLength := getQueryRangeSeconds(query)
	cached, message := queryHitCache(queryExpressionNormalized, queryExpressionRangeLength, f.lruCache)
	return cached, message
}
