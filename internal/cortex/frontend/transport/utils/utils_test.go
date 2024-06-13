// Monitoring platform team helper resources for frontend

package utils

import (
	"errors"
	"testing"
)

// TestNewFailedQueryCache tests the NewFailedQueryCache function
func TestNewFailedQueryCache(t *testing.T) {
	cache, msg := NewFailedQueryCache(2)
	if cache == nil {
		t.Fatalf("Expected cache to be created, but got nil")
	}
	if msg != "" {
		t.Fatalf("Expected no error message, but got: %s", msg)
	}
}

// TestUpdateFailedQueryCache tests the UpdateFailedQueryCache method
func TestUpdateFailedQueryCache(t *testing.T) {
	cache, _ := NewFailedQueryCache(2)
	lruCache := cache.LruCache

	tests := []struct {
		name                       string
		err                        error
		queryExpressionNormalized  string
		queryExpressionRangeLength int
		expectedResult             bool
		expectedMessageContains    string
	}{
		{
			name:                       "No error code in error message",
			err:                        errors.New("no error code here"),
			queryExpressionNormalized:  "test_query",
			queryExpressionRangeLength: 60,
			expectedResult:             false,
			expectedMessageContains:    "String regex conversion error",
		},
		{
			name:                       "Non-cacheable error code",
			err:                        errors.New("Code(500)"),
			queryExpressionNormalized:  "test_query",
			queryExpressionRangeLength: 60,
			expectedResult:             false,
			expectedMessageContains:    "Query not cached due to non-cacheable error code",
		},
		{
			name:                       "Cacheable error code",
			err:                        errors.New("Code(408)"),
			queryExpressionNormalized:  "test_query",
			queryExpressionRangeLength: 60,
			expectedResult:             true,
			expectedMessageContains:    "Cached a failed query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, message := cache.UpdateFailedQueryCache(tt.err, tt.queryExpressionNormalized, tt.queryExpressionRangeLength, lruCache)
			if result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, result)
			}
			if !contains(message, tt.expectedMessageContains) {
				t.Errorf("expected message to contain %s, got %s", tt.expectedMessageContains, message)
			}
		})
	}
}

// TestQueryHitCache tests the QueryHitCache method
func TestQueryHitCache(t *testing.T) {
	cache, _ := NewFailedQueryCache(2)
	lruCache := cache.LruCache

	lruCache.Add("test_query", 60)

	tests := []struct {
		name                       string
		queryExpressionNormalized  string
		queryExpressionRangeLength int
		expectedResult             bool
		expectedMessageContains    string
	}{
		{
			name:                       "Cache hit",
			queryExpressionNormalized:  "test_query",
			queryExpressionRangeLength: 60,
			expectedResult:             true,
			expectedMessageContains:    "Retrieved query from cache",
		},
		{
			name:                       "Cache miss",
			queryExpressionNormalized:  "nonexistent_query",
			queryExpressionRangeLength: 60,
			expectedResult:             false,
			expectedMessageContains:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, message := cache.QueryHitCache(tt.queryExpressionNormalized, tt.queryExpressionRangeLength, lruCache)
			if result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, result)
			}
			if !contains(message, tt.expectedMessageContains) {
				t.Errorf("expected message to contain %s, got %s", tt.expectedMessageContains, message)
			}
		})
	}
}

// contains checks if a string is contained within another string
func contains(str, substr string) bool {
	return len(substr) == 0 || (len(str) >= len(substr) && str[len(str)-len(substr):] == substr)
}
