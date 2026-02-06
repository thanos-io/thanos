// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sync"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// Benchmark comparing old allocation-heavy approach vs new pooled approach
// for label operations in Series() processing.

// oldApproach simulates the original implementation with allocations
func oldApproach(seriesLabels labels.Labels, extLsetToRemove map[string]struct{}, finalExtLset labels.Labels) labels.Labels {
	return labelpb.ExtendSortedLabels(rmLabels(seriesLabels, extLsetToRemove), finalExtLset)
}

// newApproach simulates the optimized implementation with builder pooling
var testLabelBuilderPool = sync.Pool{
	New: func() interface{} {
		return labels.NewBuilder(labels.EmptyLabels())
	},
}

func newApproach(builder *labels.Builder, seriesLabels labels.Labels, extLsetToRemove map[string]struct{}, finalExtLset labels.Labels) labels.Labels {
	builder.Reset(seriesLabels)
	for k := range extLsetToRemove {
		builder.Del(k)
	}
	finalExtLset.Range(func(l labels.Label) {
		builder.Set(l.Name, l.Value)
	})
	return builder.Labels()
}

// BenchmarkLabelOperations_Old benchmarks the old allocation-heavy approach
func BenchmarkLabelOperations_Old(b *testing.B) {
	// Setup realistic test data
	seriesLabels := labels.FromStrings(
		"__name__", "http_requests_total",
		"job", "api-server",
		"instance", "10.0.1.23:8080",
		"path", "/api/v1/query",
		"method", "GET",
		"status", "200",
		"tenant", "acme-corp",
		"replica", "r1",
	)

	extLsetToRemove := map[string]struct{}{
		"replica": {},
	}

	finalExtLset := labels.FromStrings(
		"cluster", "us-west-2",
		"env", "production",
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := oldApproach(seriesLabels, extLsetToRemove, finalExtLset)
		_ = result
	}
}

// BenchmarkLabelOperations_New benchmarks the new pooled approach
func BenchmarkLabelOperations_New(b *testing.B) {
	// Setup realistic test data (same as old)
	seriesLabels := labels.FromStrings(
		"__name__", "http_requests_total",
		"job", "api-server",
		"instance", "10.0.1.23:8080",
		"path", "/api/v1/query",
		"method", "GET",
		"status", "200",
		"tenant", "acme-corp",
		"replica", "r1",
	)

	extLsetToRemove := map[string]struct{}{
		"replica": {},
	}

	finalExtLset := labels.FromStrings(
		"cluster", "us-west-2",
		"env", "production",
	)

	builder := testLabelBuilderPool.Get().(*labels.Builder)
	defer testLabelBuilderPool.Put(builder)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := newApproach(builder, seriesLabels, extLsetToRemove, finalExtLset)
		_ = result
	}
}

// BenchmarkLabelOperations_Old_1M simulates processing 1M series with old approach
func BenchmarkLabelOperations_Old_1M(b *testing.B) {
	seriesLabels := labels.FromStrings(
		"__name__", "http_requests_total",
		"job", "api-server",
		"instance", "10.0.1.23:8080",
		"path", "/api/v1/query",
		"method", "GET",
		"status", "200",
		"tenant", "acme-corp",
		"replica", "r1",
	)

	extLsetToRemove := map[string]struct{}{
		"replica": {},
	}

	finalExtLset := labels.FromStrings(
		"cluster", "us-west-2",
		"env", "production",
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Process 1M series
		for j := 0; j < 1000000; j++ {
			result := oldApproach(seriesLabels, extLsetToRemove, finalExtLset)
			_ = result
		}
	}
}

// BenchmarkLabelOperations_New_1M simulates processing 1M series with new approach
func BenchmarkLabelOperations_New_1M(b *testing.B) {
	seriesLabels := labels.FromStrings(
		"__name__", "http_requests_total",
		"job", "api-server",
		"instance", "10.0.1.23:8080",
		"path", "/api/v1/query",
		"method", "GET",
		"status", "200",
		"tenant", "acme-corp",
		"replica", "r1",
	)

	extLsetToRemove := map[string]struct{}{
		"replica": {},
	}

	finalExtLset := labels.FromStrings(
		"cluster", "us-west-2",
		"env", "production",
	)

	builder := testLabelBuilderPool.Get().(*labels.Builder)
	defer testLabelBuilderPool.Put(builder)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Process 1M series
		for j := 0; j < 1000000; j++ {
			result := newApproach(builder, seriesLabels, extLsetToRemove, finalExtLset)
			_ = result
		}
	}
}

// BenchmarkLabelOperations_Parallel_Old tests old approach under concurrent load
func BenchmarkLabelOperations_Parallel_Old(b *testing.B) {
	seriesLabels := labels.FromStrings(
		"__name__", "http_requests_total",
		"job", "api-server",
		"instance", "10.0.1.23:8080",
		"path", "/api/v1/query",
		"method", "GET",
		"status", "200",
		"tenant", "acme-corp",
		"replica", "r1",
	)

	extLsetToRemove := map[string]struct{}{
		"replica": {},
	}

	finalExtLset := labels.FromStrings(
		"cluster", "us-west-2",
		"env", "production",
	)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result := oldApproach(seriesLabels, extLsetToRemove, finalExtLset)
			_ = result
		}
	})
}

// BenchmarkLabelOperations_Parallel_New tests new approach under concurrent load
func BenchmarkLabelOperations_Parallel_New(b *testing.B) {
	seriesLabels := labels.FromStrings(
		"__name__", "http_requests_total",
		"job", "api-server",
		"instance", "10.0.1.23:8080",
		"path", "/api/v1/query",
		"method", "GET",
		"status", "200",
		"tenant", "acme-corp",
		"replica", "r1",
	)

	extLsetToRemove := map[string]struct{}{
		"replica": {},
	}

	finalExtLset := labels.FromStrings(
		"cluster", "us-west-2",
		"env", "production",
	)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		builder := testLabelBuilderPool.Get().(*labels.Builder)
		defer testLabelBuilderPool.Put(builder)

		for pb.Next() {
			result := newApproach(builder, seriesLabels, extLsetToRemove, finalExtLset)
			_ = result
		}
	})
}
