// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// SeriesQueryPerformanceMetricsAggregator aggregates results from fanned-out queries into a histogram given their
// response's shape.
type SeriesQueryPerformanceMetricsAggregator struct {
	QueryDuration *prometheus.HistogramVec

	SeriesLeBuckets  []int64
	SamplesLeBuckets []int64
	SeriesStats      storepb.SeriesStatsCounter
}

// NewSeriesQueryPerformanceMetricsAggregator is a constructor for SeriesQueryPerformanceMetricsAggregator.
func NewSeriesQueryPerformanceMetricsAggregator(
	reg prometheus.Registerer,
	durationQuantiles []float64,
	sampleQuantiles []int64,
	seriesQuantiles []int64,
) *SeriesQueryPerformanceMetricsAggregator {
	return &SeriesQueryPerformanceMetricsAggregator{
		QueryDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_store_api_query_duration_seconds",
			Help:    "Duration of the Thanos Store API select phase for a query.",
			Buckets: durationQuantiles,
		}, []string{"series_le", "samples_le"}),
		SeriesLeBuckets:  seriesQuantiles,
		SamplesLeBuckets: sampleQuantiles,
		SeriesStats:      storepb.SeriesStatsCounter{},
	}
}

// Aggregate is an aggregator for merging `storepb.SeriesStatsCounter` for each incoming fanned out query.
func (s *SeriesQueryPerformanceMetricsAggregator) Aggregate(stats storepb.SeriesStatsCounter) {
	s.SeriesStats.Series += stats.Series
	s.SeriesStats.Samples += stats.Samples
	s.SeriesStats.Chunks += stats.Chunks
}

// Observe commits the aggregated SeriesStatsCounter as an observation.
func (s *SeriesQueryPerformanceMetricsAggregator) Observe(duration float64) {
	// Bucket matching for series/labels matchSeriesBucket/matchSamplesBucket => float64, float64
	seriesLeBucket := s.findBucket(float64(s.SeriesStats.Series), s.SeriesLeBuckets)
	samplesLeBucket := s.findBucket(float64(s.SeriesStats.Samples), s.SamplesLeBuckets)
	s.QueryDuration.With(prometheus.Labels{
		"series_le":  strconv.Itoa(int(seriesLeBucket)),
		"samples_le": strconv.Itoa(int(samplesLeBucket)),
	}).Observe(duration)
	s.reset()
}

func (s *SeriesQueryPerformanceMetricsAggregator) reset() {
	s.SeriesStats = storepb.SeriesStatsCounter{}
}

func (s *SeriesQueryPerformanceMetricsAggregator) findBucket(value float64, quantiles []int64) int64 {
	if len(quantiles) == 0 {
		return 0
	}
	var foundBucket int64
	for _, bucket := range quantiles {
		foundBucket = bucket
		if value < float64(bucket) {
			break
		}
	}
	return foundBucket
}

// NopSeriesQueryPerformanceMetricsAggregator is a query performance series aggregator that does nothing.
type NopSeriesQueryPerformanceMetricsAggregator struct{}

func (s *NopSeriesQueryPerformanceMetricsAggregator) Aggregate(_ storepb.SeriesStatsCounter) {}

func (s *NopSeriesQueryPerformanceMetricsAggregator) Observe(_ float64) {}
