// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// seriesStatsAggregator aggregates results from fanned-out queries into a histogram given their
// response's shape.
type seriesStatsAggregator struct {
	queryDuration *prometheus.HistogramVec

	seriesLeBuckets  []int64
	samplesLeBuckets []int64
	seriesStats      storepb.SeriesStatsCounter
}

// NewSeriesStatsAggregator is a constructor for seriesStatsAggregator.
func NewSeriesStatsAggregator(
	reg prometheus.Registerer,
	durationQuantiles []float64,
	sampleQuantiles []int64,
	seriesQuantiles []int64,
) *seriesStatsAggregator {
	return &seriesStatsAggregator{
		queryDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_store_api_query_duration_seconds",
			Help:    "Duration of the Thanos Store API select phase for a query.",
			Buckets: durationQuantiles,
		}, []string{"series_le", "samples_le"}),
		seriesLeBuckets:  seriesQuantiles,
		samplesLeBuckets: sampleQuantiles,
		seriesStats:      storepb.SeriesStatsCounter{},
	}
}

// Aggregate is an aggregator for merging `storepb.SeriesStatsCounter` for each incoming fanned out query.
func (s *seriesStatsAggregator) Aggregate(stats storepb.SeriesStatsCounter) {
	s.seriesStats.Series += stats.Series
	s.seriesStats.Samples += stats.Samples
	s.seriesStats.Chunks += stats.Chunks
}

// Observe commits the aggregated SeriesStatsCounter as an observation.
func (s *seriesStatsAggregator) Observe(duration float64) {
	if s.seriesStats.Series == 0 || s.seriesStats.Samples == 0 || s.seriesStats.Chunks == 0 {
		return
	}
	// Bucket matching for series/labels matchSeriesBucket/matchSamplesBucket => float64, float64
	seriesLeBucket := s.findBucket(float64(s.seriesStats.Series), s.seriesLeBuckets)
	samplesLeBucket := s.findBucket(float64(s.seriesStats.Samples), s.samplesLeBuckets)
	s.queryDuration.With(prometheus.Labels{
		"series_le":  strconv.Itoa(int(seriesLeBucket)),
		"samples_le": strconv.Itoa(int(samplesLeBucket)),
	}).Observe(duration)
	s.reset()
}

func (s *seriesStatsAggregator) reset() {
	s.seriesStats = storepb.SeriesStatsCounter{}
}

func (s *seriesStatsAggregator) findBucket(value float64, quantiles []int64) int64 {
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

// NoopSeriesStatsAggregator is a query performance series aggregator that does nothing.
type NoopSeriesStatsAggregator struct{}

func (s *NoopSeriesStatsAggregator) Aggregate(_ storepb.SeriesStatsCounter) {}

func (s *NoopSeriesStatsAggregator) Observe(_ float64) {}
