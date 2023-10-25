// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sort"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// seriesStatsAggregator aggregates results from fanned-out queries into a histogram given their
// response's shape.
type seriesStatsAggregator struct {
	queryDuration    *prometheus.HistogramVec
	seriesLeBuckets  []float64
	samplesLeBuckets []float64
	tenant           string

	seriesStats storepb.SeriesStatsCounter
}

type seriesStatsAggregatorFactory struct {
	queryDuration    *prometheus.HistogramVec
	seriesLeBuckets  []float64
	samplesLeBuckets []float64
}

func (f *seriesStatsAggregatorFactory) NewAggregator(tenant string) SeriesQueryPerformanceMetricsAggregator {
	return &seriesStatsAggregator{
		queryDuration:    f.queryDuration,
		seriesLeBuckets:  f.seriesLeBuckets,
		samplesLeBuckets: f.samplesLeBuckets,
		tenant:           tenant,
		seriesStats:      storepb.SeriesStatsCounter{},
	}
}

func NewSeriesStatsAggregatorFactory(
	reg prometheus.Registerer,
	durationQuantiles []float64,
	sampleQuantiles []float64,
	seriesQuantiles []float64,
) *seriesStatsAggregatorFactory {
	return &seriesStatsAggregatorFactory{
		queryDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_store_api_query_duration_seconds",
			Help:    "Duration of the Thanos Store API select phase for a query.",
			Buckets: durationQuantiles,
		}, []string{"series_le", "samples_le", "tenant"}),
		seriesLeBuckets:  seriesQuantiles,
		samplesLeBuckets: sampleQuantiles,
	}
}

// NewSeriesStatsAggregator is a constructor for seriesStatsAggregator.
func NewSeriesStatsAggregator(
	reg prometheus.Registerer,
	durationQuantiles []float64,
	sampleQuantiles []float64,
	seriesQuantiles []float64,
) *seriesStatsAggregator {
	return &seriesStatsAggregator{
		queryDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "thanos_store_api_query_duration_seconds",
			Help:    "Duration of the Thanos Store API select phase for a query.",
			Buckets: durationQuantiles,
		}, []string{"series_le", "samples_le", "tenant"}),
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
	seriesLeBucket := findBucket(float64(s.seriesStats.Series), s.seriesLeBuckets)
	samplesLeBucket := findBucket(float64(s.seriesStats.Samples), s.samplesLeBuckets)
	s.queryDuration.With(prometheus.Labels{
		"series_le":  seriesLeBucket,
		"samples_le": samplesLeBucket,
		"tenant":     s.tenant,
	}).Observe(duration)
	s.reset()
}

func (s *seriesStatsAggregator) reset() {
	s.seriesStats = storepb.SeriesStatsCounter{}
}

func findBucket(value float64, quantiles []float64) string {
	if len(quantiles) == 0 {
		return "+Inf"
	}

	// If the value is bigger than the largest bucket we return +Inf
	if value >= float64(quantiles[len(quantiles)-1]) {
		return "+Inf"
	}

	// SearchFloats64s gets the appropriate index in the quantiles array based on the value
	return strconv.FormatFloat(quantiles[sort.SearchFloat64s(quantiles, value)], 'f', -1, 64)
}

type SeriesQueryPerformanceMetricsAggregatorFactory interface {
	NewAggregator(tenant string) SeriesQueryPerformanceMetricsAggregator
}

type SeriesQueryPerformanceMetricsAggregator interface {
	Aggregate(seriesStats storepb.SeriesStatsCounter)
	Observe(duration float64)
}

// NoopSeriesStatsAggregator is a query performance series aggregator that does nothing.
type NoopSeriesStatsAggregator struct{}

func (s *NoopSeriesStatsAggregator) Aggregate(_ storepb.SeriesStatsCounter) {}

func (s *NoopSeriesStatsAggregator) Observe(_ float64) {}

// NoopSeriesStatsAggregatorFactory is a query performance series aggregator factory that does nothing.
type NoopSeriesStatsAggregatorFactory struct{}

func (s *NoopSeriesStatsAggregatorFactory) NewAggregator(tenant string) SeriesQueryPerformanceMetricsAggregator {
	return &NoopSeriesStatsAggregator{}
}

// instrumentedStoreServer is a storepb.StoreServer that exposes metrics about Series requests.
type instrumentedStoreServer struct {
	storepb.StoreServer
	seriesRequested prometheus.Histogram
	chunksRequested prometheus.Histogram
}

// NewInstrumentedStoreServer creates a new instrumentedStoreServer.
func NewInstrumentedStoreServer(reg prometheus.Registerer, store storepb.StoreServer) storepb.StoreServer {
	return &instrumentedStoreServer{
		StoreServer: store,
		seriesRequested: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "thanos_store_server_series_requested",
			Help:    "Number of requested series for Series calls",
			Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
		}),
		chunksRequested: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "thanos_store_server_chunks_requested",
			Help:    "Number of requested chunks for Series calls",
			Buckets: []float64{1, 100, 1000, 10000, 100000, 10000000, 100000000, 1000000000},
		}),
	}
}

func (s *instrumentedStoreServer) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	instrumented := newInstrumentedServer(srv)
	if err := s.StoreServer.Series(req, instrumented); err != nil {
		return err
	}

	s.seriesRequested.Observe(instrumented.seriesSent)
	s.chunksRequested.Observe(instrumented.chunksSent)
	return nil
}

// instrumentedServer is a storepb.Store_SeriesServer that tracks statistics about sent series.
type instrumentedServer struct {
	storepb.Store_SeriesServer
	seriesSent float64
	chunksSent float64
}

func newInstrumentedServer(upstream storepb.Store_SeriesServer) *instrumentedServer {
	return &instrumentedServer{Store_SeriesServer: upstream}
}

func (i *instrumentedServer) Send(response *storepb.SeriesResponse) error {
	if err := i.Store_SeriesServer.Send(response); err != nil {
		return err
	}
	if series := response.GetSeries(); series != nil {
		i.seriesSent++
		i.chunksSent += float64(len(series.Chunks))
	}
	return nil
}
