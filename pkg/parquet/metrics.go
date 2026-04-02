// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package parquet

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus metrics for Parquet operations.
type Metrics struct {
	conversionsTotal     *prometheus.CounterVec
	conversionDuration   prometheus.Histogram
	shardsCreated        prometheus.Histogram
	seriesPerShard       prometheus.Histogram
	parquetBytesWritten  prometheus.Counter
	parquetFilesUploaded prometheus.Counter
	tsdbBytesRead        prometheus.Counter
}

// NewMetrics creates a new Metrics instance.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		conversionsTotal: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Name: "thanos_parquet_conversions_total",
				Help: "Total number of TSDB to Parquet conversions, labeled by result (success/failure).",
			},
			[]string{"result"},
		),
		conversionDuration: promauto.With(reg).NewHistogram(
			prometheus.HistogramOpts{
				Name:    "thanos_parquet_conversion_duration_seconds",
				Help:    "Duration of TSDB to Parquet conversion in seconds.",
				Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
			},
		),
		shardsCreated: promauto.With(reg).NewHistogram(
			prometheus.HistogramOpts{
				Name:    "thanos_parquet_shards_created",
				Help:    "Number of shards created per conversion.",
				Buckets: []float64{1, 2, 5, 10, 20, 50, 100},
			},
		),
		seriesPerShard: promauto.With(reg).NewHistogram(
			prometheus.HistogramOpts{
				Name:    "thanos_parquet_series_per_shard",
				Help:    "Number of series per Parquet shard.",
				Buckets: prometheus.ExponentialBuckets(1000, 2, 15),
			},
		),
		parquetBytesWritten: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_parquet_bytes_written_total",
				Help: "Total bytes written to Parquet files.",
			},
		),
		parquetFilesUploaded: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_parquet_files_uploaded_total",
				Help: "Total number of Parquet files uploaded.",
			},
		),
		tsdbBytesRead: promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_parquet_tsdb_bytes_read_total",
				Help: "Total bytes read from TSDB blocks for Parquet conversion.",
			},
		),
	}
	return m
}
