package dedup

import "github.com/prometheus/client_golang/prometheus"

type DedupMetrics struct {
	deduplication         *prometheus.CounterVec
	deduplicationFailures *prometheus.CounterVec

	syncMetas        *prometheus.CounterVec
	syncMetaFailures *prometheus.CounterVec
	syncMetaDuration *prometheus.HistogramVec

	syncBlocks        *prometheus.CounterVec
	syncBlockFailures *prometheus.CounterVec
	syncBlockDuration *prometheus.HistogramVec

	operateLocalStorageFailures  *prometheus.CounterVec
	operateRemoteStorageFailures *prometheus.CounterVec
}

func NewDedupMetrics(reg prometheus.Registerer) *DedupMetrics {
	metrics := &DedupMetrics{
		deduplication: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_bucket_deduplication_total",
			Help: "Total number of bucket deduplication attempts.",
		}, []string{"bucket"}),
		deduplicationFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_bucket_deduplication_failures",
			Help: "Total number of failed bucket deduplication.",
		}, []string{"bucket", "group"}),

		syncMetas: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_sync_meta_total",
			Help: "Total number of sync meta operations.",
		}, []string{"bucket"}),
		syncMetaFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_sync_meta_failures",
			Help: "Total number of failed sync meta operations.",
		}, []string{"bucket", "block"}),
		syncMetaDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "thanos_dedup_sync_meta_duration_seconds",
			Help: "Time it took to sync meta files.",
			Buckets: []float64{
				0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
			},
		}, []string{"bucket"}),

		syncBlocks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_sync_block_total",
			Help: "Total number of sync block operations.",
		}, []string{"bucket"}),
		syncBlockFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_sync_block_failures",
			Help: "Total number of failed sync block operations",
		}, []string{"bucket", "block"}),
		syncBlockDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "thanos_dedup_sync_block_duration_seconds",
			Help: "Time it took to sync block files.",
			Buckets: []float64{
				0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
			},
		}, []string{"bucket"}),

		operateLocalStorageFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_operate_local_storage_failures",
			Help: "Total number of failed operate local storage operations",
		}, []string{"operation", "block"}),
		operateRemoteStorageFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "thanos_dedup_operate_remote_storage_failures",
			Help: "Total number of failed operate remote storage operations",
		}, []string{"operation", "bucket", "block"}),
	}
	reg.MustRegister(
		metrics.deduplication,
		metrics.deduplicationFailures,
		metrics.syncMetas,
		metrics.syncMetaFailures,
		metrics.syncMetaDuration,
		metrics.syncBlocks,
		metrics.syncBlockFailures,
		metrics.syncBlockDuration,
		metrics.operateLocalStorageFailures,
		metrics.operateRemoteStorageFailures)
	return metrics
}
