// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/units"
	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/weaveworks/common/httpgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/runutil"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type StoreDataType int

const (
	PostingsFetched StoreDataType = iota
	PostingsTouched
	SeriesFetched
	SeriesTouched
	ChunksFetched
	ChunksTouched
)

const (
	// MaxSamplesPerChunk is approximately the max number of samples that we may have in any given chunk. This is needed
	// for precalculating the number of samples that we may have to retrieve and decode for any given query
	// without downloading them. Please take a look at https://github.com/prometheus/tsdb/pull/397 to know
	// where this number comes from. Long story short: TSDB is made in such a way, and it is made in such a way
	// because you barely get any improvements in compression when the number of samples is beyond this.
	// Take a look at Figure 6 in this whitepaper http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
	MaxSamplesPerChunk = 120
	// EstimatedMaxChunkSize is average max of chunk size. This can be exceeded though in very rare (valid) cases.
	EstimatedMaxChunkSize  = 16000
	EstimatedMaxSeriesSize = 64 * 1024
	// Relatively large in order to reduce memory waste, yet small enough to avoid excessive allocations.
	chunkBytesPoolMinSize = 64 * 1024        // 64 KiB
	chunkBytesPoolMaxSize = 64 * 1024 * 1024 // 64 MiB

	// CompatibilityTypeLabelName is an artificial label that Store Gateway can optionally advertise. This is required for compatibility
	// with pre v0.8.0 Querier. Previous Queriers was strict about duplicated external labels of all StoreAPIs that had any labels.
	// Now with newer Store Gateway advertising all the external labels it has access to, there was simple case where
	// Querier was blocking Store Gateway as duplicate with sidecar.
	//
	// Newer Queriers are not strict, no duplicated external labels check is there anymore.
	// Additionally newer Queriers removes/ignore this exact labels from UI and querying.
	//
	// This label name is intentionally against Prometheus label style.
	// TODO(bwplotka): Remove it at some point.
	CompatibilityTypeLabelName = "@thanos_compatibility_store_type"

	// DefaultPostingOffsetInMemorySampling represents default value for --store.index-header-posting-offsets-in-mem-sampling.
	// 32 value is chosen as it's a good balance for common setups. Sampling that is not too large (too many CPU cycles) and
	// not too small (too much memory).
	DefaultPostingOffsetInMemorySampling = 32

	PartitionerMaxGapSize = 512 * 1024

	// Labels for metrics.
	labelEncode = "encode"
	labelDecode = "decode"

	minBlockSyncConcurrency = 1

	enableChunkHashCalculation = true

	// SeriesBatchSize is the default batch size when fetching series from object storage.
	SeriesBatchSize = 10000

	// checkContextEveryNIterations is used in some tight loops to check if the context is done.
	checkContextEveryNIterations = 128
)

var (
	errBlockSyncConcurrencyNotValid = errors.New("the block sync concurrency must be equal or greater than 1.")
	hashPool                        = sync.Pool{New: func() any { return xxhash.New() }}
	postingsPool                    zeropool.Pool[[]storage.SeriesRef]
)

func getPostingsSlice() []storage.SeriesRef {
	if p := postingsPool.Get(); p != nil {
		return p
	}

	// Pre-allocate slice with initial capacity.
	return make([]storage.SeriesRef, 0, 1024)
}

func putPostingsSlice(p []storage.SeriesRef) {
	postingsPool.Put(p[:0])
}

type bucketStoreMetrics struct {
	blocksLoaded          prometheus.Gauge
	blockLoads            prometheus.Counter
	blockLoadFailures     prometheus.Counter
	lastLoadedBlock       prometheus.Gauge
	blockDrops            prometheus.Counter
	blockDropFailures     prometheus.Counter
	blockLoadDuration     prometheus.Histogram
	seriesDataTouched     *prometheus.HistogramVec
	seriesDataFetched     *prometheus.HistogramVec
	seriesDataSizeTouched *prometheus.HistogramVec
	seriesDataSizeFetched *prometheus.HistogramVec
	seriesBlocksQueried   *prometheus.HistogramVec
	seriesGetAllDuration  *prometheus.HistogramVec
	seriesMergeDuration   *prometheus.HistogramVec
	resultSeriesCount     *prometheus.HistogramVec
	chunkSizeBytes        *prometheus.HistogramVec
	postingsSizeBytes     *prometheus.HistogramVec
	queriesDropped        *prometheus.CounterVec
	seriesRefetches       *prometheus.CounterVec
	chunkRefetches        *prometheus.CounterVec
	emptyPostingCount     *prometheus.CounterVec

	lazyExpandedPostingsCount                     prometheus.Counter
	lazyExpandedPostingGroupsByReason             *prometheus.CounterVec
	lazyExpandedPostingSizeBytes                  prometheus.Counter
	lazyExpandedPostingSeriesOverfetchedSizeBytes prometheus.Counter

	cachedPostingsCompressions           *prometheus.CounterVec
	cachedPostingsCompressionErrors      *prometheus.CounterVec
	cachedPostingsCompressionTimeSeconds *prometheus.CounterVec
	cachedPostingsOriginalSizeBytes      *prometheus.CounterVec
	cachedPostingsCompressedSizeBytes    *prometheus.CounterVec

	seriesFetchDuration *prometheus.HistogramVec
	// Counts time for fetching series across all batches.
	seriesFetchDurationSum *prometheus.HistogramVec
	postingsFetchDuration  *prometheus.HistogramVec
	// chunkFetchDuration counts total time loading chunks, but since we spawn
	// multiple goroutines the actual latency is usually much lower than it.
	chunkFetchDuration *prometheus.HistogramVec
	// Actual absolute total time for loading chunks.
	chunkFetchDurationSum *prometheus.HistogramVec
}

func newBucketStoreMetrics(reg prometheus.Registerer) *bucketStoreMetrics {
	var m bucketStoreMetrics

	m.blockLoads = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_loads_total",
		Help: "Total number of remote block loading attempts.",
	})
	m.blockLoadFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_load_failures_total",
		Help: "Total number of failed remote block loading attempts.",
	})
	m.blockDrops = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drops_total",
		Help: "Total number of local blocks that were dropped.",
	})
	m.blockDropFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drop_failures_total",
		Help: "Total number of local blocks that failed to be dropped.",
	})
	m.blocksLoaded = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	})
	m.lastLoadedBlock = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_blocks_last_loaded_timestamp_seconds",
		Help: "Timestamp when last block got loaded.",
	})
	m.blockLoadDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_block_load_duration_seconds",
		Help:    "The total time taken to load a block in seconds.",
		Buckets: []float64{0.1, 0.2, 0.5, 1, 2, 5, 15, 30, 60, 90, 120, 300},
	})
	m.seriesDataTouched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_touched",
		Help:    "Number of items of a data type touched to fulfill a single Store API series request.",
		Buckets: prometheus.ExponentialBuckets(200, 2, 15),
	}, []string{"data_type", tenancy.MetricLabel})
	m.seriesDataFetched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_fetched",
		Help:    "Number of items of a data type retrieved to fulfill a single Store API series request.",
		Buckets: prometheus.ExponentialBuckets(200, 2, 15),
	}, []string{"data_type", tenancy.MetricLabel})

	m.seriesDataSizeTouched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_size_touched_bytes",
		Help:    "Total size of items of a data type touched to fulfill a single Store API series request in Bytes.",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 15),
	}, []string{"data_type", tenancy.MetricLabel})
	m.seriesDataSizeFetched = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_data_size_fetched_bytes",
		Help:    "Total size of items of a data type fetched to fulfill a single Store API series request in Bytes.",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 15),
	}, []string{"data_type", tenancy.MetricLabel})

	m.seriesBlocksQueried = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_blocks_queried",
		Help:    "Number of blocks in a bucket store that were touched to satisfy a query.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	}, []string{tenancy.MetricLabel})
	m.seriesGetAllDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_get_all_duration_seconds",
		Help:    "Time it takes until all per-block prepares and loads for a query are finished.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{tenancy.MetricLabel})
	m.seriesMergeDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_merge_duration_seconds",
		Help:    "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{tenancy.MetricLabel})
	m.resultSeriesCount = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_result_series",
		Help:    "Number of series observed in the final result of a query.",
		Buckets: prometheus.ExponentialBuckets(100, 2, 15), // From 100 to 1638400.
	}, []string{tenancy.MetricLabel})

	m.chunkSizeBytes = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_sent_chunk_size_bytes",
		Help: "Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	}, []string{tenancy.MetricLabel})

	m.postingsSizeBytes = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_postings_size_bytes",
		Help: "Size in bytes of the postings for a single series call.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 128 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024, 768 * 1024 * 1024, 1024 * 1024 * 1024,
		},
	}, []string{tenancy.MetricLabel})

	m.queriesDropped = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_queries_dropped_total",
		Help: "Number of queries that were dropped due to the limit.",
	}, []string{"reason", tenancy.MetricLabel})
	m.seriesRefetches = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_series_refetches_total",
		Help: "Total number of cases where configured estimated series bytes was not enough was to fetch series from index, resulting in refetch.",
	}, []string{tenancy.MetricLabel})
	m.chunkRefetches = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_chunk_refetches_total",
		Help: "Total number of cases where configured estimated chunk bytes was not enough was to fetch chunks from object store, resulting in refetch.",
	}, []string{tenancy.MetricLabel})

	m.cachedPostingsCompressions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compressions_total",
		Help: "Number of postings compressions before storing to index cache.",
	}, []string{"op", tenancy.MetricLabel})
	m.cachedPostingsCompressions.WithLabelValues(labelEncode, tenancy.DefaultTenant)
	m.cachedPostingsCompressions.WithLabelValues(labelDecode, tenancy.DefaultTenant)

	m.cachedPostingsCompressionErrors = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compression_errors_total",
		Help: "Number of postings compression errors.",
	}, []string{"op", tenancy.MetricLabel})
	m.cachedPostingsCompressionErrors.WithLabelValues(labelEncode, tenancy.DefaultTenant)
	m.cachedPostingsCompressionErrors.WithLabelValues(labelDecode, tenancy.DefaultTenant)

	m.cachedPostingsCompressionTimeSeconds = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compression_time_seconds_total",
		Help: "Time spent compressing postings before storing them into postings cache.",
	}, []string{"op", tenancy.MetricLabel})
	m.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelEncode, tenancy.DefaultTenant)
	m.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelDecode, tenancy.DefaultTenant)

	m.cachedPostingsOriginalSizeBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_original_size_bytes_total",
		Help: "Original size of postings stored into cache.",
	}, []string{tenancy.MetricLabel})
	m.cachedPostingsCompressedSizeBytes = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_cached_postings_compressed_size_bytes_total",
		Help: "Compressed size of postings stored into cache.",
	}, []string{tenancy.MetricLabel})

	m.seriesFetchDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_fetch_duration_seconds",
		Help:    "The time it takes to fetch series to respond to a request sent to a store gateway. It includes both the time to fetch it from the cache and from storage in case of cache misses.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{tenancy.MetricLabel})

	m.seriesFetchDurationSum = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_series_fetch_duration_sum_seconds",
		Help:    "The total time it takes to fetch series to respond to a request sent to a store gateway across all series batches. It includes both the time to fetch it from the cache and from storage in case of cache misses.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{tenancy.MetricLabel})

	m.postingsFetchDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_postings_fetch_duration_seconds",
		Help:    "The time it takes to fetch postings to respond to a request sent to a store gateway. It includes both the time to fetch it from the cache and from storage in case of cache misses.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{tenancy.MetricLabel})

	m.chunkFetchDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_chunks_fetch_duration_seconds",
		Help:    "The total time spent fetching chunks within a single request for one block.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{tenancy.MetricLabel})

	m.chunkFetchDurationSum = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_bucket_store_chunks_fetch_duration_sum_seconds",
		Help:    "The total absolute time spent fetching chunks within a single request for one block.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	}, []string{tenancy.MetricLabel})

	m.emptyPostingCount = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_empty_postings_total",
		Help: "Total number of empty postings when fetching block series.",
	}, []string{tenancy.MetricLabel})

	m.lazyExpandedPostingsCount = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_lazy_expanded_postings_total",
		Help: "Total number of times when lazy expanded posting optimization applies.",
	})

	m.lazyExpandedPostingGroupsByReason = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_bucket_store_lazy_expanded_posting_groups_total",
		Help: "Total number of posting groups that are marked as lazy and corresponding reason",
	}, []string{"reason"})

	m.lazyExpandedPostingSizeBytes = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_lazy_expanded_posting_size_bytes_total",
		Help: "Total number of lazy posting group size in bytes.",
	})

	m.lazyExpandedPostingSeriesOverfetchedSizeBytes = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_lazy_expanded_posting_series_overfetched_size_bytes_total",
		Help: "Total number of series size in bytes overfetched due to posting lazy expansion.",
	})

	return &m
}

// FilterConfig is a configuration, which Store uses for filtering metrics based on time.
type FilterConfig struct {
	MinTime, MaxTime model.TimeOrDurationValue
}

type BlockEstimator func(meta metadata.Meta) uint64

// BucketStore implements the store API backed by a bucket. It loads all index
// files to local disk.
//
// NOTE: Bucket store reencodes postings using diff+varint+snappy when storing to cache.
// This makes them smaller, but takes extra CPU and memory.
// When used with in-memory cache, memory usage should decrease overall, thanks to postings being smaller.
type BucketStore struct {
	logger          log.Logger
	reg             prometheus.Registerer // TODO(metalmatze) remove and add via BucketStoreOption
	metrics         *bucketStoreMetrics
	bkt             objstore.InstrumentedBucketReader
	fetcher         block.MetadataFetcher
	dir             string
	indexCache      storecache.IndexCache
	matcherCache    storecache.MatchersCache
	indexReaderPool *indexheader.ReaderPool
	buffers         sync.Pool
	chunkPool       pool.Pool[byte]
	seriesBatchSize int

	// Sets of blocks that have the same labels. They are indexed by a hash over their label set.
	mtx       sync.RWMutex
	blocks    map[ulid.ULID]*bucketBlock
	blockSets map[uint64]*bucketBlockSet

	// Verbose enabled additional logging.
	debugLogging bool
	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate gate.Gate

	// chunksLimiterFactory creates a new limiter used to limit the number of chunks fetched by each Series() call.
	chunksLimiterFactory ChunksLimiterFactory
	// seriesLimiterFactory creates a new limiter used to limit the number of touched series by each Series() call,
	// or LabelName and LabelValues calls when used with matchers.
	seriesLimiterFactory SeriesLimiterFactory
	// bytesLimiterFactory creates a new limiter used to limit the amount of bytes fetched/touched by each Series() call.
	bytesLimiterFactory BytesLimiterFactory

	partitioner Partitioner

	filterConfig             *FilterConfig
	advLabelSets             []labelpb.ZLabelSet
	enableCompatibilityLabel bool

	// Every how many posting offset entry we pool in heap memory. Default in Prometheus is 32.
	postingOffsetsInMemSampling int

	// Enables hints in the Series() response.
	enableSeriesResponseHints bool

	enableChunkHashCalculation bool

	enabledLazyExpandedPostings   bool
	seriesMatchRatio              float64
	postingGroupMaxKeySeriesRatio float64

	sortingStrategy sortingStrategy
	// This flag limits memory usage when lazy retrieval strategy, newLazyRespSet(), is used.
	lazyRetrievalMaxBufferedResponses int

	blockEstimatedMaxSeriesFunc BlockEstimator
	blockEstimatedMaxChunkFunc  BlockEstimator

	indexHeaderLazyDownloadStrategy indexheader.LazyDownloadIndexHeaderFunc

	requestLoggerFunc RequestLoggerFunc

	blockLifecycleCallback BlockLifecycleCallback
}

func (s *BucketStore) validate() error {
	if s.blockSyncConcurrency < minBlockSyncConcurrency {
		return errBlockSyncConcurrencyNotValid
	}
	return nil
}

type noopCache struct{}

func (noopCache) StorePostings(ulid.ULID, labels.Label, []byte, string, time.Duration) {}
func (noopCache) FetchMultiPostings(_ context.Context, _ ulid.ULID, keys []labels.Label, tenant string) (map[labels.Label][]byte, []labels.Label) {
	return map[labels.Label][]byte{}, keys
}

func (noopCache) StoreExpandedPostings(_ ulid.ULID, _ []*labels.Matcher, _ []byte, tenant string, _ time.Duration) {
}
func (noopCache) FetchExpandedPostings(_ context.Context, _ ulid.ULID, _ []*labels.Matcher, tenant string) ([]byte, bool) {
	return []byte{}, false
}

func (noopCache) StoreSeries(ulid.ULID, storage.SeriesRef, []byte, string, time.Duration) {}
func (noopCache) FetchMultiSeries(_ context.Context, _ ulid.ULID, ids []storage.SeriesRef, tenant string) (map[storage.SeriesRef][]byte, []storage.SeriesRef) {
	return map[storage.SeriesRef][]byte{}, ids
}

// BucketStoreOption are functions that configure BucketStore.
type BucketStoreOption func(s *BucketStore)

// WithLogger sets the BucketStore logger to the one you pass.
func WithLogger(logger log.Logger) BucketStoreOption {
	return func(s *BucketStore) {
		s.logger = logger
	}
}

type RequestLoggerFunc func(ctx context.Context, log log.Logger) log.Logger

func NoopRequestLoggerFunc(_ context.Context, logger log.Logger) log.Logger {
	return logger
}

// WithRequestLoggerFunc sets the BucketStore to use the passed RequestLoggerFunc
// to initialize logger during query time.
func WithRequestLoggerFunc(loggerFunc RequestLoggerFunc) BucketStoreOption {
	return func(s *BucketStore) {
		s.requestLoggerFunc = loggerFunc
	}
}

// WithRegistry sets a registry that BucketStore uses to register metrics with.
func WithRegistry(reg prometheus.Registerer) BucketStoreOption {
	return func(s *BucketStore) {
		s.reg = reg
	}
}

// WithIndexCache sets a indexCache to use instead of a noopCache.
func WithIndexCache(cache storecache.IndexCache) BucketStoreOption {
	return func(s *BucketStore) {
		s.indexCache = cache
	}
}

// WithMatchersCache sets a matchers cache to use instead of a noopCache.
func WithMatchersCache(cache storecache.MatchersCache) BucketStoreOption {
	return func(s *BucketStore) {
		s.matcherCache = cache
	}
}

// WithQueryGate sets a queryGate to use instead of a noopGate.
func WithQueryGate(queryGate gate.Gate) BucketStoreOption {
	return func(s *BucketStore) {
		s.queryGate = queryGate
	}
}

// WithChunkPool sets a pool.Bytes to use for chunks.
func WithChunkPool(chunkPool pool.Pool[byte]) BucketStoreOption {
	return func(s *BucketStore) {
		s.chunkPool = chunkPool
	}
}

// WithFilterConfig sets a filter which Store uses for filtering metrics based on time.
func WithFilterConfig(filter *FilterConfig) BucketStoreOption {
	return func(s *BucketStore) {
		s.filterConfig = filter
	}
}

// WithDebugLogging enables debug logging.
func WithDebugLogging() BucketStoreOption {
	return func(s *BucketStore) {
		s.debugLogging = true
	}
}

func WithChunkHashCalculation(enableChunkHashCalculation bool) BucketStoreOption {
	return func(s *BucketStore) {
		s.enableChunkHashCalculation = enableChunkHashCalculation
	}
}

func WithSeriesBatchSize(seriesBatchSize int) BucketStoreOption {
	return func(s *BucketStore) {
		s.seriesBatchSize = seriesBatchSize
	}
}

func WithBlockEstimatedMaxSeriesFunc(f BlockEstimator) BucketStoreOption {
	return func(s *BucketStore) {
		s.blockEstimatedMaxSeriesFunc = f
	}
}

func WithBlockEstimatedMaxChunkFunc(f BlockEstimator) BucketStoreOption {
	return func(s *BucketStore) {
		s.blockEstimatedMaxChunkFunc = f
	}
}

// WithLazyExpandedPostings enables lazy expanded postings.
func WithLazyExpandedPostings(enabled bool) BucketStoreOption {
	return func(s *BucketStore) {
		s.enabledLazyExpandedPostings = enabled
	}
}

// WithPostingGroupMaxKeySeriesRatio configures a threshold to mark a posting group as lazy if it has more add keys or remove keys.
func WithPostingGroupMaxKeySeriesRatio(postingGroupMaxKeySeriesRatio float64) BucketStoreOption {
	return func(s *BucketStore) {
		s.postingGroupMaxKeySeriesRatio = postingGroupMaxKeySeriesRatio
	}
}

// WithSeriesMatchRatio configures how many series would match when intersecting posting groups.
// This is used for lazy posting optimization strategy. Ratio should be within (0, 1).
// The closer to 1, it means matchers have bad selectivity.
func WithSeriesMatchRatio(seriesMatchRatio float64) BucketStoreOption {
	return func(s *BucketStore) {
		s.seriesMatchRatio = seriesMatchRatio
	}
}

// WithDontResort disables series resorting in Store Gateway.
func WithDontResort(true bool) BucketStoreOption {
	return func(s *BucketStore) {
		s.sortingStrategy = sortingStrategyNone
	}
}

func WithLazyRetrievalMaxBufferedResponsesForBucket(n int) BucketStoreOption {
	return func(s *BucketStore) {
		s.lazyRetrievalMaxBufferedResponses = n
	}
}

// WithIndexHeaderLazyDownloadStrategy specifies what block to lazy download its index header.
// Only used when lazy mmap is enabled at the same time.
func WithIndexHeaderLazyDownloadStrategy(strategy indexheader.LazyDownloadIndexHeaderFunc) BucketStoreOption {
	return func(s *BucketStore) {
		s.indexHeaderLazyDownloadStrategy = strategy
	}
}

// BlockLifecycleCallback specifies callbacks that will be called during the lifecycle of a block.
type BlockLifecycleCallback interface {
	// PreAdd is called before adding a block to indicate if the block needs to be added.
	// A non nil error means the block should not be added.
	PreAdd(meta metadata.Meta) error
}

type noopBlockLifecycleCallback struct{}

func (c noopBlockLifecycleCallback) PreAdd(meta metadata.Meta) error { return nil }

// WithBlockLifecycleCallback allows customizing callbacks of block lifecycle.
func WithBlockLifecycleCallback(c BlockLifecycleCallback) BucketStoreOption {
	return func(s *BucketStore) {
		s.blockLifecycleCallback = c
	}
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	bkt objstore.InstrumentedBucketReader,
	fetcher block.MetadataFetcher,
	dir string,
	chunksLimiterFactory ChunksLimiterFactory,
	seriesLimiterFactory SeriesLimiterFactory,
	bytesLimiterFactory BytesLimiterFactory,
	partitioner Partitioner,
	blockSyncConcurrency int,
	enableCompatibilityLabel bool,
	postingOffsetsInMemSampling int,
	enableSeriesResponseHints bool, // TODO(pracucci) Thanos 0.12 and below doesn't gracefully handle new fields in SeriesResponse. Drop this flag and always enable hints once we can drop backward compatibility.
	lazyIndexReaderEnabled bool,
	lazyIndexReaderIdleTimeout time.Duration,
	options ...BucketStoreOption,
) (*BucketStore, error) {
	s := &BucketStore{
		logger:       log.NewNopLogger(),
		bkt:          bkt,
		fetcher:      fetcher,
		dir:          dir,
		indexCache:   noopCache{},
		matcherCache: storecache.NoopMatchersCache,
		buffers: sync.Pool{New: func() any {
			b := make([]byte, 0, initialBufSize)
			return &b
		}},
		chunkPool:                       pool.NoopPool[byte]{},
		blocks:                          map[ulid.ULID]*bucketBlock{},
		blockSets:                       map[uint64]*bucketBlockSet{},
		blockSyncConcurrency:            blockSyncConcurrency,
		queryGate:                       gate.NewNoop(),
		chunksLimiterFactory:            chunksLimiterFactory,
		seriesLimiterFactory:            seriesLimiterFactory,
		bytesLimiterFactory:             bytesLimiterFactory,
		partitioner:                     partitioner,
		enableCompatibilityLabel:        enableCompatibilityLabel,
		postingOffsetsInMemSampling:     postingOffsetsInMemSampling,
		enableSeriesResponseHints:       enableSeriesResponseHints,
		enableChunkHashCalculation:      enableChunkHashCalculation,
		seriesBatchSize:                 SeriesBatchSize,
		sortingStrategy:                 sortingStrategyStore,
		indexHeaderLazyDownloadStrategy: indexheader.AlwaysEagerDownloadIndexHeader,
		requestLoggerFunc:               NoopRequestLoggerFunc,
		blockLifecycleCallback:          &noopBlockLifecycleCallback{},

		lazyRetrievalMaxBufferedResponses: 20,
	}

	for _, option := range options {
		option(s)
	}

	// Depend on the options
	indexReaderPoolMetrics := indexheader.NewReaderPoolMetrics(extprom.WrapRegistererWithPrefix("thanos_bucket_store_", s.reg))
	s.indexReaderPool = indexheader.NewReaderPool(s.logger, lazyIndexReaderEnabled, lazyIndexReaderIdleTimeout, indexReaderPoolMetrics, s.indexHeaderLazyDownloadStrategy)
	s.metrics = newBucketStoreMetrics(s.reg) // TODO(metalmatze): Might be possible via Option too

	if err := s.validate(); err != nil {
		return nil, errors.Wrap(err, "validate config")
	}

	if dir == "" {
		return s, nil
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}

	return s, nil
}

// Close the store.
func (s *BucketStore) Close() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, b := range s.blocks {
		runutil.CloseWithErrCapture(&err, b, "closing Bucket Block")
	}

	s.indexReaderPool.Close()
	return err
}

// SyncBlocks synchronizes the stores state with the Bucket bucket.
// It will reuse disk space as persistent cache based on s.dir param.
func (s *BucketStore) SyncBlocks(ctx context.Context) error {
	metas, _, metaFetchErr := s.fetcher.Fetch(ctx)
	// For partial view allow adding new blocks at least.
	if metaFetchErr != nil && metas == nil {
		return metaFetchErr
	}

	var wg sync.WaitGroup
	blockc := make(chan *metadata.Meta)

	for i := 0; i < s.blockSyncConcurrency; i++ {
		wg.Go(func() {
			for meta := range blockc {
				if preAddErr := s.blockLifecycleCallback.PreAdd(*meta); preAddErr != nil {
					continue
				}
				if err := s.addBlock(ctx, meta); err != nil {
					continue
				}
			}
		})
	}

	for id, meta := range metas {
		if b := s.getBlock(id); b != nil {
			continue
		}
		select {
		case <-ctx.Done():
		case blockc <- meta:
		}
	}

	close(blockc)
	wg.Wait()

	if metaFetchErr != nil {
		return metaFetchErr
	}

	s.mtx.RLock()
	keys := make([]ulid.ULID, 0, len(s.blocks))
	for k := range s.blocks {
		keys = append(keys, k)
	}
	s.mtx.RUnlock()

	// Drop all blocks that are no longer present in the bucket.
	for _, id := range keys {
		if _, ok := metas[id]; ok {
			continue
		}
		if err := s.removeBlock(id); err != nil {
			level.Warn(s.logger).Log("msg", "drop of outdated block failed", "block", id, "err", err)
			s.metrics.blockDropFailures.Inc()
		}
		level.Info(s.logger).Log("msg", "dropped outdated block", "block", id)
		s.metrics.blockDrops.Inc()
	}

	// Sync advertise labels.
	s.mtx.Lock()
	s.advLabelSets = make([]labelpb.ZLabelSet, 0, len(s.advLabelSets))
	for _, bs := range s.blockSets {
		s.advLabelSets = append(s.advLabelSets, labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(bs.labels.Copy())})
	}
	sort.Slice(s.advLabelSets, func(i, j int) bool {
		return strings.Compare(s.advLabelSets[i].String(), s.advLabelSets[j].String()) < 0
	})
	s.mtx.Unlock()
	return nil
}

// InitialSync perform blocking sync with extra step at the end to delete locally saved blocks that are no longer
// present in the bucket. The mismatch of these can only happen between restarts, so we can do that only once per startup.
func (s *BucketStore) InitialSync(ctx context.Context) error {
	if err := s.SyncBlocks(ctx); err != nil {
		return errors.Wrap(err, "sync block")
	}

	if s.dir == "" {
		return nil
	}

	fis, err := os.ReadDir(s.dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	names := make([]string, 0, len(fis))
	for _, fi := range fis {
		names = append(names, fi.Name())
	}
	for _, n := range names {
		id, ok := block.IsBlockDir(n)
		if !ok {
			continue
		}
		if b := s.getBlock(id); b != nil {
			continue
		}

		// No such block loaded, remove the local dir.
		if err := os.RemoveAll(path.Join(s.dir, id.String())); err != nil {
			level.Warn(s.logger).Log("msg", "failed to remove block which is not needed", "err", err)
		}
	}

	return nil
}

func (s *BucketStore) getBlock(id ulid.ULID) *bucketBlock {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.blocks[id]
}

func (s *BucketStore) addBlock(ctx context.Context, meta *metadata.Meta) (err error) {
	var dir string
	if s.dir != "" {
		dir = path.Join(s.dir, meta.ULID.String())
	}
	start := time.Now()

	level.Debug(s.logger).Log("msg", "loading new block", "id", meta.ULID)
	defer func() {
		if err != nil {
			s.metrics.blockLoadFailures.Inc()
			if dir != "" {
				if err2 := os.RemoveAll(dir); err2 != nil {
					level.Warn(s.logger).Log("msg", "failed to remove block we cannot load", "err", err2)
				}
			}
			level.Warn(s.logger).Log("msg", "loading block failed", "elapsed", time.Since(start), "id", meta.ULID, "err", err)
		} else {
			level.Info(s.logger).Log("msg", "loaded new block", "elapsed", time.Since(start), "id", meta.ULID)
			s.metrics.blockLoadDuration.Observe(time.Since(start).Seconds())
		}
	}()
	s.metrics.blockLoads.Inc()

	lset := labels.FromMap(meta.Thanos.Labels)
	h := lset.Hash()

	indexHeaderReader, err := s.indexReaderPool.NewBinaryReader(
		ctx,
		s.logger,
		s.bkt,
		s.dir,
		meta.ULID,
		s.postingOffsetsInMemSampling,
		meta,
	)
	if err != nil {
		return errors.Wrap(err, "create index header reader")
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, indexHeaderReader, "index-header")
		}
	}()

	b, err := newBucketBlock(
		ctx,
		s.metrics,
		meta,
		s.bkt,
		dir,
		s.indexCache,
		s.chunkPool,
		indexHeaderReader,
		s.partitioner,
		s.blockEstimatedMaxSeriesFunc,
		s.blockEstimatedMaxChunkFunc,
	)
	if err != nil {
		return errors.Wrap(err, "new bucket block")
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, b, "index-header")
		}
	}()

	s.mtx.Lock()
	defer s.mtx.Unlock()

	set, ok := s.blockSets[h]
	if !ok {
		set = newBucketBlockSet(lset)
		s.blockSets[h] = set
	}

	if err = set.add(b); err != nil {
		return errors.Wrap(err, "add block to set")
	}
	s.blocks[b.meta.ULID] = b

	s.metrics.blocksLoaded.Inc()
	s.metrics.lastLoadedBlock.SetToCurrentTime()
	return nil
}

func (s *BucketStore) removeBlock(id ulid.ULID) error {
	s.mtx.Lock()
	b, ok := s.blocks[id]
	if ok {
		lset := labels.FromMap(b.meta.Thanos.Labels)
		s.blockSets[lset.Hash()].remove(id)
		delete(s.blocks, id)
	}
	s.mtx.Unlock()

	if !ok {
		return nil
	}

	s.metrics.blocksLoaded.Dec()
	if err := b.Close(); err != nil {
		return errors.Wrap(err, "close block")
	}

	if b.dir == "" {
		return nil
	}

	return os.RemoveAll(b.dir)
}

// TimeRange returns the minimum and maximum timestamp of data available in the store.
func (s *BucketStore) TimeRange() (mint, maxt int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	mint = math.MaxInt64
	maxt = math.MinInt64

	for _, b := range s.blocks {
		if b.meta.MinTime < mint {
			mint = b.meta.MinTime
		}
		if b.meta.MaxTime > maxt {
			maxt = b.meta.MaxTime
		}
	}

	mint = s.limitMinTime(mint)
	maxt = s.limitMaxTime(maxt)

	return mint, maxt
}

// TSDBInfos returns a list of infopb.TSDBInfos for blocks in the bucket store.
func (s *BucketStore) TSDBInfos() []infopb.TSDBInfo {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	infoMap := make(map[uint64][]infopb.TSDBInfo, len(s.blocks))
	for _, b := range s.blocks {
		lbls := labels.FromMap(b.meta.Thanos.Labels)
		hash := lbls.Hash()
		infoMap[hash] = append(infoMap[hash], infopb.TSDBInfo{
			Labels: labelpb.ZLabelSet{
				Labels: labelpb.ZLabelsFromPromLabels(lbls),
			},
			MinTime: b.meta.MinTime,
			MaxTime: b.meta.MaxTime,
		})
	}

	// join adjacent blocks so we emit less TSDBInfos
	res := make([]infopb.TSDBInfo, 0, len(s.blocks))
	for _, infos := range infoMap {
		sort.Slice(infos, func(i, j int) bool { return infos[i].MinTime < infos[j].MinTime })

		cur := infos[0]
		for _, info := range infos {
			if info.MinTime > cur.MaxTime {
				res = append(res, cur)
				cur = info
				continue
			}
			cur.MaxTime = info.MaxTime
		}
		res = append(res, cur)
	}

	return res
}

func (s *BucketStore) LabelSet() []labelpb.ZLabelSet {
	s.mtx.RLock()
	labelSets := s.advLabelSets
	s.mtx.RUnlock()

	if s.enableCompatibilityLabel && len(labelSets) > 0 {
		labelSets = append(labelSets, labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: CompatibilityTypeLabelName, Value: "store"}}})
	}

	return labelSets
}

func (s *BucketStore) limitMinTime(mint int64) int64 {
	if s.filterConfig == nil {
		return mint
	}

	filterMinTime := s.filterConfig.MinTime.PrometheusTimestamp()

	if mint < filterMinTime {
		return filterMinTime
	}

	return mint
}

func (s *BucketStore) limitMaxTime(maxt int64) int64 {
	if s.filterConfig == nil {
		return maxt
	}

	filterMaxTime := s.filterConfig.MaxTime.PrometheusTimestamp()

	if maxt > filterMaxTime {
		maxt = filterMaxTime
	}

	return maxt
}

type seriesEntry struct {
	lset labels.Labels
	refs []chunks.ChunkRef
	chks []storepb.AggrChunk
}

// blockSeriesClient is a storepb.Store_SeriesClient for a
// single TSDB block in object storage.
type blockSeriesClient struct {
	grpc.ClientStream
	ctx             context.Context
	logger          log.Logger
	extLset         labels.Labels
	extLsetToRemove map[string]struct{}

	mint           int64
	maxt           int64
	seriesLimit    int
	indexr         *bucketIndexReader
	chunkr         *bucketChunkReader
	loadAggregates []storepb.Aggr

	seriesLimiter SeriesLimiter
	chunksLimiter ChunksLimiter
	bytesLimiter  BytesLimiter

	lazyExpandedPostingEnabled bool
	seriesMatchRatio           float64
	// Mark posting group as lazy if it adds or removes too many keys. 0 to disable.
	postingGroupMaxKeySeriesRatio                 float64
	lazyExpandedPostingsCount                     prometheus.Counter
	lazyExpandedPostingGroupByReason              *prometheus.CounterVec
	lazyExpandedPostingSizeBytes                  prometheus.Counter
	lazyExpandedPostingSeriesOverfetchedSizeBytes prometheus.Counter

	skipChunks             bool
	shardMatcher           *storepb.ShardMatcher
	blockMatchers          []*labels.Matcher
	calculateChunkHash     bool
	seriesFetchDurationSum *prometheus.HistogramVec
	chunkFetchDuration     *prometheus.HistogramVec
	chunkFetchDurationSum  *prometheus.HistogramVec
	tenant                 string

	// Internal state.
	i                uint64
	lazyPostings     *lazyExpandedPostings
	expandedPostings []storage.SeriesRef
	chkMetas         []chunks.Meta
	lset             labels.Labels
	b                *labels.Builder
	symbolizedLset   []symbolizedLabel
	entries          []seriesEntry
	hasMorePostings  bool
	batchSize        int
}

func newBlockSeriesClient(
	ctx context.Context,
	logger log.Logger,
	b *bucketBlock,
	req *storepb.SeriesRequest,
	seriesLimiter SeriesLimiter,
	chunksLimiter ChunksLimiter,
	bytesLimiter BytesLimiter,
	blockMatchers []*labels.Matcher,
	shardMatcher *storepb.ShardMatcher,
	calculateChunkHash bool,
	batchSize int,
	seriesFetchDurationSum *prometheus.HistogramVec,
	chunkFetchDuration *prometheus.HistogramVec,
	chunkFetchDurationSum *prometheus.HistogramVec,
	extLsetToRemove map[string]struct{},
	lazyExpandedPostingEnabled bool,
	seriesMatchRatio float64,
	postingGroupMaxKeySeriesRatio float64,
	lazyExpandedPostingsCount prometheus.Counter,
	lazyExpandedPostingByReason *prometheus.CounterVec,
	lazyExpandedPostingSizeBytes prometheus.Counter,
	lazyExpandedPostingSeriesOverfetchedSizeBytes prometheus.Counter,
	tenant string,
) *blockSeriesClient {
	var chunkr *bucketChunkReader
	if !req.SkipChunks {
		chunkr = b.chunkReader(logger)
	}

	extLset := b.extLset
	if extLsetToRemove != nil {
		extLset = rmLabels(extLset.Copy(), extLsetToRemove)
	}

	return &blockSeriesClient{
		ctx:             ctx,
		logger:          logger,
		extLset:         extLset,
		extLsetToRemove: extLsetToRemove,

		mint:                   req.MinTime,
		maxt:                   req.MaxTime,
		seriesLimit:            int(req.Limit),
		indexr:                 b.indexReader(logger),
		chunkr:                 chunkr,
		seriesLimiter:          seriesLimiter,
		chunksLimiter:          chunksLimiter,
		bytesLimiter:           bytesLimiter,
		skipChunks:             req.SkipChunks,
		seriesFetchDurationSum: seriesFetchDurationSum,
		chunkFetchDuration:     chunkFetchDuration,
		chunkFetchDurationSum:  chunkFetchDurationSum,

		lazyExpandedPostingEnabled:                    lazyExpandedPostingEnabled,
		seriesMatchRatio:                              seriesMatchRatio,
		postingGroupMaxKeySeriesRatio:                 postingGroupMaxKeySeriesRatio,
		lazyExpandedPostingsCount:                     lazyExpandedPostingsCount,
		lazyExpandedPostingGroupByReason:              lazyExpandedPostingByReason,
		lazyExpandedPostingSizeBytes:                  lazyExpandedPostingSizeBytes,
		lazyExpandedPostingSeriesOverfetchedSizeBytes: lazyExpandedPostingSeriesOverfetchedSizeBytes,

		loadAggregates:     req.Aggregates,
		shardMatcher:       shardMatcher,
		blockMatchers:      blockMatchers,
		calculateChunkHash: calculateChunkHash,
		hasMorePostings:    true,
		batchSize:          batchSize,
		tenant:             tenant,

		b: labels.NewBuilder(labels.EmptyLabels()),
	}
}

func (b *blockSeriesClient) Close() {
	if !b.skipChunks {
		runutil.CloseWithLogOnErr(b.logger, b.chunkr, "series block")
	}

	runutil.CloseWithLogOnErr(b.logger, b.indexr, "series block")
}

func (b *blockSeriesClient) MergeStats(stats *queryStats) *queryStats {
	stats.merge(b.indexr.stats)
	if !b.skipChunks {
		stats.merge(b.chunkr.stats)
	}
	return stats
}

type sortedMatchers []*labels.Matcher

func newSortedMatchers(matchers []*labels.Matcher) sortedMatchers {
	sort.Slice(matchers, func(i, j int) bool {
		if matchers[i].Type == matchers[j].Type {
			if matchers[i].Name == matchers[j].Name {
				return matchers[i].Value < matchers[j].Value
			}
			return matchers[i].Name < matchers[j].Name
		}
		return matchers[i].Type < matchers[j].Type
	})

	return matchers
}

func (b *blockSeriesClient) ExpandPostings(
	matchers sortedMatchers,
	seriesLimiter SeriesLimiter,
) error {
	ps, err := b.indexr.ExpandedPostings(b.ctx, matchers, b.bytesLimiter, b.lazyExpandedPostingEnabled, b.seriesMatchRatio, b.postingGroupMaxKeySeriesRatio, b.lazyExpandedPostingSizeBytes, b.lazyExpandedPostingGroupByReason, b.tenant)
	if err != nil {
		return errors.Wrap(err, "expanded matching posting")
	}

	if ps == nil || len(ps.postings) == 0 {
		b.lazyPostings = emptyLazyPostings
		return nil
	}
	b.lazyPostings = ps

	if b.lazyPostings.lazyExpanded() {
		// Assume lazy expansion could cut actual expanded postings length to 50%.
		b.expandedPostings = make([]storage.SeriesRef, 0, len(b.lazyPostings.postings)/2)
		b.lazyExpandedPostingsCount.Inc()
	} else {
		// If seriesLimit is set, it can be applied here to limit the amount of series.
		// Note: This can only be done when postings are not expanded lazily.
		if b.seriesLimit > 0 && len(b.lazyPostings.postings) > b.seriesLimit {
			b.lazyPostings.postings = b.lazyPostings.postings[:b.seriesLimit]
		}

		// Apply series limiter eargerly if lazy postings not enabled.
		if err := seriesLimiter.Reserve(uint64(len(b.lazyPostings.postings))); err != nil {
			return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded series limit: %s", err)
		}
	}

	if b.batchSize > len(b.lazyPostings.postings) {
		b.batchSize = len(b.lazyPostings.postings)
	}

	b.entries = make([]seriesEntry, 0, b.batchSize)
	return nil
}

func (b *blockSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	for len(b.entries) == 0 && b.hasMorePostings {
		if err := b.nextBatch(b.tenant); err != nil {
			return nil, err
		}
	}

	if len(b.entries) == 0 {
		b.seriesFetchDurationSum.WithLabelValues(b.tenant).Observe(b.indexr.stats.SeriesDownloadLatencySum.Seconds())
		if b.chunkr != nil {
			b.chunkFetchDuration.WithLabelValues(b.tenant).Observe(b.chunkr.stats.ChunksFetchDurationSum.Seconds())
			b.chunkFetchDurationSum.WithLabelValues(b.tenant).Observe(b.chunkr.stats.ChunksDownloadLatencySum.Seconds())
		}
		return nil, io.EOF
	}

	next := b.entries[0]
	b.entries = b.entries[1:]

	return storepb.NewSeriesResponse(&storepb.Series{
		Labels: labelpb.ZLabelsFromPromLabels(next.lset),
		Chunks: next.chks,
	}), nil
}

func (b *blockSeriesClient) nextBatch(tenant string) error {
	start := b.i
	end := min(start+uint64(b.batchSize), uint64(len(b.lazyPostings.postings)))
	b.i = end

	lazyExpandedPosting := b.lazyPostings.lazyExpanded()
	postingsBatch := b.lazyPostings.postings[start:end]
	if len(postingsBatch) == 0 {
		b.hasMorePostings = false
		if lazyExpandedPosting {
			// No need to fetch index version again if lazy posting has 0 length.
			if len(b.lazyPostings.postings) > 0 {
				v, err := b.indexr.IndexVersion()
				if err != nil {
					return errors.Wrap(err, "get index version")
				}
				if v >= 2 {
					for i := range b.expandedPostings {
						b.expandedPostings[i] = b.expandedPostings[i] / 16
					}
				}
			}
			b.indexr.storeExpandedPostingsToCache(b.blockMatchers, index.NewListPostings(b.expandedPostings), len(b.expandedPostings), tenant)
		}
		return nil
	}

	b.indexr.reset(len(postingsBatch))
	if !b.skipChunks {
		b.chunkr.reset()
	}

	if err := b.indexr.PreloadSeries(b.ctx, postingsBatch, b.bytesLimiter, b.tenant); err != nil {
		return errors.Wrap(err, "preload series")
	}

	seriesMatched := 0
	b.entries = b.entries[:0]
OUTER:
	for i := range postingsBatch {
		if err := b.ctx.Err(); err != nil {
			return err
		}
		hasMatchedChunks, err := b.indexr.LoadSeriesForTime(postingsBatch[i], &b.symbolizedLset, &b.chkMetas, b.skipChunks, b.mint, b.maxt)
		if err != nil {
			return errors.Wrap(err, "read series")
		}
		// Skip getting series symbols if we know there is no matched chunks
		// and lazy expanded posting not enabled.
		if !lazyExpandedPosting && !hasMatchedChunks {
			continue
		}

		if err := b.indexr.LookupLabelsSymbols(b.ctx, b.symbolizedLset, b.b); err != nil {
			return errors.Wrap(err, "Lookup labels symbols")
		}
		b.lset = b.b.Labels()

		for _, matcher := range b.lazyPostings.matchers {
			val := b.lset.Get(matcher.Name)
			if !matcher.Matches(val) {
				// Series not matched means series we overfetched due to lazy posting expansion.
				seriesBytes := b.indexr.loadedSeries[postingsBatch[i]]
				b.lazyExpandedPostingSeriesOverfetchedSizeBytes.Add(float64(len(seriesBytes)))
				continue OUTER
			}
		}
		if lazyExpandedPosting {
			b.expandedPostings = append(b.expandedPostings, postingsBatch[i])
		}
		// If lazy expanded postings enabled, due to expanded postings cache, we need to
		// make sure we check lazy posting matchers and update expanded postings before
		// going to next series.
		if !hasMatchedChunks {
			continue
		}

		completeLabelset := labelpb.ExtendSortedLabels(b.lset, b.extLset)
		if b.extLsetToRemove != nil {
			completeLabelset = rmLabels(completeLabelset, b.extLsetToRemove)
		}

		if !b.shardMatcher.MatchesLabels(completeLabelset) {
			continue
		}

		seriesMatched++
		if b.seriesLimit > 0 && seriesMatched > b.seriesLimit {
			// Exit early if seriesLimit is set.
			b.hasMorePostings = false
			break
		}
		s := seriesEntry{lset: completeLabelset}
		if b.skipChunks {
			b.entries = append(b.entries, s)
			continue
		}

		// Schedule loading chunks.
		s.refs = make([]chunks.ChunkRef, 0, len(b.chkMetas))
		s.chks = make([]storepb.AggrChunk, 0, len(b.chkMetas))

		for j, meta := range b.chkMetas {
			if err := b.chunkr.addLoad(meta.Ref, len(b.entries), j); err != nil {
				return errors.Wrap(err, "add chunk load")
			}
			s.chks = append(s.chks, storepb.AggrChunk{
				MinTime: meta.MinTime,
				MaxTime: meta.MaxTime,
			})
			s.refs = append(s.refs, meta.Ref)
		}

		// Ensure sample limit through chunksLimiter if we return chunks.
		if err := b.chunksLimiter.Reserve(uint64(len(b.chkMetas))); err != nil {
			return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded chunks limit: %s", err)
		}

		b.entries = append(b.entries, s)
	}

	if lazyExpandedPosting {
		// Apply series limit before fetching chunks, for actual series matched.
		if err := b.seriesLimiter.Reserve(uint64(seriesMatched)); err != nil {
			return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded series limit: %s", err)
		}
	}

	if !b.skipChunks {
		if err := b.chunkr.load(b.ctx, b.entries, b.loadAggregates, b.calculateChunkHash, b.bytesLimiter, b.tenant); err != nil {
			return errors.Wrap(err, "load chunks")
		}
	}

	return nil
}

func populateChunk(out *storepb.AggrChunk, in chunkenc.Chunk, aggrs []storepb.Aggr, save func([]byte) ([]byte, error), calculateChecksum bool) error {
	hasher := hashPool.Get().(hash.Hash64)
	defer hashPool.Put(hasher)

	if in.Encoding() == chunkenc.EncXOR || in.Encoding() == chunkenc.EncHistogram || in.Encoding() == chunkenc.EncFloatHistogram {
		b, err := save(in.Bytes())
		if err != nil {
			return err
		}
		out.Raw = &storepb.Chunk{
			Data: b,
			Type: chunkToStoreEncoding(in.Encoding()),
			Hash: hashChunk(hasher, b, calculateChecksum),
		}
		return nil
	}

	if in.Encoding() != downsample.ChunkEncAggr {
		return errors.Errorf("unsupported chunk encoding %d", in.Encoding())
	}

	ac := downsample.AggrChunk(in.Bytes())

	for _, at := range aggrs {
		switch at {
		case storepb.Aggr_COUNT:
			x, err := ac.Get(downsample.AggrCount)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrCount)
			}
			b, err := save(x.Bytes())
			if err != nil {
				return err
			}
			out.Count = &storepb.Chunk{Type: chunkToStoreEncoding(x.Encoding()), Data: b, Hash: hashChunk(hasher, b, calculateChecksum)}
		case storepb.Aggr_SUM:
			x, err := ac.Get(downsample.AggrSum)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrSum)
			}
			b, err := save(x.Bytes())
			if err != nil {
				return err
			}
			out.Sum = &storepb.Chunk{Type: chunkToStoreEncoding(x.Encoding()), Data: b, Hash: hashChunk(hasher, b, calculateChecksum)}
		case storepb.Aggr_MIN:
			x, err := ac.Get(downsample.AggrMin)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrMin)
			}
			b, err := save(x.Bytes())
			if err != nil {
				return err
			}
			out.Min = &storepb.Chunk{Type: chunkToStoreEncoding(x.Encoding()), Data: b, Hash: hashChunk(hasher, b, calculateChecksum)}
		case storepb.Aggr_MAX:
			x, err := ac.Get(downsample.AggrMax)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrMax)
			}
			b, err := save(x.Bytes())
			if err != nil {
				return err
			}
			out.Max = &storepb.Chunk{Type: chunkToStoreEncoding(x.Encoding()), Data: b, Hash: hashChunk(hasher, b, calculateChecksum)}
		case storepb.Aggr_COUNTER:
			x, err := ac.Get(downsample.AggrCounter)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrCounter)
			}
			b, err := save(x.Bytes())
			if err != nil {
				return err
			}
			out.Counter = &storepb.Chunk{Type: chunkToStoreEncoding(x.Encoding()), Data: b, Hash: hashChunk(hasher, b, calculateChecksum)}
		}
	}
	return nil
}

func chunkToStoreEncoding(in chunkenc.Encoding) storepb.Chunk_Encoding {
	switch in {
	case chunkenc.EncXOR:
		return storepb.Chunk_XOR
	case chunkenc.EncHistogram:
		return storepb.Chunk_HISTOGRAM
	case chunkenc.EncFloatHistogram:
		return storepb.Chunk_FLOAT_HISTOGRAM
	default:
		panic("unknown chunk encoding")
	}
}

func hashChunk(hasher hash.Hash64, b []byte, doHash bool) uint64 {
	if !doHash {
		return 0
	}
	hasher.Reset()
	// Write never returns an error on the hasher implementation
	_, _ = hasher.Write(b)
	return hasher.Sum64()
}

// debugFoundBlockSetOverview logs on debug level what exactly blocks we used for query in terms of
// labels and resolution. This is important because we allow mixed resolution results, so it is quite crucial
// to be aware what exactly resolution we see on query.
// TODO(bplotka): Consider adding resolution label to all results to propagate that info to UI and Query API.
func debugFoundBlockSetOverview(logger log.Logger, mint, maxt, maxResolutionMillis int64, lset labels.Labels, bs []*bucketBlock) {
	if len(bs) == 0 {
		level.Debug(logger).Log("msg", "No block found", "mint", mint, "maxt", maxt, "lset", lset.String())
		return
	}

	var (
		parts            []string
		currRes          = int64(-1)
		currMin, currMax int64
	)
	for _, b := range bs {
		if currRes == b.meta.Thanos.Downsample.Resolution {
			currMax = b.meta.MaxTime
			continue
		}

		if currRes != -1 {
			parts = append(parts, fmt.Sprintf("Range: %d-%d Resolution: %d", currMin, currMax, currRes))
		}

		currRes = b.meta.Thanos.Downsample.Resolution
		currMin = b.meta.MinTime
		currMax = b.meta.MaxTime
	}

	parts = append(parts, fmt.Sprintf("Range: %d-%d Resolution: %d", currMin, currMax, currRes))

	level.Debug(logger).Log("msg", "Blocks source resolutions", "blocks", len(bs), "Maximum Resolution", maxResolutionMillis, "mint", mint, "maxt", maxt, "lset", lset.String(), "spans", strings.Join(parts, "\n"))
}

// Series implements the storepb.StoreServer interface.
func (s *BucketStore) Series(req *storepb.SeriesRequest, seriesSrv storepb.Store_SeriesServer) (err error) {
	srv := newFlushableServer(
		newBatchableServer(seriesSrv, int(req.ResponseBatchSize)),
		sortingStrategyNone)

	if s.queryGate != nil {
		tracing.DoInSpan(srv.Context(), "store_query_gate_ismyturn", func(ctx context.Context) {
			err = s.queryGate.Start(srv.Context())
		})
		if err != nil {
			return errors.Wrapf(err, "failed to wait for turn")
		}

		defer s.queryGate.Done()
	}

	tenant, _ := tenancy.GetTenantFromGRPCMetadata(srv.Context())

	matchers, err := storecache.MatchersToPromMatchersCached(s.matcherCache, req.Matchers...)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	req.MinTime = s.limitMinTime(req.MinTime)
	req.MaxTime = s.limitMaxTime(req.MaxTime)

	var (
		bytesLimiter     = s.bytesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("bytes", tenant))
		ctx              = srv.Context()
		stats            = &queryStats{}
		respSets         []respSet
		mtx              sync.Mutex
		g, gctx          = errgroup.WithContext(ctx)
		resHints         = &hintspb.SeriesResponseHints{}
		reqBlockMatchers []*labels.Matcher

		chunksLimiter = s.chunksLimiterFactory(s.metrics.queriesDropped.WithLabelValues("chunks", tenant))
		seriesLimiter = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series", tenant))

		queryStatsEnabled = false

		logger = s.requestLoggerFunc(ctx, s.logger)
	)

	if req.Hints != nil {
		reqHints := &hintspb.SeriesRequestHints{}
		if err := types.UnmarshalAny(req.Hints, reqHints); err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal series request hints").Error())
		}
		queryStatsEnabled = reqHints.EnableQueryStats

		reqBlockMatchers, err = storepb.MatchersToPromMatchers(reqHints.BlockMatchers...)
		if err != nil {
			return status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	var extLsetToRemove map[string]struct{}
	if len(req.WithoutReplicaLabels) > 0 {
		extLsetToRemove = make(map[string]struct{})
		for _, l := range req.WithoutReplicaLabels {
			extLsetToRemove[l] = struct{}{}
		}
	}

	s.mtx.RLock()
	for _, bs := range s.blockSets {
		blockMatchers, ok := bs.labelMatchers(matchers...)
		if !ok {
			continue
		}
		// Sort matchers to make sure we generate the same cache key
		// when fetching expanded postings.
		sortedBlockMatchers := newSortedMatchers(blockMatchers)

		blocks := bs.getFor(req.MinTime, req.MaxTime, req.MaxResolutionWindow, reqBlockMatchers)

		if s.debugLogging {
			debugFoundBlockSetOverview(logger, req.MinTime, req.MaxTime, req.MaxResolutionWindow, bs.labels, blocks)
		}

		for _, b := range blocks {
			blk := b
			gctx := gctx

			if s.enableSeriesResponseHints {
				// Keep track of queried blocks.
				resHints.AddQueriedBlock(blk.meta.ULID)
			}

			shardMatcher := req.ShardInfo.Matcher(&s.buffers)

			blockClient := newBlockSeriesClient(
				srv.Context(),
				log.With(logger, "block", blk.meta.ULID),
				blk,
				req,
				seriesLimiter,
				chunksLimiter,
				bytesLimiter,
				sortedBlockMatchers,
				shardMatcher,
				s.enableChunkHashCalculation,
				s.seriesBatchSize,
				s.metrics.seriesFetchDurationSum,
				s.metrics.chunkFetchDuration,
				s.metrics.chunkFetchDurationSum,
				extLsetToRemove,
				s.enabledLazyExpandedPostings,
				s.seriesMatchRatio,
				s.postingGroupMaxKeySeriesRatio,
				s.metrics.lazyExpandedPostingsCount,
				s.metrics.lazyExpandedPostingGroupsByReason,
				s.metrics.lazyExpandedPostingSizeBytes,
				s.metrics.lazyExpandedPostingSeriesOverfetchedSizeBytes,
				tenant,
			)

			defer blockClient.Close()

			g.Go(func() error {

				span, _ := tracing.StartSpan(gctx, "bucket_store_block_series", tracing.Tags{
					"block.id":         blk.meta.ULID,
					"block.mint":       blk.meta.MinTime,
					"block.maxt":       blk.meta.MaxTime,
					"block.resolution": blk.meta.Thanos.Downsample.Resolution,
				})

				onClose := func() {
					mtx.Lock()
					stats = blockClient.MergeStats(stats)
					mtx.Unlock()
				}

				if err := blockClient.ExpandPostings(
					sortedBlockMatchers,
					seriesLimiter,
				); err != nil {
					onClose()
					span.Finish()
					return errors.Wrapf(err, "fetch postings for block %s", blk.meta.ULID)
				}

				var resp respSet
				if s.sortingStrategy == sortingStrategyStore {
					resp = newEagerRespSet(
						span,
						10*time.Minute,
						blk.meta.ULID.String(),
						[]labels.Labels{blk.extLset},
						onClose,
						blockClient,
						shardMatcher,
						false,
						s.metrics.emptyPostingCount.WithLabelValues(tenant),
						nil,
					)
				} else {
					resp = newLazyRespSet(
						span,
						10*time.Minute,
						blk.meta.ULID.String(),
						[]labels.Labels{blk.extLset},
						onClose,
						blockClient,
						shardMatcher,
						false,
						s.metrics.emptyPostingCount.WithLabelValues(tenant),
						max(s.lazyRetrievalMaxBufferedResponses, 1),
					)
				}

				mtx.Lock()
				respSets = append(respSets, resp)
				mtx.Unlock()

				return nil
			})
		}
	}

	s.mtx.RUnlock()

	defer func() {
		s.metrics.seriesDataTouched.WithLabelValues("postings", tenant).Observe(float64(stats.postingsTouched))
		s.metrics.seriesDataFetched.WithLabelValues("postings", tenant).Observe(float64(stats.postingsFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("postings", tenant).Observe(float64(stats.PostingsTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("postings", tenant).Observe(float64(stats.PostingsFetchedSizeSum))
		s.metrics.seriesDataTouched.WithLabelValues("series", tenant).Observe(float64(stats.seriesTouched))
		s.metrics.seriesDataFetched.WithLabelValues("series", tenant).Observe(float64(stats.seriesFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("series", tenant).Observe(float64(stats.SeriesTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("series", tenant).Observe(float64(stats.SeriesFetchedSizeSum))
		s.metrics.seriesDataTouched.WithLabelValues("chunks", tenant).Observe(float64(stats.chunksTouched))
		s.metrics.seriesDataFetched.WithLabelValues("chunks", tenant).Observe(float64(stats.chunksFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("chunks", tenant).Observe(float64(stats.ChunksTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("chunks", tenant).Observe(float64(stats.ChunksFetchedSizeSum))
		s.metrics.resultSeriesCount.WithLabelValues(tenant).Observe(float64(stats.mergedSeriesCount))
		s.metrics.cachedPostingsCompressions.WithLabelValues(labelEncode, tenant).Add(float64(stats.cachedPostingsCompressions))
		s.metrics.cachedPostingsCompressions.WithLabelValues(labelDecode, tenant).Add(float64(stats.cachedPostingsDecompressions))
		s.metrics.cachedPostingsCompressionErrors.WithLabelValues(labelEncode, tenant).Add(float64(stats.cachedPostingsCompressionErrors))
		s.metrics.cachedPostingsCompressionErrors.WithLabelValues(labelDecode, tenant).Add(float64(stats.cachedPostingsDecompressionErrors))
		s.metrics.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelEncode, tenant).Add(stats.CachedPostingsCompressionTimeSum.Seconds())
		s.metrics.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelDecode, tenant).Add(stats.CachedPostingsDecompressionTimeSum.Seconds())
		s.metrics.cachedPostingsOriginalSizeBytes.WithLabelValues(tenant).Add(float64(stats.CachedPostingsOriginalSizeSum))
		s.metrics.cachedPostingsCompressedSizeBytes.WithLabelValues(tenant).Add(float64(stats.CachedPostingsCompressedSizeSum))
		s.metrics.postingsSizeBytes.WithLabelValues(tenant).Observe(float64(int(stats.PostingsFetchedSizeSum) + int(stats.PostingsTouchedSizeSum)))

		if s.debugLogging {
			level.Debug(logger).Log("msg", "stats query processed",
				"request", req,
				"tenant", tenant,
				"stats", fmt.Sprintf("%+v", stats), "err", err)
		}
	}()

	// Concurrently get data from all blocks.
	{
		begin := time.Now()
		tracing.DoInSpan(ctx, "bucket_store_preload_all", func(_ context.Context) {
			err = g.Wait()
		})
		if err != nil {
			for _, resp := range respSets {
				resp.Close()
			}
			code := codes.Aborted
			if s, ok := status.FromError(errors.Cause(err)); ok {
				code = s.Code()
			}
			return status.Error(code, err.Error())
		}
		stats.blocksQueried = len(respSets)
		stats.GetAllDuration = time.Since(begin)
		s.metrics.seriesGetAllDuration.WithLabelValues(tenant).Observe(stats.GetAllDuration.Seconds())
		s.metrics.seriesBlocksQueried.WithLabelValues(tenant).Observe(float64(stats.blocksQueried))
	}

	lt := NewProxyResponseLoserTree(respSets...)
	defer lt.Close()
	// Merge the sub-results from each selected block.
	tracing.DoInSpan(ctx, "bucket_store_merge_all", func(ctx context.Context) {
		begin := time.Now()
		set := NewResponseDeduplicator(lt)
		i := 0
		for set.Next() {
			i++
			if req.Limit > 0 && i > int(req.Limit) {
				break
			}
			at := set.At()
			warn := at.GetWarning()
			if warn != "" {
				// TODO(fpetkovski): Consider deprecating string based warnings in favor of a
				// separate protobuf message containing the grpc code and
				// a human readable error message.
				err = status.Error(storepb.GRPCCodeFromWarn(warn), at.GetWarning())
				return
			}

			series := at.GetSeries()
			if series != nil {
				stats.mergedSeriesCount++
				if !req.SkipChunks {
					stats.mergedChunksCount += len(series.Chunks)
					s.metrics.chunkSizeBytes.WithLabelValues(tenant).Observe(float64(chunksSize(series.Chunks)))
				}
			}
			if err = srv.Send(at); err != nil {
				err = status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
				return
			}
		}
		stats.MergeDuration = time.Since(begin)
		s.metrics.seriesMergeDuration.WithLabelValues(tenant).Observe(stats.MergeDuration.Seconds())

		err = nil
	})
	if err != nil {
		return err
	}

	if s.enableSeriesResponseHints {
		var anyHints *types.Any

		if queryStatsEnabled {
			resHints.QueryStats = stats.toHints()
		}
		if anyHints, err = types.MarshalAny(resHints); err != nil {
			err = status.Error(codes.Unknown, errors.Wrap(err, "marshal series response hints").Error())
			return
		}

		if err = srv.Send(storepb.NewHintsSeriesResponse(anyHints)); err != nil {
			err = status.Error(codes.Unknown, errors.Wrap(err, "send series response hints").Error())
			return
		}
	}

	if err != nil {
		return err
	}
	return srv.Flush()
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
}

// LabelNames implements the storepb.StoreServer interface.
func (s *BucketStore) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}

	tenant, _ := tenancy.GetTenantFromGRPCMetadata(ctx)

	resHints := &hintspb.LabelNamesResponseHints{}

	var reqBlockMatchers []*labels.Matcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelNamesRequestHints{}
		err := types.UnmarshalAny(req.Hints, reqHints)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label names request hints").Error())
		}

		reqBlockMatchers, err = storepb.MatchersToPromMatchers(reqHints.BlockMatchers...)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}
	extLsetToRemove := make(map[string]struct{})
	if len(req.WithoutReplicaLabels) > 0 {
		for _, l := range req.WithoutReplicaLabels {
			extLsetToRemove[l] = struct{}{}
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	s.mtx.RLock()

	var mtx sync.Mutex
	var sets [][]string
	var seriesLimiter = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series", tenant))
	var bytesLimiter = s.bytesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("bytes", tenant))
	var logger = s.requestLoggerFunc(ctx, s.logger)

	for _, b := range s.blocks {
		gctx := gctx

		if !b.overlapsClosedInterval(req.Start, req.End) {
			continue
		}
		if len(reqBlockMatchers) > 0 && !b.matchRelabelLabels(reqBlockMatchers) {
			continue
		}
		// Filter external labels from matchers.
		reqSeriesMatchersNoExtLabels, ok := b.FilterExtLabelsMatchers(reqSeriesMatchers)
		if !ok {
			continue
		}

		sortedReqSeriesMatchersNoExtLabels := newSortedMatchers(reqSeriesMatchersNoExtLabels)

		resHints.AddQueriedBlock(b.meta.ULID)

		blockLogger := log.With(logger, "block", b.meta.ULID)
		indexr := b.indexReader(blockLogger)

		g.Go(func() error {
			span, newCtx := tracing.StartSpan(gctx, "bucket_store_block_label_names", tracing.Tags{
				"block.id":         b.meta.ULID,
				"block.mint":       b.meta.MinTime,
				"block.maxt":       b.meta.MaxTime,
				"block.resolution": b.meta.Thanos.Downsample.Resolution,
			})
			defer span.Finish()
			defer runutil.CloseWithLogOnErr(blockLogger, indexr, "label names")

			var result []string
			if len(reqSeriesMatchersNoExtLabels) == 0 {
				// Do it via index reader to have pending reader registered correctly.
				// LabelNames are already sorted.
				res, err := indexr.block.indexHeaderReader.LabelNames()
				if err != nil {
					return errors.Wrapf(err, "label names for block %s", b.meta.ULID)
				}

				// Add  a set for the external labels as well.
				// We're not adding them directly to refs because there could be duplicates.
				// b.extLset is already sorted by label name, no need to sort it again.
				extRes := make([]string, 0, b.extLset.Len())
				b.extLset.Range(func(l labels.Label) {
					if _, ok := extLsetToRemove[l.Name]; !ok {
						extRes = append(extRes, l.Name)
					}
				})

				result = strutil.MergeSlices(int(req.Limit), res, extRes)
			} else {
				seriesReq := &storepb.SeriesRequest{
					MinTime:              req.Start,
					MaxTime:              req.End,
					SkipChunks:           true,
					WithoutReplicaLabels: req.WithoutReplicaLabels,
				}
				blockClient := newBlockSeriesClient(
					newCtx,
					blockLogger,
					b,
					seriesReq,
					seriesLimiter,
					nil,
					bytesLimiter,
					reqSeriesMatchersNoExtLabels,
					nil,
					true,
					SeriesBatchSize,
					s.metrics.seriesFetchDurationSum,
					nil,
					nil,
					extLsetToRemove,
					s.enabledLazyExpandedPostings,
					s.seriesMatchRatio,
					s.postingGroupMaxKeySeriesRatio,
					s.metrics.lazyExpandedPostingsCount,
					s.metrics.lazyExpandedPostingGroupsByReason,
					s.metrics.lazyExpandedPostingSizeBytes,
					s.metrics.lazyExpandedPostingSeriesOverfetchedSizeBytes,
					tenant,
				)
				defer blockClient.Close()

				if err := blockClient.ExpandPostings(
					sortedReqSeriesMatchersNoExtLabels,
					seriesLimiter,
				); err != nil {
					return err
				}

				// Extract label names from all series. Many label names will be the same, so we need to deduplicate them.
				// Note that label names will already include external labels (passed to blockSeries), so we don't need
				// to add them again.
				labelNames := map[string]struct{}{}
				for {
					ls, err := blockClient.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Wrapf(err, "iterate series for block %s", b.meta.ULID)
					}

					if ls.GetWarning() != "" {
						return errors.Wrapf(errors.New(ls.GetWarning()), "iterate series for block %s", b.meta.ULID)
					}
					if ls.GetSeries() == nil {
						continue
					}
					for _, l := range ls.GetSeries().Labels {
						labelNames[l.Name] = struct{}{}
					}
				}

				result = make([]string, 0, len(labelNames))
				for n := range labelNames {
					result = append(result, n)
				}
				sort.Strings(result)
			}

			if len(result) > 0 {
				mtx.Lock()
				sets = append(sets, result)
				mtx.Unlock()
			}

			return nil
		})
	}

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		code := codes.Internal
		if s, ok := status.FromError(errors.Cause(err)); ok {
			code = s.Code()
		}
		return nil, status.Error(code, err.Error())
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label names response hints").Error())
	}

	names := strutil.MergeSlices(int(req.Limit), sets...)

	return &storepb.LabelNamesResponse{
		Names: names,
		Hints: anyHints,
	}, nil
}

func (b *bucketBlock) FilterExtLabelsMatchers(matchers []*labels.Matcher) ([]*labels.Matcher, bool) {
	// We filter external labels from matchers so we won't try to match series on them.
	var result []*labels.Matcher
	for _, m := range matchers {
		// Get value of external label from block.
		v := b.extLset.Get(m.Name)
		// If value is empty string the matcher is a valid one since it's not part of external labels.
		if v == "" {
			result = append(result, m)
		} else if v != "" && !m.Matches(v) {
			// If matcher is external label but value is different we don't want to look in block anyway.
			return []*labels.Matcher{}, false
		}
	}

	return result, true
}

// LabelValues implements the storepb.StoreServer interface.
func (s *BucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	reqSeriesMatchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request labels matchers").Error())
	}
	if slices.Contains(req.WithoutReplicaLabels, req.Label) {
		return &storepb.LabelValuesResponse{}, nil
	}

	tenant, _ := tenancy.GetTenantFromGRPCMetadata(ctx)

	resHints := &hintspb.LabelValuesResponseHints{}

	var hasMetricNameEqMatcher = false
	for _, m := range reqSeriesMatchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			hasMetricNameEqMatcher = true
			break
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	var reqBlockMatchers []*labels.Matcher
	if req.Hints != nil {
		reqHints := &hintspb.LabelValuesRequestHints{}
		err := types.UnmarshalAny(req.Hints, reqHints)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "unmarshal label values request hints").Error())
		}

		reqBlockMatchers, err = storepb.MatchersToPromMatchers(reqHints.BlockMatchers...)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, errors.Wrap(err, "translate request hints labels matchers").Error())
		}
	}

	s.mtx.RLock()

	var mtx sync.Mutex
	var sets [][]string
	var seriesLimiter = s.seriesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("series", tenant))
	var bytesLimiter = s.bytesLimiterFactory(s.metrics.queriesDropped.WithLabelValues("bytes", tenant))
	var logger = s.requestLoggerFunc(ctx, s.logger)
	var stats = &queryStats{}

	for _, b := range s.blocks {

		if !b.overlapsClosedInterval(req.Start, req.End) {
			continue
		}
		if len(reqBlockMatchers) > 0 && !b.matchRelabelLabels(reqBlockMatchers) {
			continue
		}
		// Filter external labels from matchers.
		reqSeriesMatchersNoExtLabels, ok := b.FilterExtLabelsMatchers(reqSeriesMatchers)
		if !ok {
			continue
		}

		// If we have series matchers and the Label is not an external one, add <labelName> != "" matcher
		// to only select series that have given label name.
		// We don't need such matcher if matchers already contain __name__=="something" matcher.
		if !hasMetricNameEqMatcher && len(reqSeriesMatchersNoExtLabels) > 0 && !b.extLset.Has(req.Label) {
			m, err := labels.NewMatcher(labels.MatchNotEqual, req.Label, "")
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}

			reqSeriesMatchersNoExtLabels = append(reqSeriesMatchersNoExtLabels, m)
		}

		sortedReqSeriesMatchersNoExtLabels := newSortedMatchers(reqSeriesMatchersNoExtLabels)

		resHints.AddQueriedBlock(b.meta.ULID)

		blockLogger := log.With(logger, "block", b.meta.ULID)
		indexr := b.indexReader(blockLogger)

		g.Go(func() error {
			span, newCtx := tracing.StartSpan(gctx, "bucket_store_block_label_values", tracing.Tags{
				"block.id":         b.meta.ULID,
				"block.mint":       b.meta.MinTime,
				"block.maxt":       b.meta.MaxTime,
				"block.resolution": b.meta.Thanos.Downsample.Resolution,
			})
			defer span.Finish()
			defer runutil.CloseWithLogOnErr(blockLogger, indexr, "label values")

			var result []string
			if len(reqSeriesMatchersNoExtLabels) == 0 {
				// Do it via index reader to have pending reader registered correctly.
				res, err := indexr.block.indexHeaderReader.LabelValues(req.Label)
				if err != nil {
					return errors.Wrapf(err, "index header label values for block %s", b.meta.ULID)
				}

				// Add the external label value as well.
				if extLabelValue := b.extLset.Get(req.Label); extLabelValue != "" {
					res = strutil.MergeSlices(int(req.Limit), res, []string{extLabelValue})
				}
				result = res
			} else {
				seriesReq := &storepb.SeriesRequest{
					MinTime:              req.Start,
					MaxTime:              req.End,
					SkipChunks:           true,
					WithoutReplicaLabels: req.WithoutReplicaLabels,
				}
				blockClient := newBlockSeriesClient(
					newCtx,
					blockLogger,
					b,
					seriesReq,
					seriesLimiter,
					nil,
					bytesLimiter,
					reqSeriesMatchersNoExtLabels,
					nil,
					true,
					SeriesBatchSize,
					s.metrics.seriesFetchDurationSum,
					nil,
					nil,
					nil,
					s.enabledLazyExpandedPostings,
					s.seriesMatchRatio,
					s.postingGroupMaxKeySeriesRatio,
					s.metrics.lazyExpandedPostingsCount,
					s.metrics.lazyExpandedPostingGroupsByReason,
					s.metrics.lazyExpandedPostingSizeBytes,
					s.metrics.lazyExpandedPostingSeriesOverfetchedSizeBytes,
					tenant,
				)
				defer func() {
					mtx.Lock()
					stats = blockClient.MergeStats(stats)
					mtx.Unlock()
					blockClient.Close()
				}()

				if err := blockClient.ExpandPostings(
					sortedReqSeriesMatchersNoExtLabels,
					seriesLimiter,
				); err != nil {
					return err
				}

				// Extract given label's value from all series and deduplicate them.
				// We don't need to deal with external labels, since they are already added by blockSeries.
				values := map[string]struct{}{}
				for {
					ls, err := blockClient.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Wrapf(err, "iterate series for block %s", b.meta.ULID)
					}

					if ls.GetWarning() != "" {
						return errors.Wrapf(errors.New(ls.GetWarning()), "iterate series for block %s", b.meta.ULID)
					}
					if ls.GetSeries() == nil {
						continue
					}

					val := labelpb.ZLabelsToPromLabels(ls.GetSeries().Labels).Get(req.Label)
					if val != "" {
						values[val] = struct{}{}
					}
				}

				result = make([]string, 0, len(values))
				for n := range values {
					result = append(result, n)
				}
				sort.Strings(result)
			}

			if len(result) > 0 {
				mtx.Lock()
				sets = append(sets, result)
				mtx.Unlock()
			}

			return nil
		})
	}

	s.mtx.RUnlock()

	defer func() {
		if s.debugLogging {
			level.Debug(logger).Log("msg", "stats query processed",
				"request", req,
				"tenant", tenant,
				"stats", fmt.Sprintf("%+v", stats), "err", err)
		}
	}()

	if err := g.Wait(); err != nil {
		code := codes.Internal
		if s, ok := status.FromError(errors.Cause(err)); ok {
			code = s.Code()
		}
		return nil, status.Error(code, err.Error())
	}

	anyHints, err := types.MarshalAny(resHints)
	if err != nil {
		return nil, status.Error(codes.Unknown, errors.Wrap(err, "marshal label values response hints").Error())
	}

	vals := strutil.MergeSlices(int(req.Limit), sets...)

	return &storepb.LabelValuesResponse{
		Values: vals,
		Hints:  anyHints,
	}, nil
}

// bucketBlockSet holds all blocks of an equal label set. It internally splits
// them up by downsampling resolution and allows querying.
type bucketBlockSet struct {
	labels      labels.Labels
	mtx         sync.RWMutex
	resolutions []int64          // Available resolution, high to low (in milliseconds).
	blocks      [][]*bucketBlock // Ordered buckets for the existing resolutions.
}

// newBucketBlockSet initializes a new set with the known downsampling windows hard-configured.
// The set currently does not support arbitrary ranges.
func newBucketBlockSet(lset labels.Labels) *bucketBlockSet {
	return &bucketBlockSet{
		labels:      lset,
		resolutions: []int64{downsample.ResLevel2, downsample.ResLevel1, downsample.ResLevel0},
		blocks:      make([][]*bucketBlock, 3),
	}
}

func (s *bucketBlockSet) add(b *bucketBlock) error {
	if !labels.Equal(s.labels, labels.FromMap(b.meta.Thanos.Labels)) {
		return errors.New("block's label set does not match set")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	i := int64index(s.resolutions, b.meta.Thanos.Downsample.Resolution)
	if i < 0 {
		return errors.Errorf("unsupported downsampling resolution %d", b.meta.Thanos.Downsample.Resolution)
	}
	bs := append(s.blocks[i], b)
	s.blocks[i] = bs

	// Always sort blocks by min time, then max time.
	sort.Slice(bs, func(j, k int) bool {
		if bs[j].meta.MinTime == bs[k].meta.MinTime {
			return bs[j].meta.MaxTime < bs[k].meta.MaxTime
		}
		return bs[j].meta.MinTime < bs[k].meta.MinTime
	})
	return nil
}

func (s *bucketBlockSet) remove(id ulid.ULID) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i, bs := range s.blocks {
		for j, b := range bs {
			if b.meta.ULID != id {
				continue
			}
			s.blocks[i] = append(bs[:j], bs[j+1:]...)
			return
		}
	}
}

func int64index(s []int64, x int64) int {
	for i, v := range s {
		if v == x {
			return i
		}
	}
	return -1
}

// getFor returns a time-ordered list of blocks that cover date between mint and maxt.
// Blocks with the biggest resolution possible but not bigger than the given max resolution are returned.
// It supports overlapping blocks.
//
// NOTE: s.blocks are expected to be sorted in minTime order.
func (s *bucketBlockSet) getFor(mint, maxt, maxResolutionMillis int64, blockMatchers []*labels.Matcher) (bs []*bucketBlock) {
	if mint > maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	// Find first matching resolution.
	i := 0
	for ; i < len(s.resolutions) && s.resolutions[i] > maxResolutionMillis; i++ {
	}

	// Fill the given interval with the blocks for the current resolution.
	// Our current resolution might not cover all data, so recursively fill the gaps with higher resolution blocks
	// if there is any.
	start := mint
	for _, b := range s.blocks[i] {
		if b.meta.MaxTime <= mint {
			continue
		}
		// NOTE: Block intervals are half-open: [b.MinTime, b.MaxTime).
		if b.meta.MinTime > maxt {
			break
		}

		if i+1 < len(s.resolutions) {
			bs = append(bs, s.getFor(start, b.meta.MinTime-1, s.resolutions[i+1], blockMatchers)...)
		}

		// Include the block in the list of matching ones only if there are no block-level matchers
		// or they actually match.
		if len(blockMatchers) == 0 || b.matchRelabelLabels(blockMatchers) {
			bs = append(bs, b)
		}

		start = b.meta.MaxTime
	}

	if i+1 < len(s.resolutions) {
		bs = append(bs, s.getFor(start, maxt, s.resolutions[i+1], blockMatchers)...)
	}
	return bs
}

// labelMatchers verifies whether the block set matches the given matchers and returns a new
// set of matchers that is equivalent when querying data within the block.
func (s *bucketBlockSet) labelMatchers(matchers ...*labels.Matcher) ([]*labels.Matcher, bool) {
	res := make([]*labels.Matcher, 0, len(matchers))

	for _, m := range matchers {
		v := s.labels.Get(m.Name)
		if v == "" {
			res = append(res, m)
			continue
		}
		if !m.Matches(v) {
			return nil, false
		}
	}
	return res, true
}

// bucketBlock represents a block that is located in a bucket. It holds intermediate
// state for the block on local disk.
type bucketBlock struct {
	metrics    *bucketStoreMetrics
	bkt        objstore.BucketReader
	meta       *metadata.Meta
	dir        string
	indexCache storecache.IndexCache
	chunkPool  pool.Pool[byte]
	extLset    labels.Labels

	indexHeaderReader indexheader.Reader

	chunkObjs []string

	pendingReaders sync.WaitGroup

	partitioner Partitioner

	// Block's labels used by block-level matchers to filter blocks to query. These are used to select blocks using
	// request hints' BlockMatchers.
	relabelLabels labels.Labels

	estimatedMaxChunkSize  int
	estimatedMaxSeriesSize int
}

func newBucketBlock(
	ctx context.Context,
	metrics *bucketStoreMetrics,
	meta *metadata.Meta,
	bkt objstore.BucketReader,
	dir string,
	indexCache storecache.IndexCache,
	chunkPool pool.Pool[byte],
	indexHeadReader indexheader.Reader,
	p Partitioner,
	maxSeriesSizeFunc BlockEstimator,
	maxChunkSizeFunc BlockEstimator,
) (b *bucketBlock, err error) {
	maxSeriesSize := EstimatedMaxSeriesSize
	if maxSeriesSizeFunc != nil {
		maxSeriesSize = int(maxSeriesSizeFunc(*meta))
	}
	maxChunkSize := EstimatedMaxChunkSize
	if maxChunkSizeFunc != nil {
		maxChunkSize = int(maxChunkSizeFunc(*meta))
	}
	// Translate the block's labels and inject the block ID as a label
	// to allow to match blocks also by ID.
	extLset := labels.FromMap(meta.Thanos.Labels)
	relabelLabels := labels.NewBuilder(extLset).Set(block.BlockIDLabel, meta.ULID.String()).Labels()
	b = &bucketBlock{
		metrics:                metrics,
		bkt:                    bkt,
		indexCache:             indexCache,
		chunkPool:              chunkPool,
		dir:                    dir,
		partitioner:            p,
		meta:                   meta,
		indexHeaderReader:      indexHeadReader,
		extLset:                extLset,
		relabelLabels:          relabelLabels,
		estimatedMaxSeriesSize: maxSeriesSize,
		estimatedMaxChunkSize:  maxChunkSize,
	}

	// Get object handles for all chunk files (segment files) from meta.json, if available.
	if len(meta.Thanos.SegmentFiles) > 0 {
		b.chunkObjs = make([]string, 0, len(meta.Thanos.SegmentFiles))

		for _, sf := range meta.Thanos.SegmentFiles {
			b.chunkObjs = append(b.chunkObjs, path.Join(meta.ULID.String(), block.ChunksDirname, sf))
		}
		return b, nil
	}

	// Get object handles for all chunk files from storage.
	if err = bkt.Iter(ctx, path.Join(meta.ULID.String(), block.ChunksDirname), func(n string) error {
		b.chunkObjs = append(b.chunkObjs, n)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "list chunk files")
	}
	return b, nil
}

func (b *bucketBlock) indexFilename() string {
	return path.Join(b.meta.ULID.String(), block.IndexFilename)
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64, logger log.Logger) ([]byte, error) {
	r, err := b.bkt.GetRange(ctx, b.indexFilename(), off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(logger, r, "readIndexRange close range reader")

	// Preallocate the buffer with the exact size so we don't waste allocations
	// while progressively growing an initial small buffer. The buffer capacity
	// is increased by MinRead to avoid extra allocations due to how ReadFrom()
	// internally works.
	buf := bytes.NewBuffer(make([]byte, 0, length+bytes.MinRead))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, errors.Wrap(err, "read range")
	}
	return buf.Bytes(), nil
}

func (b *bucketBlock) readChunkRange(ctx context.Context, seq int, off, length int64, chunkRanges byteRanges, logger log.Logger) (*[]byte, error) {
	if seq < 0 || seq >= len(b.chunkObjs) {
		return nil, errors.Errorf("unknown segment file for index %d", seq)
	}

	// Get a reader for the required range.
	reader, err := b.bkt.GetRange(ctx, b.chunkObjs[seq], off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "readChunkRange close range reader")

	// Get a buffer from the pool.
	chunkBuffer, err := b.chunkPool.Get(chunkRanges.size())
	if err != nil {
		return nil, errors.Wrap(err, "allocate chunk bytes")
	}

	*chunkBuffer, err = readByteRanges(reader, *chunkBuffer, chunkRanges)
	if err != nil {
		return nil, err
	}

	return chunkBuffer, nil
}

func (b *bucketBlock) chunkRangeReader(ctx context.Context, seq int, off, length int64) (io.ReadCloser, error) {
	if seq < 0 || seq >= len(b.chunkObjs) {
		return nil, errors.Errorf("unknown segment file for index %d", seq)
	}

	return b.bkt.GetRange(ctx, b.chunkObjs[seq], off, length)
}

func (b *bucketBlock) indexReader(logger log.Logger) *bucketIndexReader {
	b.pendingReaders.Add(1)
	return newBucketIndexReader(b, logger)
}

func (b *bucketBlock) chunkReader(logger log.Logger) *bucketChunkReader {
	b.pendingReaders.Add(1)
	return newBucketChunkReader(b, logger)
}

// matchRelabelLabels verifies whether the block matches the given matchers.
func (b *bucketBlock) matchRelabelLabels(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(b.relabelLabels.Get(m.Name)) {
			return false
		}
	}
	return true
}

// overlapsClosedInterval returns true if the block overlaps [mint, maxt).
func (b *bucketBlock) overlapsClosedInterval(mint, maxt int64) bool {
	// The block itself is a half-open interval
	// [b.meta.MinTime, b.meta.MaxTime).
	return b.meta.MinTime <= maxt && mint < b.meta.MaxTime
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *bucketBlock) Close() error {
	b.pendingReaders.Wait()
	return b.indexHeaderReader.Close()
}

// bucketIndexReader is a custom index reader (not conforming index.Reader interface) that reads index that is stored in
// object storage without having to fully download it.
type bucketIndexReader struct {
	block *bucketBlock
	dec   *index.Decoder
	stats *queryStats

	loadedSeriesMtx sync.Mutex
	loadedSeries    map[storage.SeriesRef][]byte

	indexVersion int
	logger       log.Logger

	// Posting slice to return to the postings pool on close.
	// A single bucketIndexReader should have at most 1 postings slice to return.
	postings []storage.SeriesRef
}

func newBucketIndexReader(block *bucketBlock, logger log.Logger) *bucketIndexReader {
	r := &bucketIndexReader{
		block: block,
		dec: &index.Decoder{
			LookupSymbol: block.indexHeaderReader.LookupSymbol,
		},
		stats:        &queryStats{},
		loadedSeries: map[storage.SeriesRef][]byte{},
		logger:       logger,
	}
	return r
}

// IndexVersion caches the index header version.
func (r *bucketIndexReader) IndexVersion() (int, error) {
	if r.indexVersion != 0 {
		return r.indexVersion, nil
	}
	v, err := r.block.indexHeaderReader.IndexVersion()
	if err != nil {
		return 0, err
	}
	r.indexVersion = v
	return v, nil
}

func (r *bucketIndexReader) reset(size int) {
	r.loadedSeries = make(map[storage.SeriesRef][]byte, size)
}

// ExpandedPostings returns postings in expanded list instead of index.Postings.
// This is because we need to have them buffered anyway to perform efficient lookup
// on object storage.
// Found posting IDs (ps) are not strictly required to point to a valid Series, e.g. during
// background garbage collections.
//
// Reminder: A posting is a reference (represented as a uint64) to a series reference, which in turn points to the first
// chunk where the series contains the matching label-value pair for a given block of data. Postings can be fetched by
// single label name=value.
func (r *bucketIndexReader) ExpandedPostings(
	ctx context.Context,
	ms sortedMatchers,
	bytesLimiter BytesLimiter,
	lazyExpandedPostingEnabled bool,
	seriesMatchRatio float64,
	postingGroupMaxKeySeriesRatio float64,
	lazyExpandedPostingSizeBytes prometheus.Counter,
	lazyExpandedPostingGroupsByReason *prometheus.CounterVec,
	tenant string,
) (*lazyExpandedPostings, error) {
	// Shortcut the case of `len(postingGroups) == 0`. It will only happen when no
	// matchers specified, and we don't need to fetch expanded postings from cache.
	if len(ms) == 0 {
		return nil, nil
	}

	hit, postings, err := r.fetchExpandedPostingsFromCache(ctx, ms, bytesLimiter, tenant)
	if err != nil {
		return nil, err
	}
	if hit {
		return newLazyExpandedPostings(postings), nil
	}
	var (
		allRequested = false
		hasAdds      = false
	)

	postingGroups, err := matchersToPostingGroups(ctx, r.block.indexHeaderReader.LabelValues, ms)
	if err != nil {
		return nil, errors.Wrap(err, "matchersToPostingGroups")
	}
	if postingGroups == nil {
		r.storeExpandedPostingsToCache(ms, index.EmptyPostings(), 0, tenant)
		return nil, nil
	}
	i := 0
	for _, pg := range postingGroups {
		allRequested = allRequested || pg.addAll
		hasAdds = hasAdds || len(pg.addKeys) > 0

		// If a posting group doesn't have any keys, like posting group created
		// from `=~".*"`, we don't have to keep the posting group as long as we
		// keep track of whether we need to add all postings or not.
		if len(pg.addKeys) == 0 && len(pg.removeKeys) == 0 {
			continue
		}
		postingGroups[i] = pg
		i++
	}
	postingGroups = postingGroups[:i]

	addAllPostings := allRequested && !hasAdds
	// We only need special All postings if there are no other adds. If there are, we can skip fetching
	// special All postings completely.
	if addAllPostings {
		// add group with label to fetch "special All postings".
		name, value := index.AllPostingsKey()
		postingGroups = append(postingGroups, newPostingGroup(true, name, []string{value}, nil))
	}

	ps, err := fetchLazyExpandedPostings(ctx, postingGroups, r, bytesLimiter, addAllPostings, lazyExpandedPostingEnabled, seriesMatchRatio, postingGroupMaxKeySeriesRatio, lazyExpandedPostingSizeBytes, lazyExpandedPostingGroupsByReason, tenant)
	if err != nil {
		return nil, errors.Wrap(err, "fetch and expand postings")
	}
	// If postings still have matchers to be applied lazily, cache expanded postings after filtering series so skip here.
	if !ps.lazyExpanded() {
		r.storeExpandedPostingsToCache(ms, index.NewListPostings(ps.postings), len(ps.postings), tenant)
	}

	if len(ps.postings) > 0 {
		// As of version two all series entries are 16 byte padded. All references
		// we get have to account for that to get the correct offset.
		version, err := r.IndexVersion()
		if err != nil {
			return nil, errors.Wrap(err, "get index version")
		}
		if version >= 2 {
			for i, id := range ps.postings {
				ps.postings[i] = id * 16
			}
		}
	}
	return ps, nil
}

// ExpandPostingsWithContext returns the postings expanded as a slice and considers context.
func ExpandPostingsWithContext(ctx context.Context, p index.Postings) ([]storage.SeriesRef, error) {
	res := getPostingsSlice()
	i := 0
	for p.Next() {
		i++
		if i%checkContextEveryNIterations == 0 {
			if err := ctx.Err(); err != nil {
				return res, err
			}
		}
		res = append(res, p.At())
	}
	return res, p.Err()
}

// postingGroup keeps posting keys for one or more matchers with the same label name. Logical result of the group is:
// If addAll is set: special All postings minus postings for removeKeys labels. No need to merge postings for addKeys in this case.
// If addAll is not set: Merge of postings for "addKeys" labels minus postings for removeKeys labels
// This computation happens in ExpandedPostings.
type postingGroup struct {
	addAll       bool
	name         string
	matchers     []*labels.Matcher
	addKeys      []string
	removeKeys   []string
	cardinality  int64
	existentKeys int
	lazy         bool
}

func newPostingGroup(addAll bool, name string, addKeys, removeKeys []string) *postingGroup {
	return &postingGroup{
		name:       name,
		addAll:     addAll,
		addKeys:    addKeys,
		removeKeys: removeKeys,
	}
}

// mergeKeys merges keys from two posting groups and ignores other fields.
func (pg postingGroup) mergeKeys(other *postingGroup) *postingGroup {
	if other == nil {
		return &pg
	}
	// This shouldn't happen, but add this as a safeguard.
	if pg.name != other.name {
		return nil
	}
	var i, j int
	// Both add all, merge remove keys.
	if pg.addAll && other.addAll {
		// Fast path to not allocate output slice if no remove keys are specified.
		// This is possible when matcher is `=~".*"`.
		if len(pg.removeKeys) == 0 {
			pg.removeKeys = other.removeKeys
			return &pg
		} else if len(other.removeKeys) == 0 {
			return &pg
		}
		output := make([]string, 0, len(pg.removeKeys)+len(other.removeKeys))
		for i < len(pg.removeKeys) && j < len(other.removeKeys) {
			if pg.removeKeys[i] < other.removeKeys[j] {
				output = append(output, pg.removeKeys[i])
				i++
			} else if pg.removeKeys[i] > other.removeKeys[j] {
				output = append(output, other.removeKeys[j])
				j++
			} else {
				output = append(output, pg.removeKeys[i])
				i++
				j++
			}
		}
		if i < len(pg.removeKeys) {
			output = append(output, pg.removeKeys[i:len(pg.removeKeys)]...)
		}
		if j < len(other.removeKeys) {
			output = append(output, other.removeKeys[j:len(other.removeKeys)]...)
		}
		pg.removeKeys = output
	} else if pg.addAll || other.addAll {
		// Subtract the remove keys.
		toRemove := other
		toAdd := &pg
		if pg.addAll {
			toRemove = &pg
			toAdd = other
		}
		var k int
		for i < len(toAdd.addKeys) && j < len(toRemove.removeKeys) {
			if toAdd.addKeys[i] < toRemove.removeKeys[j] {
				toAdd.addKeys[k] = toAdd.addKeys[i]
				k++
				i++
			} else if toAdd.addKeys[i] > toRemove.removeKeys[j] {
				j++
			} else {
				i++
				j++
			}
		}
		for i < len(toAdd.addKeys) {
			toAdd.addKeys[k] = toAdd.addKeys[i]
			i++
			k++
		}
		pg.addKeys = toAdd.addKeys[:k]
		pg.addAll = false
		pg.removeKeys = nil
	} else {
		addKeys := make([]string, 0, min(len(pg.addKeys), len(other.addKeys)))
		for i < len(pg.addKeys) && j < len(other.addKeys) {
			if pg.addKeys[i] == other.addKeys[j] {
				addKeys = append(addKeys, pg.addKeys[i])
				i++
				j++
			} else if pg.addKeys[i] < other.addKeys[j] {
				i++
			} else {
				j++
			}
		}
		pg.addKeys = addKeys
	}
	return &pg
}

func checkNilPosting(name, value string, p index.Postings) index.Postings {
	if p == nil {
		// This should not happen. Debug for https://github.com/thanos-io/thanos/issues/874.
		return index.ErrPostings(errors.Errorf("postings is nil for {%s=%s}. It was never fetched.", name, value))
	}
	return p
}

func matchersToPostingGroups(ctx context.Context, lvalsFn func(name string) ([]string, error), ms []*labels.Matcher) ([]*postingGroup, error) {
	matchersMap := make(map[string]map[string]*labels.Matcher)
	for _, m := range ms {
		if _, ok := matchersMap[m.Name]; !ok {
			matchersMap[m.Name] = make(map[string]*labels.Matcher)
		}
		matchersMap[m.Name][m.String()] = m
	}

	pgs := make([]*postingGroup, 0, len(matchersMap))
	// NOTE: Derived from tsdb.PostingsForMatchers.
	for _, values := range matchersMap {
		var (
			mergedPG     *postingGroup
			pg           *postingGroup
			vals         []string
			err          error
			valuesCached bool
		)
		lvalsFunc := lvalsFn
		matchers := make([]*labels.Matcher, 0, len(vals))
		// Merge PostingGroups with the same matcher into 1 to
		// avoid fetching duplicate postings.
		for _, val := range values {
			pg, vals, err = toPostingGroup(ctx, lvalsFunc, val)
			if err != nil {
				return nil, errors.Wrap(err, "toPostingGroup")
			}
			// Cache label values because label name is the same.
			if !valuesCached && vals != nil {
				lvals := vals
				lvalsFunc = func(_ string) ([]string, error) {
					return lvals, nil
				}
				valuesCached = true
			}

			// If this groups adds nothing, it's an empty group. We can shortcut this, since intersection with empty
			// postings would return no postings anyway.
			// E.g. label="non-existing-value" returns empty group.
			if !pg.addAll && len(pg.addKeys) == 0 {
				return nil, nil
			}
			if mergedPG == nil {
				mergedPG = pg
			} else {
				mergedPG = mergedPG.mergeKeys(pg)
			}

			// If this groups adds nothing, it's an empty group. We can shortcut this, since intersection with empty
			// postings would return no postings anyway.
			// E.g. label="non-existing-value" returns empty group.
			if !mergedPG.addAll && len(mergedPG.addKeys) == 0 {
				return nil, nil
			}
			matchers = append(matchers, val)
		}
		// Set and sort matchers to be used when picking up posting fetch strategy.
		mergedPG.matchers = newSortedMatchers(matchers)
		pgs = append(pgs, mergedPG)
	}
	slices.SortFunc(pgs, func(a, b *postingGroup) int {
		return strings.Compare(a.name, b.name)
	})
	return pgs, nil
}

// NOTE: Derived from tsdb.postingsForMatcher. index.Merge is equivalent to map duplication.
func toPostingGroup(ctx context.Context, lvalsFn func(name string) ([]string, error), m *labels.Matcher) (*postingGroup, []string, error) {
	// .* regexp matches any string.
	if m.Type == labels.MatchRegexp && m.Value == ".*" {
		return newPostingGroup(true, m.Name, nil, nil), nil, nil
	}

	// .* not regexp doesn't match any string.
	if m.Type == labels.MatchNotRegexp && m.Value == ".*" {
		return newPostingGroup(false, m.Name, nil, nil), nil, nil
	}

	// If the matcher selects an empty value, it selects all the series which don't
	// have the label name set too. See: https://github.com/prometheus/prometheus/issues/3575
	// and https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555.
	if m.Matches("") {
		var toRemove []string

		// Fast-path for MatchNotRegexp matching.
		// Inverse of a MatchNotRegexp is MatchRegexp (double negation).
		// Fast-path for set matching.
		if m.Type == labels.MatchNotRegexp {
			if vals := m.SetMatches(); len(vals) > 0 {
				sort.Strings(vals)
				return newPostingGroup(true, m.Name, nil, vals), nil, nil
			}
		}

		// Fast-path for MatchNotEqual matching.
		// Inverse of a MatchNotEqual is MatchEqual (double negation).
		if m.Type == labels.MatchNotEqual {
			return newPostingGroup(true, m.Name, nil, []string{m.Value}), nil, nil
		}

		vals, err := lvalsFn(m.Name)
		if err != nil {
			return nil, nil, err
		}

		// If the matcher is ="" or =~"", it is the same as removing all values for the label.
		// We can skip calling `Matches` here.
		if m.Value == "" && (m.Type == labels.MatchEqual || m.Type == labels.MatchRegexp) {
			return newPostingGroup(true, m.Name, nil, vals), vals, nil
		}

		// .+ regexp matches any non-empty string: get postings for all label values and remove them.
		if m.Type == labels.MatchNotRegexp && m.Value == ".+" {
			return newPostingGroup(true, m.Name, nil, vals), vals, nil
		}

		for i, val := range vals {
			if (i+1)%checkContextEveryNIterations == 0 && ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if !m.Matches(val) {
				toRemove = append(toRemove, val)
			}
		}

		return newPostingGroup(true, m.Name, nil, toRemove), vals, nil
	}

	if m.Type == labels.MatchRegexp {
		if vals := m.SetMatches(); len(vals) > 0 {
			sort.Strings(vals)
			return newPostingGroup(false, m.Name, vals, nil), nil, nil
		}
	}

	// Fast-path for equal matching.
	if m.Type == labels.MatchEqual {
		return newPostingGroup(false, m.Name, []string{m.Value}, nil), nil, nil
	}

	vals, err := lvalsFn(m.Name)
	if err != nil {
		return nil, nil, err
	}

	// If the matcher is !="" or !~"", it is the same as adding all values for the label.
	// We can skip calling `Matches` here.
	if m.Value == "" && (m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp) {
		return newPostingGroup(false, m.Name, vals, nil), vals, nil
	}

	// .+ regexp matches any non-empty string: get postings for all label values.
	if m.Type == labels.MatchRegexp && m.Value == ".+" {
		return newPostingGroup(false, m.Name, vals, nil), vals, nil
	}

	var toAdd []string
	for i, val := range vals {
		if (i+1)%checkContextEveryNIterations == 0 && ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if m.Matches(val) {
			toAdd = append(toAdd, val)
		}
	}

	return newPostingGroup(false, m.Name, toAdd, nil), vals, nil
}

type postingPtr struct {
	keyID int
	ptr   index.Range
}

func (r *bucketIndexReader) fetchExpandedPostingsFromCache(ctx context.Context, ms []*labels.Matcher, bytesLimiter BytesLimiter, tenant string) (bool, []storage.SeriesRef, error) {
	dataFromCache, hit := r.block.indexCache.FetchExpandedPostings(ctx, r.block.meta.ULID, ms, tenant)
	if !hit {
		return false, nil, nil
	}
	if err := bytesLimiter.ReserveWithType(uint64(len(dataFromCache)), PostingsTouched); err != nil {
		return false, nil, httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while loading expanded postings from index cache: %s", err)
	}

	r.stats.add(PostingsTouched, 1, len(dataFromCache))
	p, closeFns, err := r.decodeCachedPostings(dataFromCache)
	defer func() {
		for _, closeFn := range closeFns {
			closeFn()
		}
	}()
	// If failed to decode or expand cached postings, return and expand postings again.
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to decode cached expanded postings, refetch postings", "id", r.block.meta.ULID.String(), "err", err)
		return false, nil, nil
	}

	ps, err := ExpandPostingsWithContext(ctx, p)
	r.postings = ps
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to expand cached expanded postings, refetch postings", "id", r.block.meta.ULID.String(), "err", err)
		return false, nil, nil
	}

	if len(ps) > 0 {
		// As of version two all series entries are 16 byte padded. All references
		// we get have to account for that to get the correct offset.
		version, err := r.block.indexHeaderReader.IndexVersion()
		if err != nil {
			return false, nil, errors.Wrap(err, "get index version")
		}
		if version >= 2 {
			for i, id := range ps {
				ps[i] = id * 16
			}
		}
	}
	return true, ps, nil
}

func (r *bucketIndexReader) storeExpandedPostingsToCache(ms []*labels.Matcher, ps index.Postings, length int, tenant string) {
	// Encode postings to cache. We compress and cache postings before adding
	// 16 bytes padding in order to make compressed size smaller.
	dataToCache, compressionDuration, compressionErrors, compressedSize := r.encodePostingsToCache(ps, length)
	r.stats.cachedPostingsCompressions++
	r.stats.cachedPostingsCompressionErrors += compressionErrors
	r.stats.CachedPostingsCompressionTimeSum += compressionDuration
	r.stats.CachedPostingsCompressedSizeSum += units.Base2Bytes(compressedSize)
	r.stats.CachedPostingsOriginalSizeSum += units.Base2Bytes(length * 4) // Estimate the posting list size.
	r.block.indexCache.StoreExpandedPostings(r.block.meta.ULID, ms, dataToCache, tenant, storecache.CacheTTL(r.block.meta))
}

var bufioReaderPool = sync.Pool{
	New: func() any {
		return bufio.NewReader(nil)
	},
}

// fetchPostings fill postings requested by posting groups.
// It returns one posting for each key, in the same order.
// If postings for given key is not fetched, entry at given index will be nil.
func (r *bucketIndexReader) fetchPostings(ctx context.Context, keys []labels.Label, bytesLimiter BytesLimiter, tenant string) ([]index.Postings, []func(), error) {
	var closeFns []func()

	timer := prometheus.NewTimer(r.block.metrics.postingsFetchDuration.WithLabelValues(tenant))
	defer timer.ObserveDuration()

	var ptrs []postingPtr

	output := make([]index.Postings, len(keys))

	var size int64
	// Fetch postings from the cache with a single call.
	fromCache, _ := r.block.indexCache.FetchMultiPostings(ctx, r.block.meta.ULID, keys, tenant)
	for _, dataFromCache := range fromCache {
		size += int64(len(dataFromCache))
	}
	if err := bytesLimiter.ReserveWithType(uint64(size), PostingsTouched); err != nil {
		return nil, closeFns, httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while loading postings from index cache: %s", err)
	}

	// Iterate over all groups and fetch posting from cache.
	// If we have a miss, mark key to be fetched in `ptrs` slice.
	// Overlaps are well handled by partitioner, so we don't need to deduplicate keys.
	for ix, key := range keys {
		if (ix+1)%checkContextEveryNIterations == 0 {
			if err := ctx.Err(); err != nil {
				return nil, closeFns, err
			}
		}
		// Get postings for the given key from cache first.
		if b, ok := fromCache[key]; ok {
			r.stats.add(PostingsTouched, 1, len(b))

			l, closer, err := r.decodeCachedPostings(b)
			if err != nil {
				return nil, closeFns, errors.Wrap(err, "decode postings")
			}
			output[ix] = l
			closeFns = append(closeFns, closer...)
			continue
		}

		// Cache miss; save pointer for actual posting in index stored in object store.
		ptr, err := r.block.indexHeaderReader.PostingsOffset(key.Name, key.Value)
		if err == indexheader.NotFoundRangeErr {
			// This block does not have any posting for given key.
			output[ix] = index.EmptyPostings()
			continue
		}

		if err != nil {
			return nil, closeFns, errors.Wrap(err, "index header PostingsOffset")
		}

		r.stats.postingsToFetch++
		ptrs = append(ptrs, postingPtr{ptr: ptr, keyID: ix})
	}

	sort.Slice(ptrs, func(i, j int) bool {
		return ptrs[i].ptr.Start < ptrs[j].ptr.Start
	})

	// TODO(bwplotka): Asses how large in worst case scenario this can be. (e.g fetch for AllPostingsKeys)
	// Consider sub split if too big.
	parts := r.block.partitioner.Partition(len(ptrs), func(i int) (start, end uint64) {
		return uint64(ptrs[i].ptr.Start), uint64(ptrs[i].ptr.End)
	})

	size = 0
	for _, part := range parts {
		start := int64(part.Start)
		length := int64(part.End) - start
		size += length
	}
	if err := bytesLimiter.ReserveWithType(uint64(size), PostingsFetched); err != nil {
		return nil, closeFns, httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while fetching postings: %s", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	for _, part := range parts {
		i, j := part.ElemRng[0], part.ElemRng[1]

		start := int64(part.Start)
		// We assume index does not have any ptrs that has 0 length.
		length := int64(part.End) - start

		// Fetch from object storage concurrently and update stats and posting list.
		g.Go(func() error {
			begin := time.Now()
			stats := new(queryStats)
			defer func() {
				r.stats.merge(stats)
			}()

			brdr := bufioReaderPool.Get().(*bufio.Reader)
			defer bufioReaderPool.Put(brdr)

			partReader, err := r.block.bkt.GetRange(ctx, r.block.indexFilename(), start, length)
			if err != nil {
				return errors.Wrap(err, "read postings range")
			}
			defer runutil.CloseWithLogOnErr(r.logger, partReader, "readIndexRange close range reader")
			brdr.Reset(partReader)

			rdr := newPostingsReaderBuilder(ctx, brdr, ptrs[i:j], start, length)

			stats.postingsFetchCount++
			stats.add(PostingsFetched, j-i, int(length))

			for rdr.Next() {
				diffVarintPostings, postingsCount, keyID := rdr.AtDiffVarint()

				output[keyID] = newDiffVarintPostings(diffVarintPostings, nil)

				startCompression := time.Now()
				dataToCache, err := snappyStreamedEncode(int(postingsCount), diffVarintPostings)
				if err != nil {
					stats.cachedPostingsCompressionErrors += 1
					return errors.Wrap(err, "encoding with snappy")
				}

				stats.cachedPostingsCompressions += 1
				stats.CachedPostingsOriginalSizeSum += units.Base2Bytes(len(diffVarintPostings))
				stats.CachedPostingsCompressedSizeSum += units.Base2Bytes(len(dataToCache))
				stats.CachedPostingsCompressionTimeSum += time.Since(startCompression)
				stats.add(PostingsTouched, 1, len(diffVarintPostings))

				r.block.indexCache.StorePostings(r.block.meta.ULID, keys[keyID], dataToCache, tenant, storecache.CacheTTL(r.block.meta))
			}

			stats.PostingsFetchDurationSum += time.Since(begin)

			if err := rdr.Error(); err != nil {
				return errors.Wrap(err, "reading postings")
			}
			return nil
		})
	}

	return output, closeFns, g.Wait()
}

func (r *bucketIndexReader) decodeCachedPostings(b []byte) (index.Postings, []func(), error) {
	// Even if this instance is not using compression, there may be compressed
	// entries in the cache written by other stores.
	var (
		l        index.Postings
		err      error
		closeFns []func()
	)
	if isDiffVarintSnappyEncodedPostings(b) || isDiffVarintSnappyStreamedEncodedPostings(b) {
		s := time.Now()
		l, err = decodePostings(b)
		r.stats.cachedPostingsDecompressions += 1
		r.stats.CachedPostingsDecompressionTimeSum += time.Since(s)
		if err != nil {
			r.stats.cachedPostingsDecompressionErrors += 1
		} else {
			closeFns = append(closeFns, l.(closeablePostings).close)
		}
	} else {
		_, l, err = index.DecodePostingsRaw(encoding.Decbuf{B: b})
	}
	return l, closeFns, err
}

func (r *bucketIndexReader) encodePostingsToCache(p index.Postings, length int) ([]byte, time.Duration, int, int) {
	var dataToCache []byte
	compressionTime := time.Duration(0)
	compressionErrors, compressedSize := 0, 0
	s := time.Now()
	data, err := diffVarintSnappyStreamedEncode(p, length)
	compressionTime = time.Since(s)
	if err == nil {
		dataToCache = data
		compressedSize = len(data)
	} else {
		compressionErrors = 1
	}
	return dataToCache, compressionTime, compressionErrors, compressedSize
}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
type bigEndianPostings struct {
	list []byte
	cur  uint32
}

// TODO(bwplotka): Expose those inside Prometheus.
func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{list: list}
}

func (it *bigEndianPostings) At() storage.SeriesRef {
	return storage.SeriesRef(it.cur)
}

func (it *bigEndianPostings) Next() bool {
	if len(it.list) >= 4 {
		it.cur = binary.BigEndian.Uint32(it.list)
		it.list = it.list[4:]
		return true
	}
	return false
}

func (it *bigEndianPostings) Seek(x storage.SeriesRef) bool {
	if storage.SeriesRef(it.cur) >= x {
		return true
	}

	num := len(it.list) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(it.list[i*4:]) >= uint32(x)
	})
	if i < num {
		j := i * 4
		it.cur = binary.BigEndian.Uint32(it.list[j:])
		it.list = it.list[j+4:]
		return true
	}
	it.list = nil
	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}

// Returns number of remaining postings values.
func (it *bigEndianPostings) length() int {
	return len(it.list) / 4
}

func (r *bucketIndexReader) PreloadSeries(ctx context.Context, ids []storage.SeriesRef, bytesLimiter BytesLimiter, tenant string) error {
	timer := prometheus.NewTimer(r.block.metrics.seriesFetchDuration.WithLabelValues(tenant))
	defer func() {
		d := timer.ObserveDuration()
		r.stats.SeriesDownloadLatencySum += d
	}()

	// Load series from cache, overwriting the list of ids to preload
	// with the missing ones.
	fromCache, ids := r.block.indexCache.FetchMultiSeries(ctx, r.block.meta.ULID, ids, tenant)
	var size uint64
	for id, b := range fromCache {
		r.loadedSeries[id] = b
		size += uint64(len(b))
	}
	if err := bytesLimiter.ReserveWithType(size, SeriesTouched); err != nil {
		return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while loading series from index cache: %s", err)
	}

	parts := r.block.partitioner.Partition(len(ids), func(i int) (start, end uint64) {
		return uint64(ids[i]), uint64(ids[i]) + uint64(r.block.estimatedMaxSeriesSize)
	})

	g, ctx := errgroup.WithContext(ctx)
	for _, p := range parts {
		s, e := p.Start, p.End
		i, j := p.ElemRng[0], p.ElemRng[1]

		g.Go(func() error {
			return r.loadSeries(ctx, ids[i:j], false, s, e, bytesLimiter, tenant)
		})
	}
	return g.Wait()
}

func (r *bucketIndexReader) loadSeries(ctx context.Context, ids []storage.SeriesRef, refetch bool, start, end uint64, bytesLimiter BytesLimiter, tenant string) error {
	begin := time.Now()
	stats := new(queryStats)
	defer func() {
		r.stats.merge(stats)
	}()

	if err := bytesLimiter.ReserveWithType(uint64(end-start), SeriesFetched); err != nil {
		return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while fetching series: %s", err)
	}

	b, err := r.block.readIndexRange(ctx, int64(start), int64(end-start), r.logger)
	if err != nil {
		return errors.Wrap(err, "read series range")
	}

	stats.seriesFetchCount++
	stats.SeriesFetchDurationSum += time.Since(begin)
	stats.add(SeriesFetched, len(ids), int(end-start))

	for i, id := range ids {
		c := b[uint64(id)-start:]

		l, n := binary.Uvarint(c)
		if n < 1 {
			return errors.New("reading series length failed")
		}
		if len(c) < n+int(l) {
			if i == 0 && refetch {
				return errors.Errorf("invalid remaining size, even after refetch, remaining: %d, expected %d", len(c), n+int(l))
			}

			// Inefficient, but should be rare.
			r.block.metrics.seriesRefetches.WithLabelValues(tenant).Inc()
			level.Warn(r.logger).Log("msg", "series size exceeded expected size; refetching", "id", id, "series length", n+int(l), "maxSeriesSize", r.block.estimatedMaxSeriesSize)

			// Fetch plus to get the size of next one if exists.
			return r.loadSeries(ctx, ids[i:], true, uint64(id), uint64(id)+uint64(n+int(l)+1), bytesLimiter, tenant)
		}
		c = c[n : n+int(l)]
		r.loadedSeriesMtx.Lock()
		r.loadedSeries[id] = c
		r.loadedSeriesMtx.Unlock()

		r.block.indexCache.StoreSeries(r.block.meta.ULID, id, c, tenant, storecache.CacheTTL(r.block.meta))
	}
	return nil
}

type Part struct {
	Start uint64
	End   uint64

	ElemRng [2]int
}

type Partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all
	// input ranges
	// It supports overlapping ranges.
	// NOTE: It expects range to be sorted by start time.
	Partition(length int, rng func(int) (uint64, uint64)) []Part
}

type gapBasedPartitioner struct {
	maxGapSize uint64
}

func NewGapBasedPartitioner(maxGapSize uint64) Partitioner {
	return gapBasedPartitioner{
		maxGapSize: maxGapSize,
	}
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []Part) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := Part{}
		p.Start, p.End = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.End+g.maxGapSize < s {
				break
			}

			if p.End <= e {
				p.End = e
			}
		}
		p.ElemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}

type symbolizedLabel struct {
	name, value uint32
}

// LoadSeriesForTime populates the given symbolized labels for the series identified by the reference if at least one chunk is within
// time selection.
// LoadSeriesForTime also populates chunk metas slices if skipChunks if set to false. Chunks are also limited by the given time selection.
// LoadSeriesForTime returns false, when there are no series data for given time range.
//
// Error is returned on decoding error or if the reference does not resolve to a known series.
func (r *bucketIndexReader) LoadSeriesForTime(ref storage.SeriesRef, lset *[]symbolizedLabel, chks *[]chunks.Meta, skipChunks bool, mint, maxt int64) (ok bool, err error) {
	b, ok := r.loadedSeries[ref]
	if !ok {
		return false, errors.Errorf("series %d not found", ref)
	}

	r.stats.add(SeriesTouched, 1, len(b))
	return decodeSeriesForTime(b, lset, chks, skipChunks, mint, maxt)
}

// Close released the underlying resources of the reader.
func (r *bucketIndexReader) Close() error {
	r.block.pendingReaders.Done()

	if r.postings != nil {
		putPostingsSlice(r.postings)
	}
	return nil
}

func (b *blockSeriesClient) CloseSend() error {
	return nil
}

// LookupLabelsSymbols allows populates label set strings from symbolized label set.
func (r *bucketIndexReader) LookupLabelsSymbols(ctx context.Context, symbolized []symbolizedLabel, b *labels.Builder) error {
	b.Reset(labels.EmptyLabels())
	for _, s := range symbolized {
		ln, err := r.dec.LookupSymbol(ctx, s.name)
		if err != nil {
			return errors.Wrap(err, "lookup label name")
		}
		lv, err := r.dec.LookupSymbol(ctx, s.value)
		if err != nil {
			return errors.Wrap(err, "lookup label value")
		}
		b.Set(ln, lv)
	}
	return nil
}

// decodeSeriesForTime decodes a series entry from the given byte slice decoding only chunk metas that are within given min and max time.
// If skipChunks is specified decodeSeriesForTime does not return any chunks, but only labels and only if at least single chunk is within time range.
// decodeSeriesForTime returns false, when there are no series data for given time range.
func decodeSeriesForTime(b []byte, lset *[]symbolizedLabel, chks *[]chunks.Meta, skipChunks bool, selectMint, selectMaxt int64) (ok bool, err error) {
	*lset = (*lset)[:0]
	*chks = (*chks)[:0]

	d := encoding.Decbuf{B: b}

	// Read labels without looking up symbols.
	k := d.Uvarint()
	for i := 0; i < k; i++ {
		lno := uint32(d.Uvarint())
		lvo := uint32(d.Uvarint())
		*lset = append(*lset, symbolizedLabel{name: lno, value: lvo})
	}
	// Read the chunks meta data.
	k = d.Uvarint()
	if k == 0 {
		return false, d.Err()
	}

	// First t0 is absolute, rest is just diff so different type is used (Uvarint64).
	mint := d.Varint64()
	maxt := int64(d.Uvarint64()) + mint
	// Similar for first ref.
	ref := int64(d.Uvarint64())

	for i := 0; i < k; i++ {
		if i > 0 {
			mint += int64(d.Uvarint64())
			maxt = int64(d.Uvarint64()) + mint
			ref += d.Varint64()
		}

		if mint > selectMaxt {
			break
		}

		if maxt >= selectMint {
			// Found a chunk.
			if skipChunks {
				// We are not interested in chunks and we know there is at least one, that's enough to return series.
				return true, nil
			}

			*chks = append(*chks, chunks.Meta{
				Ref:     chunks.ChunkRef(ref),
				MinTime: mint,
				MaxTime: maxt,
			})
		}

		mint = maxt
	}
	return len(*chks) > 0, d.Err()
}

type loadIdx struct {
	offset uint32
	// Indices, not actual entries and chunks.
	seriesEntry int
	chunk       int
}

type bucketChunkReader struct {
	block *bucketBlock

	toLoad [][]loadIdx

	// chunkBytesMtx protects access to chunkBytes, when updated from chunks-loading goroutines.
	// After chunks are loaded, mutex is no longer used.
	chunkBytesMtx sync.Mutex
	stats         *queryStats
	chunkBytes    []*[]byte // Byte slice to return to the chunk pool on close.
	logger        log.Logger

	loadingChunksMtx  sync.Mutex
	loadingChunks     bool
	finishLoadingChks chan struct{}
}

func newBucketChunkReader(block *bucketBlock, logger log.Logger) *bucketChunkReader {
	return &bucketChunkReader{
		block:  block,
		stats:  &queryStats{},
		toLoad: make([][]loadIdx, len(block.chunkObjs)),
		logger: logger,
	}
}

func (r *bucketChunkReader) reset() {
	for i := range r.toLoad {
		r.toLoad[i] = r.toLoad[i][:0]
	}
	r.loadingChunksMtx.Lock()
	r.loadingChunks = false
	r.finishLoadingChks = make(chan struct{})
	r.loadingChunksMtx.Unlock()
}

func (r *bucketChunkReader) Close() error {
	// NOTE(GiedriusS): we need to wait until loading chunks because loading
	// chunks modifies r.block.chunkPool.
	r.loadingChunksMtx.Lock()
	loadingChks := r.loadingChunks
	r.loadingChunksMtx.Unlock()

	if loadingChks {
		<-r.finishLoadingChks
	}
	r.block.pendingReaders.Done()

	for _, b := range r.chunkBytes {
		r.block.chunkPool.Put(b)
	}
	return nil
}

// addLoad adds the chunk with id to the data set to be fetched.
// Chunk will be fetched and saved to refs[seriesEntry][chunk] upon r.load(refs, <...>) call.
func (r *bucketChunkReader) addLoad(id chunks.ChunkRef, seriesEntry, chunk int) error {
	var (
		seq = int(id >> 32)
		off = uint32(id)
	)
	if seq >= len(r.toLoad) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.toLoad[seq] = append(r.toLoad[seq], loadIdx{off, seriesEntry, chunk})
	return nil
}

// load loads all added chunks and saves resulting aggrs to refs.
func (r *bucketChunkReader) load(ctx context.Context, res []seriesEntry, aggrs []storepb.Aggr, calculateChunkChecksum bool, bytesLimiter BytesLimiter, tenant string) error {
	r.loadingChunksMtx.Lock()
	r.loadingChunks = true
	r.loadingChunksMtx.Unlock()

	begin := time.Now()
	defer func() {
		r.stats.ChunksDownloadLatencySum += time.Since(begin)

		r.loadingChunksMtx.Lock()
		r.loadingChunks = false
		r.loadingChunksMtx.Unlock()

		close(r.finishLoadingChks)
	}()

	g, ctx := errgroup.WithContext(ctx)

	for seq, pIdxs := range r.toLoad {
		sort.Slice(pIdxs, func(i, j int) bool {
			return pIdxs[i].offset < pIdxs[j].offset
		})
		parts := r.block.partitioner.Partition(len(pIdxs), func(i int) (start, end uint64) {
			return uint64(pIdxs[i].offset), uint64(pIdxs[i].offset) + uint64(r.block.estimatedMaxChunkSize)
		})

		var size uint64
		for _, p := range parts {
			size += p.End - p.Start
		}
		if err := bytesLimiter.ReserveWithType(size, ChunksFetched); err != nil {
			return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while fetching chunks: %s", err)
		}

		for _, p := range parts {
			seq := seq
			p := p
			indices := pIdxs[p.ElemRng[0]:p.ElemRng[1]]
			g.Go(func() error {
				return r.loadChunks(ctx, res, aggrs, seq, p, indices, calculateChunkChecksum, bytesLimiter, tenant)
			})
		}
	}
	return g.Wait()
}

// loadChunks will read range [start, end] from the segment file with sequence number seq.
// This data range covers chunks starting at supplied offsets.
func (r *bucketChunkReader) loadChunks(ctx context.Context, res []seriesEntry, aggrs []storepb.Aggr, seq int, part Part, pIdxs []loadIdx, calculateChunkChecksum bool, bytesLimiter BytesLimiter, tenant string) error {
	fetchBegin := time.Now()
	stats := new(queryStats)
	defer func() {
		stats.ChunksFetchDurationSum += time.Since(fetchBegin)
		r.stats.merge(stats)
	}()

	// Get a reader for the required range.
	reader, err := r.block.chunkRangeReader(ctx, seq, int64(part.Start), int64(part.End-part.Start))
	if err != nil {
		return errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(r.logger, reader, "readChunkRange close range reader")
	bufReader := bufio.NewReaderSize(reader, r.block.estimatedMaxChunkSize)

	stats.chunksFetchCount++
	stats.add(ChunksFetched, len(pIdxs), int(part.End-part.Start))

	var (
		buf        []byte
		readOffset = int(pIdxs[0].offset)

		// Save a few allocations.
		written  int
		diff     uint32
		chunkLen int
		n        int
	)

	bufPooled, err := r.block.chunkPool.Get(r.block.estimatedMaxChunkSize)
	if err == nil {
		buf = *bufPooled
		defer r.block.chunkPool.Put(&buf)
	} else {
		buf = make([]byte, r.block.estimatedMaxChunkSize)
	}

	for i, pIdx := range pIdxs {
		// Fast forward range reader to the next chunk start in case of sparse (for our purposes) byte range.
		for readOffset < int(pIdx.offset) {
			written, err = bufReader.Discard(int(pIdx.offset) - int(readOffset))
			if err != nil {
				return errors.Wrap(err, "fast forward range reader")
			}
			readOffset += written
		}
		// Presume chunk length to be reasonably large for common use cases.
		// However, declaration for EstimatedMaxChunkSize warns us some chunks could be larger in some rare cases.
		// This is handled further down below.
		chunkLen = r.block.estimatedMaxChunkSize
		if i+1 < len(pIdxs) {
			if diff = pIdxs[i+1].offset - pIdx.offset; int(diff) < chunkLen {
				chunkLen = int(diff)
			}
		}
		cb := buf[:chunkLen]
		n, err = io.ReadFull(bufReader, cb)
		readOffset += n
		// Unexpected EOF for last chunk could be a valid case. Any other errors are definitely real.
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && i != len(pIdxs)-1 {
			return errors.Wrapf(err, "read range for seq %d offset %x", seq, pIdx.offset)
		}

		chunkDataLen, n := binary.Uvarint(cb)
		if n < 1 {
			return errors.New("reading chunk length failed")
		}

		// Chunk length is n (number of bytes used to encode chunk data), 1 for chunk encoding and chunkDataLen for actual chunk data.
		// There is also crc32 after the chunk, but we ignore that.
		chunkLen = n + 1 + int(chunkDataLen)
		if chunkLen <= len(cb) {
			c := rawChunk(cb[n:chunkLen])
			err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), &c, aggrs, r.save, calculateChunkChecksum)
			if err != nil {
				return errors.Wrap(err, "populate chunk")
			}
			stats.add(ChunksTouched, 1, int(chunkDataLen))
			continue
		}

		r.block.metrics.chunkRefetches.WithLabelValues(tenant).Inc()
		// If we didn't fetch enough data for the chunk, fetch more.
		fetchBegin = time.Now()
		// Read entire chunk into new buffer.
		// TODO: readChunkRange call could be avoided for any chunk but last in this particular part.
		if err := bytesLimiter.ReserveWithType(uint64(chunkLen), ChunksTouched); err != nil {
			return httpgrpc.Errorf(int(codes.ResourceExhausted), "exceeded bytes limit while fetching chunks: %s", err)
		}

		nb, err := r.block.readChunkRange(ctx, seq, int64(pIdx.offset), int64(chunkLen), []byteRange{{offset: 0, length: chunkLen}}, r.logger)
		if err != nil {
			return errors.Wrapf(err, "preloaded chunk too small, expecting %d, and failed to fetch full chunk", chunkLen)
		}
		if len(*nb) != chunkLen {
			return errors.Errorf("preloaded chunk too small, expecting %d", chunkLen)
		}

		stats.add(ChunksFetched, 1, len(*nb))
		c := rawChunk((*nb)[n:])
		err = populateChunk(&(res[pIdx.seriesEntry].chks[pIdx.chunk]), &c, aggrs, r.save, calculateChunkChecksum)
		if err != nil {
			r.block.chunkPool.Put(nb)
			return errors.Wrap(err, "populate chunk")
		}

		stats.add(ChunksTouched, 1, int(chunkDataLen))

		r.block.chunkPool.Put(nb)
	}
	return nil
}

// save saves a copy of b's payload to a memory pool of its own and returns a new byte slice referencing said copy.
// Returned slice becomes invalid once r.block.chunkPool.Put() is called.
func (r *bucketChunkReader) save(b []byte) ([]byte, error) {
	r.chunkBytesMtx.Lock()
	defer r.chunkBytesMtx.Unlock()
	// Ensure we never grow slab beyond original capacity.
	if len(r.chunkBytes) == 0 ||
		cap(*r.chunkBytes[len(r.chunkBytes)-1])-len(*r.chunkBytes[len(r.chunkBytes)-1]) < len(b) {
		s, err := r.block.chunkPool.Get(len(b))
		if err != nil {
			return nil, errors.Wrap(err, "allocate chunk bytes")
		}
		r.chunkBytes = append(r.chunkBytes, s)
	}
	slab := r.chunkBytes[len(r.chunkBytes)-1]
	*slab = append(*slab, b...)
	return (*slab)[len(*slab)-len(b):], nil
}

// rawChunk is a helper type that wraps a chunk's raw bytes and implements the chunkenc.Chunk
// interface over it.
// It is used to Store API responses which don't need to introspect and validate the chunk's contents.
type rawChunk []byte

func (b *rawChunk) Reset(stream []byte) {
	(*b) = stream
}

func (b *rawChunk) Encoding() chunkenc.Encoding {
	return chunkenc.Encoding((*b)[0])
}

func (b *rawChunk) Bytes() []byte {
	return (*b)[1:]
}
func (b *rawChunk) Compact() {}

func (b *rawChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	panic("invalid call")
}

func (b *rawChunk) Appender() (chunkenc.Appender, error) {
	panic("invalid call")
}

func (b *rawChunk) NumSamples() int {
	panic("invalid call")
}

type queryStats struct {
	mtx sync.Mutex

	blocksQueried            int
	postingsTouched          int
	PostingsTouchedSizeSum   units.Base2Bytes
	postingsToFetch          int
	postingsFetched          int
	PostingsFetchedSizeSum   units.Base2Bytes
	postingsFetchCount       int
	PostingsFetchDurationSum time.Duration

	cachedPostingsCompressions         int
	cachedPostingsCompressionErrors    int
	CachedPostingsOriginalSizeSum      units.Base2Bytes
	CachedPostingsCompressedSizeSum    units.Base2Bytes
	CachedPostingsCompressionTimeSum   time.Duration
	cachedPostingsDecompressions       int
	cachedPostingsDecompressionErrors  int
	CachedPostingsDecompressionTimeSum time.Duration

	seriesTouched            int
	SeriesTouchedSizeSum     units.Base2Bytes
	seriesFetched            int
	SeriesFetchedSizeSum     units.Base2Bytes
	seriesFetchCount         int
	SeriesFetchDurationSum   time.Duration
	SeriesDownloadLatencySum time.Duration

	chunksTouched            int
	ChunksTouchedSizeSum     units.Base2Bytes
	chunksFetched            int
	ChunksFetchedSizeSum     units.Base2Bytes
	chunksFetchCount         int
	ChunksFetchDurationSum   time.Duration
	ChunksDownloadLatencySum time.Duration

	GetAllDuration    time.Duration
	mergedSeriesCount int
	mergedChunksCount int
	MergeDuration     time.Duration

	DataDownloadedSizeSum units.Base2Bytes
}

func (s *queryStats) add(dataType StoreDataType, dataCount int, dataSize int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	switch dataType {
	case PostingsFetched:
		s.postingsFetched += dataCount
		s.PostingsFetchedSizeSum += units.Base2Bytes(dataSize)
	case PostingsTouched:
		s.postingsTouched += dataCount
		s.PostingsTouchedSizeSum += units.Base2Bytes(dataSize)
	case SeriesFetched:
		s.seriesFetched += dataCount
		s.SeriesFetchedSizeSum += units.Base2Bytes(dataSize)
	case SeriesTouched:
		s.seriesTouched += dataCount
		s.SeriesTouchedSizeSum += units.Base2Bytes(dataSize)
	case ChunksFetched:
		s.chunksFetched += dataCount
		s.ChunksFetchedSizeSum += units.Base2Bytes(dataSize)
	case ChunksTouched:
		s.chunksTouched += dataCount
		s.ChunksTouchedSizeSum += units.Base2Bytes(dataSize)
	default:
		return
	}
	s.DataDownloadedSizeSum += units.Base2Bytes(dataSize)
}

func (s *queryStats) merge(o *queryStats) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.blocksQueried += o.blocksQueried

	s.postingsToFetch += o.postingsToFetch
	s.postingsTouched += o.postingsTouched
	s.PostingsTouchedSizeSum += o.PostingsTouchedSizeSum
	s.postingsFetched += o.postingsFetched
	s.PostingsFetchedSizeSum += o.PostingsFetchedSizeSum
	s.postingsFetchCount += o.postingsFetchCount
	s.PostingsFetchDurationSum += o.PostingsFetchDurationSum

	s.cachedPostingsCompressions += o.cachedPostingsCompressions
	s.cachedPostingsCompressionErrors += o.cachedPostingsCompressionErrors
	s.CachedPostingsOriginalSizeSum += o.CachedPostingsOriginalSizeSum
	s.CachedPostingsCompressedSizeSum += o.CachedPostingsCompressedSizeSum
	s.CachedPostingsCompressionTimeSum += o.CachedPostingsCompressionTimeSum
	s.cachedPostingsDecompressions += o.cachedPostingsDecompressions
	s.cachedPostingsDecompressionErrors += o.cachedPostingsDecompressionErrors
	s.CachedPostingsDecompressionTimeSum += o.CachedPostingsDecompressionTimeSum

	s.seriesTouched += o.seriesTouched
	s.SeriesTouchedSizeSum += o.SeriesTouchedSizeSum
	s.seriesFetched += o.seriesFetched
	s.SeriesFetchedSizeSum += o.SeriesFetchedSizeSum
	s.seriesFetchCount += o.seriesFetchCount
	s.SeriesFetchDurationSum += o.SeriesFetchDurationSum
	s.SeriesDownloadLatencySum += o.SeriesDownloadLatencySum

	s.chunksTouched += o.chunksTouched
	s.ChunksTouchedSizeSum += o.ChunksTouchedSizeSum
	s.chunksFetched += o.chunksFetched
	s.ChunksFetchedSizeSum += o.ChunksFetchedSizeSum
	s.chunksFetchCount += o.chunksFetchCount
	s.ChunksFetchDurationSum += o.ChunksFetchDurationSum
	s.ChunksDownloadLatencySum += o.ChunksDownloadLatencySum

	s.GetAllDuration += o.GetAllDuration
	s.mergedSeriesCount += o.mergedSeriesCount
	s.mergedChunksCount += o.mergedChunksCount
	s.MergeDuration += o.MergeDuration

	s.DataDownloadedSizeSum += o.DataDownloadedSizeSum
}

func (s *queryStats) toHints() *hintspb.QueryStats {
	return &hintspb.QueryStats{
		BlocksQueried:          int64(s.blocksQueried),
		PostingsTouched:        int64(s.postingsTouched),
		PostingsTouchedSizeSum: int64(s.PostingsTouchedSizeSum),
		PostingsToFetch:        int64(s.postingsToFetch),
		PostingsFetched:        int64(s.postingsFetched),
		PostingsFetchedSizeSum: int64(s.PostingsFetchedSizeSum),
		PostingsFetchCount:     int64(s.postingsFetchCount),
		SeriesTouched:          int64(s.seriesTouched),
		SeriesTouchedSizeSum:   int64(s.SeriesTouchedSizeSum),
		SeriesFetched:          int64(s.seriesFetched),
		SeriesFetchedSizeSum:   int64(s.SeriesFetchedSizeSum),
		SeriesFetchCount:       int64(s.seriesFetchCount),
		ChunksTouched:          int64(s.chunksTouched),
		ChunksTouchedSizeSum:   int64(s.ChunksTouchedSizeSum),
		ChunksFetched:          int64(s.chunksFetched),
		ChunksFetchedSizeSum:   int64(s.ChunksFetchedSizeSum),
		ChunksFetchCount:       int64(s.chunksFetchCount),
		MergedSeriesCount:      int64(s.mergedSeriesCount),
		MergedChunksCount:      int64(s.mergedChunksCount),
		DataDownloadedSizeSum:  int64(s.DataDownloadedSizeSum),
		GetAllDuration:         s.GetAllDuration,
		MergeDuration:          s.MergeDuration,
	}
}

// NewDefaultChunkBytesPool returns a chunk bytes pool with default settings.
func NewDefaultChunkBytesPool(maxChunkPoolBytes uint64) (pool.Pool[byte], error) {
	return pool.NewBucketedPool[byte](chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, maxChunkPoolBytes)
}
