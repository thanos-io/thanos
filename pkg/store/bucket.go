package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tracing"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// maxSamplesPerChunk is approximately the max number of samples that we may have in any given chunk. This is needed
	// for precalculating the number of samples that we may have to retrieve and decode for any given query
	// without downloading them. Please take a look at https://github.com/prometheus/tsdb/pull/397 to know
	// where this number comes from. Long story short: TSDB is made in such a way, and it is made in such a way
	// because you barely get any improvements in compression when the number of samples is beyond this.
	// Take a look at Figure 6 in this whitepaper http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
	maxSamplesPerChunk = 120

	maxChunkSize = 16000

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
)

type bucketStoreMetrics struct {
	blocksLoaded          prometheus.Gauge
	blockLoads            prometheus.Counter
	blockLoadFailures     prometheus.Counter
	blockDrops            prometheus.Counter
	blockDropFailures     prometheus.Counter
	seriesDataTouched     *prometheus.SummaryVec
	seriesDataFetched     *prometheus.SummaryVec
	seriesDataSizeTouched *prometheus.SummaryVec
	seriesDataSizeFetched *prometheus.SummaryVec
	seriesBlocksQueried   prometheus.Summary
	seriesGetAllDuration  prometheus.Histogram
	seriesMergeDuration   prometheus.Histogram
	resultSeriesCount     prometheus.Summary
	chunkSizeBytes        prometheus.Histogram
	queriesDropped        prometheus.Counter
	queriesLimit          prometheus.Gauge
}

func newBucketStoreMetrics(reg prometheus.Registerer) *bucketStoreMetrics {
	var m bucketStoreMetrics

	m.blockLoads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_loads_total",
		Help: "Total number of remote block loading attempts.",
	})
	m.blockLoadFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_load_failures_total",
		Help: "Total number of failed remote block loading attempts.",
	})
	m.blockDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drops_total",
		Help: "Total number of local blocks that were dropped.",
	})
	m.blockDropFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_block_drop_failures_total",
		Help: "Total number of local blocks that failed to be dropped.",
	})
	m.blocksLoaded = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	})

	m.seriesDataTouched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_touched",
		Help: "How many items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataFetched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_fetched",
		Help: "How many items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesDataSizeTouched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_touched_bytes",
		Help: "Size of all items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataSizeFetched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_fetched_bytes",
		Help: "Size of all items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesBlocksQueried = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_blocks_queried",
		Help: "Number of blocks in a bucket store that were touched to satisfy a query.",
	})
	m.seriesGetAllDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_series_get_all_duration_seconds",
		Help: "Time it takes until all per-block prepares and preloads for a query are finished.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60,
		},
	})
	m.seriesMergeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_series_merge_duration_seconds",
		Help: "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60,
		},
	})
	m.resultSeriesCount = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_result_series",
		Help: "Number of series observed in the final result of a query.",
	})

	m.chunkSizeBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_sent_chunk_size_bytes",
		Help: "Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	})

	m.queriesDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_bucket_store_queries_dropped_total",
		Help: "Number of queries that were dropped due to the sample limit.",
	})
	m.queriesLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_queries_concurrent_max",
		Help: "Number of maximum concurrent queries.",
	})

	if reg != nil {
		reg.MustRegister(
			m.blockLoads,
			m.blockLoadFailures,
			m.blockDrops,
			m.blockDropFailures,
			m.blocksLoaded,
			m.seriesDataTouched,
			m.seriesDataFetched,
			m.seriesDataSizeTouched,
			m.seriesDataSizeFetched,
			m.seriesBlocksQueried,
			m.seriesGetAllDuration,
			m.seriesMergeDuration,
			m.resultSeriesCount,
			m.chunkSizeBytes,
			m.queriesDropped,
			m.queriesLimit,
		)
	}
	return &m
}

type indexCache interface {
	SetPostings(b ulid.ULID, l labels.Label, v []byte)
	Postings(b ulid.ULID, l labels.Label) ([]byte, bool)
	SetSeries(b ulid.ULID, id uint64, v []byte)
	Series(b ulid.ULID, id uint64) ([]byte, bool)
}

// FilterConfig is a configuration, which Store uses for filtering metrics.
type FilterConfig struct {
	MinTime, MaxTime model.TimeOrDurationValue
}

// BucketStore implements the store API backed by a bucket. It loads all index
// files to local disk.
type BucketStore struct {
	logger     log.Logger
	metrics    *bucketStoreMetrics
	bucket     objstore.BucketReader
	dir        string
	indexCache indexCache
	chunkPool  *pool.BytesPool

	// Sets of blocks that have the same labels. They are indexed by a hash over their label set.
	mtx       sync.RWMutex
	blocks    map[ulid.ULID]*bucketBlock
	blockSets map[uint64]*bucketBlockSet

	// Verbose enabled additional logging.
	debugLogging bool
	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	// Query gate which limits the maximum amount of concurrent queries.
	queryGate *Gate

	// samplesLimiter limits the number of samples per each Series() call.
	samplesLimiter *Limiter
	partitioner    partitioner

	filterConfig  *FilterConfig
	relabelConfig []*relabel.Config

	labelSets                map[uint64]labels.Labels
	enableCompatibilityLabel bool
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	logger log.Logger,
	reg prometheus.Registerer,
	bucket objstore.BucketReader,
	dir string,
	indexCache indexCache,
	maxChunkPoolBytes uint64,
	maxSampleCount uint64,
	maxConcurrent int,
	debugLogging bool,
	blockSyncConcurrency int,
	filterConf *FilterConfig,
	relabelConfig []*relabel.Config,
	enableCompatibilityLabel bool,
) (*BucketStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	if maxConcurrent < 0 {
		return nil, errors.Errorf("max concurrency value cannot be lower than 0 (got %v)", maxConcurrent)
	}

	chunkPool, err := pool.NewBytesPool(maxChunkSize, 50e6, 2, maxChunkPoolBytes)
	if err != nil {
		return nil, errors.Wrap(err, "create chunk pool")
	}

	const maxGapSize = 512 * 1024

	metrics := newBucketStoreMetrics(reg)
	s := &BucketStore{
		logger:               logger,
		bucket:               bucket,
		dir:                  dir,
		indexCache:           indexCache,
		chunkPool:            chunkPool,
		blocks:               map[ulid.ULID]*bucketBlock{},
		blockSets:            map[uint64]*bucketBlockSet{},
		debugLogging:         debugLogging,
		blockSyncConcurrency: blockSyncConcurrency,
		queryGate: NewGate(
			maxConcurrent,
			extprom.WrapRegistererWithPrefix("thanos_bucket_store_series_", reg),
		),
		samplesLimiter:           NewLimiter(maxSampleCount, metrics.queriesDropped),
		partitioner:              gapBasedPartitioner{maxGapSize: maxGapSize},
		filterConfig:             filterConf,
		relabelConfig:            relabelConfig,
		enableCompatibilityLabel: enableCompatibilityLabel,
	}
	s.metrics = metrics

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}

	s.metrics.queriesLimit.Set(float64(maxConcurrent))

	return s, nil
}

// Close the store.
func (s *BucketStore) Close() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, b := range s.blocks {
		if e := b.Close(); e != nil {
			level.Warn(s.logger).Log("msg", "closing Bucket block failed", "err", err)
			err = e
		}
	}
	return err
}

// SyncBlocks synchronizes the stores state with the Bucket bucket.
// It will reuse disk space as persistent cache based on s.dir param.
func (s *BucketStore) SyncBlocks(ctx context.Context) error {
	var wg sync.WaitGroup
	blockc := make(chan ulid.ULID)

	for i := 0; i < s.blockSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			for id := range blockc {
				if err := s.addBlock(ctx, id); err != nil {
					level.Warn(s.logger).Log("msg", "loading block failed", "id", id, "err", err)
					continue
				}
			}
			wg.Done()
		}()
	}

	allIDs := map[ulid.ULID]struct{}{}

	err := s.bucket.Iter(ctx, "", func(name string) error {
		// Strip trailing slash indicating a directory.
		id, err := ulid.Parse(name[:len(name)-1])
		if err != nil {
			return nil
		}

		inRange, err := s.isBlockInMinMaxRange(ctx, id)
		if err != nil {
			level.Warn(s.logger).Log("msg", "error parsing block range", "block", id, "err", err)
			return nil
		}

		if !inRange {
			return nil
		}

		allIDs[id] = struct{}{}

		if b := s.getBlock(id); b != nil {
			return nil
		}
		select {
		case <-ctx.Done():
		case blockc <- id:
		}
		return nil
	})

	close(blockc)
	wg.Wait()

	if err != nil {
		return errors.Wrap(err, "iter")
	}
	// Drop all blocks that are no longer present in the bucket.
	for id := range s.blocks {
		if _, ok := allIDs[id]; ok {
			continue
		}
		if err := s.removeBlock(id); err != nil {
			level.Warn(s.logger).Log("msg", "drop outdated block", "block", id, "err", err)
			s.metrics.blockDropFailures.Inc()
		}
		s.metrics.blockDrops.Inc()
	}

	// Sync advertise labels.
	s.mtx.Lock()
	s.labelSets = make(map[uint64]labels.Labels, len(s.blocks))
	for _, bs := range s.blocks {
		s.labelSets[bs.labels.Hash()] = append(labels.Labels(nil), bs.labels...)
	}
	s.mtx.Unlock()

	return nil
}

// InitialSync perform blocking sync with extra step at the end to delete locally saved blocks that are no longer
// present in the bucket. The mismatch of these can only happen between restarts, so we can do that only once per startup.
func (s *BucketStore) InitialSync(ctx context.Context) error {
	if err := s.SyncBlocks(ctx); err != nil {
		return errors.Wrap(err, "sync block")
	}

	names, err := fileutil.ReadDir(s.dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
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

func (s *BucketStore) numBlocks() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.blocks)
}

func (s *BucketStore) isBlockInMinMaxRange(ctx context.Context, id ulid.ULID) (bool, error) {
	dir := filepath.Join(s.dir, id.String())

	err, meta := loadMeta(ctx, s.logger, s.bucket, dir, id)
	if err != nil {
		return false, err
	}

	// We check for blocks in configured minTime, maxTime range.
	switch {
	case meta.MaxTime <= s.filterConfig.MinTime.PrometheusTimestamp():
		return false, nil

	case meta.MinTime >= s.filterConfig.MaxTime.PrometheusTimestamp():
		return false, nil
	}

	return true, nil
}

func (s *BucketStore) getBlock(id ulid.ULID) *bucketBlock {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.blocks[id]
}

func (s *BucketStore) addBlock(ctx context.Context, id ulid.ULID) (err error) {
	dir := filepath.Join(s.dir, id.String())

	defer func() {
		if err != nil {
			s.metrics.blockLoadFailures.Inc()
			if err2 := os.RemoveAll(dir); err2 != nil {
				level.Warn(s.logger).Log("msg", "failed to remove block we cannot load", "err", err2)
			}
		}
	}()
	s.metrics.blockLoads.Inc()

	b, err := newBucketBlock(
		ctx,
		log.With(s.logger, "block", id),
		s.bucket,
		id,
		dir,
		s.indexCache,
		s.chunkPool,
		s.partitioner,
	)
	if err != nil {
		return errors.Wrap(err, "new bucket block")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := labels.FromMap(b.meta.Thanos.Labels)
	h := lset.Hash()

	// Check for block labels by relabeling.
	// If output is empty, the block will be dropped.
	if processedLabels := relabel.Process(promlabels.FromMap(lset.Map()), s.relabelConfig...); processedLabels == nil {
		level.Debug(s.logger).Log("msg", "dropping block(drop in relabeling)", "block", id)
		return os.RemoveAll(dir)
	}
	b.labels = lset
	sort.Sort(b.labels)

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

// Info implements the storepb.StoreServer interface.
func (s *BucketStore) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	mint, maxt := s.TimeRange()
	res := &storepb.InfoResponse{
		StoreType: component.Store.ToProto(),
		MinTime:   mint,
		MaxTime:   maxt,
	}

	s.mtx.RLock()
	res.LabelSets = make([]storepb.LabelSet, 0, len(s.labelSets))
	for _, ls := range s.labelSets {
		lset := make([]storepb.Label, 0, len(ls))
		for _, l := range ls {
			lset = append(lset, storepb.Label{Name: l.Name, Value: l.Value})
		}
		res.LabelSets = append(res.LabelSets, storepb.LabelSet{Labels: lset})
	}
	s.mtx.RUnlock()

	if s.enableCompatibilityLabel && len(res.LabelSets) > 0 {
		// This is for compatibility with Querier v0.7.0.
		// See query.StoreCompatibilityTypeLabelName comment for details.
		res.LabelSets = append(res.LabelSets, storepb.LabelSet{Labels: []storepb.Label{{Name: CompatibilityTypeLabelName, Value: "store"}}})
	}
	return res, nil
}

func (s *BucketStore) limitMinTime(mint int64) int64 {
	filterMinTime := s.filterConfig.MinTime.PrometheusTimestamp()

	if mint < filterMinTime {
		return filterMinTime
	}

	return mint
}

func (s *BucketStore) limitMaxTime(maxt int64) int64 {
	filterMaxTime := s.filterConfig.MaxTime.PrometheusTimestamp()

	if maxt > filterMaxTime {
		maxt = filterMaxTime
	}

	return maxt
}

type seriesEntry struct {
	lset []storepb.Label
	refs []uint64
	chks []storepb.AggrChunk
}

type bucketSeriesSet struct {
	set []seriesEntry
	i   int
	err error
}

func newBucketSeriesSet(set []seriesEntry) *bucketSeriesSet {
	return &bucketSeriesSet{
		set: set,
		i:   -1,
	}
}

func (s *bucketSeriesSet) Next() bool {
	if s.i >= len(s.set)-1 {
		return false
	}
	s.i++
	return true
}

func (s *bucketSeriesSet) At() ([]storepb.Label, []storepb.AggrChunk) {
	return s.set[s.i].lset, s.set[s.i].chks
}

func (s *bucketSeriesSet) Err() error {
	return s.err
}

func blockSeries(
	ctx context.Context,
	ulid ulid.ULID,
	extLset map[string]string,
	indexr *bucketIndexReader,
	chunkr *bucketChunkReader,
	matchers []labels.Matcher,
	req *storepb.SeriesRequest,
	samplesLimiter *Limiter,
) (storepb.SeriesSet, *queryStats, error) {
	ps, err := indexr.ExpandedPostings(matchers)
	if err != nil {
		return nil, nil, errors.Wrap(err, "expanded matching posting")
	}

	if len(ps) == 0 {
		return storepb.EmptySeriesSet(), indexr.stats, nil
	}

	// Preload all series index data.
	// TODO(bwplotka): Consider not keeping all series in memory all the time.
	// TODO(bwplotka): Do lazy loading in one step as `ExpandingPostings` method.
	if err := indexr.PreloadSeries(ps); err != nil {
		return nil, nil, errors.Wrap(err, "preload series")
	}

	// Transform all series into the response types and mark their relevant chunks
	// for preloading.
	var (
		res  []seriesEntry
		lset labels.Labels
		chks []chunks.Meta
	)
	for _, id := range ps {
		if err := indexr.LoadedSeries(id, &lset, &chks); err != nil {
			return nil, nil, errors.Wrap(err, "read series")
		}
		s := seriesEntry{
			lset: make([]storepb.Label, 0, len(lset)),
			refs: make([]uint64, 0, len(chks)),
			chks: make([]storepb.AggrChunk, 0, len(chks)),
		}
		for _, l := range lset {
			// Skip if the external labels of the block overrule the series' label.
			// NOTE(fabxc): maybe move it to a prefixed version to still ensure uniqueness of series?
			if extLset[l.Name] != "" {
				continue
			}
			s.lset = append(s.lset, storepb.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		for ln, lv := range extLset {
			s.lset = append(s.lset, storepb.Label{
				Name:  ln,
				Value: lv,
			})
		}
		sort.Slice(s.lset, func(i, j int) bool {
			return s.lset[i].Name < s.lset[j].Name
		})

		for _, meta := range chks {
			if meta.MaxTime < req.MinTime {
				continue
			}
			if meta.MinTime > req.MaxTime {
				break
			}

			if err := chunkr.addPreload(meta.Ref); err != nil {
				return nil, nil, errors.Wrap(err, "add chunk preload")
			}
			s.chks = append(s.chks, storepb.AggrChunk{
				MinTime: meta.MinTime,
				MaxTime: meta.MaxTime,
			})
			s.refs = append(s.refs, meta.Ref)
		}
		if len(s.chks) > 0 {
			res = append(res, s)
		}
	}

	// Preload all chunks that were marked in the previous stage.
	if err := chunkr.preload(samplesLimiter); err != nil {
		return nil, nil, errors.Wrap(err, "preload chunks")
	}

	// Transform all chunks into the response format.
	for _, s := range res {
		for i, ref := range s.refs {
			chk, err := chunkr.Chunk(ref)
			if err != nil {
				return nil, nil, errors.Wrap(err, "get chunk")
			}
			if err := populateChunk(&s.chks[i], chk, req.Aggregates); err != nil {
				return nil, nil, errors.Wrap(err, "populate chunk")
			}
		}
	}

	return newBucketSeriesSet(res), indexr.stats.merge(chunkr.stats), nil
}

func populateChunk(out *storepb.AggrChunk, in chunkenc.Chunk, aggrs []storepb.Aggr) error {
	if in.Encoding() == chunkenc.EncXOR {
		out.Raw = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: in.Bytes()}
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
			out.Count = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_SUM:
			x, err := ac.Get(downsample.AggrSum)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrSum)
			}
			out.Sum = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_MIN:
			x, err := ac.Get(downsample.AggrMin)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrMin)
			}
			out.Min = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_MAX:
			x, err := ac.Get(downsample.AggrMax)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrMax)
			}
			out.Max = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		case storepb.Aggr_COUNTER:
			x, err := ac.Get(downsample.AggrCounter)
			if err != nil {
				return errors.Errorf("aggregate %s does not exist", downsample.AggrCounter)
			}
			out.Counter = &storepb.Chunk{Type: storepb.Chunk_XOR, Data: x.Bytes()}
		}
	}
	return nil
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
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) (err error) {
	{
		span, _ := tracing.StartSpan(srv.Context(), "store_query_gate_ismyturn")
		err := s.queryGate.IsMyTurn(srv.Context())
		span.Finish()
		if err != nil {
			return errors.Wrapf(err, "failed to wait for turn")
		}
	}
	defer s.queryGate.Done()

	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	req.MinTime = s.limitMinTime(req.MinTime)
	req.MaxTime = s.limitMaxTime(req.MaxTime)

	var (
		stats = &queryStats{}
		g     run.Group
		res   []storepb.SeriesSet
		mtx   sync.Mutex
	)
	s.mtx.RLock()

	for _, bs := range s.blockSets {
		blockMatchers, ok := bs.labelMatchers(matchers...)
		if !ok {
			continue
		}
		blocks := bs.getFor(req.MinTime, req.MaxTime, req.MaxResolutionWindow)

		if s.debugLogging {
			debugFoundBlockSetOverview(s.logger, req.MinTime, req.MaxTime, req.MaxResolutionWindow, bs.labels, blocks)
		}

		for _, b := range blocks {
			stats.blocksQueried++

			b := b
			ctx, cancel := context.WithCancel(srv.Context())

			// We must keep the readers open until all their data has been sent.
			indexr := b.indexReader(ctx)
			chunkr := b.chunkReader(ctx)

			// Defer all closes to the end of Series method.
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "series block")
			defer runutil.CloseWithLogOnErr(s.logger, chunkr, "series block")

			g.Add(func() error {
				part, pstats, err := blockSeries(ctx,
					b.meta.ULID,
					b.meta.Thanos.Labels,
					indexr,
					chunkr,
					blockMatchers,
					req,
					s.samplesLimiter,
				)
				if err != nil {
					return errors.Wrapf(err, "fetch series for block %s", b.meta.ULID)
				}

				mtx.Lock()
				res = append(res, part)
				stats = stats.merge(pstats)
				mtx.Unlock()

				return nil
			}, func(err error) {
				if err != nil {
					cancel()
				}
			})
		}
	}

	s.mtx.RUnlock()

	defer func() {
		s.metrics.seriesDataTouched.WithLabelValues("postings").Observe(float64(stats.postingsTouched))
		s.metrics.seriesDataFetched.WithLabelValues("postings").Observe(float64(stats.postingsFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("postings").Observe(float64(stats.postingsTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("postings").Observe(float64(stats.postingsFetchedSizeSum))
		s.metrics.seriesDataTouched.WithLabelValues("series").Observe(float64(stats.seriesTouched))
		s.metrics.seriesDataFetched.WithLabelValues("series").Observe(float64(stats.seriesFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("series").Observe(float64(stats.seriesTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("series").Observe(float64(stats.seriesFetchedSizeSum))
		s.metrics.seriesDataTouched.WithLabelValues("chunks").Observe(float64(stats.chunksTouched))
		s.metrics.seriesDataFetched.WithLabelValues("chunks").Observe(float64(stats.chunksFetched))
		s.metrics.seriesDataSizeTouched.WithLabelValues("chunks").Observe(float64(stats.chunksTouchedSizeSum))
		s.metrics.seriesDataSizeFetched.WithLabelValues("chunks").Observe(float64(stats.chunksFetchedSizeSum))
		s.metrics.resultSeriesCount.Observe(float64(stats.mergedSeriesCount))

		level.Debug(s.logger).Log("msg", "stats query processed",
			"stats", fmt.Sprintf("%+v", stats), "err", err)
	}()

	// Concurrently get data from all blocks.
	{
		span, _ := tracing.StartSpan(srv.Context(), "bucket_store_preload_all")
		begin := time.Now()
		err := g.Run()
		span.Finish()

		if err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
		stats.getAllDuration = time.Since(begin)
		s.metrics.seriesGetAllDuration.Observe(stats.getAllDuration.Seconds())
		s.metrics.seriesBlocksQueried.Observe(float64(stats.blocksQueried))
	}
	// Merge the sub-results from each selected block.
	{
		span, _ := tracing.StartSpan(srv.Context(), "bucket_store_merge_all")
		defer span.Finish()

		begin := time.Now()

		// Merge series set into an union of all block sets. This exposes all blocks are single seriesSet.
		// Chunks of returned series might be out of order w.r.t to their time range.
		// This must be accounted for later by clients.
		set := storepb.MergeSeriesSets(res...)
		for set.Next() {
			var series storepb.Series

			series.Labels, series.Chunks = set.At()

			stats.mergedSeriesCount++
			stats.mergedChunksCount += len(series.Chunks)
			s.metrics.chunkSizeBytes.Observe(float64(chunksSize(series.Chunks)))

			if err := srv.Send(storepb.NewSeriesResponse(&series)); err != nil {
				return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
			}
		}
		if set.Err() != nil {
			return status.Error(codes.Unknown, errors.Wrap(set.Err(), "expand series set").Error())
		}
		stats.mergeDuration = time.Since(begin)
		s.metrics.seriesMergeDuration.Observe(stats.mergeDuration.Seconds())
	}
	return nil
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
}

// LabelNames implements the storepb.StoreServer interface.
func (s *BucketStore) LabelNames(ctx context.Context, _ *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	g, gctx := errgroup.WithContext(ctx)

	s.mtx.RLock()

	var mtx sync.Mutex
	var sets [][]string

	for _, b := range s.blocks {
		indexr := b.indexReader(gctx)
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label names")

			res := indexr.LabelNames()
			sort.Strings(res)

			mtx.Lock()
			sets = append(sets, res)
			mtx.Unlock()

			return nil
		})
	}

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &storepb.LabelNamesResponse{
		Names: strutil.MergeSlices(sets...),
	}, nil
}

// LabelValues implements the storepb.StoreServer interface.
func (s *BucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	g, gctx := errgroup.WithContext(ctx)

	s.mtx.RLock()

	var mtx sync.Mutex
	var sets [][]string

	for _, b := range s.blocks {
		indexr := b.indexReader(gctx)
		// TODO(fabxc): only aggregate chunk metas first and add a subsequent fetch stage
		// where we consolidate requests.
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label values")

			res := indexr.LabelValues(req.Label)

			mtx.Lock()
			sets = append(sets, res)
			mtx.Unlock()

			return nil
		})
	}

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &storepb.LabelValuesResponse{
		Values: strutil.MergeSlices(sets...),
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
	if !s.labels.Equals(labels.FromMap(b.meta.Thanos.Labels)) {
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

	sort.Slice(bs, func(j, k int) bool {
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
func (s *bucketBlockSet) getFor(mint, maxt, maxResolutionMillis int64) (bs []*bucketBlock) {
	if mint == maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	// Find first matching resolution.
	i := 0
	for ; i < len(s.resolutions) && s.resolutions[i] > maxResolutionMillis; i++ {
	}

	// Fill the given interval with the blocks for the current resolution.
	// Our current resolution might not cover all data, so recursively fill the gaps with higher resolution blocks if there is any.
	start := mint
	for _, b := range s.blocks[i] {
		if b.meta.MaxTime <= mint {
			continue
		}
		if b.meta.MinTime >= maxt {
			break
		}

		if i+1 < len(s.resolutions) {
			bs = append(bs, s.getFor(start, b.meta.MinTime, s.resolutions[i+1])...)
		}
		bs = append(bs, b)
		start = b.meta.MaxTime
	}

	if i+1 < len(s.resolutions) {
		bs = append(bs, s.getFor(start, maxt, s.resolutions[i+1])...)
	}
	return bs
}

// labelMatchers verifies whether the block set matches the given matchers and returns a new
// set of matchers that is equivalent when querying data within the block.
func (s *bucketBlockSet) labelMatchers(matchers ...labels.Matcher) ([]labels.Matcher, bool) {
	res := make([]labels.Matcher, 0, len(matchers))

	for _, m := range matchers {
		v := s.labels.Get(m.Name())
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
	logger     log.Logger
	bucket     objstore.BucketReader
	meta       *metadata.Meta
	dir        string
	indexCache indexCache
	chunkPool  *pool.BytesPool

	indexVersion int
	symbols      []string
	lvals        map[string][]string
	postings     map[labels.Label]index.Range

	id        ulid.ULID
	chunkObjs []string

	pendingReaders sync.WaitGroup

	partitioner partitioner

	labels labels.Labels
}

func newBucketBlock(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.BucketReader,
	id ulid.ULID,
	dir string,
	indexCache indexCache,
	chunkPool *pool.BytesPool,
	p partitioner,
) (b *bucketBlock, err error) {
	b = &bucketBlock{
		logger:      logger,
		bucket:      bkt,
		id:          id,
		indexCache:  indexCache,
		chunkPool:   chunkPool,
		dir:         dir,
		partitioner: p,
	}
	err, meta := loadMeta(ctx, logger, bkt, dir, id)
	if err != nil {
		return nil, errors.Wrap(err, "load meta")
	}
	b.meta = meta

	if err = b.loadIndexCacheFile(ctx); err != nil {
		return nil, errors.Wrap(err, "load index cache")
	}
	// Get object handles for all chunk files.
	err = bkt.Iter(ctx, path.Join(id.String(), block.ChunksDirname), func(n string) error {
		b.chunkObjs = append(b.chunkObjs, n)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "list chunk files")
	}
	return b, nil
}

func (b *bucketBlock) indexFilename() string {
	return path.Join(b.id.String(), block.IndexFilename)
}

func (b *bucketBlock) indexCacheFilename() string {
	return path.Join(b.id.String(), block.IndexCacheFilename)
}

func loadMeta(ctx context.Context, logger log.Logger, bucket objstore.BucketReader, dir string, id ulid.ULID) (error, *metadata.Meta) {
	// If we haven't seen the block before or it is missing the meta.json, download it.
	if _, err := os.Stat(path.Join(dir, block.MetaFilename)); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return errors.Wrap(err, "create dir"), nil
		}
		src := path.Join(id.String(), block.MetaFilename)

		if err := objstore.DownloadFile(ctx, logger, bucket, src, dir); err != nil {
			return errors.Wrap(err, "download meta.json"), nil
		}
	} else if err != nil {
		return err, nil
	}
	meta, err := metadata.Read(dir)
	if err != nil {
		return errors.Wrap(err, "read meta.json"), nil
	}

	return nil, meta
}

func (b *bucketBlock) loadIndexCacheFile(ctx context.Context) (err error) {
	cachefn := filepath.Join(b.dir, block.IndexCacheFilename)
	if err = b.loadIndexCacheFileFromFile(ctx, cachefn); err == nil {
		return nil
	}
	if !os.IsNotExist(errors.Cause(err)) {
		return errors.Wrap(err, "read index cache")
	}

	// Try to download index cache file from object store.
	if err = objstore.DownloadFile(ctx, b.logger, b.bucket, b.indexCacheFilename(), cachefn); err == nil {
		return b.loadIndexCacheFileFromFile(ctx, cachefn)
	}

	if !b.bucket.IsObjNotFoundErr(errors.Cause(err)) {
		return errors.Wrap(err, "download index cache file")
	}

	// No cache exists on disk yet, build it from the downloaded index and retry.
	fn := filepath.Join(b.dir, block.IndexFilename)

	if err := objstore.DownloadFile(ctx, b.logger, b.bucket, b.indexFilename(), fn); err != nil {
		return errors.Wrap(err, "download index file")
	}

	defer func() {
		if rerr := os.Remove(fn); rerr != nil {
			level.Error(b.logger).Log("msg", "failed to remove temp index file", "path", fn, "err", rerr)
		}
	}()

	if err := block.WriteIndexCache(b.logger, fn, cachefn); err != nil {
		return errors.Wrap(err, "write index cache")
	}

	return errors.Wrap(b.loadIndexCacheFileFromFile(ctx, cachefn), "read index cache")
}

func (b *bucketBlock) loadIndexCacheFileFromFile(ctx context.Context, cache string) (err error) {
	b.indexVersion, b.symbols, b.lvals, b.postings, err = block.ReadIndexCache(b.logger, cache)
	return err
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64) ([]byte, error) {
	r, err := b.bucket.GetRange(ctx, b.indexFilename(), off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readIndexRange close range reader")

	c, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read range")
	}
	return c, nil
}

func (b *bucketBlock) readChunkRange(ctx context.Context, seq int, off, length int64) (*[]byte, error) {
	c, err := b.chunkPool.Get(int(length))
	if err != nil {
		return nil, errors.Wrap(err, "allocate chunk bytes")
	}
	buf := bytes.NewBuffer(*c)

	r, err := b.bucket.GetRange(ctx, b.chunkObjs[seq], off, length)
	if err != nil {
		b.chunkPool.Put(c)
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readChunkRange close range reader")

	if _, err = io.Copy(buf, r); err != nil {
		b.chunkPool.Put(c)
		return nil, errors.Wrap(err, "read range")
	}
	internalBuf := buf.Bytes()
	return &internalBuf, nil
}

func (b *bucketBlock) indexReader(ctx context.Context) *bucketIndexReader {
	b.pendingReaders.Add(1)
	return newBucketIndexReader(ctx, b.logger, b, b.indexCache)
}

func (b *bucketBlock) chunkReader(ctx context.Context) *bucketChunkReader {
	b.pendingReaders.Add(1)
	return newBucketChunkReader(ctx, b)
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *bucketBlock) Close() error {
	b.pendingReaders.Wait()
	return nil
}

// bucketIndexReader is a custom index reader (not conforming index.Reader interface) that gets postings.
type bucketIndexReader struct {
	logger log.Logger
	ctx    context.Context
	block  *bucketBlock
	dec    *index.Decoder
	stats  *queryStats
	cache  indexCache

	mtx          sync.Mutex
	loadedSeries map[uint64][]byte
}

func newBucketIndexReader(ctx context.Context, logger log.Logger, block *bucketBlock, cache indexCache) *bucketIndexReader {
	r := &bucketIndexReader{
		logger:       logger,
		ctx:          ctx,
		block:        block,
		dec:          &index.Decoder{},
		stats:        &queryStats{},
		cache:        cache,
		loadedSeries: map[uint64][]byte{},
	}
	r.dec.LookupSymbol = r.lookupSymbol
	return r
}

func (r *bucketIndexReader) lookupSymbol(o uint32) (string, error) {
	idx := int(o)
	if idx >= len(r.block.symbols) {
		return "", errors.Errorf("bucketIndexReader: unknown symbol offset %d", o)
	}

	return r.block.symbols[idx], nil
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
func (r *bucketIndexReader) ExpandedPostings(ms []labels.Matcher) ([]uint64, error) {
	var postingGroups []*postingGroup

	// NOTE: Derived from tsdb.PostingsForMatchers.
	for _, m := range ms {
		// Each group is separate to tell later what postings are intersecting with what.
		postingGroups = append(postingGroups, toPostingGroup(r.LabelValues, m))
	}

	if len(postingGroups) == 0 {
		return nil, nil
	}

	if err := r.fetchPostings(postingGroups); err != nil {
		return nil, errors.Wrap(err, "get postings")
	}

	var postings []index.Postings
	for _, g := range postingGroups {
		postings = append(postings, g.Postings())
	}

	ps, err := index.ExpandPostings(index.Intersect(postings...))
	if err != nil {
		return nil, errors.Wrap(err, "expand")
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	if r.block.indexVersion >= 2 {
		for i, id := range ps {
			ps[i] = id * 16
		}
	}

	return ps, nil
}

type postingGroup struct {
	keys     labels.Labels
	postings []index.Postings

	aggregate func(postings []index.Postings) index.Postings
}

func newPostingGroup(keys labels.Labels, aggr func(postings []index.Postings) index.Postings) *postingGroup {
	return &postingGroup{
		keys:      keys,
		postings:  make([]index.Postings, len(keys)),
		aggregate: aggr,
	}
}

func (p *postingGroup) Fill(i int, posting index.Postings) {
	p.postings[i] = posting
}

func (p *postingGroup) Postings() index.Postings {
	if len(p.keys) == 0 {
		return index.EmptyPostings()
	}

	for i, posting := range p.postings {
		if posting == nil {
			// This should not happen. Debug for https://github.com/thanos-io/thanos/issues/874.
			return index.ErrPostings(errors.Errorf("at least one of %d postings is nil for %s. It was never fetched.", i, p.keys[i]))
		}
	}

	return p.aggregate(p.postings)
}

func merge(p []index.Postings) index.Postings {
	return index.Merge(p...)
}

func allWithout(p []index.Postings) index.Postings {
	return index.Without(p[0], index.Merge(p[1:]...))
}

// NOTE: Derived from tsdb.postingsForMatcher. index.Merge is equivalent to map duplication.
func toPostingGroup(lvalsFn func(name string) []string, m labels.Matcher) *postingGroup {
	var matchingLabels labels.Labels

	// If the matcher selects an empty value, it selects all the series which don't
	// have the label name set too. See: https://github.com/prometheus/prometheus/issues/3575
	// and https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555.
	if m.Matches("") {
		allName, allValue := index.AllPostingsKey()

		matchingLabels = append(matchingLabels, labels.Label{Name: allName, Value: allValue})
		for _, val := range lvalsFn(m.Name()) {
			if !m.Matches(val) {
				matchingLabels = append(matchingLabels, labels.Label{Name: m.Name(), Value: val})
			}
		}

		if len(matchingLabels) == 1 {
			// This is known hack to return all series.
			// Ask for x != <not existing value>. Allow for that as Prometheus does,
			// even though it is expensive.
			return newPostingGroup(matchingLabels, merge)
		}

		return newPostingGroup(matchingLabels, allWithout)
	}

	// Fast-path for equal matching.
	if em, ok := m.(*labels.EqualMatcher); ok {
		return newPostingGroup(labels.Labels{{Name: em.Name(), Value: em.Value()}}, merge)
	}

	for _, val := range lvalsFn(m.Name()) {
		if m.Matches(val) {
			matchingLabels = append(matchingLabels, labels.Label{Name: m.Name(), Value: val})
		}
	}

	return newPostingGroup(matchingLabels, merge)
}

type postingPtr struct {
	groupID int
	keyID   int
	ptr     index.Range
}

// fetchPostings fill postings requested by posting groups.
func (r *bucketIndexReader) fetchPostings(groups []*postingGroup) error {
	var ptrs []postingPtr

	// Iterate over all groups and fetch posting from cache.
	// If we have a miss, mark key to be fetched in `ptrs` slice.
	// Overlaps are well handled by partitioner, so we don't need to deduplicate keys.
	for i, g := range groups {
		for j, key := range g.keys {
			// Get postings for the given key from cache first.
			if b, ok := r.cache.Postings(r.block.meta.ULID, key); ok {
				r.stats.postingsTouched++
				r.stats.postingsTouchedSizeSum += len(b)

				_, l, err := r.dec.Postings(b)
				if err != nil {
					return errors.Wrap(err, "decode postings")
				}
				g.Fill(j, l)
				continue
			}

			// Cache miss; save pointer for actual posting in index stored in object store.
			ptr, ok := r.block.postings[key]
			if !ok {
				// This block does not have any posting for given key.
				g.Fill(j, index.EmptyPostings())
				continue
			}

			r.stats.postingsToFetch++
			ptrs = append(ptrs, postingPtr{ptr: ptr, groupID: i, keyID: j})
		}
	}

	sort.Slice(ptrs, func(i, j int) bool {
		return ptrs[i].ptr.Start < ptrs[j].ptr.Start
	})

	// TODO(bwplotka): Asses how large in worst case scenario this can be. (e.g fetch for AllPostingsKeys)
	// Consider sub split if too big.
	parts := r.block.partitioner.Partition(len(ptrs), func(i int) (start, end uint64) {
		return uint64(ptrs[i].ptr.Start), uint64(ptrs[i].ptr.End)
	})

	var g run.Group
	for _, part := range parts {
		ctx, cancel := context.WithCancel(r.ctx)
		i, j := part.elemRng[0], part.elemRng[1]

		start := int64(part.start)
		// We assume index does not have any ptrs that has 0 length.
		length := int64(part.end) - start

		// Fetch from object storage concurrently and update stats and posting list.
		g.Add(func() error {
			begin := time.Now()

			b, err := r.block.readIndexRange(ctx, start, length)
			if err != nil {
				return errors.Wrap(err, "read postings range")
			}
			fetchTime := time.Since(begin)

			r.mtx.Lock()
			defer r.mtx.Unlock()

			r.stats.postingsFetchCount++
			r.stats.postingsFetched += j - i
			r.stats.postingsFetchDurationSum += fetchTime
			r.stats.postingsFetchedSizeSum += int(length)

			for _, p := range ptrs[i:j] {
				c := b[p.ptr.Start-start : p.ptr.End-start]

				_, fetchedPostings, err := r.dec.Postings(c)
				if err != nil {
					return errors.Wrap(err, "read postings list")
				}

				// Return postings and fill LRU cache.
				groups[p.groupID].Fill(p.keyID, fetchedPostings)
				r.cache.SetPostings(r.block.meta.ULID, groups[p.groupID].keys[p.keyID], c)

				// If we just fetched it we still have to update the stats for touched postings.
				r.stats.postingsTouched++
				r.stats.postingsTouchedSizeSum += len(c)
			}
			return nil
		}, func(err error) {
			if err != nil {
				cancel()
			}
		})
	}

	return g.Run()
}

func (r *bucketIndexReader) PreloadSeries(ids []uint64) error {
	const maxSeriesSize = 64 * 1024

	var newIDs []uint64

	for _, id := range ids {
		if b, ok := r.cache.Series(r.block.meta.ULID, id); ok {
			r.loadedSeries[id] = b
			continue
		}
		newIDs = append(newIDs, id)
	}
	ids = newIDs

	parts := r.block.partitioner.Partition(len(ids), func(i int) (start, end uint64) {
		return ids[i], ids[i] + maxSeriesSize
	})
	var g run.Group

	for _, p := range parts {
		ctx, cancel := context.WithCancel(r.ctx)
		s, e := p.start, p.end
		i, j := p.elemRng[0], p.elemRng[1]

		g.Add(func() error {
			return r.loadSeries(ctx, ids[i:j], s, e)
		}, func(err error) {
			if err != nil {
				cancel()
			}
		})
	}
	return g.Run()
}

func (r *bucketIndexReader) loadSeries(ctx context.Context, ids []uint64, start, end uint64) error {
	begin := time.Now()

	b, err := r.block.readIndexRange(ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "read series range")
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.stats.seriesFetchCount++
	r.stats.seriesFetched += len(ids)
	r.stats.seriesFetchDurationSum += time.Since(begin)
	r.stats.seriesFetchedSizeSum += int(end - start)

	for _, id := range ids {
		c := b[id-start:]

		l, n := binary.Uvarint(c)
		if n < 1 {
			return errors.New("reading series length failed")
		}
		if len(c) < n+int(l) {
			return errors.Errorf("invalid remaining size %d, expected %d", len(c), n+int(l))
		}
		c = c[n : n+int(l)]
		r.loadedSeries[id] = c
		r.cache.SetSeries(r.block.meta.ULID, id, c)
	}
	return nil
}

type part struct {
	start uint64
	end   uint64

	elemRng [2]int
}

type partitioner interface {
	// Partition partitions length entries into n <= length ranges that cover all
	// input ranges
	// It supports overlapping ranges.
	// NOTE: It expects range to be sorted by start time.
	Partition(length int, rng func(int) (uint64, uint64)) []part
}

type gapBasedPartitioner struct {
	maxGapSize uint64
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []part) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := part{}
		p.start, p.end = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.end+g.maxGapSize < s {
				break
			}

			if p.end <= e {
				p.end = e
			}
		}
		p.elemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}

// LoadedSeries populates the given labels and chunk metas for the series identified
// by the reference.
// Returns ErrNotFound if the ref does not resolve to a known series.
func (r *bucketIndexReader) LoadedSeries(ref uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	b, ok := r.loadedSeries[ref]
	if !ok {
		return errors.Errorf("series %d not found", ref)
	}

	r.stats.seriesTouched++
	r.stats.seriesTouchedSizeSum += len(b)

	return r.dec.Series(b, lset, chks)
}

// LabelValues returns label values for single name.
func (r *bucketIndexReader) LabelValues(name string) []string {
	res := make([]string, 0, len(r.block.lvals[name]))
	return append(res, r.block.lvals[name]...)
}

// LabelNames returns a list of label names.
func (r *bucketIndexReader) LabelNames() []string {
	res := make([]string, 0, len(r.block.lvals))
	for ln := range r.block.lvals {
		res = append(res, ln)
	}
	return res
}

// Close released the underlying resources of the reader.
func (r *bucketIndexReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

type bucketChunkReader struct {
	ctx   context.Context
	block *bucketBlock
	stats *queryStats

	preloads [][]uint32
	mtx      sync.Mutex
	chunks   map[uint64]chunkenc.Chunk

	// Byte slice to return to the chunk pool on close.
	chunkBytes []*[]byte
}

func newBucketChunkReader(ctx context.Context, block *bucketBlock) *bucketChunkReader {
	return &bucketChunkReader{
		ctx:      ctx,
		block:    block,
		stats:    &queryStats{},
		preloads: make([][]uint32, len(block.chunkObjs)),
		chunks:   map[uint64]chunkenc.Chunk{},
	}
}

// addPreload adds the chunk with id to the data set that will be fetched on calling preload.
func (r *bucketChunkReader) addPreload(id uint64) error {
	var (
		seq = int(id >> 32)
		off = uint32(id)
	)
	if seq >= len(r.preloads) {
		return errors.Errorf("reference sequence %d out of range", seq)
	}
	r.preloads[seq] = append(r.preloads[seq], off)
	return nil
}

// preload all added chunk IDs. Must be called before the first call to Chunk is made.
func (r *bucketChunkReader) preload(samplesLimiter *Limiter) error {
	var g run.Group

	numChunks := uint64(0)
	for _, offsets := range r.preloads {
		for range offsets {
			numChunks++
		}
	}
	if err := samplesLimiter.Check(numChunks * maxSamplesPerChunk); err != nil {
		return errors.Wrap(err, "exceeded samples limit")
	}

	for seq, offsets := range r.preloads {
		sort.Slice(offsets, func(i, j int) bool {
			return offsets[i] < offsets[j]
		})
		parts := r.block.partitioner.Partition(len(offsets), func(i int) (start, end uint64) {
			return uint64(offsets[i]), uint64(offsets[i]) + maxChunkSize
		})

		seq := seq
		offsets := offsets

		for _, p := range parts {
			ctx, cancel := context.WithCancel(r.ctx)
			s, e := uint32(p.start), uint32(p.end)
			m, n := p.elemRng[0], p.elemRng[1]

			g.Add(func() error {
				return r.loadChunks(ctx, offsets[m:n], seq, s, e)
			}, func(err error) {
				if err != nil {
					cancel()
				}
			})
		}
	}
	return g.Run()
}

func (r *bucketChunkReader) loadChunks(ctx context.Context, offs []uint32, seq int, start, end uint32) error {
	begin := time.Now()

	b, err := r.block.readChunkRange(ctx, seq, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrapf(err, "read range for %d", seq)
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.chunkBytes = append(r.chunkBytes, b)
	r.stats.chunksFetchCount++
	r.stats.chunksFetched += len(offs)
	r.stats.chunksFetchDurationSum += time.Since(begin)
	r.stats.chunksFetchedSizeSum += int(end - start)

	for _, o := range offs {
		cb := (*b)[o-start:]

		l, n := binary.Uvarint(cb)
		if n < 1 {
			return errors.Errorf("reading chunk length failed")
		}
		if len(cb) < n+int(l)+1 {
			return errors.Errorf("preloaded chunk too small, expecting %d", n+int(l)+1)
		}
		cid := uint64(seq<<32) | uint64(o)
		r.chunks[cid] = rawChunk(cb[n : n+int(l)+1])
	}
	return nil
}

func (r *bucketChunkReader) Chunk(id uint64) (chunkenc.Chunk, error) {
	c, ok := r.chunks[id]
	if !ok {
		return nil, errors.Errorf("chunk with ID %d not found", id)
	}

	r.stats.chunksTouched++
	r.stats.chunksTouchedSizeSum += len(c.Bytes())

	return c, nil
}

// rawChunk is a helper type that wraps a chunk's raw bytes and implements the chunkenc.Chunk
// interface over it.
// It is used to Store API responses which don't need to introspect and validate the chunk's contents.
type rawChunk []byte

func (b rawChunk) Encoding() chunkenc.Encoding {
	return chunkenc.Encoding(b[0])
}

func (b rawChunk) Bytes() []byte {
	return b[1:]
}

func (b rawChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	panic("invalid call")
}

func (b rawChunk) Appender() (chunkenc.Appender, error) {
	panic("invalid call")
}

func (b rawChunk) NumSamples() int {
	panic("invalid call")
}

func (r *bucketChunkReader) Close() error {
	r.block.pendingReaders.Done()

	for _, b := range r.chunkBytes {
		r.block.chunkPool.Put(b)
	}
	return nil
}

type queryStats struct {
	blocksQueried int

	postingsTouched          int
	postingsTouchedSizeSum   int
	postingsToFetch          int
	postingsFetched          int
	postingsFetchedSizeSum   int
	postingsFetchCount       int
	postingsFetchDurationSum time.Duration

	seriesTouched          int
	seriesTouchedSizeSum   int
	seriesFetched          int
	seriesFetchedSizeSum   int
	seriesFetchCount       int
	seriesFetchDurationSum time.Duration

	chunksTouched          int
	chunksTouchedSizeSum   int
	chunksFetched          int
	chunksFetchedSizeSum   int
	chunksFetchCount       int
	chunksFetchDurationSum time.Duration

	getAllDuration    time.Duration
	mergedSeriesCount int
	mergedChunksCount int
	mergeDuration     time.Duration
}

func (s queryStats) merge(o *queryStats) *queryStats {
	s.blocksQueried += o.blocksQueried

	s.postingsTouched += o.postingsTouched
	s.postingsTouchedSizeSum += o.postingsTouchedSizeSum
	s.postingsFetched += o.postingsFetched
	s.postingsFetchedSizeSum += o.postingsFetchedSizeSum
	s.postingsFetchCount += o.postingsFetchCount
	s.postingsFetchDurationSum += o.postingsFetchDurationSum

	s.seriesTouched += o.seriesTouched
	s.seriesTouchedSizeSum += o.seriesTouchedSizeSum
	s.seriesFetched += o.seriesFetched
	s.seriesFetchedSizeSum += o.seriesFetchedSizeSum
	s.seriesFetchCount += o.seriesFetchCount
	s.seriesFetchDurationSum += o.seriesFetchDurationSum

	s.chunksTouched += o.chunksTouched
	s.chunksTouchedSizeSum += o.chunksTouchedSizeSum
	s.chunksFetched += o.chunksFetched
	s.chunksFetchedSizeSum += o.chunksFetchedSizeSum
	s.chunksFetchCount += o.chunksFetchCount
	s.chunksFetchDurationSum += o.chunksFetchDurationSum

	s.getAllDuration += o.getAllDuration
	s.mergedSeriesCount += o.mergedSeriesCount
	s.mergedChunksCount += o.mergedChunksCount
	s.mergeDuration += o.mergeDuration

	return &s
}
