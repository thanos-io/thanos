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
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/pool"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/strutil"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		)
	}
	return &m
}

// BucketStore implements the store API backed by a bucket. It loads all index
// files to local disk.
type BucketStore struct {
	logger     log.Logger
	metrics    *bucketStoreMetrics
	bucket     objstore.BucketReader
	dir        string
	indexCache *indexCache
	chunkPool  *pool.BytesPool

	// Sets of blocks that have the same labels. They are indexed by a hash over their label set.
	mtx       sync.RWMutex
	blocks    map[ulid.ULID]*bucketBlock
	blockSets map[uint64]*bucketBlockSet

	// Verbose enabled additional logging.
	debugLogging bool
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	logger log.Logger,
	reg prometheus.Registerer,
	bucket objstore.BucketReader,
	dir string,
	indexCacheSizeBytes uint64,
	maxChunkPoolBytes uint64,
	debugLogging bool,
) (*BucketStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	indexCache, err := newIndexCache(reg, indexCacheSizeBytes)
	if err != nil {
		return nil, errors.Wrap(err, "create index cache")
	}
	chunkPool, err := pool.NewBytesPool(2e5, 50e6, 2, maxChunkPoolBytes)
	if err != nil {
		return nil, errors.Wrap(err, "create chunk pool")
	}
	s := &BucketStore{
		logger:       logger,
		bucket:       bucket,
		dir:          dir,
		indexCache:   indexCache,
		chunkPool:    chunkPool,
		blocks:       map[ulid.ULID]*bucketBlock{},
		blockSets:    map[uint64]*bucketBlockSet{},
		debugLogging: debugLogging,
	}
	s.metrics = newBucketStoreMetrics(reg)

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}

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

	for i := 0; i < 20; i++ {
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
	)
	if err != nil {
		return errors.Wrap(err, "new bucket block")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := labels.FromMap(b.meta.Thanos.Labels)
	h := lset.Hash()

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
	return mint, maxt
}

// Info implements the storepb.StoreServer interface.
func (s *BucketStore) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	mint, maxt := s.TimeRange()
	// Store nodes hold global data and thus have no labels.
	return &storepb.InfoResponse{
		MinTime: mint,
		MaxTime: maxt,
	}, nil
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

func (s *BucketStore) blockSeries(
	ctx context.Context,
	ulid ulid.ULID,
	extLset map[string]string,
	indexr *bucketIndexReader,
	chunkr *bucketChunkReader,
	matchers []labels.Matcher,
	req *storepb.SeriesRequest,
) (storepb.SeriesSet, *queryStats, error) {
	stats := &queryStats{}

	// The postings to preload are registered within the call to PostingsForMatchers,
	// when it invokes indexr.Postings for each underlying postings list.
	// They are ready to use ONLY after preloadPostings was called successfully.
	lazyPostings, err := tsdb.PostingsForMatchers(indexr, matchers...)
	if err != nil {
		return nil, stats, errors.Wrap(err, "get postings for matchers")
	}
	// If the tree was reduced to the empty postings list, don't preload the registered
	// leaf postings and return early with an empty result.
	if lazyPostings == index.EmptyPostings() {
		return storepb.EmptySeriesSet(), stats, nil
	}

	if err := indexr.preloadPostings(); err != nil {
		return nil, stats, errors.Wrap(err, "preload postings")
	}

	// Get result postings list by resolving the postings tree.
	// TODO(bwplotka): Users are seeing panics here, because of lazyPosting being not loaded by preloadPostings.
	ps, err := index.ExpandPostings(lazyPostings)
	if err != nil {
		return nil, stats, errors.Wrap(err, "expand postings")
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	// We do it right at the beginning as it's easier than doing it more fine-grained
	// at the loading level.
	if indexr.block.indexVersion >= 2 {
		for i, id := range ps {
			ps[i] = id * 16
		}
	}

	// Preload all series index data
	if err := indexr.preloadSeries(ps); err != nil {
		return nil, stats, errors.Wrap(err, "preload series")
	}

	// Transform all series into the response types and mark their relevant chunks
	// for preloading.
	var (
		res  []seriesEntry
		lset labels.Labels
		chks []chunks.Meta
	)
	for _, id := range ps {
		if err := indexr.Series(id, &lset, &chks); err != nil {
			return nil, stats, errors.Wrap(err, "read series")
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
				return nil, stats, errors.Wrap(err, "add chunk preload")
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
	if err := chunkr.preload(); err != nil {
		return nil, stats, errors.Wrap(err, "preload chunks")
	}

	// Transform all chunks into the response format.
	for _, s := range res {
		for i, ref := range s.refs {
			chk, err := chunkr.Chunk(ref)
			if err != nil {
				return nil, stats, errors.Wrap(err, "get chunk")
			}
			if err := populateChunk(&s.chks[i], chk, req.Aggregates); err != nil {
				return nil, stats, errors.Wrap(err, "populate chunk")
			}
		}
	}

	stats = stats.merge(indexr.stats)
	stats = stats.merge(chunkr.stats)

	return newBucketSeriesSet(res), stats, nil
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
func debugFoundBlockSetOverview(logger log.Logger, mint, maxt int64, lset labels.Labels, bs []*bucketBlock) {
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

	level.Debug(logger).Log("msg", "Blocks source resolutions", "blocks", len(bs), "mint", mint, "maxt", maxt, "lset", lset.String(), "spans", strings.Join(parts, "\n"))
}

// Series implements the storepb.StoreServer interface.
// TODO(bwplotka): It buffers all chunks in memory and only then streams to client.
// 1. Either count chunk sizes and error out too big query.
// 2. Stream posting -> series -> chunk all together.
func (s *BucketStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
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
			debugFoundBlockSetOverview(s.logger, req.MinTime, req.MaxTime, bs.labels, blocks)
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
				part, pstats, err := s.blockSeries(ctx,
					b.meta.ULID,
					b.meta.Thanos.Labels,
					indexr,
					chunkr,
					blockMatchers,
					req,
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

	level.Debug(s.logger).Log("msg", "series query processed",
		"stats", fmt.Sprintf("%+v", stats))

	return nil
}

func chunksSize(chks []storepb.AggrChunk) (size int) {
	for _, chk := range chks {
		size += chk.Size() // This gets the encoded proto size.
	}
	return size
}

// LabelNames implements the storepb.StoreServer interface.
func (s *BucketStore) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues implements the storepb.StoreServer interface.
func (s *BucketStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	var g errgroup.Group

	s.mtx.RLock()

	var mtx sync.Mutex
	var sets [][]string

	for _, b := range s.blocks {
		indexr := b.indexReader(ctx)
		// TODO(fabxc): only aggregate chunk metas first and add a subsequent fetch stage
		// where we consolidate requests.
		g.Go(func() error {
			defer runutil.CloseWithLogOnErr(s.logger, indexr, "label values")

			tpls, err := indexr.LabelValues(req.Label)
			if err != nil {
				return errors.Wrap(err, "lookup label values")
			}
			res := make([]string, 0, tpls.Len())

			for i := 0; i < tpls.Len(); i++ {
				e, err := tpls.At(i)
				if err != nil {
					return errors.Wrap(err, "get string tuple entry")
				}
				res = append(res, e[0])
			}

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
// them up by downsampling resolution and allows querying
type bucketBlockSet struct {
	labels      labels.Labels
	mtx         sync.RWMutex
	resolutions []int64          // available resolution, high to low
	blocks      [][]*bucketBlock // ordered buckets for the existing resolutions
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
		if bs[j].meta.MinTime < bs[k].meta.MinTime {
			return true
		}
		return false
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
// Blocks with the lowest resolution possible but not lower than the given resolution are returned.
func (s *bucketBlockSet) getFor(mint, maxt, minResolution int64) (bs []*bucketBlock) {
	if mint == maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	// Find first matching resolution.
	i := 0
	for ; i < len(s.resolutions) && s.resolutions[i] > minResolution; i++ {
	}

	// Base case, we fill the given interval with the closest resolution.
	for _, b := range s.blocks[i] {
		if b.meta.MaxTime <= mint {
			continue
		}
		if b.meta.MinTime >= maxt {
			break
		}
		bs = append(bs, b)
	}
	// Our current resolution might not cover all data, recursively fill the gaps at the start
	// and end of [mint, maxt] with higher resolution blocks.
	i++
	// No higher resolution left, we are done.
	if i >= len(s.resolutions) {
		return bs
	}
	if len(bs) == 0 {
		return s.getFor(mint, maxt, s.resolutions[i])
	}
	left := s.getFor(mint, bs[0].meta.MinTime, s.resolutions[i])
	right := s.getFor(bs[len(bs)-1].meta.MaxTime, maxt, s.resolutions[i])

	return append(left, append(bs, right...)...)
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
	meta       *block.Meta
	dir        string
	indexCache *indexCache
	chunkPool  *pool.BytesPool

	indexVersion int
	symbols      map[uint32]string
	lvals        map[string][]string
	postings     map[labels.Label]index.Range

	indexObj  string
	chunkObjs []string

	pendingReaders sync.WaitGroup
}

func newBucketBlock(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.BucketReader,
	id ulid.ULID,
	dir string,
	indexCache *indexCache,
	chunkPool *pool.BytesPool,
) (b *bucketBlock, err error) {
	b = &bucketBlock{
		logger:     logger,
		bucket:     bkt,
		indexObj:   path.Join(id.String(), block.IndexFilename),
		indexCache: indexCache,
		chunkPool:  chunkPool,
		dir:        dir,
	}
	if err = b.loadMeta(ctx, id); err != nil {
		return nil, errors.Wrap(err, "load meta")
	}
	if err = b.loadIndexCache(ctx); err != nil {
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

func (b *bucketBlock) loadMeta(ctx context.Context, id ulid.ULID) error {
	// If we haven't seen the block before download the meta.json file.
	if _, err := os.Stat(b.dir); os.IsNotExist(err) {
		if err := os.MkdirAll(b.dir, 0777); err != nil {
			return errors.Wrap(err, "create dir")
		}
		src := path.Join(id.String(), block.MetaFilename)

		if err := objstore.DownloadFile(ctx, b.logger, b.bucket, src, b.dir); err != nil {
			return errors.Wrap(err, "download meta.json")
		}
	} else if err != nil {
		return err
	}
	meta, err := block.ReadMetaFile(b.dir)
	if err != nil {
		return errors.Wrap(err, "read meta.json")
	}
	b.meta = meta
	return nil
}

func (b *bucketBlock) loadIndexCache(ctx context.Context) (err error) {
	cachefn := filepath.Join(b.dir, block.IndexCacheFilename)

	b.indexVersion, b.symbols, b.lvals, b.postings, err = block.ReadIndexCache(b.logger, cachefn)
	if err == nil {
		return nil
	}
	if !os.IsNotExist(errors.Cause(err)) {
		return errors.Wrap(err, "read index cache")
	}
	// No cache exists is on disk yet, build it from the downloaded index and retry.
	fn := filepath.Join(b.dir, block.IndexFilename)

	if err := objstore.DownloadFile(ctx, b.logger, b.bucket, b.indexObj, fn); err != nil {
		return errors.Wrap(err, "download index file")
	}
	defer func() {
		if rerr := os.Remove(fn); rerr != nil {
			level.Error(b.logger).Log("msg", "failed to remove temp index file", "path", fn, "err", rerr)
		}
	}()

	indexr, err := index.NewFileReader(fn)
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, indexr, "load index cache reader")

	if err := block.WriteIndexCache(b.logger, cachefn, indexr); err != nil {
		return errors.Wrap(err, "write index cache")
	}

	b.indexVersion, b.symbols, b.lvals, b.postings, err = block.ReadIndexCache(b.logger, cachefn)
	if err != nil {
		return errors.Wrap(err, "read index cache")
	}
	return nil
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64) ([]byte, error) {
	r, err := b.bucket.GetRange(ctx, b.indexObj, off, length)
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

func (b *bucketBlock) readChunkRange(ctx context.Context, seq int, off, length int64) ([]byte, error) {
	c, err := b.chunkPool.Get(int(length))
	if err != nil {
		return nil, errors.Wrap(err, "allocate chunk bytes")
	}
	buf := bytes.NewBuffer(c)

	r, err := b.bucket.GetRange(ctx, b.chunkObjs[seq], off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readChunkRange close range reader")

	if _, err = io.Copy(buf, r); err != nil {
		return nil, errors.Wrap(err, "read range")
	}
	return buf.Bytes(), nil
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

type bucketIndexReader struct {
	logger log.Logger
	ctx    context.Context
	block  *bucketBlock
	dec    *index.Decoder
	stats  *queryStats
	cache  *indexCache

	mtx            sync.Mutex
	loadedPostings []*lazyPostings
	loadedSeries   map[uint64][]byte
}

func newBucketIndexReader(ctx context.Context, logger log.Logger, block *bucketBlock, cache *indexCache) *bucketIndexReader {
	r := &bucketIndexReader{
		logger:       logger,
		ctx:          ctx,
		block:        block,
		dec:          &index.Decoder{},
		stats:        &queryStats{},
		cache:        cache,
		loadedSeries: map[uint64][]byte{},
	}
	r.dec.SetSymbolTable(r.block.symbols)
	return r
}

func (r *bucketIndexReader) preloadPostings() error {
	const maxGapSize = 512 * 1024

	ps := r.loadedPostings

	sort.Slice(ps, func(i, j int) bool {
		return ps[i].ptr.Start < ps[j].ptr.Start
	})
	parts := partitionRanges(len(ps), func(i int) (start, end uint64) {
		return uint64(ps[i].ptr.Start), uint64(ps[i].ptr.End)
	}, maxGapSize)
	var g run.Group

	for _, p := range parts {
		ctx, cancel := context.WithCancel(r.ctx)
		i, j := p[0], p[1]

		g.Add(func() error {
			return r.loadPostings(ctx, ps[i:j], ps[i].ptr.Start, ps[j-1].ptr.End)
		}, func(err error) {
			if err != nil {
				cancel()
			}
		})
	}

	if err := g.Run(); err != nil {
		return err
	}

	// TODO(bwplotka): Users are seeing lazyPostings not fully loaded. Double checking it here and printing more info
	// on failure case.
	for _, postings := range ps {
		if postings.Postings != nil {
			continue
		}

		msg := fmt.Sprintf("found parts: %v bases on:", parts)
		for _, p := range ps {
			msg += fmt.Sprintf(" [start: %v; end: %v]", p.ptr.Start, p.ptr.End)
		}

		return errors.Errorf("expected all postings to be loaded but spotted malformed one with key: %v; start: %v; end: %v. Additional info: %s",
			postings.key, postings.ptr.Start, postings.ptr.End, msg)
	}

	return nil
}

// loadPostings loads given postings using given start + length. It is expected to have given postings data within given range.
func (r *bucketIndexReader) loadPostings(ctx context.Context, postings []*lazyPostings, start, end int64) error {
	begin := time.Now()

	b, err := r.block.readIndexRange(ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "read postings range")
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.stats.postingsFetchCount++
	r.stats.postingsFetched += len(postings)
	r.stats.postingsFetchDurationSum += time.Since(begin)
	r.stats.postingsFetchedSizeSum += int(end - start)

	for _, p := range postings {
		c := b[p.ptr.Start-start : p.ptr.End-start]

		_, l, err := r.dec.Postings(c)
		if err != nil {
			return errors.Wrap(err, "read postings list")
		}
		p.set(l)
		r.cache.setPostings(r.block.meta.ULID, p.key, c)
		// If we just fetched it we still have to update the stats for touched postings.
		r.stats.postingsTouched++
		r.stats.postingsTouchedSizeSum += len(c)
	}
	return nil
}

func (r *bucketIndexReader) preloadSeries(ids []uint64) error {
	const maxSeriesSize = 64 * 1024
	const maxGapSize = 512 * 1024

	var newIDs []uint64

	for _, id := range ids {
		if b, ok := r.cache.series(r.block.meta.ULID, id); ok {
			r.loadedSeries[id] = b
			continue
		}
		newIDs = append(newIDs, id)
	}
	ids = newIDs

	parts := partitionRanges(len(ids), func(i int) (start, end uint64) {
		return ids[i], ids[i] + maxSeriesSize
	}, maxGapSize)
	var g run.Group

	for _, p := range parts {
		ctx, cancel := context.WithCancel(r.ctx)
		i, j := p[0], p[1]

		g.Add(func() error {
			return r.loadSeries(ctx, ids[i:j], ids[i], ids[j-1]+maxSeriesSize)
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
		r.cache.setSeries(r.block.meta.ULID, id, c)
	}
	return nil
}

// partitionRanges partitions length entries into n <= length ranges that cover all
// input ranges.
// It combines entries that are separated by reasonably small gaps.
func partitionRanges(length int, rng func(int) (uint64, uint64), maxGapSize uint64) (parts [][2]int) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		_, end := rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if end+maxGapSize < s {
				break
			}
			end = e
		}
		parts = append(parts, [2]int{j, k})
	}
	return parts
}

func (r *bucketIndexReader) Symbols() (map[string]struct{}, error) {
	return nil, errors.New("not implemented")
}

// LabelValues returns the possible label values.
func (r *bucketIndexReader) LabelValues(names ...string) (index.StringTuples, error) {
	if len(names) != 1 {
		return nil, errors.New("label value lookups only supported for single name")
	}
	return index.NewStringTuples(r.block.lvals[names[0]], 1)
}

type lazyPostings struct {
	index.Postings
	key labels.Label
	ptr index.Range
}

func (p *lazyPostings) set(v index.Postings) {
	p.Postings = v
}

// Postings returns the postings list iterator for the label pair.
// The Postings here contain the offsets to the series inside the index.
// Found IDs are not strictly required to point to a valid Series, e.g. during
// background garbage collections.
func (r *bucketIndexReader) Postings(name, value string) (index.Postings, error) {
	l := labels.Label{Name: name, Value: value}
	ptr, ok := r.block.postings[l]
	if !ok {
		return index.EmptyPostings(), nil
	}
	if b, ok := r.cache.postings(r.block.meta.ULID, l); ok {
		r.stats.postingsTouched++
		r.stats.postingsTouchedSizeSum += len(b)

		_, p, err := r.dec.Postings(b)
		if err != nil {
			return nil, errors.Wrap(err, "decode postings")
		}
		return p, nil
	}
	// The stats for touched postings are updated as they are loaded.
	p := &lazyPostings{key: l, ptr: ptr}
	r.loadedPostings = append(r.loadedPostings, p)
	return p, nil
}

// SortedPostings returns a postings list that is reordered to be sorted
// by the label set of the underlying series.
func (r *bucketIndexReader) SortedPostings(p index.Postings) index.Postings {
	return p
}

// Series populates the given labels and chunk metas for the series identified
// by the reference.
// Returns ErrNotFound if the ref does not resolve to a known series.
func (r *bucketIndexReader) Series(ref uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	b, ok := r.loadedSeries[ref]
	if !ok {
		return errors.Errorf("series %d not found", ref)
	}

	r.stats.seriesTouched++
	r.stats.seriesTouchedSizeSum += len(b)

	return r.dec.Series(b, lset, chks)
}

// LabelIndices returns the label pairs for which indices exist.
func (r *bucketIndexReader) LabelIndices() ([][]string, error) {
	return nil, errors.New("not implemented")
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
	chunkBytes [][]byte
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
func (r *bucketChunkReader) preload() error {
	const maxChunkSize = 16000
	const maxGapSize = 512 * 1024

	var g run.Group

	for seq, offsets := range r.preloads {
		sort.Slice(offsets, func(i, j int) bool {
			return offsets[i] < offsets[j]
		})
		parts := partitionRanges(len(offsets), func(i int) (start, end uint64) {
			return uint64(offsets[i]), uint64(offsets[i]) + maxChunkSize
		}, maxGapSize)

		seq := seq
		offsets := offsets

		for _, p := range parts {
			ctx, cancel := context.WithCancel(r.ctx)
			m, n := p[0], p[1]

			g.Add(func() error {
				return r.loadChunks(ctx, offsets[m:n], seq, offsets[m], offsets[n-1]+maxChunkSize)
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
	r.chunkBytes = append(r.chunkBytes, b)

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.stats.chunksFetchCount++
	r.stats.chunksFetched += len(offs)
	r.stats.chunksFetchDurationSum += time.Since(begin)
	r.stats.chunksFetchedSizeSum += int(end - start)

	for _, o := range offs {
		cb := b[o-start:]

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

func (b rawChunk) Iterator() chunkenc.Iterator {
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
