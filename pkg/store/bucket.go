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
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/pool"
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

// BucketStore implements the store API backed by a Bucket bucket. It loads all index
// files to local disk.
type BucketStore struct {
	logger     log.Logger
	metrics    *bucketStoreMetrics
	bucket     objstore.BucketReader
	dir        string
	indexCache *indexCache
	chunkPool  *pool.BytesPool

	mtx    sync.RWMutex
	blocks map[ulid.ULID]*bucketBlock
}

type bucketStoreMetrics struct {
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

func newBucketStoreMetrics(reg prometheus.Registerer, s *BucketStore) *bucketStoreMetrics {
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
	blocksLoaded := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_bucket_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	}, func() float64 {
		return float64(s.numBlocks())
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
		Name: "thanos_bucket_store_series_data_size_touched",
		Help: "Size of all items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataSizeFetched = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "thanos_bucket_store_series_data_size_fetched",
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
			blocksLoaded,
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

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	logger log.Logger,
	reg prometheus.Registerer,
	bucket objstore.BucketReader,
	dir string,
	indexCacheSizeBytes uint64,
	maxChunkPoolBytes uint64,
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
		logger:     logger,
		bucket:     bucket,
		dir:        dir,
		indexCache: indexCache,
		chunkPool:  chunkPool,
		blocks:     map[ulid.ULID]*bucketBlock{},
	}
	s.metrics = newBucketStoreMetrics(reg, s)

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}
	fns, err := fileutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read dir")
	}
	for _, dn := range fns {
		id, err := ulid.Parse(dn)
		if err != nil {
			continue
		}
		blockDir := filepath.Join(dir, dn)

		b, err := newBucketBlock(
			context.TODO(),
			log.With(logger, "block", id),
			bucket,
			id,
			blockDir,
			s.indexCache,
			s.chunkPool,
		)
		if err != nil {
			level.Warn(s.logger).Log("msg", "loading block failed", "id", id, "err", err)
			// Wipe the directory so we can cleanly try again later.
			os.RemoveAll(blockDir)
			continue
		}
		s.setBlock(id, b)
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
func (s *BucketStore) SyncBlocks(ctx context.Context) error {
	var wg sync.WaitGroup
	blockc := make(chan ulid.ULID)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for id := range blockc {
				dir := filepath.Join(s.dir, id.String())

				b, err := newBucketBlock(ctx, s.logger, s.bucket, id, dir, s.indexCache, s.chunkPool)
				if err != nil {
					level.Warn(s.logger).Log("msg", "loading block failed", "id", id, "err", err)
					// Wipe the directory so we can cleanly try again later.
					os.RemoveAll(dir)
					s.metrics.blockLoadFailures.Inc()
					continue
				}
				s.metrics.blockLoads.Inc()
				s.setBlock(id, b)
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
		return err
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

func (s *BucketStore) setBlock(id ulid.ULID, b *bucketBlock) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.blocks[id] = b
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

func (s *BucketStore) removeBlock(id ulid.ULID) error {
	s.mtx.Lock()
	b, ok := s.blocks[id]
	delete(s.blocks, id)
	s.mtx.Unlock()

	if !ok {
		return nil
	}
	if err := b.Close(); err != nil {
		return errors.Wrap(err, "close block")
	}
	return os.RemoveAll(b.dir)
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
	chks []chunks.Meta
}
type bucketSeriesSet struct {
	set  []seriesEntry
	i    int
	chks []storepb.AggrChunk
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
	s.chks = make([]storepb.AggrChunk, 0, len(s.set[s.i].chks))

	// TODO(bplotka): If spotted troubles with gRPC overhead, split chunks to max 4MB chunks if needed. Currently
	// we have huge limit for message size ~2GB.
	for _, c := range s.set[s.i].chks {
		s.chks = append(s.chks, storepb.AggrChunk{
			MinTime: c.MinTime,
			MaxTime: c.MaxTime,
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Chunk.Bytes()},
		})
	}
	return true
}

func (s *bucketSeriesSet) At() ([]storepb.Label, []storepb.AggrChunk) {
	return s.set[s.i].lset, s.chks
}

func (s *bucketSeriesSet) Err() error {
	return nil
}

func (s *BucketStore) blockSeries(
	ctx context.Context,
	extLset map[string]string,
	indexr *bucketIndexReader,
	chunkr *bucketChunkReader,
	matchers []labels.Matcher,
	mint, maxt int64,
) (storepb.SeriesSet, *queryStats, error) {
	stats := &queryStats{}

	// The postings to preload are registered within the call to PostingsForMatchers,
	// when it invokes indexr.Postings for each underlying postings list.
	// They are ready to use ONLY after preloadPostings was called successfully.
	lazyPostings, err := tsdb.PostingsForMatchers(indexr, matchers...)
	if err != nil {
		return nil, stats, err
	}
	// If the tree was reduced to the empty postings list, don't preload the registered
	// leaf postings and return early with an empty result.
	if lazyPostings == index.EmptyPostings() {
		return storepb.EmptySeriesSet(), stats, nil
	}
	if err := indexr.preloadPostings(); err != nil {
		return nil, stats, err
	}
	// Get result postings list by resolving the postings tree.
	ps, err := index.ExpandPostings(lazyPostings)
	if err != nil {
		return nil, stats, err
	}

	// Preload all series index data
	if err := indexr.preloadSeries(ps); err != nil {
		return nil, stats, err
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
			return nil, stats, err
		}
		s := seriesEntry{
			lset: make([]storepb.Label, 0, len(lset)),
			chks: make([]chunks.Meta, 0, len(chks)),
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
			if meta.MaxTime < mint {
				continue
			}
			if meta.MinTime > maxt {
				break
			}
			if err := chunkr.addPreload(meta.Ref); err != nil {
				return nil, stats, errors.Wrap(err, "add chunk preload")
			}
			s.chks = append(s.chks, meta)
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
		for i := range s.chks {
			chk, err := chunkr.Chunk(s.chks[i].Ref)
			if err != nil {
				return nil, stats, errors.Wrap(err, "get chunk")
			}
			s.chks[i].Chunk = chk
		}
	}

	stats = stats.merge(indexr.stats)
	stats = stats.merge(chunkr.stats)

	return newBucketSeriesSet(res), stats, nil
}

// Series implements the storepb.StoreServer interface.
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

	// Select blocks relevant for the query and prepare getters for their data.
	for _, b := range s.blocks {
		// NOTE(fabxc): we skip all downsampled blocks for now until support for the rest of the
		// chain is implemented.
		if b.meta.Thanos.DownsamplingWindow > 0 {
			continue
		}
		blockMatchers, ok := b.blockMatchers(req.MinTime, req.MaxTime, matchers...)
		if !ok {
			continue
		}
		stats.blocksQueried++

		b := b
		ctx, cancel := context.WithCancel(srv.Context())

		// We must keep the readers open until all their data has been sent.
		indexr := b.indexReader(ctx)
		chunkr := b.chunkReader(ctx)
		defer indexr.Close()
		defer chunkr.Close()

		g.Add(func() error {
			part, pstats, err := s.blockSeries(ctx,
				b.meta.Thanos.Labels,
				indexr,
				chunkr,
				blockMatchers,
				req.MinTime, req.MaxTime,
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
		var series storepb.Series

		// Merge series set into an union of all block sets. This exposes all blocks are single seriesSet.
		// Returned set is can be out of order in terms of series time ranges. It is fixed later on, inside querier.
		set := storepb.MergeSeriesSets(res...)
		for set.Next() {
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
			defer indexr.Close()

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

// bucketBlock represents a block that is located in a bucket. It holds intermediate
// state for the block on local disk.
type bucketBlock struct {
	logger     log.Logger
	bucket     objstore.BucketReader
	meta       *block.Meta
	dir        string
	indexCache *indexCache
	chunkPool  *pool.BytesPool

	symbols  map[uint32]string
	lvals    map[string][]string
	postings map[labels.Label]index.Range

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
) (*bucketBlock, error) {
	b := &bucketBlock{
		logger:     logger,
		bucket:     bkt,
		indexObj:   path.Join(id.String(), "index"),
		indexCache: indexCache,
		chunkPool:  chunkPool,
	}
	if err := b.loadMeta(ctx, id, dir); err != nil {
		return nil, errors.Wrap(err, "load meta")
	}
	if err := b.loadIndexCache(ctx, dir); err != nil {
		return nil, errors.Wrap(err, "load index cache")
	}
	// Get object handles for all chunk files.
	err := bkt.Iter(ctx, path.Join(id.String(), "chunks"), func(n string) error {
		b.chunkObjs = append(b.chunkObjs, n)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "list chunk files")
	}
	return b, nil
}

func (b *bucketBlock) loadMeta(ctx context.Context, id ulid.ULID, dir string) error {
	// If we haven't seen the block before download the meta.json file.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return errors.Wrap(err, "create dir")
		}
		src := path.Join(id.String(), "meta.json")

		if err := objstore.DownloadFile(ctx, b.bucket, src, dir); err != nil {
			return errors.Wrap(err, "download meta.json")
		}
	} else if err != nil {
		return err
	}
	meta, err := block.ReadMetaFile(dir)
	if err != nil {
		return errors.Wrap(err, "read meta.json")
	}
	b.meta = meta
	return nil
}

func (b *bucketBlock) loadIndexCache(ctx context.Context, dir string) (err error) {
	cachefn := filepath.Join(dir, block.IndexCacheFilename)

	b.symbols, b.lvals, b.postings, err = block.ReadIndexCache(cachefn)
	if err == nil {
		return nil
	}
	if !os.IsNotExist(errors.Cause(err)) {
		return errors.Wrap(err, "read index cache")
	}
	// No cache exists is on disk yet, build it from a the downloaded index and retry.
	fn := filepath.Join(dir, "index")

	if err := objstore.DownloadFile(ctx, b.bucket, b.indexObj, fn); err != nil {
		return errors.Wrap(err, "download index file")
	}
	defer os.Remove(fn)

	indexr, err := index.NewFileReader(fn)
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer indexr.Close()

	if err := block.WriteIndexCache(cachefn, indexr); err != nil {
		return errors.Wrap(err, "write index cache")
	}

	b.symbols, b.lvals, b.postings, err = block.ReadIndexCache(cachefn)
	if err != nil {
		return errors.Wrap(err, "read index cache")
	}
	return nil
}

// blockMatchers checks whether the block potentially holds data for the given
// time range and label matchers and returns proper matches for this block that
// are stripped from external label matchers.
func (b *bucketBlock) blockMatchers(mint, maxt int64, matchers ...labels.Matcher) ([]labels.Matcher, bool) {
	if b.meta.MaxTime < mint {
		return nil, false
	}
	if b.meta.MinTime > maxt {
		return nil, false
	}

	var blockMatchers []labels.Matcher
	for _, m := range matchers {
		v, ok := b.meta.Thanos.Labels[m.Name()]
		if !ok {
			blockMatchers = append(blockMatchers, m)
			continue
		}
		if !m.Matches(v) {
			return nil, false
		}
	}
	return blockMatchers, true
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64) ([]byte, error) {
	r, err := b.bucket.GetRange(ctx, b.indexObj, off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer r.Close()

	// NOTE(bplotka): Huge amount of memory is allocated here. We need to cache it.
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
	defer r.Close()

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
	dec    *index.DecoderV1
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
		dec:          &index.DecoderV1{},
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
	return g.Run()
}

// loadPostings loads given postings using given start + length. It is expected to have given postings data within given range.
func (r *bucketIndexReader) loadPostings(ctx context.Context, postings []*lazyPostings, start, end int64) error {
	begin := time.Now()

	b, err := r.block.readIndexRange(r.ctx, int64(start), int64(end-start))
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
	const maxChunkSize = 2048
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
		cb = cb[n : n+int(l)+1]

		c, err := chunkenc.FromData(chunkenc.Encoding(cb[0]), cb[1:])
		if err != nil {
			return errors.Wrap(err, "instantiate chunk")
		}

		cid := uint64(seq<<32) | uint64(o)
		r.chunks[cid] = c
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

func (r *bucketChunkReader) Close() error {
	r.block.pendingReaders.Done()

	for _, b := range r.chunkBytes {
		r.block.chunkPool.Put(b)
	}
	return nil
}

func renameFile(from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fsync(pdir); err != nil {
		pdir.Close()
		return err
	}
	return pdir.Close()
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
