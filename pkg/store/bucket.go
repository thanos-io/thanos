package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/strutil"

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

	"math"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Bucket represents a readable bucket of data objects.
type Bucket interface {
	// Iter calls the given function with each found top-level object name in the bucket.
	// It exits if the context is canceled or the function returns an error.
	Iter(ctx context.Context, dir string, f func(name string) error) error

	// Get returns a new reader against the object with the given name.
	Get(ctx context.Context, name string) (io.ReadCloser, error)

	// GerRange returns a new reader against the object that reads len bytes
	// starting at off.
	GetRange(ctx context.Context, name string, off, len int64) (io.ReadCloser, error)
}

// BucketWithMetrics takes a bucket and registers metrics with the given registry for
// operations run against the bucket.
func BucketWithMetrics(name string, b Bucket, r prometheus.Registerer) Bucket {
	bkt := &metricBucket{
		Bucket: b,
		ops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_store_bucket_operations_total",
			Help:        "Total number of store operations that were executed against a bucket.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),
	}
	if r != nil {
		r.MustRegister(bkt.ops)
	}
	return bkt
}

type metricBucket struct {
	Bucket
	ops *prometheus.CounterVec
}

func (b *metricBucket) Iter(ctx context.Context, dir string, f func(name string) error) error {
	b.ops.WithLabelValues("iter").Inc()
	return b.Bucket.Iter(ctx, dir, f)
}

func (b *metricBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.ops.WithLabelValues("get").Inc()
	return b.Bucket.Get(ctx, name)
}

func (b *metricBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.ops.WithLabelValues("get_range").Inc()
	return b.Bucket.GetRange(ctx, name, off, length)
}

// BucketStore implements the store API backed by a Bucket bucket. It loads all index
// files to local disk.
type BucketStore struct {
	logger  log.Logger
	metrics *bucketStoreMetrics
	bucket  Bucket
	dir     string

	mtx                sync.RWMutex
	blocks             map[ulid.ULID]*bucketBlock
	gossipTimestampsFn func(mint int64, maxt int64)

	oldestBlockMinTime   int64
	youngestBlockMaxTime int64
}

var _ storepb.StoreServer = (*BucketStore)(nil)

type bucketStoreMetrics struct {
	blockLoads               prometheus.Counter
	blockLoadFailures        prometheus.Counter
	blockDrops               prometheus.Counter
	blockDropFailures        prometheus.Counter
	seriesPrepareDuration    prometheus.Histogram
	seriesPreloadDuration    prometheus.Histogram
	seriesPreloadAllDuration prometheus.Histogram
	seriesMergeDuration      prometheus.Histogram
}

func newBucketStoreMetrics(reg *prometheus.Registry, s *BucketStore) *bucketStoreMetrics {
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

	m.seriesPrepareDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_series_prepare_duration_seconds",
		Help: "Time it takes to prepare a query against a single block.",
		Buckets: []float64{
			0.0005, 0.001, 0.01, 0.05, 0.1, 0.3, 0.7, 1.5, 3,
		},
	})
	m.seriesPreloadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_series_preload_duration_seconds",
		Help: "Time it takes to load all chunks for a block query from Bucket into memory.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15,
		},
	})
	m.seriesPreloadAllDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_series_preload_all_duration_seconds",
		Help: "Time it takes until all per-block prepares and preloads for a query are finished.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15,
		},
	})
	m.seriesMergeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_bucket_store_series_merge_duration_seconds",
		Help: "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 1, 3, 5, 10,
		},
	})

	if reg != nil {
		reg.MustRegister(
			m.blockLoads,
			m.blockLoadFailures,
			m.blockDrops,
			m.blockDropFailures,
			blocksLoaded,
			m.seriesPrepareDuration,
			m.seriesPreloadDuration,
			m.seriesPreloadAllDuration,
			m.seriesMergeDuration,
		)
	}
	return &m
}

// NewBucketStore creates a new bucket backed store that implements the store API against
// an object store bucket. It is optimized to work against high latency backends.
func NewBucketStore(
	logger log.Logger,
	reg *prometheus.Registry,
	bucket Bucket,
	gossipTimestampsFn func(mint int64, maxt int64),
	dir string,
) (*BucketStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if gossipTimestampsFn == nil {
		gossipTimestampsFn = func(mint int64, maxt int64) {}
	}
	s := &BucketStore{
		logger:               logger,
		bucket:               bucket,
		dir:                  dir,
		blocks:               map[ulid.ULID]*bucketBlock{},
		gossipTimestampsFn:   gossipTimestampsFn,
		oldestBlockMinTime:   math.MaxInt64,
		youngestBlockMaxTime: math.MaxInt64,
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
		d := filepath.Join(dir, dn)

		b, err := newBucketBlock(context.TODO(), logger, bucket, id, d)
		if err != nil {
			level.Warn(s.logger).Log("msg", "loading block failed", "id", id, "err", err)
			// Wipe the directory so we can cleanly try again later.
			os.RemoveAll(d)
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

				b, err := newBucketBlock(ctx, s.logger, s.bucket, id, dir)
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

	if s.oldestBlockMinTime > b.meta.MinTime || s.oldestBlockMinTime == math.MaxInt64 {
		s.oldestBlockMinTime = b.meta.MinTime
	}
	if s.youngestBlockMaxTime < b.meta.MaxTime || s.youngestBlockMaxTime == math.MaxInt64 {
		s.youngestBlockMaxTime = b.meta.MaxTime
	}
	s.gossipTimestampsFn(s.oldestBlockMinTime, s.youngestBlockMaxTime)
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
	// Store nodes hold global data and thus have no labels.
	return &storepb.InfoResponse{}, nil
}

type seriesEntry struct {
	lset []storepb.Label
	chks []chunks.Meta
}
type bucketSeriesSet struct {
	set  []seriesEntry
	i    int
	chks []storepb.Chunk
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
	s.chks = make([]storepb.Chunk, 0, len(s.set[s.i].chks))

	for _, c := range s.set[s.i].chks {
		s.chks = append(s.chks, storepb.Chunk{
			MinTime: c.MinTime,
			MaxTime: c.MaxTime,
			Type:    storepb.Chunk_XOR,
			Data:    c.Chunk.Bytes(),
		})
	}
	return true
}

func (s *bucketSeriesSet) At() ([]storepb.Label, []storepb.Chunk) {
	return s.set[s.i].lset, s.chks
}

func (s *bucketSeriesSet) Err() error {
	return nil
}

func (s *BucketStore) blockSeries(ctx context.Context, b *bucketBlock, matchers []labels.Matcher, mint, maxt int64) (storepb.SeriesSet, *queryStats, error) {
	var (
		stats   = &queryStats{}
		extLset = b.meta.Thanos.Labels
		indexr  = b.indexReader(ctx)
		chunkr  = b.chunkReader(ctx)
	)
	defer indexr.Close()
	defer chunkr.Close()

	blockPrepareBegin := time.Now()

	// The postings to preload are registered within the call to PostingsForMatchers,
	// when it invokes indexr.Postings for each underlying postings list.
	// They are ready to use ONLY after preloadPostings was called successfully.
	lazyPostings, absent, err := tsdb.PostingsForMatchers(indexr, matchers...)
	if err != nil {
		return nil, stats, err
	}

	begin := time.Now()

	if err := indexr.preloadPostings(); err != nil {
		return nil, stats, err
	}

	begin = time.Now()
	ps, err := index.ExpandPostings(lazyPostings)
	if err != nil {
		return nil, stats, err
	}
	if err := indexr.preloadSeries(ps); err != nil {
		return nil, stats, err
	}

	begin = time.Now()
	var (
		res  []seriesEntry
		lset labels.Labels
		chks []chunks.Meta
	)
Outer:
	for _, id := range ps {
		if err := indexr.Series(id, &lset, &chks); err != nil {
			return nil, stats, err
		}
		// We must check all returned series whether they have one of the labels that should be
		// empty/absent set. If yes, we need to skip them.
		// NOTE(fabxc): ideally we'd solve this upstream with an inverted postings iterator.
		for _, l := range absent {
			if lset.Get(l) != "" {
				continue Outer
			}
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
	s.metrics.seriesPrepareDuration.Observe(time.Since(blockPrepareBegin).Seconds())

	begin = time.Now()
	if err := chunkr.preload(); err != nil {
		return nil, stats, errors.Wrap(err, "preload chunks")
	}
	s.metrics.seriesPreloadDuration.Observe(time.Since(begin).Seconds())

	for i := range res {
		for j := range res[i].chks {
			chk, err := chunkr.Chunk(res[i].chks[j].Ref)
			if err != nil {
				return nil, stats, errors.Wrap(err, "get chunk")
			}
			res[i].chks[j].Chunk = chk
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
	// TODO(fabxc): get rid of this long lock time.
	s.mtx.RLock()

	for _, b := range s.blocks {
		blockMatchers, ok := b.blockMatchers(req.MinTime, req.MaxTime, matchers...)
		if !ok {
			continue
		}
		stats.blocksQueried++

		block := b
		ctx, cancel := context.WithCancel(srv.Context())

		g.Add(func() error {
			part, pstats, err := s.blockSeries(ctx, block, blockMatchers, req.MinTime, req.MaxTime)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", block.meta.ULID)
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

	span, _ := tracing.StartSpan(srv.Context(), "gcs_store_preload_all")
	begin := time.Now()
	if err := g.Run(); err != nil {
		span.Finish()
		return status.Error(codes.Aborted, err.Error())
	}
	s.metrics.seriesPreloadAllDuration.Observe(time.Since(begin).Seconds())
	span.Finish()

	span, _ = tracing.StartSpan(srv.Context(), "gcs_store_merge_all")
	defer span.Finish()

	begin = time.Now()
	resp := &storepb.SeriesResponse{}

	// Merge series set into an union of all block sets. This exposes all blocks are single seriesSet.
	// Returned set is can be out of order in terms of series time ranges. It is fixed later on, inside querier.
	set := storepb.MergeSeriesSets(res...)
	for set.Next() {
		resp.Series.Labels, resp.Series.Chunks = set.At()

		stats.mergedSeriesCount++
		stats.mergedChunksCount += len(resp.Series.Chunks)

		if err := srv.Send(resp); err != nil {
			return errors.Wrap(err, "send series response")
		}
	}
	if set.Err() != nil {
		return errors.Wrap(set.Err(), "expand series set")
	}
	s.metrics.seriesMergeDuration.Observe(time.Since(begin).Seconds())

	stats.mergeDuration = time.Since(begin)

	level.Debug(s.logger).Log("msg", "series query processed",
		"stats", fmt.Sprintf("%+v", stats))

	return nil
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
	logger log.Logger
	bucket Bucket
	meta   *block.Meta
	dir    string

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
	bkt Bucket,
	id ulid.ULID,
	dir string,
) (*bucketBlock, error) {
	b := &bucketBlock{
		logger:   logger,
		bucket:   bkt,
		indexObj: path.Join(id.String(), "index"),
	}
	if err := b.loadMeta(ctx, id, dir); err != nil {
		return nil, errors.Wrap(err, "load meta")
	}
	if err := b.loadIndexCache(ctx, dir); err != nil {
		return nil, errors.Wrap(err, "load index cache")
	}
	// Get object handles for all chunk files.
	err := bkt.Iter(ctx, id.String()+"/chunks/", func(n string) error {
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
		dst := filepath.Join(dir, "meta.json")
		src := path.Join(id.String(), "meta.json")

		if err := downloadBucketObject(ctx, b.bucket, dst, src); err != nil {
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

	if err := downloadBucketObject(ctx, b.bucket, fn, b.indexObj); err != nil {
		return errors.Wrap(err, "download index file")
	}
	indexr, err := index.NewFileReader(fn)
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer os.Remove(fn)
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
	r, err := b.bucket.GetRange(ctx, b.chunkObjs[seq], off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer r.Close()

	c, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read range")
	}
	return c, nil
}

func (b *bucketBlock) indexReader(ctx context.Context) *bucketIndexReader {
	b.pendingReaders.Add(1)
	return newBucketIndexReader(ctx, b.logger, b)
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

	mtx            sync.Mutex
	loadedPostings []*lazyPostings
	loadedSeries   map[uint64][]byte
}

func newBucketIndexReader(ctx context.Context, logger log.Logger, block *bucketBlock) *bucketIndexReader {
	r := &bucketIndexReader{
		logger:       logger,
		ctx:          ctx,
		block:        block,
		dec:          &index.DecoderV1{},
		stats:        &queryStats{},
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

		// If we just fetched it we still have to update the stats for touched postings.
		r.stats.postingsTouched++
		r.stats.postingsTouchedSizeSum += len(c)
	}
	return nil
}

func (r *bucketIndexReader) preloadSeries(ids []uint64) error {
	const maxSeriesSize = 4096
	const maxGapSize = 512 * 1024

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
		r.loadedSeries[id] = c[n : n+int(l)]
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
	// The stats for touched postings are updated as they are loaded.
	p := &lazyPostings{ptr: ptr}
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

func downloadBucketObject(ctx context.Context, bkt Bucket, dst, src string) error {
	r, err := bkt.Get(ctx, src)
	if err != nil {
		return errors.Wrap(err, "create reader")
	}
	defer r.Close()

	f, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(dst)
		}
	}()
	_, err = io.Copy(f, r)
	return err
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

	preloadDuration   time.Duration
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

	s.mergedSeriesCount += o.mergedSeriesCount
	s.mergedChunksCount += o.mergedChunksCount
	s.mergeDuration += o.mergeDuration

	return &s
}
