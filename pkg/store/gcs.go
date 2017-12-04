package store

import (
	"context"
	"encoding/binary"
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

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Class A operation.
	gcsOperationObjectsList = "objects.list"

	// Class B operation.
	gcsOperationObjectGet = "object.get"
)

// GCSStore implements the store API backed by a GCS bucket. It loads all index
// files to local disk.
type GCSStore struct {
	logger  log.Logger
	metrics *gcsStoreMetrics
	bucket  *storage.BucketHandle
	dir     string

	mtx                sync.RWMutex
	blocks             map[ulid.ULID]*gcsBlock
	gossipTimestampsFn func(mint int64, maxt int64)

	oldestBlockMinTime   int64
	youngestBlockMaxTime int64
}

var _ storepb.StoreServer = (*GCSStore)(nil)

type gcsStoreMetrics struct {
	blockDownloads           prometheus.Counter
	blockDownloadsFailed     prometheus.Counter
	seriesPrepareDuration    prometheus.Histogram
	seriesPreloadDuration    prometheus.Histogram
	seriesPreloadAllDuration prometheus.Histogram
	seriesMergeDuration      prometheus.Histogram
	gcsOperations            *prometheus.CounterVec
}

func newGCSStoreMetrics(reg *prometheus.Registry, s *GCSStore) *gcsStoreMetrics {
	var m gcsStoreMetrics

	m.blockDownloads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gcs_store_block_downloads_total",
		Help: "Total number of block download attempts.",
	})
	m.blockDownloadsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gcs_store_block_downloads_failed_total",
		Help: "Total number of failed block download attempts.",
	})
	blocksLoaded := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "thanos_gcs_store_blocks_loaded",
		Help: "Number of currently loaded blocks.",
	}, func() float64 {
		return float64(s.numBlocks())
	})
	m.seriesPrepareDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_gcs_store_series_prepare_duration_seconds",
		Help: "Time it takes to prepare a query against a single block.",
		Buckets: []float64{
			0.0005, 0.001, 0.01, 0.05, 0.1, 0.3, 0.7, 1.5, 3,
		},
	})
	m.seriesPreloadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_gcs_store_series_preload_duration_seconds",
		Help: "Time it takes to load all chunks for a block query from GCS into memory.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15,
		},
	})
	m.seriesPreloadAllDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_gcs_series_preload_all_duration_seconds",
		Help: "Time it takes until all per-block prepares and preloads for a query are finished.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15,
		},
	})
	m.seriesMergeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_gcs_store_series_merge_duration_seconds",
		Help: "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{
			0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 1, 3, 5, 10,
		},
	})
	m.gcsOperations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_gcs_operations_total",
		Help: "Number of Google Storage operations.",
	}, []string{"type"})

	if reg != nil {
		reg.MustRegister(
			m.blockDownloads,
			m.blockDownloadsFailed,
			blocksLoaded,
			m.seriesPrepareDuration,
			m.seriesPreloadDuration,
			m.seriesPreloadAllDuration,
			m.seriesMergeDuration,
			m.gcsOperations,
		)
	}
	return &m
}

// NewGCSStore creates a new GCS backed store that caches index files to disk. It loads
// pre-exisiting cache entries in dir on creation.
func NewGCSStore(logger log.Logger, reg *prometheus.Registry, bucket *storage.BucketHandle, gossipTimestampsFn func(mint int64, maxt int64), dir string) (*GCSStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if gossipTimestampsFn == nil {
		gossipTimestampsFn = func(mint int64, maxt int64) {}
	}
	s := &GCSStore{
		logger:               logger,
		bucket:               bucket,
		dir:                  dir,
		blocks:               map[ulid.ULID]*gcsBlock{},
		gossipTimestampsFn:   gossipTimestampsFn,
		oldestBlockMinTime:   math.MaxInt64,
		youngestBlockMaxTime: math.MaxInt64,
	}
	s.metrics = newGCSStoreMetrics(reg, s)

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

		b, err := newGCSBlock(context.TODO(), logger, id, d, bucket, s.metrics.gcsOperations)
		if err != nil {
			level.Warn(s.logger).Log("msg", "loading block failed", "id", id, "err", err)
			// Wipe the directory so we can cleanly try again later.
			os.RemoveAll(dir)
			continue
		}
		s.setBlock(id, b)
	}
	return s, nil
}

// Close the store.
func (s *GCSStore) Close() (err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, b := range s.blocks {
		if e := b.Close(); e != nil {
			level.Warn(s.logger).Log("msg", "closing GCS block failed", "err", err)
			err = e
		}
	}
	return err
}

// SyncBlocks synchronizes the stores state with the GCS bucket.
func (s *GCSStore) SyncBlocks(ctx context.Context) error {
	s.metrics.gcsOperations.WithLabelValues(gcsOperationObjectsList).Inc()

	objs := s.bucket.Objects(ctx, &storage.Query{Delimiter: "/"})

	var wg sync.WaitGroup
	blockc := make(chan ulid.ULID)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for id := range blockc {
				dir := filepath.Join(s.dir, id.String())

				b, err := newGCSBlock(ctx, s.logger, id, dir, s.bucket, s.metrics.gcsOperations)
				if err != nil {
					level.Warn(s.logger).Log("msg", "loading block failed", "id", id, "err", err)
					// Wipe the directory so we can cleanly try again later.
					os.RemoveAll(dir)
					continue
				}
				s.setBlock(id, b)
			}
			wg.Done()
		}()
	}

	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			close(blockc)
			wg.Wait()
			return err
		}
		// Remove trailing slash from directory name.
		id, err := ulid.Parse(oi.Prefix[:len(oi.Prefix)-1])
		if err != nil {
			continue
		}
		if b := s.getBlock(id); b != nil {
			continue
		}
		select {
		case <-ctx.Done():
			close(blockc)
			wg.Wait()
			return nil
		case blockc <- id:
		}
	}

	close(blockc)
	wg.Wait()

	return nil
}

func (s *GCSStore) numBlocks() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.blocks)
}

func (s *GCSStore) getBlock(id ulid.ULID) *gcsBlock {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.blocks[id]
}

func (s *GCSStore) setBlock(id ulid.ULID, b *gcsBlock) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	s.blocks[id] = b

	if s.oldestBlockMinTime > b.meta.MinTime || s.oldestBlockMinTime == math.MaxInt64 {
		s.oldestBlockMinTime = b.meta.MinTime
	}
	if s.youngestBlockMaxTime < b.meta.MaxTime || s.youngestBlockMaxTime == math.MaxInt64 {
		s.youngestBlockMaxTime = b.meta.MaxTime
	}
	s.gossipTimestampsFn(s.oldestBlockMinTime, s.youngestBlockMaxTime)
}

// Info implements the storepb.StoreServer interface.
func (s *GCSStore) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	// Store nodes hold global data and thus have no labels.
	return &storepb.InfoResponse{}, nil
}

type seriesEntry struct {
	lset []storepb.Label
	chks []chunks.Meta
}
type gcsSeriesSet struct {
	set    []seriesEntry
	chunkr *gcsChunkReader
	i      int
	err    error
	chks   []storepb.Chunk
}

func newGCSSeriesSet(chunkr *gcsChunkReader, set []seriesEntry) *gcsSeriesSet {
	return &gcsSeriesSet{
		chunkr: chunkr,
		set:    set,
		i:      -1,
	}
}

func (s *gcsSeriesSet) Next() bool {
	if s.i >= len(s.set)-1 {
		return false
	}
	s.i++
	s.chks = make([]storepb.Chunk, 0, len(s.set[s.i].chks))

	for _, c := range s.set[s.i].chks {
		chk, err := s.chunkr.Chunk(c.Ref)
		if err != nil {
			s.err = err
			return false
		}
		s.chks = append(s.chks, storepb.Chunk{
			MinTime: c.MinTime,
			MaxTime: c.MaxTime,
			Type:    storepb.Chunk_XOR,
			Data:    chk.Bytes(),
		})
	}
	return true
}

func (s *gcsSeriesSet) At() ([]storepb.Label, []storepb.Chunk) {
	return s.set[s.i].lset, s.chks
}

func (s *gcsSeriesSet) Err() error {
	return s.err
}

func (s *GCSStore) blockSeries(ctx context.Context, b *gcsBlock, matchers []labels.Matcher, mint, maxt int64) (storepb.SeriesSet, error) {
	var (
		extLset = b.meta.Thanos.Labels
		indexr  = b.indexReader(ctx)
		chunkr  = b.chunkReader(ctx)
	)
	defer indexr.Close()
	defer chunkr.Close()

	blockPrepareBegin := time.Now()
	// The postings to preload are registered within the call to PostingsForMatchers,
	// when it invokes indexr.Postings for each underlying postings list.
	p, absent, err := tsdb.PostingsForMatchers(indexr, matchers...)
	if err != nil {
		return nil, err
	}
	level.Debug(s.logger).Log("msg", "setup postings", "duration", time.Since(blockPrepareBegin))

	begin := time.Now()
	if err := indexr.preloadPostings(); err != nil {
		return nil, err
	}

	level.Debug(s.logger).Log("msg", "preload postings", "duration", time.Since(begin))

	begin = time.Now()
	var ps []uint64
	for p.Next() {
		ps = append(ps, p.At())
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	if err := indexr.preloadSeries(ps); err != nil {
		return nil, err
	}
	level.Debug(s.logger).Log("msg", "preload index series", "count", len(ps), "duration", time.Since(begin))

	begin = time.Now()
	var (
		res  []seriesEntry
		lset labels.Labels
		chks []chunks.Meta
	)
Outer:
	for _, id := range ps {
		if err := indexr.Series(id, &lset, &chks); err != nil {
			return nil, err
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
				return nil, errors.Wrap(err, "add chunk preload")
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
		return nil, errors.Wrap(err, "preload chunks")
	}
	s.metrics.seriesPreloadDuration.Observe(time.Since(begin).Seconds())

	return newGCSSeriesSet(chunkr, res), nil
}

// Series implements the storepb.StoreServer interface.
func (s *GCSStore) Series(req *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	var (
		g         run.Group
		numBlocks int
		res       []storepb.SeriesSet
		mtx       sync.Mutex
	)
	s.mtx.RLock()

	for _, b := range s.blocks {
		blockMatchers, ok := b.blockMatchers(req.MinTime, req.MaxTime, matchers...)
		if !ok {
			continue
		}
		numBlocks++

		block := b
		ctx, cancel := context.WithCancel(srv.Context())

		g.Add(func() error {
			part, err := s.blockSeries(ctx, block, blockMatchers, req.MinTime, req.MaxTime)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", block.meta.ULID)
			}

			mtx.Lock()
			res = append(res, part)
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
	level.Debug(s.logger).Log("msg", "preload all block data",
		"numBlocks", numBlocks,
		"duration", time.Since(begin))
	s.metrics.seriesPreloadAllDuration.Observe(time.Since(begin).Seconds())
	span.Finish()

	span, _ = tracing.StartSpan(srv.Context(), "gcs_store_merge_all")
	defer span.Finish()

	begin = time.Now()
	resp := &storepb.SeriesResponse{}
	set := storepb.MergeSeriesSets(res...)

	for set.Next() {
		resp.Series.Labels, resp.Series.Chunks = set.At()

		if err := srv.Send(resp); err != nil {
			return errors.Wrap(err, "send series response")
		}
	}
	if set.Err() != nil {
		return errors.Wrap(set.Err(), "expand series set")
	}
	s.metrics.seriesMergeDuration.Observe(time.Since(begin).Seconds())
	return nil
}

// LabelNames implements the storepb.StoreServer interface.
func (s *GCSStore) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues implements the storepb.StoreServer interface.
func (s *GCSStore) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
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

type gcsBlock struct {
	logger         log.Logger
	meta           *block.Meta
	dir            string
	pendingReaders sync.WaitGroup
	gcsOperations  *prometheus.CounterVec

	index     *gcsIndex
	chunkObjs []*storage.ObjectHandle
}

func newGCSBlock(
	ctx context.Context,
	logger log.Logger,
	id ulid.ULID,
	dir string,
	bkt *storage.BucketHandle,
	gcsOperations *prometheus.CounterVec,
) (*gcsBlock, error) {
	// If we haven't seen the block before download the meta.json file.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, errors.Wrap(err, "create dir")
		}
		dst := filepath.Join(dir, "meta.json")
		src := path.Join(id.String(), "meta.json")

		gcsOperations.WithLabelValues(gcsOperationObjectGet).Inc()

		if err := downloadGCSObject(ctx, dst, bkt.Object(src)); err != nil {
			return nil, errors.Wrap(err, "download meta.json")
		}
	} else if err != nil {
		return nil, err
	}
	meta, err := block.ReadMetaFile(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read meta.json")
	}

	ix, err := newGCSIndex(ctx, dir, bkt.Object(path.Join(id.String(), "index")), gcsOperations)
	if err != nil {
		return nil, errors.Wrap(err, "initialize index")
	}

	// Get object handles for all chunk files.
	gcsOperations.WithLabelValues(gcsOperationObjectsList).Inc()

	objs := bkt.Objects(ctx, &storage.Query{
		Prefix: path.Join(id.String(), "chunks/"),
	})
	var chunkObjs []*storage.ObjectHandle
	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "list chunk files")
		}
		chunkObjs = append(chunkObjs, bkt.Object(oi.Name))
	}
	return &gcsBlock{
		logger:        logger,
		meta:          meta,
		index:         ix,
		chunkObjs:     chunkObjs,
		gcsOperations: gcsOperations,
	}, nil
}

// blockMatchers checks whether the block potentially holds data for the given
// time range and label matchers and returns proper matches for this block that
// are stripped from external label matchers.
func (b *gcsBlock) blockMatchers(mint, maxt int64, matchers ...labels.Matcher) ([]labels.Matcher, bool) {
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

func (b *gcsBlock) indexReader(ctx context.Context) *gcsIndexReader {
	b.pendingReaders.Add(1)
	return newGCSIndexReader(ctx, b.logger, b.meta.ULID, b.index, b.pendingReaders.Done, b.gcsOperations)
}

func (b *gcsBlock) chunkReader(ctx context.Context) *gcsChunkReader {
	b.pendingReaders.Add(1)
	return newGCSChunkReader(ctx, b.logger, b.meta.ULID, b.chunkObjs, b.pendingReaders.Done, b.gcsOperations)
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *gcsBlock) Close() error {
	b.pendingReaders.Wait()
	return nil
}

type gcsIndex struct {
	obj *storage.ObjectHandle
	dec *index.DecoderV1

	symbols  map[uint32]string
	lvals    map[string][]string
	postings map[labels.Label]index.Range
}

func newGCSIndex(
	ctx context.Context,
	dir string,
	obj *storage.ObjectHandle,
	gcsOperations *prometheus.CounterVec,
) (*gcsIndex, error) {
	// Attempt to load cached index state first.
	cachefn := filepath.Join(dir, block.IndexCacheFilename)

	sym, lvals, pranges, err := block.ReadIndexCache(cachefn)
	if err == nil {
		ix := &gcsIndex{
			obj:      obj,
			symbols:  sym,
			lvals:    lvals,
			postings: pranges,
			dec:      &index.DecoderV1{},
		}
		ix.dec.SetSymbolTable(ix.symbols)

		return ix, nil
	}
	if !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Wrap(err, "read index cache")
	}
	// No cache exists is on disk yet, build it from a the downloaded index and retry.
	fn := filepath.Join(dir, "index")

	gcsOperations.WithLabelValues(gcsOperationObjectGet).Inc()

	if err := downloadGCSObject(ctx, fn, obj); err != nil {
		return nil, errors.Wrap(err, "download index file")
	}
	indexr, err := index.NewFileReader(fn)
	if err != nil {
		return nil, errors.Wrap(err, "open index reader")
	}
	defer os.Remove(fn)
	defer indexr.Close()

	if err := block.WriteIndexCache(cachefn, indexr); err != nil {
		return nil, errors.Wrap(err, "write index cache")
	}

	sym, lvals, pranges, err = block.ReadIndexCache(cachefn)
	if err != nil {
		return nil, errors.Wrap(err, "read index cache")
	}
	ix := &gcsIndex{
		obj:      obj,
		symbols:  sym,
		lvals:    lvals,
		postings: pranges,
		dec:      &index.DecoderV1{},
	}
	ix.dec.SetSymbolTable(ix.symbols)

	return ix, nil
}

type gcsIndexReader struct {
	logger log.Logger
	ctx    context.Context
	close  func()
	id     ulid.ULID
	index  *gcsIndex

	gcsOperations *prometheus.CounterVec

	loadedPostings map[labels.Label]*lazyPostings
	loadedSeries   map[uint64][]byte
}

func newGCSIndexReader(
	ctx context.Context,
	logger log.Logger,
	id ulid.ULID,
	index *gcsIndex,
	close func(),
	gcsOps *prometheus.CounterVec,
) *gcsIndexReader {
	return &gcsIndexReader{
		ctx:           ctx,
		logger:        logger,
		id:            id,
		index:         index,
		close:         close,
		gcsOperations: gcsOps,

		loadedPostings: map[labels.Label]*lazyPostings{},
		loadedSeries:   map[uint64][]byte{},
	}
}

func (r *gcsIndexReader) preloadPostings() error {
	if len(r.loadedPostings) == 0 {
		return nil
	}

	all := make([]*lazyPostings, 0, len(r.loadedPostings))
	for _, p := range r.loadedPostings {
		all = append(all, p)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].ptr.Start < all[j].ptr.Start
	})
	// TODO(fabxc): detect gaps and split up into multiple requests as we do for chunks.
	start := all[0].ptr.Start
	end := all[len(all)-1].ptr.End

	r.gcsOperations.WithLabelValues(gcsOperationObjectGet).Inc()

	objr, err := r.index.obj.NewRangeReader(r.ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "create range reader")
	}
	b, err := ioutil.ReadAll(objr)
	if err != nil {
		return errors.Wrap(err, "read entire range")
	}
	for _, p := range all {
		_, l, err := r.index.dec.Postings(b[p.ptr.Start-start : p.ptr.End-start])
		if err != nil {
			return errors.Wrap(err, "read postings list")
		}
		p.set(l)
	}
	return nil
}

func (r *gcsIndexReader) preloadSeries(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	// We don't know how long a series entry will. We use this constant as a buffer size
	// bytes which we read beyond an entries start position.
	const maxSeriesSize = 4096

	// The series IDs in the postings list are equivalent to the offset of the respective series entry.
	// TODO(fabxc): detect gaps and split up requests as we do for chunks.
	start := ids[0]
	end := ids[len(ids)-1] + maxSeriesSize

	r.gcsOperations.WithLabelValues(gcsOperationObjectGet).Inc()

	objr, err := r.index.obj.NewRangeReader(r.ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "create range reader")
	}
	b, err := ioutil.ReadAll(objr)
	if err != nil {
		return errors.Wrap(err, "read entire range")
	}
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

func (r *gcsIndexReader) Symbols() (map[string]struct{}, error) {
	return nil, errors.New("not implemented")
}

// LabelValues returns the possible label values.
func (r *gcsIndexReader) LabelValues(names ...string) (index.StringTuples, error) {
	if len(names) != 1 {
		return nil, errors.New("label value lookups only supported for single name")
	}
	return index.NewStringTuples(r.index.lvals[names[0]], 1)
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
func (r *gcsIndexReader) Postings(name, value string) (index.Postings, error) {
	l := labels.Label{Name: name, Value: value}
	ptr, ok := r.index.postings[l]
	if !ok {
		return index.EmptyPostings(), nil
	}
	p := &lazyPostings{ptr: ptr}
	r.loadedPostings[l] = p
	return p, nil
}

// SortedPostings returns a postings list that is reordered to be sorted
// by the label set of the underlying series.
func (r *gcsIndexReader) SortedPostings(p index.Postings) index.Postings {
	return p
}

// Series populates the given labels and chunk metas for the series identified
// by the reference.
// Returns ErrNotFound if the ref does not resolve to a known series.
func (r *gcsIndexReader) Series(ref uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	b, ok := r.loadedSeries[ref]
	if !ok {
		return errors.New("series not found")
	}
	return r.index.dec.Series(b, lset, chks)
}

// LabelIndices returns the label pairs for which indices exist.
func (r *gcsIndexReader) LabelIndices() ([][]string, error) {
	return nil, errors.New("not implemented")
}

// Close released the underlying resources of the reader.
func (r *gcsIndexReader) Close() error {
	r.close()
	return nil
}

type gcsChunkReader struct {
	logger log.Logger
	ctx    context.Context
	close  func()
	id     ulid.ULID

	files    []*storage.ObjectHandle
	preloads [][]uint32
	mtx      sync.Mutex
	chunks   map[uint64]chunkenc.Chunk

	gcsOperations *prometheus.CounterVec
}

func newGCSChunkReader(ctx context.Context, logger log.Logger, id ulid.ULID, files []*storage.ObjectHandle, close func(), gcsOperations *prometheus.CounterVec) *gcsChunkReader {
	ctx, cancel := context.WithCancel(ctx)

	return &gcsChunkReader{
		logger:   logger,
		ctx:      ctx,
		id:       id,
		files:    files,
		preloads: make([][]uint32, len(files)),
		chunks:   map[uint64]chunkenc.Chunk{},
		close: func() {
			cancel()
			close()
		},
		gcsOperations: gcsOperations,
	}
}

// addPreload adds the chunk with id to the data set that will be fetched on calling preload.
func (r *gcsChunkReader) addPreload(id uint64) error {
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

// preloadFile adds actors to load all chunks referenced by the offsets from the given file.
// It attempts to conslidate requests for multiple chunks into a single one and populates
// the reader's chunk map.
func (r *gcsChunkReader) preloadFile(g *run.Group, seq int, file *storage.ObjectHandle, offsets []uint32) {
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	const (
		// Maximum amount of irrelevant bytes between chunks we are willing to fetch.
		maxChunkGap = 512 * 1024
		// Maximum length we expect a chunk to have and prefetch.
		maxChunkLen = 2048
	)
	j := 0
	k := 0

	for k < len(offsets) {
		j = k
		k++

		start := offsets[j]
		end := start + maxChunkLen

		// Extend the range if the next chunk is no further than 0.5MB away.
		// Otherwise, break out and fetch the current range.
		for k < len(offsets) {
			nextEnd := offsets[k] + maxChunkLen
			if nextEnd-end > maxChunkGap {
				break
			}
			k++
			end = nextEnd
		}

		inclOffs := offsets[j:k]
		ctx, cancel := context.WithCancel(r.ctx)

		g.Add(func() error {
			now := time.Now()
			defer func() {
				level.Debug(r.logger).Log(
					"msg", "preloaded range",
					"block", r.id,
					"file", seq,
					"numOffsets", len(inclOffs),
					"length", end-start,
					"duration", time.Since(now))
			}()

			r.gcsOperations.WithLabelValues(gcsOperationObjectGet).Inc()
			objr, err := file.NewRangeReader(ctx, int64(start), int64(end-start))
			if err != nil {
				return errors.Wrap(err, "create reader")
			}
			defer objr.Close()

			b, err := ioutil.ReadAll(objr)
			if err != nil {
				return errors.Wrap(err, "load byte range for chunks")
			}
			for _, o := range inclOffs {
				cb := b[o-start:]

				l, n := binary.Uvarint(cb)
				if n < 0 {
					return errors.Errorf("reading chunk length failed")
				}
				if len(cb) < n+int(l)+1 {
					return errors.Errorf("preloaded chunk too small, expecting %d", n+int(l))
				}
				cb = cb[n : n+int(l)+1]

				c, err := chunkenc.FromData(chunkenc.Encoding(cb[0]), cb[1:])
				if err != nil {
					return errors.Wrap(err, "instantiate chunk")
				}

				r.mtx.Lock()
				cid := uint64(seq<<32) | uint64(o)
				r.chunks[cid] = c
				r.mtx.Unlock()
			}
			return nil
		}, func(err error) {
			if err != nil {
				cancel()
			}
		})
	}
}

// preload all added chunk IDs. Must be called before the first call to Chunk is made.
func (r *gcsChunkReader) preload() error {
	var g run.Group

	for i, offsets := range r.preloads {
		if len(offsets) == 0 {
			continue
		}
		r.preloadFile(&g, i, r.files[i], offsets)
	}
	return g.Run()
}

func (r *gcsChunkReader) Chunk(id uint64) (chunkenc.Chunk, error) {
	c, ok := r.chunks[id]
	if !ok {
		return nil, errors.Errorf("chunk with ID %d not found", id)
	}
	return c, nil
}

func (r *gcsChunkReader) Close() error {
	r.close()
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

func downloadGCSObject(ctx context.Context, dst string, src *storage.ObjectHandle) error {
	r, err := src.NewReader(ctx)
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
