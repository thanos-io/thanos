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
	"strings"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/block"

	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/sync/errgroup"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GCSStore implements the store API backed by a GCS bucket. It loads all index
// files to local disk.
type GCSStore struct {
	logger  log.Logger
	metrics *gcsStoreMetrics
	bucket  *storage.BucketHandle
	dir     string

	mtx    sync.RWMutex
	blocks map[ulid.ULID]*gcsBlock
}

var _ storepb.StoreServer = (*GCSStore)(nil)

type gcsStoreMetrics struct {
	blockDownloads           prometheus.Counter
	blockDownloadsFailed     prometheus.Counter
	seriesPrepareDuration    prometheus.Histogram
	seriesPreloadDuration    prometheus.Histogram
	seriesPreloadAllDuration prometheus.Histogram
	seriesMergeDuration      prometheus.Histogram
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

	if reg != nil {
		reg.MustRegister(
			m.blockDownloads,
			m.blockDownloadsFailed,
			blocksLoaded,
			m.seriesPrepareDuration,
			m.seriesPreloadDuration,
			m.seriesPreloadAllDuration,
			m.seriesMergeDuration,
		)
	}
	return &m
}

// NewGCSStore creates a new GCS backed store that caches index files to disk. It loads
// pre-exisiting cache entries in dir on creation.
func NewGCSStore(logger log.Logger, reg *prometheus.Registry, bucket *storage.BucketHandle, dir string) (*GCSStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	s := &GCSStore{
		logger: logger,
		bucket: bucket,
		dir:    dir,
		blocks: map[ulid.ULID]*gcsBlock{},
	}
	s.metrics = newGCSStoreMetrics(reg, s)

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}
	if err := s.loadBlocks(); err != nil {
		return nil, errors.Wrap(err, "loading blocks from disk failed")
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
func (s *GCSStore) SyncBlocks(ctx context.Context, interval time.Duration) {
	// NOTE(fabxc): watches are not yet supported by the Go client library so we just
	// do a periodic refresh.
	err := runutil.Repeat(interval, ctx.Done(), func() error {
		if err := s.downloadBlocks(ctx); err != nil {
			level.Warn(s.logger).Log("msg", "downloading missing blocks failed", "err", err)
		}
		if err := s.loadBlocks(); err != nil {
			level.Warn(s.logger).Log("msg", "loading disk blocks failed", "err", err)
		}
		return nil
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "unexpected error", "err", err)
	}
}

func (s *GCSStore) downloadBlocks(ctx context.Context) error {
	objs := s.bucket.Objects(ctx, &storage.Query{Delimiter: "/"})

	// Fetch a maximum of 20 blocks in parallel.
	var wg sync.WaitGroup
	workc := make(chan struct{}, 20)

	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
		id, err := ulid.Parse(oi.Prefix[:len(oi.Prefix)-1])
		if err != nil {
			continue
		}
		if b := s.getBlock(id); b != nil {
			continue
		}
		level.Info(s.logger).Log("msg", "sync block from GCS", "id", id)

		wg.Add(1)
		go func() {
			workc <- struct{}{}
			s.metrics.blockDownloads.Inc()

			if err := s.downloadBlock(ctx, id); err != nil {
				level.Error(s.logger).Log("msg", "syncing block failed", "err", err, "block", id.String())
				s.metrics.blockDownloadsFailed.Inc()
			}
			wg.Done()
			<-workc
		}()
	}
	wg.Wait()

	return nil
}

// downloadBlock downloads the index and meta.json file for the given block ID and opens a reader
// against the block.
func (s *GCSStore) downloadBlock(ctx context.Context, id ulid.ULID) error {
	bdir := filepath.Join(s.dir, id.String())
	tmpdir := bdir + ".tmp"

	if err := os.MkdirAll(tmpdir, 0777); err != nil {
		return errors.Wrap(err, "create temp dir")
	}

	for _, fn := range []string{
		"index",
		"meta.json",
	} {
		obj := s.bucket.Object(path.Join(id.String(), fn))

		f, err := os.Create(filepath.Join(tmpdir, fn))
		if err != nil {
			return errors.Wrap(err, "create local index copy")
		}
		r, err := obj.NewReader(ctx)
		if err != nil {
			return errors.Wrap(err, "create index object reader")
		}
		_, copyErr := io.Copy(f, r)

		if err := f.Close(); err != nil {
			level.Warn(s.logger).Log("msg", "close file", "err", err)
		}
		if err := r.Close(); err != nil {
			level.Warn(s.logger).Log("msg", "close object reader", "err", err)
		}
		if copyErr != nil {
			if err := os.RemoveAll(tmpdir); err != nil {
				level.Warn(s.logger).Log("msg", "cleanup temp dir after failure", "err", err)
			}
			return errors.Wrap(copyErr, "copy index file to disk")
		}
	}

	if err := renameFile(tmpdir, bdir); err != nil {
		return errors.Wrap(err, "rename block directory")
	}
	return nil
}

// loadBlocks ensures that all blocks in the data directory are loaded into memory.
func (s *GCSStore) loadBlocks() error {
	fns, err := fileutil.ReadDir(s.dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	for _, fn := range fns {
		id, err := ulid.Parse(fn)
		if err != nil {
			continue
		}
		if b := s.getBlock(id); b != nil {
			continue
		}
		b, err := s.loadFromDisk(id)
		if err != nil {
			level.Warn(s.logger).Log("msg", "loading block failed", "err", err)
		}
		s.setBlock(id, b)
	}
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
}

// loadFromDisk loads a block with the given ID from the disk cache.
func (s *GCSStore) loadFromDisk(id ulid.ULID) (*gcsBlock, error) {
	dir := filepath.Join(s.dir, id.String())

	indexr, err := tsdb.NewFileIndexReader(filepath.Join(dir, "index"))
	if err != nil {
		return nil, errors.Wrap(err, "open index reader")
	}
	meta, err := block.ReadMetaFile(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read meta file")
	}
	b, err := newGCSBlock(context.TODO(), s.logger, meta, indexr, s.bucket)
	if err != nil {
		return nil, errors.Wrap(err, "open GCS block")
	}
	return b, nil
}

// Info implements the storepb.StoreServer interface.
func (s *GCSStore) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	// Store nodes hold global data and thus have no labels.
	return &storepb.InfoResponse{}, nil
}

type seriesEntry struct {
	lset []storepb.Label
	chks []tsdb.ChunkMeta
}
type gcsSeriesSet struct {
	set    []seriesEntry
	chunkr *gcsChunkReader
	i      int
	err    error
	chks   []storepb.Chunk
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

func (s *GCSStore) blockSeries(ctx context.Context, b *gcsBlock, matchers []labels.Matcher, mint, maxt int64) (chunkSeriesSet, error) {
	var (
		extLset = b.meta.Thanos.Labels
		indexr  = b.indexReader()
		chunkr  = b.chunkReader(ctx)
	)
	defer indexr.Close()
	defer chunkr.Close()

	begin := time.Now()
	set, err := tsdb.LookupChunkSeries(indexr, nil, matchers...)
	if err != nil {
		return nil, errors.Wrap(err, "get series set")
	}
	var res []seriesEntry

	for set.Next() {
		lset, chks, _ := set.At()

		s := seriesEntry{
			lset: make([]storepb.Label, 0, len(lset)),
			chks: make([]tsdb.ChunkMeta, 0, len(chks)),
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

		res = append(res, s)
	}
	s.metrics.seriesPrepareDuration.Observe(time.Since(begin).Seconds())

	if err := set.Err(); err != nil {
		return nil, errors.Wrap(err, "read series set")
	}

	begin = time.Now()
	if err := chunkr.preload(); err != nil {
		return nil, errors.Wrap(err, "preload chunks")
	}
	s.metrics.seriesPreloadDuration.Observe(time.Since(begin).Seconds())

	return &gcsSeriesSet{chunkr: chunkr, set: res}, nil
}

// Series implements the storepb.StoreServer interface.
func (s *GCSStore) Series(ctx context.Context, req *storepb.SeriesRequest) (*storepb.SeriesResponse, error) {
	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var g run.Group
	var res []chunkSeriesSet

	s.mtx.RLock()

	for _, b := range s.blocks {
		if !b.matches(req.MinTime, req.MaxTime, matchers...) {
			continue
		}
		block := b
		ctx, cancel := context.WithCancel(ctx)

		g.Add(func() error {
			part, err := s.blockSeries(ctx, block, matchers, req.MinTime, req.MaxTime)
			if err != nil {
				return errors.Wrapf(err, "fetch series for block %s", block.meta.ULID)
			}
			res = append(res, part)
			return nil
		}, func(err error) {
			if err != nil {
				cancel()
			}
		})
	}

	s.mtx.RUnlock()

	begin := time.Now()
	if err := g.Run(); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	s.metrics.seriesPreloadAllDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	resp := &storepb.SeriesResponse{}
	set := mergeAllSeriesSets(res...)

	for set.Next() {
		lset, chks := set.At()

		resp.Series = append(resp.Series, storepb.Series{
			Labels: lset,
			Chunks: chks,
		})
	}
	s.metrics.seriesMergeDuration.Observe(time.Since(begin).Seconds())

	if err := set.Err(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.metrics.seriesMergeDuration.Observe(time.Since(begin).Seconds())

	return resp, nil
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
		indexr := b.indexReader()
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
		Values: mergeStringSlices(sets...),
	}, nil
}

type gcsBlock struct {
	logger         log.Logger
	meta           *block.Meta
	dir            string
	pendingReaders sync.WaitGroup
	index          tsdb.IndexReader
	chunkObjs      []*storage.ObjectHandle
}

func newGCSBlock(
	ctx context.Context,
	logger log.Logger,
	meta *block.Meta,
	index tsdb.IndexReader,
	bkt *storage.BucketHandle,
) (*gcsBlock, error) {
	b := &gcsBlock{logger: logger, meta: meta, index: index}

	// Get object handles for all chunk files.
	objs := bkt.Objects(ctx, &storage.Query{
		Prefix: path.Join(meta.ULID.String(), "chunks/"),
	})
	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "list chunk files")
		}
		b.chunkObjs = append(b.chunkObjs, bkt.Object(oi.Name))
	}
	return b, nil
}

// matches checks whether the block potentially holds data for the given
// time range and label matchers.
func (b *gcsBlock) matches(mint, maxt int64, matchers ...labels.Matcher) bool {
	if b.meta.MaxTime < mint {
		return false
	}
	if b.meta.MinTime > maxt {
		return false
	}
	for _, m := range matchers {
		v, ok := b.meta.Thanos.Labels[m.Name()]
		if !ok {
			continue
		}
		if !m.Matches(v) {
			return false
		}
	}
	return true
}

func (b *gcsBlock) indexReader() tsdb.IndexReader {
	b.pendingReaders.Add(1)
	return &closeIndex{b.index, b.pendingReaders.Done}
}

func (b *gcsBlock) chunkReader(ctx context.Context) *gcsChunkReader {
	b.pendingReaders.Add(1)
	return newGCSChunkReader(ctx, b.logger, b.meta.ULID, b.chunkObjs, b.pendingReaders.Done)
}

type closeIndex struct {
	tsdb.IndexReader
	close func()
}

func (c *closeIndex) Close() error {
	c.close()
	return nil
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *gcsBlock) Close() error {
	b.pendingReaders.Wait()
	b.index.Close()
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
	chunks   map[uint64]chunks.Chunk
}

func newGCSChunkReader(ctx context.Context, logger log.Logger, id ulid.ULID, files []*storage.ObjectHandle, close func()) *gcsChunkReader {
	ctx, cancel := context.WithCancel(ctx)

	return &gcsChunkReader{
		logger:   logger,
		ctx:      ctx,
		id:       id,
		files:    files,
		preloads: make([][]uint32, len(files)),
		chunks:   map[uint64]chunks.Chunk{},
		close: func() {
			cancel()
			close()
		},
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

				c, err := chunks.FromData(chunks.Encoding(cb[0]), cb[1:])
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

func (r *gcsChunkReader) Chunk(id uint64) (chunks.Chunk, error) {
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

func translateMatcher(m storepb.LabelMatcher) (labels.Matcher, error) {
	switch m.Type {
	case storepb.LabelMatcher_EQ:
		return labels.NewEqualMatcher(m.Name, m.Value), nil

	case storepb.LabelMatcher_NEQ:
		return labels.Not(labels.NewEqualMatcher(m.Name, m.Value)), nil

	case storepb.LabelMatcher_RE:
		return labels.NewRegexpMatcher(m.Name, m.Value)

	case storepb.LabelMatcher_NRE:
		m, err := labels.NewRegexpMatcher(m.Name, m.Value)
		if err != nil {
			return nil, err
		}
		return labels.Not(m), nil
	}
	return nil, errors.Errorf("unknown label matcher type %d", m.Type)
}

func translateMatchers(ms []storepb.LabelMatcher) (res []labels.Matcher, err error) {
	for _, m := range ms {
		r, err := translateMatcher(m)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

func mergeStringSlices(a ...[]string) []string {
	if len(a) == 0 {
		return nil
	}
	if len(a) == 1 {
		return a[0]
	}
	l := len(a) / 2

	return mergeTwoStringSlices(
		mergeStringSlices(a[:l]...),
		mergeStringSlices(a[l:]...),
	)
}

func mergeTwoStringSlices(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		d := strings.Compare(a[0], b[0])

		if d == 0 {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if d < 0 {
			res = append(res, a[0])
			a = a[1:]
		} else if d > 0 {
			res = append(res, b[0])
			b = b[1:]
		}
	}

	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}

func mergeAllSeriesSets(all ...chunkSeriesSet) chunkSeriesSet {
	switch len(all) {
	case 0:
		return &gcsSeriesSet{}
	case 1:
		return all[0]
	}
	h := len(all) / 2

	return newMergedSeriesSet(
		mergeAllSeriesSets(all[:h]...),
		mergeAllSeriesSets(all[h:]...),
	)
}

type chunkSeriesSet interface {
	Next() bool
	At() ([]storepb.Label, []storepb.Chunk)
	Err() error
}

// mergedSeriesSet takes two series sets as a single series set. The input series sets
// must be sorted and sequential in time, i.e. if they have the same label set,
// the datapoints of a must be before the datapoints of b.
type mergedSeriesSet struct {
	a, b chunkSeriesSet

	lset         []storepb.Label
	chunks       []storepb.Chunk
	adone, bdone bool
}

// NewMergedSeriesSet takes two series sets as a single series set.
// Series that occur in both sets should have disjoint time ranges and a should com before b.
// If the ranges overlap, the result series will still have monotonically increasing timestamps,
// but all samples in the overlapping range in b will be dropped.
func newMergedSeriesSet(a, b chunkSeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedSeriesSet) At() ([]storepb.Label, []storepb.Chunk) {
	return s.lset, s.chunks
}

func (s *mergedSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedSeriesSet) compare() int {
	if s.adone {
		return 1
	}
	if s.bdone {
		return -1
	}
	lsetA, _ := s.a.At()
	lsetB, _ := s.b.At()
	return storepb.CompareLabels(lsetA, lsetB)
}

func (s *mergedSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()

	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		s.lset, s.chunks = s.b.At()
		s.bdone = !s.b.Next()
	} else if d < 0 {
		s.lset, s.chunks = s.a.At()
		s.adone = !s.a.Next()
	} else {
		// Concatenate chunks from both series sets. They may be out of order
		// w.r.t to their time range. This must be accounted for later.
		lset, chksA := s.a.At()
		_, chksB := s.b.At()

		s.lset = lset
		// Slice reuse is not generally safe with nested merge iterators.
		// We err on the safe side an create a new slice.
		s.chunks = make([]storepb.Chunk, 0, len(chksA)+len(chksB))
		s.chunks = append(s.chunks, chksA...)
		s.chunks = append(s.chunks, chksB...)

		s.adone = !s.a.Next()
		s.bdone = !s.b.Next()
	}
	return true
}
