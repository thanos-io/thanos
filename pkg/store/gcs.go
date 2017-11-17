package store

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/block"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
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
	logger log.Logger
	bucket *storage.BucketHandle
	dir    string

	mtx    sync.RWMutex
	blocks map[ulid.ULID]*gcsBlock
}

var _ storepb.StoreServer = (*GCSStore)(nil)

// NewGCSStore creates a new GCS backed store that caches index files to disk. It loads
// pre-exisiting cache entries in dir on creation.
func NewGCSStore(logger log.Logger, bucket *storage.BucketHandle, dir string) (*GCSStore, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	s := &GCSStore{
		logger: logger,
		bucket: bucket,
		dir:    dir,
		blocks: map[ulid.ULID]*gcsBlock{},
	}

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

	// Fetch a maximum of 20 blocks in parallel
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

			if err := s.downloadBlock(ctx, id); err != nil {
				level.Error(s.logger).Log("msg", "syncing block failed", "err", err, "block", id.String())
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
	b, err := newGCSBlock(context.TODO(), meta, indexr, s.bucket)
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

type seriesResult struct {
	mtx sync.Mutex
	res map[uint64]*seriesEntry
}

type seriesEntry struct {
	lset         labels.Labels
	chunks       []tsdb.ChunkMeta
	externalLset map[string]string // extra labels to attach, overrules lset
}

func (r *seriesResult) add(lset labels.Labels, externalLset map[string]string, chks []tsdb.ChunkMeta) {
	if len(chks) == 0 {
		return
	}
	h := lset.Hash()

	r.mtx.Lock()
	defer r.mtx.Unlock()

	e, ok := r.res[h]
	if !ok {
		e = &seriesEntry{lset: lset, externalLset: externalLset}
		r.res[h] = e
	}
	e.chunks = append(e.chunks, chks...)
}

func (r *seriesResult) response() ([]storepb.Series, error) {
	res := make([]storepb.Series, 0, len(r.res))

	for _, e := range r.res {
		s := storepb.Series{
			Labels: make([]storepb.Label, 0, len(e.lset)),
			Chunks: make([]storepb.Chunk, 0, len(e.chunks)),
		}
		for _, l := range e.lset {
			// Skip if the external labels of the block overrule the series' label.
			// NOTE(fabxc): maybe move it to a prefixed version to still ensure uniqueness of series?
			if e.externalLset[l.Name] != "" {
				continue
			}
			s.Labels = append(s.Labels, storepb.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		for ln, lv := range e.externalLset {
			s.Labels = append(s.Labels, storepb.Label{
				Name:  ln,
				Value: lv,
			})
		}
		sort.Slice(s.Labels, func(i, j int) bool {
			return s.Labels[i].Name < s.Labels[j].Name
		})

		for _, cm := range e.chunks {
			s.Chunks = append(s.Chunks, storepb.Chunk{
				Type:    storepb.Chunk_XOR,
				MinTime: cm.MinTime,
				MaxTime: cm.MaxTime,
				Data:    cm.Chunk.Bytes(),
			})
		}
		sort.Slice(s.Chunks, func(i, j int) bool {
			return s.Chunks[i].MinTime < s.Chunks[j].MinTime
		})
		res = append(res, s)
	}
	sort.Slice(res, func(i, j int) bool {
		return storepb.CompareLabels(res[i].Labels, res[j].Labels) < 0
	})
	return res, nil
}

// Series implements the storepb.StoreServer interface.
func (s *GCSStore) Series(ctx context.Context, req *storepb.SeriesRequest) (*storepb.SeriesResponse, error) {
	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var g errgroup.Group

	res := &seriesResult{res: map[uint64]*seriesEntry{}}

	s.mtx.RLock()

	for _, b := range s.blocks {
		if !b.matches(req.MinTime, req.MaxTime, matchers...) {
			continue
		}
		var (
			extLset = b.meta.Thanos.Labels
			indexr  = b.indexReader()
			chunkr  = b.chunkReader()
		)
		// TODO(fabxc): only aggregate chunk metas first and add a subsequent fetch stage
		// where we consolidate requests.
		g.Go(func() error {
			defer indexr.Close()
			defer chunkr.Close()

			set, err := tsdb.LookupChunkSeries(indexr, nil, matchers...)
			if err != nil {
				return errors.Wrap(err, "get series set")
			}
			var chks []tsdb.ChunkMeta

			for set.Next() {
				chks = chks[:0]
				lset, chunks, _ := set.At()

				for _, meta := range chunks {
					if meta.MaxTime < req.MinTime {
						continue
					}
					if meta.MinTime > req.MaxTime {
						break
					}
					meta.Chunk, err = chunkr.Chunk(meta.Ref)
					if err != nil {
						return errors.Wrap(err, "fetch chunk")
					}
					chks = append(chks, meta)
				}
				res.add(lset, extLset, chks)
			}
			if err := set.Err(); err != nil {
				return errors.Wrap(err, "read series set")
			}
			return nil
		})
	}

	s.mtx.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	series, err := res.response()
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	return &storepb.SeriesResponse{Series: series}, nil
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
	meta           *block.Meta
	dir            string
	pendingReaders sync.WaitGroup
	index          tsdb.IndexReader
	chunks         tsdb.ChunkReader
}

func newGCSBlock(
	ctx context.Context,
	meta *block.Meta,
	index tsdb.IndexReader,
	bkt *storage.BucketHandle,
) (*gcsBlock, error) {
	cr, err := newGCSChunkReader(ctx, meta.ULID, bkt)
	if err != nil {
		return nil, err
	}
	return &gcsBlock{
		meta:   meta,
		index:  index,
		chunks: cr,
	}, nil
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

func (b *gcsBlock) chunkReader() tsdb.ChunkReader {
	b.pendingReaders.Add(1)
	return &closeChunks{b.chunks, b.pendingReaders.Done}
}

type closeIndex struct {
	tsdb.IndexReader
	close func()
}

func (c *closeIndex) Close() error {
	c.close()
	return nil
}

type closeChunks struct {
	tsdb.ChunkReader
	close func()
}

func (c *closeChunks) Close() error {
	c.close()
	return nil
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *gcsBlock) Close() error {
	b.pendingReaders.Wait()
	b.index.Close()
	b.chunks.Close()
	return nil
}

type gcsChunkReader struct {
	id    ulid.ULID
	bkt   *storage.BucketHandle
	files []*storage.ObjectHandle
}

func newGCSChunkReader(ctx context.Context, id ulid.ULID, bkt *storage.BucketHandle) (*gcsChunkReader, error) {
	r := &gcsChunkReader{
		id:  id,
		bkt: bkt,
	}
	objs := bkt.Objects(ctx, &storage.Query{
		Prefix: path.Join(id.String(), "chunks/"),
	})
	for {
		oi, err := objs.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "list chunk files")
		}
		r.files = append(r.files, bkt.Object(oi.Name))
	}
	return r, nil
}

func (r *gcsChunkReader) Chunk(id uint64) (chunks.Chunk, error) {
	var (
		seq = int(id >> 32)
		off = int((id << 32) >> 32)
	)
	if seq >= len(r.files) {
		return nil, errors.Errorf("reference sequence %d out of range", seq)
	}
	obj := r.files[seq]

	// We don't know the length of the chunk, load 1024 bytes, which should be enough
	// even for long ones.
	rd, err := obj.NewRangeReader(context.TODO(), int64(off), int64(off+1024))
	if err != nil {
		return nil, errors.Wrap(err, "create reader")
	}
	defer r.Close()

	b := make([]byte, 1024)
	if _, err := rd.Read(b); err != nil {
		return nil, errors.Wrap(err, "fetch chunk range")
	}

	l, n := binary.Uvarint(b)
	if n < 0 {
		return nil, errors.Errorf("reading chunk length failed")
	}
	if len(b) < n+int(l)+1 {
		return nil, errors.Errorf("preloaded chunk too small, expecting %d", n+int(l))
	}
	b = b[n : n+int(l)+1]

	return chunks.FromData(chunks.Encoding(b[0]), b[1:1+l])
}

func (r *gcsChunkReader) Close() error {
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
