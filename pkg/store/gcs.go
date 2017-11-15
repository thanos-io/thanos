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
		if _, ok := s.blocks[id]; ok {
			continue
		}
		level.Info(s.logger).Log("msg", "sync block from GCS", "id", id)

		if err := s.downloadBlock(ctx, id); err != nil {
			level.Error(s.logger).Log("msg", "syncing block failed", "err", err, "block", id.String())
		}
	}
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

	indexObj := s.bucket.Object(path.Join(id.String(), "index"))

	f, err := os.Create(filepath.Join(tmpdir, "index"))
	if err != nil {
		return errors.Wrap(err, "create local index copy")
	}
	r, err := indexObj.NewReader(ctx)
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
		if _, ok := s.blocks[id]; ok {
			continue
		}
		b, err := s.loadFromDisk(id)
		if err != nil {
			level.Warn(s.logger).Log("msg", "loading block failed", "err", err)
		}
		s.mtx.Lock()
		s.blocks[id] = b
		s.mtx.Unlock()
	}
	return nil
}

func (s *GCSStore) numBlocks() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.blocks)
}

// loadFromDisk loads a block with the given ID from the disk cache.
func (s *GCSStore) loadFromDisk(id ulid.ULID) (*gcsBlock, error) {
	dir := filepath.Join(s.dir, id.String())

	indexr, err := tsdb.NewFileIndexReader(filepath.Join(dir, "index"))
	if err != nil {
		return nil, errors.Wrap(err, "open index reader")
	}
	b, err := newGCSBlock(context.TODO(), id, indexr, s.bucket)
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
	mtx sync.RWMutex
	res map[uint64]*seriesEntry
}

type seriesEntry struct {
	lset   labels.Labels
	chunks []tsdb.ChunkMeta
}

func (r *seriesResult) add(lset labels.Labels, chks []tsdb.ChunkMeta) {
	if len(chks) == 0 {
		return
	}
	h := lset.Hash()

	r.mtx.Lock()
	defer r.mtx.Unlock()

	e, ok := r.res[h]
	if !ok {
		e = &seriesEntry{lset: lset}
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
			s.Labels = append(s.Labels, storepb.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
		for _, cm := range e.chunks {
			s.Chunks = append(s.Chunks, storepb.Chunk{
				Type:    storepb.Chunk_XOR,
				MinTime: cm.MinTime,
				MaxTime: cm.MaxTime,
				Data:    cm.Chunk.Bytes(),
			})
		}
		sort.Slice(s.Chunks, func(i, j int) bool {
			return s.Chunks[i].MinTime < s.Chunks[j].MaxTime
		})
		// We do not yet handle overlapping chunks in the querying layer and should only
		// have blocks disjoint in time and series space.
		// Error early here until the query layer can handle it.
		maxTime := s.Chunks[0].MaxTime
		for _, c := range s.Chunks[1:] {
			if c.MinTime <= maxTime {
				return nil, errors.Errorf("chunks overlap: max %d, min %d", maxTime, c.MinTime)
			}
			maxTime = c.MaxTime
		}

		res = append(res, s)
	}
	sort.Slice(res, func(i, j int) bool {
		return compareLabels(res[i].Labels, res[j].Labels) < 0
	})
	return res, nil
}

func compareLabels(a, b []storepb.Label) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		if d := strings.Compare(a[i].Name, b[i].Name); d != 0 {
			return d
		}
		if d := strings.Compare(a[i].Value, b[i].Value); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

// Series implements the storepb.StoreServer interface.
func (s *GCSStore) Series(ctx context.Context, req *storepb.SeriesRequest) (*storepb.SeriesResponse, error) {
	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var g errgroup.Group
	// TODO(fabxc): filter inspected blocks by labels.

	res := &seriesResult{res: map[uint64]*seriesEntry{}}

	for _, b := range s.blocks {
		block := b

		// TODO(fabxc): only aggregate chunk metas first and add a subsequent fetch stage
		// where we consolidate requests.
		g.Go(func() error {
			set, err := tsdb.LookupChunkSeries(block.index, nil, matchers...)
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
					meta.Chunk, err = block.chunks.Chunk(meta.Ref)
					if err != nil {
						return errors.Wrap(err, "fetch chunk")
					}
					chks = append(chks, meta)
				}
				res.add(lset, chks)
			}
			if err := set.Err(); err != nil {
				return errors.Wrap(err, "read series set")
			}
			return nil
		})
	}
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
func (s *GCSStore) LabelValues(context.Context, *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{}, nil
}

type gcsBlock struct {
	id     ulid.ULID
	dir    string
	index  tsdb.IndexReader
	chunks tsdb.ChunkReader
}

func newGCSBlock(
	ctx context.Context,
	id ulid.ULID,
	index tsdb.IndexReader,
	bkt *storage.BucketHandle,
) (*gcsBlock, error) {
	cr, err := newGCSChunkReader(ctx, id, bkt)
	if err != nil {
		return nil, err
	}
	return &gcsBlock{
		id:     id,
		index:  index,
		chunks: cr,
	}, nil
}

func (b *gcsBlock) Close() error {
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
