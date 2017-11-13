package store

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
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
	s := &GCSStore{
		logger: logger,
		bucket: bucket,
		dir:    dir,
		blocks: map[ulid.ULID]*gcsBlock{},
	}

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, errors.Wrap(err, "create dir")
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read dir")
	}

	for _, fi := range files {
		if !fi.IsDir() {
			continue
		}
		id, err := ulid.Parse(fi.Name())
		if err != nil {
			continue
		}
		if _, err := s.loadFromDisk(id); err != nil {
			level.Warn(logger).Log("msg", "loading block from disk failed; deleting", "id", id)
			// Since the disk just acts as a persistent cache, simply deleting
			// the corrupted directory is fine.
			os.RemoveAll(filepath.Join(dir, fi.Name()))
		}
	}
	return s, nil
}

// Close the store.
func (s *GCSStore) Close() (err error) {
	for _, b := range s.blocks {
		if e := b.indexr.Close(); e != nil {
			level.Warn(s.logger).Log("msg", "closing GCS block failed", "err", err)
			err = e
		}
	}
	return err
}

// SyncBlocks synchronizes the stores state with the GCS bucket.
func (s *GCSStore) SyncBlocks(ctx context.Context) {
	// NOTE(fabxc): watches are not yet supported by the Go client library so we just
	// do a periodic refresh.
	err := runutil.Repeat(60*time.Second, ctx.Done(), func() error {
		if err := s.syncBlocks(ctx); err != nil {
			level.Warn(s.logger).Log("msg", "syncing blocks failed", "err", err)
		}
		return nil
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "unexpected error", "err", err)
	}
}

func (s *GCSStore) syncBlocks(ctx context.Context) error {
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

		if err := s.syncBlock(ctx, id); err != nil {
			level.Error(s.logger).Log("msg", "syncing block failed", "err", err, "block", id.String())
		}
	}
	return nil
}

// loadFromDisk loads a block with the given ID from the disk cache.
func (s *GCSStore) loadFromDisk(id ulid.ULID) (*gcsBlock, error) {
	dir := filepath.Join(s.dir, id.String())

	indexr, err := tsdb.NewFileIndexReader(filepath.Join(dir, "index"))
	if err != nil {
		return nil, errors.Wrap(err, "open index reader")
	}
	b := &gcsBlock{indexr: indexr, dir: dir}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.blocks[id] = b
	return b, nil
}

// syncBlock downloads the index and meta.json file for the given block ID and opens a reader
// against the block.
func (s *GCSStore) syncBlock(ctx context.Context, id ulid.ULID) error {
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
	_, err = io.Copy(f, r)

	if err := f.Close(); err != nil {
		level.Warn(s.logger).Log("msg", "close file", "err", err)
	}
	if err := r.Close(); err != nil {
		level.Warn(s.logger).Log("msg", "close object reader", "err", err)
	}
	if err != nil {
		if err := os.RemoveAll(tmpdir); err != nil {
			level.Warn(s.logger).Log("msg", "cleanup temp dir after failure", "err", err)
		}
		return errors.Wrap(err, "copy index file to disk")
	}

	if err := renameFile(tmpdir, bdir); err != nil {
		return errors.Wrap(err, "rename block directory")
	}

	_, err = s.loadFromDisk(id)
	return errors.Wrap(err, "load synced block")
}

type gcsBlock struct {
	dir    string
	indexr tsdb.IndexReader
	chunks tsdb.ChunkReader
}

// Info implements the storepb.StoreServer interface.
func (s *GCSStore) Info(context.Context, *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	// Store nodes hold global data and thus have no labels.
	return &storepb.InfoResponse{}, nil
}

// Series implements the storepb.StoreServer interface.
func (s *GCSStore) Series(ctx context.Context, req *storepb.SeriesRequest) (*storepb.SeriesResponse, error) {
	resp := &storepb.SeriesResponse{}

	matchers, err := translateMatchers(req.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var g errgroup.Group
	// TODO(fabxc): filter inspected blocks by labels.

	for _, b := range s.blocks {
		b := b

		// TODO(fabxc): only aggregate chunk metas first and add a subsequent fetch stage
		// where we consolidate requests.
		g.Go(func() error {
			set, err := tsdb.LookupChunkSeries(b.indexr, nil, matchers...)
			if err != nil {
				return errors.Wrap(err, "get series set")
			}
			for set.Next() {
				lset, chunks, _ := set.At()

				series := &storepb.Series{
					Labels: make([]storepb.Label, 0, len(lset)),
					Chunks: make([]storepb.Chunk, 0, len(chunks)),
				}
				for _, l := range lset {
					series.Labels = append(series.Labels, storepb.Label{
						Name:  l.Name,
						Value: l.Value,
					})
				}
				for _, meta := range chunks {
					c, err := b.chunks.Chunk(meta.Ref)
					if err != nil {
						return errors.Wrap(err, "fetch chunk")
					}
					// TODO(fabxc): validate encoding.
					series.Chunks = append(series.Chunks, storepb.Chunk{
						Type:    storepb.Chunk_XOR,
						MinTime: meta.MinTime,
						MaxTime: meta.MaxTime,
						Data:    c.Bytes(),
					})
				}
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
	return resp, nil
}

// LabelNames implements the storepb.StoreServer interface.
func (s *GCSStore) LabelNames(context.Context, *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues implements the storepb.StoreServer interface.
func (s *GCSStore) LabelValues(context.Context, *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{}, nil
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
