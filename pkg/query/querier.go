package query

import (
	"context"
	"sync"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"

	"github.com/improbable-eng/promlts/pkg/store/storepb"
	"github.com/pkg/errors"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var _ promql.Queryable = (*Queryable)(nil)

type Queryable struct {
	logger         log.Logger
	storeAddresses []string
}

// NewQueryable creates implementation of promql.Queryable that fetches data from the given
// store API endpoints.
func NewQueryable(logger log.Logger, storeAddresses []string) *Queryable {
	return &Queryable{
		logger:         logger,
		storeAddresses: storeAddresses,
	}
}

func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(q.logger, ctx, q.storeAddresses, mint, maxt), nil
}

type querier struct {
	logger         log.Logger
	ctx            context.Context
	cancel         func()
	mint, maxt     int64
	storeAddresses []string
}

// newQuerier creates implementation of storage.Querier that fetches data from the given
// store API endpoints.
func newQuerier(logger log.Logger, ctx context.Context, storeAddresses []string, mint, maxt int64) *querier {
	ctx, cancel := context.WithCancel(ctx)

	return &querier{
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
		storeAddresses: storeAddresses,
		mint:           mint,
		maxt:           maxt,
	}
}

func (q *querier) Select(ms ...*promlabels.Matcher) storage.SeriesSet {
	var (
		mtx sync.Mutex
		all []tsdb.SeriesSet
		// TODO(fabxc): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)

	sms, err := translateMatchers(ms...)
	if err != nil {
		return promSeriesSet{set: errSeriesSet{err: err}}
	}
	for _, s := range q.storeAddresses {
		g.Go(func() error {
			set, err := q.selectSingle(s, sms...)
			if err != nil {
				return err
			}

			mtx.Lock()
			all = append(all, set)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return promSeriesSet{errSeriesSet{err: err}}
	}
	return promSeriesSet{set: mergeAllSeriesSets(all...)}
}

func mergeAllSeriesSets(all ...tsdb.SeriesSet) tsdb.SeriesSet {
	switch len(all) {
	case 0:
		return errSeriesSet{err: nil}
	case 1:
		return all[0]
	}
	h := len(all) / 2

	return tsdb.NewMergedSeriesSet(
		mergeAllSeriesSets(all[:h]...),
		mergeAllSeriesSets(all[h:]...),
	)
}

func (q *querier) selectSingle(addr string, ms ...storepb.LabelMatcher) (tsdb.SeriesSet, error) {
	conn, err := grpc.DialContext(q.ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrap(err, "grpc dial connection")
	}
	defer conn.Close()

	c := storepb.NewStoreClient(conn)

	resp, err := c.Series(q.ctx, &storepb.SeriesRequest{
		MinTime:  q.mint,
		MaxTime:  q.maxt,
		Matchers: ms,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	return &storeSeriesSet{series: resp.Series, i: -1, mint: q.mint, maxt: q.maxt}, nil
}

func (*querier) LabelValues(name string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (*querier) LabelValuesFor(string, labels.Label) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}

func translateChunk(c storepb.Chunk) (tsdb.ChunkMeta, error) {
	if c.Type != storepb.Chunk_XOR {
		return tsdb.ChunkMeta{}, errors.Errorf("unrecognized chunk encoding %d", c.Type)
	}
	cc, err := chunks.FromData(chunks.EncXOR, c.Data)
	if err != nil {
		return tsdb.ChunkMeta{}, errors.Wrap(err, "convert chunk")
	}
	return tsdb.ChunkMeta{MinTime: c.MinTime, MaxTime: c.MaxTime, Chunk: cc}, nil
}

type errSeriesSet struct {
	err error
}

var _ tsdb.SeriesSet = (*errSeriesSet)(nil)

func (errSeriesSet) Next() bool      { return false }
func (s errSeriesSet) Err() error    { return s.err }
func (errSeriesSet) At() tsdb.Series { return nil }

type storeSeriesSet struct {
	series     []storepb.Series
	mint, maxt int64

	i   int
	cur *storeSeries
}

var _ tsdb.SeriesSet = (*storeSeriesSet)(nil)

func (s *storeSeriesSet) Next() bool {
	if s.i >= len(s.series)-1 {
		return false
	}
	s.i++
	// Skip empty series.
	if len(s.series[s.i].Chunks) == 0 {
		return s.Next()
	}
	s.cur = &storeSeries{s: s.series[s.i], mint: s.mint, maxt: s.maxt}
	return true
}

func (storeSeriesSet) Err() error {
	return nil
}

func (s storeSeriesSet) At() tsdb.Series {
	return s.cur
}

// storeSeries implements storage.Series for a series retrieved from the store API.
type storeSeries struct {
	s          storepb.Series
	mint, maxt int64
}

var _ tsdb.Series = (*storeSeries)(nil)

func (s *storeSeries) Labels() labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&s.s.Labels)) // YOLO!
}

func (s *storeSeries) Iterator() tsdb.SeriesIterator {
	return newChunkSeriesIterator(s.s.Chunks, s.mint, s.maxt)
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) bool      { return false }
func (errSeriesIterator) Next() bool           { return false }
func (errSeriesIterator) At() (int64, float64) { return 0, 0 }
func (s errSeriesIterator) Err() error         { return s.err }

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks     []tsdb.ChunkMeta
	maxt, mint int64

	i   int
	cur chunks.Iterator
}

func newChunkSeriesIterator(cs []storepb.Chunk, mint, maxt int64) storage.SeriesIterator {
	cms := make([]tsdb.ChunkMeta, 0, len(cs))

	for _, c := range cs {
		tc, err := translateChunk(c)
		if err != nil {
			return errSeriesIterator{err: err}
		}
		cms = append(cms, tc)
	}

	it := cms[0].Chunk.Iterator()

	return &chunkSeriesIterator{
		chunks: cms,
		i:      0,
		cur:    it,

		mint: mint,
		maxt: maxt,
	}
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	if t > it.maxt {
		return false
	}

	// Seek to the first valid value after t.
	if t < it.mint {
		t = it.mint
	}

	for ; it.chunks[it.i].MaxTime < t; it.i++ {
		if it.i == len(it.chunks)-1 {
			return false
		}
	}

	it.cur = it.chunks[it.i].Chunk.Iterator()

	for it.cur.Next() {
		t0, _ := it.cur.At()
		if t0 >= t {
			return true
		}
	}
	return false
}

func (it *chunkSeriesIterator) At() (t int64, v float64) {
	return it.cur.At()
}

func (it *chunkSeriesIterator) Next() bool {
	if it.cur.Next() {
		t, _ := it.cur.At()

		if t < it.mint {
			if !it.Seek(it.mint) {
				return false
			}
			t, _ = it.At()

			return t <= it.maxt
		}
		if t > it.maxt {
			return false
		}
		return true
	}
	if err := it.cur.Err(); err != nil {
		return false
	}
	if it.i == len(it.chunks)-1 {
		return false
	}

	it.i++
	it.cur = it.chunks[it.i].Chunk.Iterator()

	return it.Next()
}

func (it *chunkSeriesIterator) Err() error {
	return it.cur.Err()
}
