package query

import (
	"github.com/prometheus/tsdb/chunks"
	"net/http"
	"sync"

	"context"

	"github.com/improbable-eng/promlts/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"google.golang.org/grpc"
)

var _ promql.Queryable = (*Queryable)(nil)
var _ storage.Querier = (*querier)(nil)
var _ storage.SeriesSet = (*seriesSet)(nil)

type Queryable struct {
	client         *http.Client
	storeAddresses []string
}

// NewQueryable creates implementation of promql.Queryable that uses given HTTP client
// to talk to each store node.
func NewQueryable(ctx context.Context, storeAddresses []string) *Queryable {
	return &Queryable{
		storeAddresses: storeAddresses,
	}
}

func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(ctx, q.storeAddresses, mint, maxt), nil
}

type querier struct {
	ctx            context.Context
	cancel         func()
	mint, maxt     int64
	storeAddresses []string
}

// newQuerier creates implementation of storage.Querier that uses given HTTP client
// to talk to each store node.
func newQuerier(ctx context.Context, storeAddresses []string, mint, maxt int64) *querier {
	ctx, cancel := context.WithCancel(ctx)

	return &querier{
		ctx:            ctx,
		cancel:         cancel,
		storeAddresses: storeAddresses,
		mint:           mint,
		maxt:           maxt,
	}
}

func (q *querier) Select(ms ...*labels.Matcher) storage.SeriesSet {
	var (
		wg  sync.WaitGroup
		mtx sync.Mutex
		all []storage.SeriesSet
	)
	wg.Add(len(q.storeAddresses))

	sms, err := translateMatchers(ms...)
	if err != nil {
		return errSeriesSet{err: err}
	}
	for _, s := range q.storeAddresses {
		go func(s string) {
			set := q.selectSingle(sms...)
			mtx.Lock()
			all = append(all, set)
			mtx.Unlock()
		}(s)
	}
	return newMergedSeriesSet(all...)
}

func (q *querier) selectSingle(addr string, ms ...storepb.LabelMatcher) storage.SeriesSet {
	conn, err := grpc.DialContext(q.ctx, addr, grpc.WithInsecure())
	if err != nil {
		return errSeriesSet{err: err}
	}
	c := storepb.NewStoreClient(conn)

	resp, err := c.Series(q.ctx, &storepb.SeriesRequest{
		MinTime:  q.mint,
		MaxTime:  q.maxt,
		Matchers: ms,
	})
	if err != nil {
		return errSeriesSet{err: err}
	}
	return &staticSeriesSet{series: resp.Series, i: -1}
}

func translateMatchers(in ...*labels.Matcher) ([]storepb.LabelMatcher, error) {
	out := make([]storepb.LabelMatcher, 0, len(in))

	for _, m := range in {
		var t storepb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			t = storepb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = storepb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = storepb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = storepb.LabelMatcher_NRE
		}
		out = append(out, storepb.LabelMatcher{
			Name:  m.Name,
			Value: m.Value,
			Type:  t,
		})
	}
	return out, nil
}

type errSeriesSet struct {
	err error
}

var _ storage.SeriesSet = (*errSeriesSet)(nil)

func (errSeriesSet) Next() bool         { return false }
func (s errSeriesSet) Err() error       { return s.err }
func (errSeriesSet) At() storage.Series { return nil }

type storeSeriesSet struct {
	series []storepb.Series
	i      int
}

var _ storage.SeriesSet = (*storeSeriesSet)(nil)

func (s *storeSeriesSet) Next() bool {
	if s.i >= len(s.series)-1 {
		return false
	}
	s.i++
	return true
}

func (storeSeriesSet) Err() error {
	return nil
}

func (s storeSeriesSet) At() storage.Series {
	return s.series[s.i]
}

type storeSeries struct {
	lset labels.Labels
	s    storepb.Series
}

var _ storage.Series = (*storeSeries)(nil)

func newStoreSeries(s storepb.Series) *storeSeries {
	lset := translateLabels(s)
	return &storeSeries{lset: lset, s: s}
}

func translateLabels(lset []storepb.Label) labels.Labels {

}

func (s *storeSeries) Labels() labels.Labels {
	return s.lset
}

func (s *storeSeries) Iterator() storage.SeriesIterator {
	return s.s.
}

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunks.Chunk

	i   int
	cur chunks.Iterator

	maxt, mint int64
}

func newChunkSeriesIterator(cs []ChunkMeta, dranges Intervals, mint, maxt int64) *chunkSeriesIterator {
	it := cs[0].Chunk.Iterator()

	if len(dranges) > 0 {
		it = &deletedIterator{it: it, intervals: dranges}
	}
	return &chunkSeriesIterator{
		chunks: cs,
		i:      0,
		cur:    it,

		mint: mint,
		maxt: maxt,

		intervals: dranges,
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
	if len(it.intervals) > 0 {
		it.cur = &deletedIterator{it: it.cur, intervals: it.intervals}
	}

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
	if len(it.intervals) > 0 {
		it.cur = &deletedIterator{it: it.cur, intervals: it.intervals}
	}

	return it.Next()
}

func (it *chunkSeriesIterator) Err() error {
	return it.cur.Err()
}



func (*querier) LabelValues(name string) ([]string, error) {

	return nil, errors.New("not implemented")
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}

type seriesSet struct{}

func newSeriesSet() *seriesSet {
	return &seriesSet{}
}

func (*seriesSet) Next() bool {
	return false
}

func (*seriesSet) At() storage.Series {
	return nil
}

func (*seriesSet) Err() error {
	return errors.New("not implemented")
}
