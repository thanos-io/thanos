package query

import (
	"context"
	"io"
	"sort"
	"sync"
	"unsafe"

	"github.com/go-kit/kit/log"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/strutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"
)

var _ promql.Queryable = (*Queryable)(nil)

// StoreInfo holds meta information about a store used by query.
type StoreInfo interface {
	// Client to access the store.
	Client() storepb.StoreClient

	// Labels returns store labels that should be appended to every metric returned by this store.
	Labels() []storepb.Label
}

// Queryable allows to open a querier against a dynamic set of stores.
type Queryable struct {
	logger       log.Logger
	stores       func() []StoreInfo
	replicaLabel string
}

// NewQueryable creates implementation of promql.Queryable that fetches data from the given
// store API endpoints.
// All data retrieved from store nodes will be deduplicated along the replicaLabel by default.
func NewQueryable(logger log.Logger, stores func() []StoreInfo, replicaLabel string) *Queryable {
	return &Queryable{
		logger:       logger,
		stores:       stores,
		replicaLabel: replicaLabel,
	}
}

// Querier returns a new storage querier against the underlying stores.
func (q *Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(ctx, q.logger, q.stores(), mint, maxt, q.replicaLabel), nil
}

type querier struct {
	logger       log.Logger
	ctx          context.Context
	cancel       func()
	mint, maxt   int64
	stores       []StoreInfo
	replicaLabel string
}

// newQuerier creates implementation of storage.Querier that fetches data from the given
// store API endpoints.
func newQuerier(
	ctx context.Context,
	logger log.Logger,
	stores []StoreInfo,
	mint, maxt int64,
	replicaLabel string,
) *querier {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &querier{
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		mint:         mint,
		maxt:         maxt,
		stores:       stores,
		replicaLabel: replicaLabel,
	}
}

// matchStore returns true iff the given store may hold data for the given label matchers.
func storeMatches(s StoreInfo, matchers ...*labels.Matcher) bool {
	for _, m := range matchers {
		for _, l := range s.Labels() {
			if l.Name != m.Name {
				continue
			}
			if !m.Matches(l.Value) {
				return false
			}
		}
	}
	return true
}

func (q *querier) Select(ms ...*labels.Matcher) storage.SeriesSet {
	var (
		mtx sync.Mutex
		all []storepb.SeriesSet
		// TODO(fabxc): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)

	sms, err := translateMatchers(ms...)
	if err != nil {
		return promSeriesSet{set: errSeriesSet{err: err}}
	}
	for _, s := range q.stores {
		// We might be able to skip the store if its meta information indicates
		// it cannot have series matching our query.
		if !storeMatches(s, ms...) {
			continue
		}
		store := s

		g.Go(func() error {
			set, err := q.selectSingle(store.Client(), sms...)
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
		return promSeriesSet{set: errSeriesSet{err: err}}
	}
	set := promSeriesSet{
		mint: q.mint,
		maxt: q.maxt,
		set:  storepb.MergeSeriesSets(all...),
	}
	// The merged series set assembles all potentially-overlapping time ranges
	// of the same series into a single one. The series are ordered so that equal series
	// from different replicas are sequential. We can now deduplicate those.
	if q.replicaLabel == "" {
		return set
	}
	return newDedupSeriesSet(set, q.replicaLabel)
}

func (q *querier) selectSingle(client storepb.StoreClient, ms ...storepb.LabelMatcher) (storepb.SeriesSet, error) {
	sc, err := client.Series(q.ctx, &storepb.SeriesRequest{
		MinTime:  q.mint,
		MaxTime:  q.maxt,
		Matchers: ms,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	var set []storepb.Series

	for {
		r, err := sc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		set = append(set, r.Series)
	}
	res := newStoreSeriesSet(set)

	if q.replicaLabel == "" {
		return res, nil
	}
	// Resort the result so that the same series with different replica
	// labels are coming right after each other.
	// TODO(fabxc): this could potentially pushed further down into the store API
	// to make true streaming possible.
	for _, s := range set {
		// Move the replica label to the very end.
		sort.Slice(s.Labels, func(i, j int) bool {
			if s.Labels[i].Name == q.replicaLabel {
				return false
			}
			if s.Labels[j].Name == q.replicaLabel {
				return true
			}
			return s.Labels[i].Name < s.Labels[j].Name
		})
	}
	// With the re-ordered label sets, re-sorting all series aligns the same series
	// from different replicas sequentially.
	sort.Slice(set, func(i, j int) bool {
		return storepb.CompareLabels(set[i].Labels, set[j].Labels) < 0
	})
	return res, nil
}

func (q *querier) LabelValues(name string) ([]string, error) {
	var (
		mtx sync.Mutex
		all [][]string
		// TODO(bplotka): errgroup will fail the whole query on the first encountered error.
		// Add support for partial results/errors.
		g errgroup.Group
	)

	for _, s := range q.stores {
		store := s

		g.Go(func() error {
			values, err := q.labelValuesSingle(store.Client(), name)
			if err != nil {
				return err
			}

			mtx.Lock()
			all = append(all, values)
			mtx.Unlock()

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return strutil.MergeUnsortedSlices(all...), nil
}

func (q *querier) labelValuesSingle(client storepb.StoreClient, name string) ([]string, error) {
	resp, err := client.LabelValues(q.ctx, &storepb.LabelValuesRequest{
		Label: name,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetch series")
	}
	return resp.Values, nil
}

func (q *querier) Close() error {
	q.cancel()
	return nil
}

// promSeriesSet implements the SeriesSet interface of the Prometheus storage
// package on top of our storepb SeriesSet.
type promSeriesSet struct {
	set        storepb.SeriesSet
	mint, maxt int64
}

func (s promSeriesSet) Next() bool { return s.set.Next() }
func (s promSeriesSet) Err() error { return s.set.Err() }

func (s promSeriesSet) At() storage.Series {
	lset, chunks := s.set.At()
	return newChunkSeries(lset, chunks, s.mint, s.maxt)
}

func translateMatcher(m *labels.Matcher) (storepb.LabelMatcher, error) {
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
	default:
		return storepb.LabelMatcher{}, errors.Errorf("unrecognized matcher type %d", m.Type)
	}
	return storepb.LabelMatcher{Type: t, Name: m.Name, Value: m.Value}, nil
}

func translateMatchers(ms ...*labels.Matcher) ([]storepb.LabelMatcher, error) {
	res := make([]storepb.LabelMatcher, 0, len(ms))
	for _, m := range ms {
		r, err := translateMatcher(m)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool                             { return false }
func (s errSeriesSet) Err() error                           { return s.err }
func (errSeriesSet) At() ([]storepb.Label, []storepb.Chunk) { return nil, nil }

// storeSeriesSet implements a storepb SeriesSet against a list of storepb.Series.
type storeSeriesSet struct {
	series []storepb.Series
	i      int
}

func newStoreSeriesSet(s []storepb.Series) *storeSeriesSet {
	return &storeSeriesSet{series: s, i: -1}
}

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

func (s storeSeriesSet) At() ([]storepb.Label, []storepb.Chunk) {
	ser := s.series[s.i]
	return ser.Labels, ser.Chunks
}

// chunkSeries implements storage.Series for a series on storepb types.
type chunkSeries struct {
	lset       labels.Labels
	chunks     []storepb.Chunk
	mint, maxt int64
}

func newChunkSeries(lset []storepb.Label, chunks []storepb.Chunk, mint, maxt int64) *chunkSeries {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].MinTime < chunks[j].MinTime
	})
	l := *(*labels.Labels)(unsafe.Pointer(&lset)) // YOLO!

	return &chunkSeries{lset: l, chunks: chunks, mint: mint, maxt: maxt}
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator() storage.SeriesIterator {
	return newChunkSeriesIterator(s.chunks, s.mint, s.maxt)
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) bool      { return false }
func (errSeriesIterator) Next() bool           { return false }
func (errSeriesIterator) At() (int64, float64) { return 0, 0 }
func (s errSeriesIterator) Err() error         { return s.err }

type nopSeriesIterator struct{}

func (nopSeriesIterator) Seek(int64) bool      { return false }
func (nopSeriesIterator) Next() bool           { return false }
func (nopSeriesIterator) At() (int64, float64) { return 0, 0 }
func (nopSeriesIterator) Err() error           { return nil }

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks     []storepb.Chunk
	maxt, mint int64

	i   int
	cur chunks.Iterator
	err error
}

func newChunkSeriesIterator(cs []storepb.Chunk, mint, maxt int64) storage.SeriesIterator {
	if len(cs) == 0 {
		// This should not happen. StoreAPI implementations should not send empty results.
		// NOTE(bplotka): Metric, err log here?
		return nopSeriesIterator{}
	}
	it := &chunkSeriesIterator{
		chunks: cs,
		i:      0,
		mint:   mint,
		maxt:   maxt,
	}
	it.openChunk()
	return it
}

func (it *chunkSeriesIterator) openChunk() bool {
	c, err := chunks.FromData(chunks.EncXOR, it.chunks[it.i].Data)
	if err != nil {
		it.err = err
		return false
	}
	it.cur = c.Iterator()
	return true
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
	if it.err != nil {
		return false
	}
	if t > it.maxt {
		return false
	}
	// We generally expect the chunks already to be cut down
	// to the range we are interested in. There's not much to be gained from
	// hopping across chunks so we just call next until we reach t.
	for {
		ct, _ := it.At()
		if ct >= t {
			return true
		}
		if !it.Next() {
			return false
		}
	}
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

	lastT, _ := it.At()

	it.i++
	if !it.openChunk() {
		return false
	}
	// Ensure we don't go back in time.
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.cur.Err()
}

type dedupSeriesSet struct {
	set          storage.SeriesSet
	replicaLabel string

	// Gathered replica series so far.
	replicas []storage.Series
	lset     labels.Labels
	peek     storage.Series
	// lastReplica string
	ok bool
}

func newDedupSeriesSet(set storage.SeriesSet, replicaLabel string) storage.SeriesSet {
	s := &dedupSeriesSet{set: set, replicaLabel: replicaLabel}
	s.ok = s.set.Next()
	if s.ok {
		s.peek = s.set.At()
	}
	return s
}

func (s *dedupSeriesSet) Next() bool {
	if !s.ok {
		return false
	}
	// Set the label set we are currently gathering to the peek element
	// without the replica label if it exists.
	s.lset = s.peekLset()
	s.replicas = append(s.replicas[:0], s.peek)
	s.peek = nil
	return s.next()
}

// peekLset returns the label set of the current peek element stripped from the
// replica label if it exists
func (s *dedupSeriesSet) peekLset() labels.Labels {
	lset := s.peek.Labels()
	if lset[len(lset)-1].Name != s.replicaLabel {
		return lset
	}
	return lset[:len(lset)-1]
}

func (s *dedupSeriesSet) next() bool {
	// Peek the next series to see whether it's a replica for the current series.
	s.ok = s.set.Next()
	if !s.ok {
		// There's no next series, the current replicas are the last element.
		return len(s.replicas) > 0
	}
	s.peek = s.set.At()
	nextLset := s.peekLset()

	// If the label set modulo the replica label is equal to the current label set
	// look for more replicas, otherwise a series is complete.
	if !labels.Equal(s.lset, nextLset) {
		return true
	}
	s.replicas = append(s.replicas, s.peek)
	return s.next()
}

func (s *dedupSeriesSet) At() storage.Series {
	if len(s.replicas) == 1 {
		return seriesWithLabels{Series: s.replicas[0], lset: s.lset}
	}
	return newDedupSeries(s.lset, s.replicas...)
}

func (s *dedupSeriesSet) Err() error {
	return s.set.Err()
}

type seriesWithLabels struct {
	storage.Series
	lset labels.Labels
}

func (s seriesWithLabels) Labels() labels.Labels { return s.lset }

type dedupSeries struct {
	lset     labels.Labels
	replicas []storage.Series
}

func newDedupSeries(lset labels.Labels, replicas ...storage.Series) *dedupSeries {
	return &dedupSeries{lset: lset, replicas: replicas}
}

func (s *dedupSeries) Labels() labels.Labels {
	return s.lset
}

func (s *dedupSeries) Iterator() storage.SeriesIterator {
	return newDedupSeriesIterator(s.replicas[0].Iterator(), s.replicas[1].Iterator())
}
