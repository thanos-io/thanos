// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/dedup"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

var PartialResponseStrategyValues = func() []string {
	var s []string
	for k := range PartialResponseStrategy_value {
		s = append(s, k)
	}
	sort.Strings(s)
	return s
}()

func NewWarnSeriesResponse(err error) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Warning{
			Warning: err.Error(),
		},
	}
}

func NewSeriesResponse(series *Series) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Series{
			Series: series,
		},
	}
}

func NewHintsSeriesResponse(hints *types.Any) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Hints{
			Hints: hints,
		},
	}
}

type emptySeriesSet struct {
	err error
}

func (emptySeriesSet) Next() bool                       { return false }
func (emptySeriesSet) At() (labels.Labels, []AggrChunk) { return nil, nil }
func (e emptySeriesSet) Err() error                     { return e.err }

// EmptySeriesSet returns a new series set that contains no series.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet{}
}

// ErrSeriesSet returns a new series set that contains no series and error.
func ErrSeriesSet(err error) SeriesSet {
	return emptySeriesSet{err: err}
}

// MergeSeriesSets takes all series sets and returns as a union single series set.
// It assumes series are sorted by labels within single SeriesSet, similar to remote read guarantees.
// However, they can be partial: in such case, if the single SeriesSet returns the same series within many iterations,
// MergeSeriesSets will merge those into one.
//
// It also assumes in a "best effort" way that chunks are sorted by min time. It's done as an optimization only, so if input
// series' chunks are NOT sorted, the only consequence is that the duplicates might be not correctly removed. This is double checked
// which on just-before PromQL level as well, so the only consequence is increased network bandwidth.
// If all chunks were sorted, MergeSeriesSet ALSO returns sorted chunks by min time.
//
// Chunks within the same series can also overlap (within all SeriesSet
// as well as single SeriesSet alone). If the chunk ranges overlap, the *exact* chunk duplicates will be removed
// (except one), and any other overlaps will be appended into on chunks slice.
func MergeSeriesSets(all ...SeriesSet) SeriesSet {
	switch len(all) {
	case 0:
		return emptySeriesSet{}
	case 1:
		return newUniqueSeriesSet(all[0])
	}
	h := len(all) / 2

	return newMergedSeriesSet(
		MergeSeriesSets(all[:h]...),
		MergeSeriesSets(all[h:]...),
	)
}

// SeriesSet is a set of series and their corresponding chunks.
// The set is sorted by the label sets. Chunks may be overlapping or expected of order.
type SeriesSet interface {
	Next() bool
	At() (labels.Labels, []AggrChunk)
	Err() error
}

// mergedSeriesSet takes two series sets as a single series set.
type mergedSeriesSet struct {
	a, b SeriesSet

	lset         labels.Labels
	chunks       []AggrChunk
	adone, bdone bool
}

func newMergedSeriesSet(a, b SeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()

	return s
}

func (s *mergedSeriesSet) At() (labels.Labels, []AggrChunk) {
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
	return labels.Compare(lsetA, lsetB)
}

func (s *mergedSeriesSet) Next() bool {
	if s.adone && s.bdone || s.Err() != nil {
		return false
	}

	d := s.compare()
	if d > 0 {
		s.lset, s.chunks = s.b.At()
		s.bdone = !s.b.Next()
		return true
	}
	if d < 0 {
		s.lset, s.chunks = s.a.At()
		s.adone = !s.a.Next()
		return true
	}

	// Both a and b contains the same series. Go through all chunks, remove duplicates and concatenate chunks from both
	// series sets. We best effortly assume chunks are sorted by min time. If not, we will not detect all deduplicate which will
	// be account on select layer anyway. We do it still for early optimization.
	lset, chksA := s.a.At()
	_, chksB := s.b.At()
	s.lset = lset

	// Slice reuse is not generally safe with nested merge iterators.
	// We err on the safe side an create a new slice.
	s.chunks = make([]AggrChunk, 0, len(chksA)+len(chksB))

	b := 0
Outer:
	for a := range chksA {
		for {
			if b >= len(chksB) {
				// No more b chunks.
				s.chunks = append(s.chunks, chksA[a:]...)
				break Outer
			}

			cmp := chksA[a].Compare(chksB[b])
			if cmp > 0 {
				s.chunks = append(s.chunks, chksA[a])
				break
			}
			if cmp < 0 {
				s.chunks = append(s.chunks, chksB[b])
				b++
				continue
			}

			// Exact duplicated chunks, discard one from b.
			b++
		}
	}

	if b < len(chksB) {
		s.chunks = append(s.chunks, chksB[b:]...)
	}

	s.adone = !s.a.Next()
	s.bdone = !s.b.Next()
	return true
}

// uniqueSeriesSet takes one series set and ensures each iteration contains single, full series.
type uniqueSeriesSet struct {
	SeriesSet
	done bool

	peek *Series

	lset   labels.Labels
	chunks []AggrChunk
}

func newUniqueSeriesSet(wrapped SeriesSet) *uniqueSeriesSet {
	return &uniqueSeriesSet{SeriesSet: wrapped}
}

func (s *uniqueSeriesSet) At() (labels.Labels, []AggrChunk) {
	return s.lset, s.chunks
}

func (s *uniqueSeriesSet) Next() bool {
	if s.Err() != nil {
		return false
	}

	for !s.done {
		if s.done = !s.SeriesSet.Next(); s.done {
			break
		}
		lset, chks := s.SeriesSet.At()
		if s.peek == nil {
			s.peek = &Series{Labels: labelpb.ZLabelsFromPromLabels(lset), Chunks: chks}
			continue
		}

		if labels.Compare(lset, s.peek.PromLabels()) != 0 {
			s.lset, s.chunks = s.peek.PromLabels(), s.peek.Chunks
			s.peek = &Series{Labels: labelpb.ZLabelsFromPromLabels(lset), Chunks: chks}
			return true
		}

		// We assume non-overlapping, sorted chunks. This is best effort only, if it's otherwise it
		// will just be duplicated, but well handled by StoreAPI consumers.
		s.peek.Chunks = append(s.peek.Chunks, chks...)
	}

	if s.peek == nil {
		return false
	}

	s.lset, s.chunks = s.peek.PromLabels(), s.peek.Chunks
	s.peek = nil
	return true
}

// Compare returns positive 1 if chunk is smaller -1 if larger than b by min time, then max time.
// It returns 0 if chunks are exactly the same.
func (m AggrChunk) Compare(b AggrChunk) int {
	if m.MinTime < b.MinTime {
		return 1
	}
	if m.MinTime > b.MinTime {
		return -1
	}

	// Same min time.
	if m.MaxTime < b.MaxTime {
		return 1
	}
	if m.MaxTime > b.MaxTime {
		return -1
	}

	// We could use proto.Equal, but we need ordering as well.
	for _, cmp := range []func() int{
		func() int { return m.Raw.Compare(b.Raw) },
		func() int { return m.Count.Compare(b.Count) },
		func() int { return m.Sum.Compare(b.Sum) },
		func() int { return m.Min.Compare(b.Min) },
		func() int { return m.Max.Compare(b.Max) },
		func() int { return m.Counter.Compare(b.Counter) },
	} {
		if c := cmp(); c == 0 {
			continue
		} else {
			return c
		}
	}
	return 0
}

// Compare returns positive 1 if chunk is smaller -1 if larger.
// It returns 0 if chunks are exactly the same.
func (m *Chunk) Compare(b *Chunk) int {
	if m == nil && b == nil {
		return 0
	}
	if b == nil {
		return 1
	}
	if m == nil {
		return -1
	}

	if m.Type < b.Type {
		return 1
	}
	if m.Type > b.Type {
		return -1
	}
	return bytes.Compare(m.Data, b.Data)
}

func (x *PartialResponseStrategy) UnmarshalJSON(entry []byte) error {
	fieldStr, err := strconv.Unquote(string(entry))
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("failed to unqote %v, in order to unmarshal as 'partial_response_strategy'. Possible values are %s", string(entry), strings.Join(PartialResponseStrategyValues, ",")))
	}

	if fieldStr == "" {
		// NOTE: For Rule default is abort as this is recommended for alerting.
		*x = PartialResponseStrategy_ABORT
		return nil
	}

	strategy, ok := PartialResponseStrategy_value[strings.ToUpper(fieldStr)]
	if !ok {
		return errors.Errorf(fmt.Sprintf("failed to unmarshal %v as 'partial_response_strategy'. Possible values are %s", string(entry), strings.Join(PartialResponseStrategyValues, ",")))
	}
	*x = PartialResponseStrategy(strategy)
	return nil
}

func (x *PartialResponseStrategy) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(x.String())), nil
}

// PromMatchersToMatchers returns proto matchers from Prometheus matchers.
// NOTE: It allocates memory.
func PromMatchersToMatchers(ms ...*labels.Matcher) ([]LabelMatcher, error) {
	res := make([]LabelMatcher, 0, len(ms))
	for _, m := range ms {
		var t LabelMatcher_Type

		switch m.Type {
		case labels.MatchEqual:
			t = LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = LabelMatcher_NRE
		default:
			return nil, errors.Errorf("unrecognized matcher type %d", m.Type)
		}
		res = append(res, LabelMatcher{Type: t, Name: m.Name, Value: m.Value})
	}
	return res, nil
}

// MatchersToPromMatchers returns Prometheus matchers from proto matchers.
// NOTE: It allocates memory.
func MatchersToPromMatchers(ms ...LabelMatcher) ([]*labels.Matcher, error) {
	res := make([]*labels.Matcher, 0, len(ms))
	for _, m := range ms {
		var t labels.MatchType

		switch m.Type {
		case LabelMatcher_EQ:
			t = labels.MatchEqual
		case LabelMatcher_NEQ:
			t = labels.MatchNotEqual
		case LabelMatcher_RE:
			t = labels.MatchRegexp
		case LabelMatcher_NRE:
			t = labels.MatchNotRegexp
		default:
			return nil, errors.Errorf("unrecognized label matcher type %d", m.Type)
		}
		m, err := labels.NewMatcher(t, m.Name, m.Value)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, nil
}

// MatchersToString converts label matchers to string format.
// String should be parsable as a valid PromQL query metric selector.
func MatchersToString(ms ...LabelMatcher) string {
	var res string
	for i, m := range ms {
		res += m.PromString()
		if i < len(ms)-1 {
			res += ", "
		}
	}
	return "{" + res + "}"
}

// PromMatchersToString converts prometheus label matchers to string format.
// String should be parsable as a valid PromQL query metric selector.
func PromMatchersToString(ms ...*labels.Matcher) string {
	var res string
	for i, m := range ms {
		res += m.String()
		if i < len(ms)-1 {
			res += ", "
		}
	}
	return "{" + res + "}"
}

func (m *LabelMatcher) PromString() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type.PromString(), m.Value)
}

func (x LabelMatcher_Type) PromString() string {
	typeToStr := map[LabelMatcher_Type]string{
		LabelMatcher_EQ:  "=",
		LabelMatcher_NEQ: "!=",
		LabelMatcher_RE:  "=~",
		LabelMatcher_NRE: "!~",
	}
	if str, ok := typeToStr[x]; ok {
		return str
	}
	panic("unknown match type")
}

// PromLabels return Prometheus labels.Labels without extra allocation.
func (m *Series) PromLabels() labels.Labels {
	return labelpb.ZLabelsToPromLabels(m.Labels)
}

// Deprecated.
// TODO(bwplotka): Remove this once Cortex dep will stop using it.
type Label = labelpb.ZLabel

// Deprecated.
// TODO(bwplotka): Remove this in next PR. Done to reduce diff only.
type LabelSet = labelpb.ZLabelSet

// Deprecated.
// TODO(bwplotka): Remove this once Cortex dep will stop using it.
func CompareLabels(a, b []Label) int {
	return labels.Compare(labelpb.ZLabelsToPromLabels(a), labelpb.ZLabelsToPromLabels(b))
}

// Deprecated.
// TODO(bwplotka): Remove this once Cortex dep will stop using it.
func LabelsToPromLabelsUnsafe(lset []Label) labels.Labels {
	return labelpb.ZLabelsToPromLabels(lset)
}

// XORNumSamples return number of samples. Returns 0 if it's not XOR chunk.
func (m *Chunk) XORNumSamples() int {
	if m.Type == Chunk_XOR {
		return int(binary.BigEndian.Uint16(m.Data))
	}
	return 0
}

type SeriesStatsCounter struct {
	lastSeriesHash uint64

	Series  int
	Chunks  int
	Samples int
}

func (c *SeriesStatsCounter) CountSeries(seriesLabels []labelpb.ZLabel) {
	seriesHash := labelpb.HashWithPrefix("", seriesLabels)
	if c.lastSeriesHash != 0 || seriesHash != c.lastSeriesHash {
		c.lastSeriesHash = seriesHash
		c.Series++
	}
}

func (c *SeriesStatsCounter) Count(series *Series) {
	c.CountSeries(series.Labels)
	for _, chk := range series.Chunks {
		if chk.Raw != nil {
			c.Chunks++
			c.Samples += chk.Raw.XORNumSamples()
		}

		if chk.Count != nil {
			c.Chunks++
			c.Samples += chk.Count.XORNumSamples()
		}

		if chk.Counter != nil {
			c.Chunks++
			c.Samples += chk.Counter.XORNumSamples()
		}

		if chk.Max != nil {
			c.Chunks++
			c.Samples += chk.Max.XORNumSamples()
		}

		if chk.Min != nil {
			c.Chunks++
			c.Samples += chk.Min.XORNumSamples()
		}

		if chk.Sum != nil {
			c.Chunks++
			c.Samples += chk.Sum.XORNumSamples()
		}
	}
}

func NewPromSeriesSet(mint, maxt int64, set SeriesSet, aggrs []Aggr, warns storage.Warnings) *promSeriesSet {
	return &promSeriesSet{
		mint:  mint,
		maxt:  maxt,
		set:   set,
		aggrs: aggrs,
		warns: warns,
	}
}

// promSeriesSet implements the SeriesSet interface of the Prometheus storage
// package on top of our storepb SeriesSet.
type promSeriesSet struct {
	set  SeriesSet
	done bool

	mint, maxt int64
	aggrs      []Aggr
	initiated  bool

	currLset   labels.Labels
	currChunks []AggrChunk

	warns storage.Warnings
}

func (s *promSeriesSet) Next() bool {
	if !s.initiated {
		s.initiated = true
		s.done = s.set.Next()
	}

	if !s.done {
		return false
	}

	// storage.Series are more strict then SeriesSet:
	// * It requires storage.Series to iterate over full series.
	s.currLset, s.currChunks = s.set.At()
	for {
		s.done = s.set.Next()
		if !s.done {
			break
		}
		nextLset, nextChunks := s.set.At()
		if labels.Compare(s.currLset, nextLset) != 0 {
			break
		}
		s.currChunks = append(s.currChunks, nextChunks...)
	}

	// Samples (so chunks as well) have to be sorted by time.
	// TODO(bwplotka): Benchmark if we can do better.
	// For example we could iterate in above loop and write our own binary search based insert sort.
	// We could also remove duplicates in same loop.
	sort.Slice(s.currChunks, func(i, j int) bool {
		return s.currChunks[i].MinTime < s.currChunks[j].MinTime
	})

	// Proxy handles duplicates between different series, let's handle duplicates within single series now as well.
	// We don't need to decode those.
	s.currChunks = removeExactDuplicates(s.currChunks)
	return true
}

// removeExactDuplicates returns chunks without 1:1 duplicates.
// NOTE: input chunks has to be sorted by minTime.
func removeExactDuplicates(chks []AggrChunk) []AggrChunk {
	if len(chks) <= 1 {
		return chks
	}

	ret := make([]AggrChunk, 0, len(chks))
	ret = append(ret, chks[0])

	for _, c := range chks[1:] {
		if ret[len(ret)-1].Compare(c) == 0 {
			continue
		}
		ret = append(ret, c)
	}
	return ret
}

func (s *promSeriesSet) At() storage.Series {
	if !s.initiated || s.set.Err() != nil {
		return nil
	}
	return newChunkSeries(s.currLset, s.currChunks, s.mint, s.maxt, s.aggrs)
}

func (s *promSeriesSet) Err() error {
	return s.set.Err()
}

func (s *promSeriesSet) Warnings() storage.Warnings {
	return s.warns
}

// chunkSeries implements storage.Series for a series on storepb types.
type chunkSeries struct {
	lset       labels.Labels
	chunks     []AggrChunk
	mint, maxt int64
	aggrs      []Aggr
}

// newChunkSeries allows to iterate over samples for each sorted and non-overlapped chunks.
func newChunkSeries(lset labels.Labels, chunks []AggrChunk, mint, maxt int64, aggrs []Aggr) *chunkSeries {
	return &chunkSeries{
		lset:   lset,
		chunks: chunks,
		mint:   mint,
		maxt:   maxt,
		aggrs:  aggrs,
	}
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.lset
}

func (s *chunkSeries) Iterator() chunkenc.Iterator {
	var sit chunkenc.Iterator
	its := make([]chunkenc.Iterator, 0, len(s.chunks))

	if len(s.aggrs) == 1 {
		switch s.aggrs[0] {
		case Aggr_COUNT:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Count, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case Aggr_SUM:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Sum, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case Aggr_MIN:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Min, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case Aggr_MAX:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Max, c.Raw))
			}
			sit = newChunkSeriesIterator(its)
		case Aggr_COUNTER:
			for _, c := range s.chunks {
				its = append(its, getFirstIterator(c.Counter, c.Raw))
			}
			// TODO(bwplotka): This breaks resets function. See https://github.com/thanos-io/thanos/issues/3644
			sit = downsample.NewApplyCounterResetsIterator(its...)
		default:
			return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
		}
		return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
	}

	if len(s.aggrs) != 2 {
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}

	switch {
	case s.aggrs[0] == Aggr_SUM && s.aggrs[1] == Aggr_COUNT,
		s.aggrs[0] == Aggr_COUNT && s.aggrs[1] == Aggr_SUM:

		for _, c := range s.chunks {
			if c.Raw != nil {
				its = append(its, getFirstIterator(c.Raw))
			} else {
				sum, cnt := getFirstIterator(c.Sum), getFirstIterator(c.Count)
				its = append(its, downsample.NewAverageChunkIterator(cnt, sum))
			}
		}
		sit = newChunkSeriesIterator(its)
	default:
		return errSeriesIterator{err: errors.Errorf("unexpected result aggregate type %v", s.aggrs)}
	}
	return dedup.NewBoundedSeriesIterator(sit, s.mint, s.maxt)
}

func getFirstIterator(cs ...*Chunk) chunkenc.Iterator {
	for _, c := range cs {
		if c == nil {
			continue
		}
		chk, err := chunkenc.FromData(chunkEncoding(c.Type), c.Data)
		if err != nil {
			return errSeriesIterator{err}
		}
		return chk.Iterator(nil)
	}
	return errSeriesIterator{errors.New("no valid chunk found")}
}

func chunkEncoding(e Chunk_Encoding) chunkenc.Encoding {
	switch e {
	case Chunk_XOR:
		return chunkenc.EncXOR
	}
	return 255 // Invalid.
}

type errSeriesIterator struct {
	err error
}

func (errSeriesIterator) Seek(int64) bool      { return false }
func (errSeriesIterator) Next() bool           { return false }
func (errSeriesIterator) At() (int64, float64) { return 0, 0 }
func (it errSeriesIterator) Err() error        { return it.err }

// chunkSeriesIterator implements a series iterator on top
// of a list of time-sorted, non-overlapping chunks.
type chunkSeriesIterator struct {
	chunks []chunkenc.Iterator
	i      int
}

func newChunkSeriesIterator(cs []chunkenc.Iterator) chunkenc.Iterator {
	if len(cs) == 0 {
		// This should not happen. StoreAPI implementations should not send empty results.
		return errSeriesIterator{err: errors.Errorf("store returned an empty result")}
	}
	return &chunkSeriesIterator{chunks: cs}
}

func (it *chunkSeriesIterator) Seek(t int64) (ok bool) {
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
	return it.chunks[it.i].At()
}

func (it *chunkSeriesIterator) Next() bool {
	lastT, _ := it.At()

	if it.chunks[it.i].Next() {
		return true
	}
	if it.Err() != nil {
		return false
	}
	if it.i >= len(it.chunks)-1 {
		return false
	}
	// Chunks are guaranteed to be ordered but not generally guaranteed to not overlap.
	// We must ensure to skip any overlapping range between adjacent chunks.
	it.i++
	return it.Seek(lastT + 1)
}

func (it *chunkSeriesIterator) Err() error {
	return it.chunks[it.i].Err()
}
