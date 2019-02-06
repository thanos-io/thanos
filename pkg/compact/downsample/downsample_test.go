package downsample

import (
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

func TestExpandChunkIterator(t *testing.T) {
	// Validate that expanding the chunk iterator filters out-of-order samples
	// and staleness markers.
	// Same timestamps are okay since we use them for counter markers.
	var res []sample
	testutil.Ok(t,
		expandChunkIterator(
			newSampleIterator([]sample{
				{100, 1}, {200, 2}, {200, 3}, {201, 4}, {200, 5},
				{300, 6}, {400, math.Float64frombits(value.StaleNaN)}, {500, 5},
			}), &res,
		),
	)

	testutil.Equals(t, []sample{{100, 1}, {200, 2}, {200, 3}, {201, 4}, {300, 6}, {500, 5}}, res)
}

func TestDownsampleRaw(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	staleMarker := math.Float64frombits(value.StaleNaN)

	input := []*downsampleTestSet{
		{
			lset: labels.FromStrings("__name__", "a"),
			inRaw: []sample{
				{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {101, staleMarker}, {120, 5}, {180, 10}, {250, 1},
			},
			output: map[AggrType][]sample{
				AggrCount:   {{99, 4}, {199, 3}, {250, 1}},
				AggrSum:     {{99, 7}, {199, 17}, {250, 1}},
				AggrMin:     {{99, 1}, {199, 2}, {250, 1}},
				AggrMax:     {{99, 3}, {199, 10}, {250, 1}},
				AggrCounter: {{99, 4}, {199, 13}, {250, 14}, {250, 1}},
			},
		},
	}
	testDownsample(t, input, &metadata.Meta{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 250}}, 100)
}

func TestDownsampleAggr(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	input := []*downsampleTestSet{
		{
			lset: labels.FromStrings("__name__", "a"),
			inAggr: map[AggrType][]sample{
				AggrCount: {
					{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrSum: {
					{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrMin: {
					{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrMax: {
					{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrCounter: {
					{99, 100}, {299, 150}, {499, 210}, {499, 10}, // chunk 1
					{599, 20}, {799, 50}, {999, 120}, {999, 50}, // chunk 2, no reset
					{1099, 40}, {1199, 80}, {1299, 110}, // chunk 3, reset
				},
			},
			output: map[AggrType][]sample{
				AggrCount:   {{499, 29}, {999, 100}},
				AggrSum:     {{499, 29}, {999, 100}},
				AggrMin:     {{499, -3}, {999, 0}},
				AggrMax:     {{499, 10}, {999, 100}},
				AggrCounter: {{499, 210}, {999, 320}, {1299, 430}, {1299, 110}},
			},
		},
	}
	var meta metadata.Meta
	meta.Thanos.Downsample.Resolution = 10
	meta.BlockMeta = tsdb.BlockMeta{MinTime: 99, MaxTime: 1300}

	testDownsample(t, input, &meta, 500)
}

func encodeTestAggrSeries(v map[AggrType][]sample) chunks.Meta {
	b := newAggrChunkBuilder()

	for at, d := range v {
		for _, s := range d {
			b.apps[at].Append(s.t, s.v)
		}
	}

	return b.encode()
}

type downsampleTestSet struct {
	lset   labels.Labels
	inRaw  []sample
	inAggr map[AggrType][]sample
	output map[AggrType][]sample
}

// testDownsample inserts the input into a block and invokes the downsampler with the given resolution.
// The chunk ranges within the input block are aligned at 500 time units.
func testDownsample(t *testing.T, data []*downsampleTestSet, meta *metadata.Meta, resolution int64) {
	t.Helper()

	dir, err := ioutil.TempDir("", "downsample-raw")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	// Ideally we would use tsdb.HeadBlock here for less dependency on our own code. However,
	// it cannot accept the counter signal sample with the same timestamp as the previous sample.
	mb := newMemBlock()

	for _, d := range data {
		if len(d.inRaw) > 0 && len(d.inAggr) > 0 {
			t.Fatalf("test must not have raw and aggregate input data at once")
		}
		ser := &series{lset: d.lset}

		if len(d.inRaw) > 0 {
			chk := chunkenc.NewXORChunk()
			app, _ := chk.Appender()

			for _, s := range d.inRaw {
				app.Append(s.t, s.v)
			}
			ser.chunks = append(ser.chunks, chunks.Meta{
				MinTime: d.inRaw[0].t,
				MaxTime: d.inRaw[len(d.inRaw)-1].t,
				Chunk:   chk,
			})
		} else {
			ser.chunks = append(ser.chunks, encodeTestAggrSeries(d.inAggr))
		}
		mb.addSeries(ser)
	}

	id, err := Downsample(log.NewNopLogger(), meta, mb, dir, resolution)
	testutil.Ok(t, err)

	exp := map[uint64]map[AggrType][]sample{}
	got := map[uint64]map[AggrType][]sample{}

	for _, d := range data {
		exp[d.lset.Hash()] = d.output
	}
	indexr, err := index.NewFileReader(filepath.Join(dir, id.String(), block.IndexFilename))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, indexr.Close()) }()

	chunkr, err := chunks.NewDirReader(filepath.Join(dir, id.String(), block.ChunksDirname), NewPool())
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, chunkr.Close()) }()

	pall, err := indexr.Postings(index.AllPostingsKey())
	testutil.Ok(t, err)

	for pall.Next() {
		id := pall.At()

		var lset labels.Labels
		var chks []chunks.Meta
		testutil.Ok(t, indexr.Series(id, &lset, &chks))

		m := map[AggrType][]sample{}
		got[lset.Hash()] = m

		for _, c := range chks {
			chk, err := chunkr.Chunk(c.Ref)
			testutil.Ok(t, err)

			for _, at := range []AggrType{AggrCount, AggrSum, AggrMin, AggrMax, AggrCounter} {
				c, err := chk.(*AggrChunk).Get(at)
				if err == ErrAggrNotExist {
					continue
				}
				testutil.Ok(t, err)

				buf := m[at]
				testutil.Ok(t, expandChunkIterator(c.Iterator(), &buf))
				m[at] = buf
			}
		}
	}

	testutil.Equals(t, len(exp), len(got))

	for h, ser := range exp {
		for _, at := range []AggrType{AggrCount, AggrSum, AggrMin, AggrMax, AggrCounter} {
			t.Logf("series %d, type %s", h, at)
			testutil.Equals(t, ser[at], got[h][at])
		}
	}
}

func TestAverageChunkIterator(t *testing.T) {
	sum := []sample{{100, 30}, {200, 40}, {300, 5}, {400, -10}}
	cnt := []sample{{100, 1}, {200, 5}, {300, 2}, {400, 10}}
	exp := []sample{{100, 30}, {200, 8}, {300, 2.5}, {400, -1}}

	x := NewAverageChunkIterator(newSampleIterator(cnt), newSampleIterator(sum))

	var res []sample
	for x.Next() {
		t, v := x.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, x.Err())
	testutil.Equals(t, exp, res)
}

func TestCounterSeriesIterator(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	staleMarker := math.Float64frombits(value.StaleNaN)

	chunks := [][]sample{
		{{100, 10}, {200, 20}, {300, 10}, {400, 20}, {400, 5}},
		{{500, 10}, {600, 20}, {700, 30}, {800, 40}, {800, 10}}, // no actual reset
		{{900, 5}, {1000, 10}, {1100, 15}},                      // actual reset
		{{1200, 20}, {1250, staleMarker}, {1300, 40}},           // no special last sample, no reset
		{{1400, 30}, {1500, 30}, {1600, 50}},                    // no special last sample, reset
	}
	exp := []sample{
		{100, 10}, {200, 20}, {300, 30}, {400, 40}, {500, 45},
		{600, 55}, {700, 65}, {800, 75}, {900, 80}, {1000, 85},
		{1100, 90}, {1200, 95}, {1300, 115}, {1400, 145}, {1500, 145}, {1600, 165},
	}

	var its []chunkenc.Iterator
	for _, c := range chunks {
		its = append(its, newSampleIterator(c))
	}

	x := NewCounterSeriesIterator(its...)

	var res []sample
	for x.Next() {
		t, v := x.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, x.Err())
	testutil.Equals(t, exp, res)
}

func TestCounterSeriesIteratorSeek(t *testing.T) {
	chunks := [][]sample{
		{{100, 10}, {200, 20}, {300, 10}, {400, 20}, {400, 5}},
	}

	exp := []sample{
		{200, 20}, {300, 30}, {400, 40},
	}

	var its []chunkenc.Iterator
	for _, c := range chunks {
		its = append(its, newSampleIterator(c))
	}

	var res []sample
	x := NewCounterSeriesIterator(its...)

	ok := x.Seek(150)
	testutil.Assert(t, ok, "Seek should return true")
	testutil.Ok(t, x.Err())
	for {
		ts, v := x.At()
		res = append(res, sample{ts, v})

		ok = x.Next()
		if !ok {
			break
		}
	}
	testutil.Equals(t, exp, res)
}

func TestCounterSeriesIteratorSeekExtendTs(t *testing.T) {
	chunks := [][]sample{
		{{100, 10}, {200, 20}, {300, 10}, {400, 20}, {400, 5}},
	}

	var its []chunkenc.Iterator
	for _, c := range chunks {
		its = append(its, newSampleIterator(c))
	}

	x := NewCounterSeriesIterator(its...)

	ok := x.Seek(500)
	testutil.Assert(t, !ok, "Seek should return false")
}

func TestCounterSeriesIteratorSeekAfterNext(t *testing.T) {
	chunks := [][]sample{
		{{100, 10}},
	}
	exp := []sample{
		{100, 10},
	}

	var its []chunkenc.Iterator
	for _, c := range chunks {
		its = append(its, newSampleIterator(c))
	}

	var res []sample
	x := NewCounterSeriesIterator(its...)

	x.Next()

	ok := x.Seek(50)
	testutil.Assert(t, ok, "Seek should return true")
	testutil.Ok(t, x.Err())
	for {
		ts, v := x.At()
		res = append(res, sample{ts, v})

		ok = x.Next()
		if !ok {
			break
		}
	}
	testutil.Equals(t, exp, res)
}

type sampleIterator struct {
	l []sample
	i int
}

func newSampleIterator(l []sample) *sampleIterator {
	return &sampleIterator{l: l, i: -1}
}

func (it *sampleIterator) Err() error {
	return nil
}

func (it *sampleIterator) Next() bool {
	if it.i >= len(it.l)-1 {
		return false
	}
	it.i++
	return true
}

func (it *sampleIterator) Seek(int64) bool {
	panic("unexpected")
}

func (it *sampleIterator) At() (t int64, v float64) {
	return it.l[it.i].t, it.l[it.i].v
}

// memBlock is an in-memory block that implements a subset of the tsdb.BlockReader interface
// to allow tsdb.StreamedBlockWriter to persist the data as a block.
type memBlock struct {
	// Dummies to implement unused methods.
	tsdb.IndexReader

	symbols  map[string]struct{}
	postings []uint64
	series   []*series
	chunks   []chunkenc.Chunk

	numberOfChunks uint64
}

type series struct {
	lset   labels.Labels
	chunks []chunks.Meta
}

func newMemBlock() *memBlock {
	return &memBlock{symbols: map[string]struct{}{}}
}

func (b *memBlock) addSeries(s *series) {
	sid := uint64(len(b.series))
	b.postings = append(b.postings, sid)
	b.series = append(b.series, s)

	for _, l := range s.lset {
		b.symbols[l.Name] = struct{}{}
		b.symbols[l.Value] = struct{}{}
	}

	for i, cm := range s.chunks {
		s.chunks[i].Ref = b.numberOfChunks
		b.chunks = append(b.chunks, cm.Chunk)
		b.numberOfChunks++
	}
}

func (b *memBlock) Postings(name, val string) (index.Postings, error) {
	allName, allVal := index.AllPostingsKey()

	if name != allName || val != allVal {
		return nil, errors.New("unsupported call to Postings()")
	}
	sort.Slice(b.postings, func(i, j int) bool {
		return labels.Compare(b.series[b.postings[i]].lset, b.series[b.postings[j]].lset) < 0
	})
	return index.NewListPostings(b.postings), nil
}

func (b *memBlock) Series(id uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	if id >= uint64(len(b.series)) {
		return errors.Wrapf(tsdb.ErrNotFound, "series with ID %d does not exist", id)
	}
	s := b.series[id]

	*lset = append((*lset)[:0], s.lset...)
	*chks = append((*chks)[:0], s.chunks...)

	return nil
}

func (b *memBlock) Chunk(id uint64) (chunkenc.Chunk, error) {
	if id >= uint64(b.numberOfChunks) {
		return nil, errors.Wrapf(tsdb.ErrNotFound, "chunk with ID %d does not exist", id)
	}

	return b.chunks[id], nil
}

func (b *memBlock) Symbols() (map[string]struct{}, error) {
	return b.symbols, nil
}

func (b *memBlock) SortedPostings(p index.Postings) index.Postings {
	return p
}

func (b *memBlock) Index() (tsdb.IndexReader, error) {
	return b, nil
}

func (b *memBlock) Chunks() (tsdb.ChunkReader, error) {
	return b, nil
}

func (b *memBlock) Tombstones() (tsdb.TombstoneReader, error) {
	return emptyTombstoneReader{}, nil
}

func (b *memBlock) Close() error {
	return nil
}

type emptyTombstoneReader struct{}

func (emptyTombstoneReader) Get(ref uint64) (tsdb.Intervals, error)        { return nil, nil }
func (emptyTombstoneReader) Iter(func(uint64, tsdb.Intervals) error) error { return nil }
func (emptyTombstoneReader) Total() uint64                                 { return 0 }
func (emptyTombstoneReader) Close() error                                  { return nil }
