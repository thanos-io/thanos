package downsample

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/tsdb/chunks"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

func TestAggrChunk(t *testing.T) {
	var input [5][]sample

	input[AggrCount] = []sample{{100, 30}, {200, 50}, {300, 60}, {400, 67}}
	input[AggrSum] = []sample{{100, 130}, {200, 1000}, {300, 2000}, {400, 5555}}
	input[AggrMin] = []sample{{100, 0}, {200, -10}, {300, 1000}, {400, -9.5}}
	// Maximum is absent.
	input[AggrCounter] = []sample{{100, 5}, {200, 10}, {300, 10.1}, {400, 15}, {400, 3}}

	var chks [5]chunkenc.Chunk

	for i, smpls := range input {
		if len(smpls) == 0 {
			continue
		}
		chks[i] = chunkenc.NewXORChunk()
		a, err := chks[i].Appender()
		testutil.Ok(t, err)

		for _, s := range smpls {
			a.Append(s.t, s.v)
		}
	}

	var res [5][]sample
	ac := EncodeAggrChunk(chks)

	for _, at := range []AggrType{AggrCount, AggrSum, AggrMin, AggrMax, AggrCounter} {
		if c, err := ac.Get(at); err != ErrAggrNotExist {
			testutil.Ok(t, err)
			testutil.Ok(t, expandChunkIterator(c.Iterator(), &res[at]))
		}
	}
	testutil.Equals(t, input, res)
}

type testAggrSeries struct {
	lset labels.Labels
	data map[AggrType][]sample
}

func TestDownsampleRaw(t *testing.T) {
	input := []*downsampleTestSet{
		{
			lset: labels.FromStrings("__name__", "a"),
			inRaw: []sample{
				{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {120, 5}, {180, 10}, {250, 1},
			},
			output: map[AggrType][]sample{
				AggrCount:   {{99, 4}, {199, 3}, {299, 1}},
				AggrSum:     {{99, 7}, {199, 17}, {299, 1}},
				AggrMin:     {{99, 1}, {199, 2}, {299, 1}},
				AggrMax:     {{99, 3}, {199, 10}, {299, 1}},
				AggrCounter: {{99, 4}, {199, 13}, {299, 14}, {299, 1}},
			},
		},
	}
	testDownsample(t, input, &block.Meta{}, 100)
}

func TestDownsampleAggr(t *testing.T) {
	input := []*downsampleTestSet{
		{
			lset: labels.FromStrings("__name__", "a"),
			inAggr: map[AggrType][]sample{
				AggrCount: []sample{
					{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrSum: []sample{
					{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrMin: []sample{
					{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrMax: []sample{
					{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100},
				},
				AggrCounter: []sample{
					{99, 100}, {299, 150}, {499, 210}, {499, 10}, // chunk 1
					{599, 20}, {799, 50}, {999, 120}, {999, 50}, // chunk 2, no reset
					{1099, 40}, {1199, 80}, {1299, 110}, // chunk 3, reset
				},
			},
			output: map[AggrType][]sample{
				AggrCount:   []sample{{499, 29}, {999, 100}},
				AggrSum:     []sample{{499, 29}, {999, 100}},
				AggrMin:     []sample{{499, -3}, {999, 0}},
				AggrMax:     []sample{{499, 10}, {999, 100}},
				AggrCounter: []sample{{499, 210}, {999, 320}, {1499, 430}, {1499, 110}},
			},
		},
	}
	var meta block.Meta
	meta.Thanos.Downsample.Window = 10

	testDownsample(t, input, &meta, 500)
}

type testSeries struct {
	lset labels.Labels
	data []sample
}

func encodeTestAggrSeries(v map[AggrType][]sample) (*AggrChunk, int64, int64) {
	b := newAggrChunkBuilder(false)

	for at, d := range v {
		for _, s := range d {
			b.apps[at].Append(s.t, s.v)
		}
	}
	return b.encode(), b.mint, b.maxt
}

type downsampleTestSet struct {
	lset   labels.Labels
	inRaw  []sample
	inAggr map[AggrType][]sample
	output map[AggrType][]sample
}

// testDownsample inserts the input into a block and invokes the downsampler with the given window.
// The chunk ranges within the input block are aligned at 500 time units.
func testDownsample(t *testing.T, data []*downsampleTestSet, meta *block.Meta, window int64) {
	t.Helper()

	dir, err := ioutil.TempDir("", "downsample-raw")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

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
			ac, mint, maxt := encodeTestAggrSeries(d.inAggr)

			ser.chunks = append(ser.chunks, chunks.Meta{
				MinTime: mint,
				MaxTime: maxt,
				Chunk:   ac,
			})
		}
		mb.addSeries(ser)
	}

	id, err := Downsample(context.Background(), meta, mb, dir, window)
	testutil.Ok(t, err)

	exp := map[uint64]map[AggrType][]sample{}
	got := map[uint64]map[AggrType][]sample{}

	for _, d := range data {
		exp[d.lset.Hash()] = d.output
	}
	indexr, err := index.NewFileReader(filepath.Join(dir, id.String(), "index"))
	testutil.Ok(t, err)
	defer indexr.Close()

	chunkr, err := chunks.NewDirReader(filepath.Join(dir, id.String(), "chunks"), aggrPool{})
	testutil.Ok(t, err)
	defer chunkr.Close()

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

func TestCounterSeriesIterator(t *testing.T) {
	chunks := [][]sample{
		{{100, 10}, {200, 20}, {300, 10}, {400, 20}, {400, 5}},
		{{500, 10}, {600, 20}, {700, 30}, {800, 40}, {800, 10}}, // no actual reset
		{{900, 5}, {1000, 10}, {1100, 15}},                      // actual reset
		{{1200, 20}, {1300, 40}},                                // no special last sample, no reset
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
