package downsample

import (
	"context"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

func TestDownsampleRaw(t *testing.T) {
	input := []*downsampleTestSet{
		{
			input: &testSeries{
				lset: labels.FromStrings("__name__", "a"),
				data: []sample{
					{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {120, 5}, {180, 10}, {250, 1},
				},
			},
			output: []*testSeries{

				{
					lset: labels.FromStrings("__name__", "a$sum"),
					data: []sample{{99, 7}, {199, 17}, {299, 1}},
				}, {
					lset: labels.FromStrings("__name__", "a$count"),
					data: []sample{{99, 4}, {199, 3}, {299, 1}},
				}, {
					lset: labels.FromStrings("__name__", "a$max"),
					data: []sample{{99, 3}, {199, 10}, {299, 1}},
				}, {
					lset: labels.FromStrings("__name__", "a$min"),
					data: []sample{{99, 1}, {199, 2}, {299, 1}},
				}, {
					lset: labels.FromStrings("__name__", "a$counter"),
					data: []sample{{99, 4}, {199, 13}, {299, 14}, {299, 1}},
				},
			},
		},
	}
	testDownsample(t, input, &block.Meta{}, 100)
}

func TestDownsampleAggr(t *testing.T) {
	input := []*downsampleTestSet{
		{
			input: &testSeries{
				lset: labels.FromStrings("__name__", "a$counter"),
				data: []sample{
					{99, 100}, {299, 150}, {499, 210}, {499, 10}, // chunk 1
					{599, 20}, {799, 50}, {999, 120}, {999, 50}, // chunk 2, no reset
					{1099, 40}, {1199, 80}, {1299, 110}, // chunk 3, reset
				},
			},
			output: []*testSeries{
				{
					lset: labels.FromStrings("__name__", "a$counter"),
					data: []sample{
						{499, 210}, {999, 320}, {1499, 430}, {1499, 110},
					},
				},
			},
		}, {
			input: &testSeries{
				lset: labels.FromStrings("__name__", "a$min"),
				data: []sample{{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100}},
			},
			output: []*testSeries{
				{
					lset: labels.FromStrings("__name__", "a$min"),
					data: []sample{{499, -3}, {999, 0}},
				},
			},
		}, {
			input: &testSeries{
				lset: labels.FromStrings("__name__", "a$max"),
				data: []sample{{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100}},
			},
			output: []*testSeries{
				{
					lset: labels.FromStrings("__name__", "a$max"),
					data: []sample{{499, 10}, {999, 100}},
				},
			},
		}, {
			input: &testSeries{
				lset: labels.FromStrings("__name__", "a$sum"),
				data: []sample{{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
			},
			output: []*testSeries{
				{
					lset: labels.FromStrings("__name__", "a$sum"),
					data: []sample{{499, 29}, {999, 100}},
				},
			},
		}, {
			input: &testSeries{
				lset: labels.FromStrings("__name__", "a$count"),
				data: []sample{{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
			},
			output: []*testSeries{
				{
					lset: labels.FromStrings("__name__", "a$count"),
					data: []sample{{499, 29}, {999, 100}},
				},
			},
		},
	}
	var meta block.Meta
	meta.Thanos.DownsamplingWindow = 10

	testDownsample(t, input, &meta, 500)
}

type testSeries struct {
	lset labels.Labels
	data []sample
}

type downsampleTestSet struct {
	input  *testSeries
	output []*testSeries
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
		ser := newSeries(d.input.lset, 0)
		for _, s := range d.input.data {
			ser.add(s.t, s.v)
			// Cut a new chunk whenever we cross a 500 boundary.
			if ser.cmax/500 < s.t/500 {
				ser.cut()
			}
		}
		ser.close()
		mb.addSeries(ser)
	}

	id, err := Downsample(context.Background(), meta, mb, dir, window)
	testutil.Ok(t, err)

	exp := map[uint64]*testSeries{}
	got := map[uint64]*testSeries{}

	for _, d := range data {
		for _, ser := range d.output {
			exp[ser.lset.Hash()] = ser
		}
	}

	b, err := tsdb.OpenBlock(filepath.Join(dir, id.String()), nil)
	testutil.Ok(t, err)

	q, err := tsdb.NewBlockQuerier(b, 0, math.MaxInt64)
	testutil.Ok(t, err)
	defer q.Close()

	set, err := q.Select(labels.NewEqualMatcher(index.AllPostingsKey()))
	testutil.Ok(t, err)

	for set.Next() {
		ser := &testSeries{lset: set.At().Labels()}
		got[ser.lset.Hash()] = ser

		it := set.At().Iterator()

		for it.Next() {
			t, v := it.At()
			ser.data = append(ser.data, sample{t, v})
		}
		testutil.Ok(t, it.Err())
	}
	testutil.Ok(t, set.Err())

	testutil.Equals(t, len(exp), len(got))

	for h, ser := range exp {
		o := got[h]
		testutil.Equals(t, ser, o)
	}
}

func TestCountChunkSeriesIterator(t *testing.T) {
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

	x := countChunkSeriesIterator{chks: its}

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
