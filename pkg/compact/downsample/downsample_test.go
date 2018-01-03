package compact

import (
	"context"
	"testing"

	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"

	"github.com/prometheus/tsdb/testutil"
)

func TestDownsample(t *testing.T) {
	b, err := tsdb.OpenBlock("testdata/01C2ECS0JYXBYFTENS4FDPTPRA", nil)
	testutil.Ok(t, err)

	_, err = Downsample(context.Background(), b, "testdata/res", 5*60*1000)
	testutil.Ok(t, err)
}

type testSeries struct {
	lset labels.Labels
	data []sample
}

func TestDownsampleAggregator(t *testing.T) {
	var cases = []struct {
		lset labels.Labels
		data []sample
		exp  []testSeries
	}{
		{
			lset: labels.FromStrings("__name__", "a"),
			data: []sample{
				{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {120, 5}, {180, 10}, {250, 1},
			},
			exp: []testSeries{
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

	for _, c := range cases {
		series := downsampleRaw(c.lset, c.data, 100)

		res := map[uint64][]sample{}

		for _, s := range series {
			var data []sample

			for _, c := range s.chunks {
				it := c.Chunk.Iterator()
				for it.Next() {
					t, v := it.At()
					data = append(data, sample{t, v})
				}
			}
			res[s.lset.Hash()] = data
		}

		testutil.Equals(t, len(c.exp), len(series))

		for _, exp := range c.exp {
			testutil.Equals(t, exp.data, res[exp.lset.Hash()])
		}
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
		its = append(its, newListSeriesIterator(c))
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
