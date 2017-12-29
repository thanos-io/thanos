package compact

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestDownsampleAggregator(t *testing.T) {
	var cases = []struct {
		lset labels.Labels
		data []sample
		exp  []series
	}{
		{
			lset: labels.FromStrings("__name__", "a"),
			data: []sample{
				{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {120, 5}, {180, 10}, {250, 1},
			},
			exp: []series{
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
					data: []sample{{99, 4}, {199, 13}, {299, 14}, {0, 1}},
				},
			},
		},
	}

	for _, c := range cases {
		app := &recordAppender{series: map[uint64][]sample{}}

		downsampleAggr(app, c.lset, newListSeriesIterator(c.data), 100)

		testutil.Equals(t, len(c.exp), len(app.series))
		for _, exp := range c.exp {
			testutil.Equals(t, exp.data, app.series[exp.lset.Hash()])
		}
	}
}

type recordAppender struct {
	series map[uint64][]sample
}

type series struct {
	lset labels.Labels
	data []sample
}

type sample struct {
	t int64
	v float64
}

func (a *recordAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	h := lset.Hash()
	a.series[h] = append(a.series[h], sample{t, v})
	return h, nil
}

func (a *recordAppender) AddFast(ref uint64, t int64, v float64) error {
	a.series[ref] = append(a.series[ref], sample{t, v})
	return nil
}

func (a *recordAppender) Commit() error {
	return nil
}

func (a *recordAppender) Rollback() error {
	return nil
}

type listSeriesIterator struct {
	l []sample
	i int
}

func newListSeriesIterator(l []sample) *listSeriesIterator {
	return &listSeriesIterator{l: l, i: -1}
}

func (it *listSeriesIterator) Err() error {
	return nil
}

func (it *listSeriesIterator) Next() bool {
	if it.i >= len(it.l)-1 {
		return false
	}
	it.i++
	return true
}

func (it *listSeriesIterator) Seek(int64) bool {
	panic("unexpected")
}

func (it *listSeriesIterator) At() (t int64, v float64) {
	return it.l[it.i].t, it.l[it.i].v
}
