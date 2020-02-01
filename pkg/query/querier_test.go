// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"time"

	"github.com/fortytw2/leaktest"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestQueryableCreator_MaxResolution(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	testProxy := &storeServer{resps: []*storepb.SeriesResponse{}}
	queryableCreator := NewQueryableCreator(nil, testProxy)

	oneHourMillis := int64(1*time.Hour) / int64(time.Millisecond)
	queryable := queryableCreator(false, nil, oneHourMillis, false, false)

	q, err := queryable.Querier(context.Background(), 0, 42)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, q.Close()) }()

	querierActual, ok := q.(*querier)

	testutil.Assert(t, ok == true, "expected it to be a querier")
	testutil.Assert(t, querierActual.maxResolutionMillis == oneHourMillis, "expected max source resolution to be 1 hour in milliseconds")

}

// Tests E2E how PromQL works with downsampled data.
func TestQuerier_DownsampledData(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	testProxy := &storeServer{
		resps: []*storepb.SeriesResponse{
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "a", "aaa", "bbb"), []sample{{99, 1}, {199, 5}}),                   // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "b", "bbbb", "eee"), []sample{{99, 3}, {199, 8}}),                  // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "c", "qwe", "wqeqw"), []sample{{99, 5}, {199, 15}}),                // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "c", "htgtreytr", "vbnbv"), []sample{{99, 123}, {199, 15}}),        // Downsampled chunk from Store.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "d", "asdsad", "qweqwewq"), []sample{{22, 5}, {44, 8}, {199, 15}}), // Raw chunk from Sidecar.
			storeSeriesResponse(t, labels.FromStrings("__name__", "a", "zzz", "d", "asdsad", "qweqwebb"), []sample{{22, 5}, {44, 8}, {199, 15}}), // Raw chunk from Sidecar.
		},
	}

	q := NewQueryableCreator(nil, testProxy)(false, nil, 9999999, false, false)

	engine := promql.NewEngine(
		promql.EngineOpts{
			MaxConcurrent: 10,
			MaxSamples:    math.MaxInt32,
			Timeout:       10 * time.Second,
		},
	)

	// Minimal function to parse time.Time.
	ptm := func(in string) time.Time {
		fl, _ := strconv.ParseFloat(in, 64)
		s, ns := math.Modf(fl)
		return time.Unix(int64(s), int64(ns*float64(time.Second)))
	}

	st := ptm("0")
	ed := ptm("0.2")
	qry, err := engine.NewRangeQuery(
		q,
		"sum(a) by (zzz)",
		st,
		ed,
		100*time.Millisecond,
	)
	testutil.Ok(t, err)

	res := qry.Exec(context.Background())
	testutil.Ok(t, res.Err)
	m, err := res.Matrix()
	testutil.Ok(t, err)
	ser := []promql.Series(m)

	testutil.Assert(t, len(ser) == 4, "should return 4 series (got %d)", len(ser))

	exp := []promql.Series{
		promql.Series{
			Metric: labels.FromStrings("zzz", "a"),
			Points: []promql.Point{
				promql.Point{
					T: 100,
					V: 1,
				},
				promql.Point{
					T: 200,
					V: 5,
				},
			},
		},
		promql.Series{
			Metric: labels.FromStrings("zzz", "b"),
			Points: []promql.Point{
				promql.Point{
					T: 100,
					V: 3,
				},
				promql.Point{
					T: 200,
					V: 8,
				},
			},
		},
		promql.Series{
			Metric: labels.FromStrings("zzz", "c"),
			// Test case: downsampling code adds all of the samples in the
			// 5 minute window of each series and pre-aggregates the data. However,
			// Prometheus engine code only takes the latest sample in each time window of
			// the retrieved data. Since we were operating in pre-aggregated data here, it lead
			// to overinflated values.
			Points: []promql.Point{
				promql.Point{
					T: 100,
					V: 128,
				},
				promql.Point{
					T: 200,
					V: 30,
				},
			},
		},
		promql.Series{
			Metric: labels.FromStrings("zzz", "d"),
			// Test case: Prometheus engine in each time window selects the sample
			// which is closest to the boundaries and adds up the different dimensions.
			Points: []promql.Point{
				promql.Point{
					T: 100,
					V: 16,
				},
				promql.Point{
					T: 200,
					V: 30,
				},
			},
		},
	}

	if !reflect.DeepEqual(ser, exp) {
		t.Fatalf("response does not match, expected:\n%+v\ngot:\n%+v", exp, ser)
	}
}

func TestQuerier_Series(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	testProxy := &storeServer{
		resps: []*storepb.SeriesResponse{
			// Expected sorted  series per seriesSet input. However we Series API allows for single series being chunks across multiple frames.
			// This should be handled here.
			storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			storepb.NewWarnSeriesResponse(errors.New("partial error")),
			storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{5, 5}, {6, 6}, {7, 7}}),
			storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{5, 5}, {6, 66}}), // Overlap samples for some reason.
			storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}, []sample{{1, 1}, {2, 2}, {3, 3}}),
			storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
		},
	}

	// Querier clamps the range to [1,300], which should drop some samples of the result above.
	// The store API allows endpoints to send more data then initially requested.
	q := newQuerier(context.Background(), nil, 1, 300, []string{""}, testProxy, false, 0, true, false)
	defer func() { testutil.Ok(t, q.Close()) }()

	res, _, err := q.Select(&storage.SelectParams{})
	testutil.Ok(t, err)

	expected := []struct {
		lset    labels.Labels
		samples []sample
	}{
		{
			lset:    labels.FromStrings("a", "a"),
			samples: []sample{{2, 1}, {3, 2}, {5, 5}, {6, 6}, {7, 7}},
		},
		{
			lset:    labels.FromStrings("a", "b"),
			samples: []sample{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
		},
		{
			lset: labels.FromStrings("a", "c"),

			samples: []sample{{100, 1}, {300, 3}},
		},
	}

	i := 0
	for res.Next() {
		testutil.Assert(t, i < len(expected), "more series than expected")

		testutil.Equals(t, expected[i].lset, res.At().Labels())

		samples := expandSeries(t, res.At().Iterator())
		testutil.Equals(t, expected[i].samples, samples)

		i++
	}
	testutil.Ok(t, res.Err())

	testutil.Equals(t, len(expected), i)
}

func TestSortReplicaLabel(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	tests := []struct {
		input       []storepb.Series
		exp         []storepb.Series
		dedupLabels map[string]struct{}
	}{
		// 0 Single deduplication label.
		{
			input: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "c", Value: "3"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "c", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-2"},
					{Name: "c", Value: "3"},
				}},
			},
			exp: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-2"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
					{Name: "b", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "4"},
					{Name: "b", Value: "replica-1"},
				}},
			},
			dedupLabels: map[string]struct{}{"b": struct{}{}},
		},
		// 1 Multi deduplication labels.
		{
			input: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
					{Name: "c", Value: "3"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
					{Name: "c", Value: "4"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-2"},
					{Name: "b1", Value: "replica-2"},
					{Name: "c", Value: "3"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "replica-2"},
					{Name: "c", Value: "3"},
				}},
			},
			exp: []storepb.Series{
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-2"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "b", Value: "replica-2"},
					{Name: "b1", Value: "replica-2"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "3"},
					{Name: "d", Value: "4"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
				}},
				{Labels: []storepb.Label{
					{Name: "a", Value: "1"},
					{Name: "c", Value: "4"},
					{Name: "b", Value: "replica-1"},
					{Name: "b1", Value: "replica-1"},
				}},
			},
			dedupLabels: map[string]struct{}{
				"b":  struct{}{},
				"b1": struct{}{},
			},
		},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			sortDedupLabels(test.input, test.dedupLabels)
			testutil.Equals(t, test.exp, test.input)
		})
	}
}

func expandSeries(t testing.TB, it storage.SeriesIterator) (res []sample) {
	for it.Next() {
		t, v := it.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, it.Err())
	return res
}

func TestDedupSeriesSet(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	tests := []struct {
		input []struct {
			lset []storepb.Label
			vals []sample
		}
		exp []struct {
			lset labels.Labels
			vals []sample
		}
		dedupLabels map[string]struct{}
	}{
		{ // 0 Single dedup label.
			input: []struct {
				lset []storepb.Label
				vals []sample
			}{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					vals: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}, {Name: "replica", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []struct {
				lset labels.Labels
				vals []sample
			}{
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica": struct{}{},
			},
		},
		{ // 1 Multi dedup label.
			input: []struct {
				lset []storepb.Label
				vals []sample
			}{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}, {Name: "replicaA", Value: "replica-2"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					vals: []sample{{200000, 5}, {210000, 6}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-3"}, {Name: "replicaA", Value: "replica-3"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []struct {
				lset labels.Labels
				vals []sample
			}{
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "4"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				},
				{
					lset: labels.Labels{{Name: "a", Value: "2"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica":  struct{}{},
				"replicaA": struct{}{},
			},
		},
		{ // 2 Multi dedup label - some series don't have all dedup labels.
			input: []struct {
				lset []storepb.Label
				vals []sample
			}{
				{
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}, {Name: "replicaA", Value: "replica-1"}},
					vals: []sample{{10000, 1}, {20000, 2}},
				}, {
					lset: []storepb.Label{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-2"}},
					vals: []sample{{60000, 3}, {70000, 4}},
				},
			},
			exp: []struct {
				lset labels.Labels
				vals []sample
			}{
				{
					lset: labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}},
					vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
				},
			},
			dedupLabels: map[string]struct{}{
				"replica":  struct{}{},
				"replicaA": struct{}{},
			},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			var series []storepb.Series
			for _, c := range test.input {
				chk := chunkenc.NewXORChunk()
				app, _ := chk.Appender()
				for _, s := range c.vals {
					app.Append(s.t, s.v)
				}
				series = append(series, storepb.Series{
					Labels: c.lset,
					Chunks: []storepb.AggrChunk{
						{Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chk.Bytes()}},
					},
				})
			}
			set := &promSeriesSet{
				mint: 1,
				maxt: math.MaxInt64,
				set:  newStoreSeriesSet(series),
			}
			dedupSet := newDedupSeriesSet(set, test.dedupLabels)

			i := 0
			for dedupSet.Next() {
				testutil.Equals(t, test.exp[i].lset, dedupSet.At().Labels(), "labels mismatch at index:%v", i)
				res := expandSeries(t, dedupSet.At().Iterator())
				testutil.Equals(t, test.exp[i].vals, res, "values mismatch at index:%v", i)
				i++
			}
			testutil.Ok(t, dedupSet.Err())
		})
	}
}

func TestDedupSeriesIterator(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	// The deltas between timestamps should be at least 10000 to not be affected
	// by the initial penalty of 5000, that will cause the second iterator to seek
	// ahead this far at least once.
	cases := []struct {
		a, b, exp []sample
	}{
		{ // Generally prefer the first series.
			a:   []sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
			b:   []sample{{10000, 20}, {20000, 21}, {30000, 22}, {40000, 23}},
			exp: []sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
		},
		{ // Prefer b if it starts earlier.
			a:   []sample{{10100, 1}, {20100, 1}, {30100, 1}, {40100, 1}},
			b:   []sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
			exp: []sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
		},
		{ // Don't switch series on a single delta sized gap.
			a:   []sample{{10000, 1}, {20000, 1}, {40000, 1}},
			b:   []sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
			exp: []sample{{10000, 1}, {20000, 1}, {40000, 1}},
		},
		{
			a:   []sample{{10000, 1}, {20000, 1}, {40000, 1}},
			b:   []sample{{15000, 2}, {25000, 2}, {35000, 2}, {45000, 2}},
			exp: []sample{{10000, 1}, {20000, 1}, {40000, 1}},
		},
		{ // Once the gap gets bigger than 2 deltas, switch and stay with the new series.
			a:   []sample{{10000, 1}, {20000, 1}, {30000, 1}, {60000, 1}, {70000, 1}},
			b:   []sample{{10100, 2}, {20100, 2}, {30100, 2}, {40100, 2}, {50100, 2}, {60100, 2}},
			exp: []sample{{10000, 1}, {20000, 1}, {30000, 1}, {50100, 2}, {60100, 2}},
		},
	}
	for i, c := range cases {
		t.Logf("case %d:", i)
		it := newDedupSeriesIterator(
			&SampleIterator{l: c.a, i: -1},
			&SampleIterator{l: c.b, i: -1},
		)
		res := expandSeries(t, it)
		testutil.Equals(t, c.exp, res)
	}
}

func BenchmarkDedupSeriesIterator(b *testing.B) {
	run := func(b *testing.B, s1, s2 []sample) {
		it := newDedupSeriesIterator(
			&SampleIterator{l: s1, i: -1},
			&SampleIterator{l: s2, i: -1},
		)
		b.ResetTimer()
		var total int64

		for it.Next() {
			t, _ := it.At()
			total += t
		}
		fmt.Fprint(ioutil.Discard, total)
	}
	b.Run("equal", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, sample{t: int64(i * 10000), v: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, sample{t: int64(i * 10000), v: 2})
		}
		run(b, s1, s2)
	})
	b.Run("fixed-delta", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, sample{t: int64(i * 10000), v: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, sample{t: int64(i*10000) + 10, v: 2})
		}
		run(b, s1, s2)
	})
	b.Run("minor-rand-delta", func(b *testing.B) {
		var s1, s2 []sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, sample{t: int64(i*10000) + rand.Int63n(5000), v: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, sample{t: int64(i*10000) + +rand.Int63n(5000), v: 2})
		}
		run(b, s1, s2)
	})
}

type sample struct {
	t int64
	v float64
}

type SampleIterator struct {
	l []sample
	i int
}

func (s *SampleIterator) Err() error {
	return nil
}

func (s *SampleIterator) At() (int64, float64) {
	return s.l[s.i].t, s.l[s.i].v
}

func (s *SampleIterator) Next() bool {
	if s.i >= len(s.l) {
		return false
	}
	s.i++
	return true
}

func (s *SampleIterator) Seek(t int64) bool {
	if s.i < 0 {
		s.i = 0
	}
	for {
		if s.i >= len(s.l) {
			return false
		}
		if s.l[s.i].t >= t {
			return true
		}
		s.i++
	}
}

type storeServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.StoreServer

	resps []*storepb.SeriesResponse
}

func (s *storeServer) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	for _, resp := range s.resps {
		err := srv.Send(resp)
		if err != nil {
			return err
		}
	}
	return nil
}

// storeSeriesResponse creates test storepb.SeriesResponse that includes series with single chunk that stores all the given samples.
func storeSeriesResponse(t testing.TB, lset labels.Labels, smplChunks ...[]sample) *storepb.SeriesResponse {
	var s storepb.Series

	for _, l := range lset {
		s.Labels = append(s.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(t, err)

		for _, smpl := range smpls {
			a.Append(smpl.t, smpl.v)
		}

		ch := storepb.AggrChunk{
			MinTime: smpls[0].t,
			MaxTime: smpls[len(smpls)-1].t,
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return storepb.NewSeriesResponse(&s)
}
