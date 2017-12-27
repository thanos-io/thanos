package query

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"testing"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunkenc"
)

func TestQuerier_LabelValues(t *testing.T) {
	a := &testutil.StoreClient{
		Values: map[string][]string{
			"test": {"a", "b", "c", "d"},
		},
	}
	b := &testutil.StoreClient{
		Values: map[string][]string{
			// The contract is that label values are sorted but we should be resilient
			// to misbehaving clients.
			"test": {"a", "out-of-order", "d", "x", "y"},
		},
	}
	c := &testutil.StoreClient{
		Values: map[string][]string{
			"test": {"e"},
		},
	}
	expected := []string{"a", "b", "c", "d", "e", "out-of-order", "x", "y"}

	q := newQuerier(context.Background(), nil, []*StoreInfo{
		{Client: a},
		{Client: b},
		{Client: c},
	}, 0, 10000, "")
	defer q.Close()

	vals, err := q.LabelValues("test")
	testutil.Ok(t, err)
	testutil.Equals(t, expected, vals)
}

// TestQuerier_Series catches common edge cases encountered when querying multiple store nodes.
// It is not a subtitute for testing fanin/merge procedures in depth.
func TestQuerier_Series(t *testing.T) {
	a := &testutil.StoreClient{
		SeriesSet: []storepb.Series{
			testutil.StoreSeries(t, labels.FromStrings("a", "a"), []testutil.Sample{{0, 0}, {2, 1}, {3, 2}}),
			testutil.StoreSeries(t, labels.FromStrings("a", "b"), []testutil.Sample{{2, 2}, {3, 3}, {4, 4}}),
		},
	}
	b := &testutil.StoreClient{
		SeriesSet: []storepb.Series{
			testutil.StoreSeries(t, labels.FromStrings("a", "b"), []testutil.Sample{{1, 1}, {2, 2}, {3, 3}}),
		},
	}
	c := &testutil.StoreClient{
		SeriesSet: []storepb.Series{
			testutil.StoreSeries(t, labels.FromStrings("a", "c"), []testutil.Sample{{100, 1}, {300, 3}, {400, 4}}),
		},
	}
	// Querier clamps the range to [1,300], which should drop some testutil.Samples of the result above.
	// The store API allows endpoints to send more data then initially requested.
	q := newQuerier(context.Background(), nil, []*StoreInfo{
		{Client: a},
		{Client: b},
		{Client: c},
	}, 1, 300, "")
	defer q.Close()

	res, err := q.Select()
	testutil.Ok(t, err)

	expected := []struct {
		lset    labels.Labels
		samples []testutil.Sample
	}{
		{
			lset:    labels.FromStrings("a", "a"),
			samples: []testutil.Sample{{2, 1}, {3, 2}},
		},
		{
			lset:    labels.FromStrings("a", "b"),
			samples: []testutil.Sample{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
		},
		{
			lset:    labels.FromStrings("a", "c"),
			samples: []testutil.Sample{{100, 1}, {300, 3}},
		},
	}

	i := 0
	for res.Next() {
		testutil.Equals(t, expected[i].lset, res.At().Labels())

		samples := expandSeries(t, res.At().Iterator())
		testutil.Equals(t, expected[i].samples, samples)

		i++
	}
	testutil.Ok(t, res.Err())
}

func TestSortReplicaLabel(t *testing.T) {
	set := []storepb.Series{
		{Labels: []storepb.Label{
			{"a", "1"},
			{"b", "replica-1"},
			{"c", "3"},
		}},
		{Labels: []storepb.Label{
			{"a", "1"},
			{"b", "replica-1"},
			{"c", "3"},
			{"d", "4"},
		}},
		{Labels: []storepb.Label{
			{"a", "1"},
			{"b", "replica-1"},
			{"c", "4"},
		}},
		{Labels: []storepb.Label{
			{"a", "1"},
			{"b", "replica-2"},
			{"c", "3"},
		}},
	}

	sortDedupLabels(set, "b")

	exp := []storepb.Series{
		{Labels: []storepb.Label{
			{"a", "1"},
			{"c", "3"},
			{"b", "replica-1"},
		}},
		{Labels: []storepb.Label{
			{"a", "1"},
			{"c", "3"},
			{"b", "replica-2"},
		}},
		{Labels: []storepb.Label{
			{"a", "1"},
			{"c", "3"},
			{"d", "4"},
			{"b", "replica-1"},
		}},
		{Labels: []storepb.Label{
			{"a", "1"},
			{"c", "4"},
			{"b", "replica-1"},
		}},
	}
	testutil.Equals(t, exp, set)
}

func TestStoreMatches(t *testing.T) {
	mustMatcher := func(mt labels.MatchType, n, v string) *labels.Matcher {
		m, err := labels.NewMatcher(mt, n, v)
		testutil.Ok(t, err)
		return m
	}
	cases := []struct {
		s          *StoreInfo
		mint, maxt int64
		ms         []*labels.Matcher
		ok         bool
	}{
		{
			s: &StoreInfo{Labels: []storepb.Label{{"a", "b"}}},
			ms: []*labels.Matcher{
				mustMatcher(labels.MatchEqual, "b", "1"),
			},
			ok: true,
		},
		{
			s:    &StoreInfo{MinTime: 100, MaxTime: 200},
			mint: 201,
			maxt: 300,
			ok:   false,
		},
		{
			s:    &StoreInfo{MinTime: 100, MaxTime: 200},
			mint: 200,
			maxt: 300,
			ok:   true,
		},
		{
			s:    &StoreInfo{MinTime: 100, MaxTime: 200},
			mint: 50,
			maxt: 99,
			ok:   false,
		},
		{
			s:    &StoreInfo{MinTime: 100, MaxTime: 200},
			mint: 50,
			maxt: 100,
			ok:   true,
		},
		{
			s: &StoreInfo{Labels: []storepb.Label{{"a", "b"}}},
			ms: []*labels.Matcher{
				mustMatcher(labels.MatchEqual, "a", "b"),
			},
			ok: true,
		},
		{
			s: &StoreInfo{Labels: []storepb.Label{{"a", "b"}}},
			ms: []*labels.Matcher{
				mustMatcher(labels.MatchEqual, "a", "c"),
			},
			ok: false,
		},
		{
			s: &StoreInfo{Labels: []storepb.Label{{"a", "b"}}},
			ms: []*labels.Matcher{
				mustMatcher(labels.MatchRegexp, "a", "b|c"),
			},
			ok: true,
		},
		{
			s: &StoreInfo{Labels: []storepb.Label{{"a", "b"}}},
			ms: []*labels.Matcher{
				mustMatcher(labels.MatchNotEqual, "a", ""),
			},
			ok: true,
		},
	}

	for i, c := range cases {
		ok := storeMatches(c.s, c.mint, c.maxt, c.ms...)
		testutil.Assert(t, c.ok == ok, "test case %d failed", i)
	}
}

func expandSeries(t testing.TB, it storage.SeriesIterator) (res []testutil.Sample) {
	for it.Next() {
		t, v := it.At()
		res = append(res, testutil.Sample{t, v})
	}
	testutil.Ok(t, it.Err())
	return res
}

func TestDedupSeriesSet(t *testing.T) {
	input := []struct {
		lset []storepb.Label
		vals []testutil.Sample
	}{
		{
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"replica", "replica-1"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"replica", "replica-2"}},
			vals: []testutil.Sample{{60000, 3}, {70000, 4}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"replica", "replica-3"}},
			vals: []testutil.Sample{{200000, 5}, {210000, 6}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"d", "4"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "4"}, {"replica", "replica-1"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "2"}, {"c", "3"}, {"replica", "replica-3"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "2"}, {"c", "3"}, {"replica", "replica-3"}},
			vals: []testutil.Sample{{60000, 3}, {70000, 4}},
		},
	}
	exp := []struct {
		lset labels.Labels
		vals []testutil.Sample
	}{
		{
			lset: labels.Labels{{"a", "1"}, {"c", "3"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
		},
		{
			lset: labels.Labels{{"a", "1"}, {"c", "3"}, {"d", "4"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		},
		{
			lset: labels.Labels{{"a", "1"}, {"c", "3"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		},
		{
			lset: labels.Labels{{"a", "1"}, {"c", "4"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}},
		},
		{
			lset: labels.Labels{{"a", "2"}, {"c", "3"}},
			vals: []testutil.Sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
		},
	}
	var series []storepb.Series
	for _, c := range input {
		chk := chunkenc.NewXORChunk()
		app, _ := chk.Appender()
		for _, s := range c.vals {
			app.Append(s.T, s.V)
		}
		series = append(series, storepb.Series{
			Labels: c.lset,
			Chunks: []storepb.Chunk{{
				Type: storepb.Chunk_XOR,
				Data: chk.Bytes(),
			}},
		})
	}
	set := promSeriesSet{
		mint: math.MinInt64,
		maxt: math.MaxInt64,
		set:  newStoreSeriesSet(series),
	}
	dedupSet := newDedupSeriesSet(set, "replica")

	i := 0
	for dedupSet.Next() {
		testutil.Equals(t, exp[i].lset, dedupSet.At().Labels())

		res := expandSeries(t, dedupSet.At().Iterator())
		testutil.Equals(t, exp[i].vals, res)
		i++
	}
	testutil.Ok(t, dedupSet.Err())
}

func TestDedupSeriesIterator(t *testing.T) {
	// The deltas between timestamps should be at least 10000 to not be affected
	// by the initial penalty of 5000, that will cause the second iterator to seek
	// ahead this far at least once.
	cases := []struct {
		a, b, exp []testutil.Sample
	}{
		{ // Generally prefer the first series.
			a:   []testutil.Sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
			b:   []testutil.Sample{{10000, 20}, {20000, 21}, {30000, 22}, {40000, 23}},
			exp: []testutil.Sample{{10000, 10}, {20000, 11}, {30000, 12}, {40000, 13}},
		},
		{ // Prefer b if it starts earlier.
			a:   []testutil.Sample{{10100, 1}, {20100, 1}, {30100, 1}, {40100, 1}},
			b:   []testutil.Sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
			exp: []testutil.Sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
		},
		{ // Don't switch series on a single delta sized gap.
			a:   []testutil.Sample{{10000, 1}, {20000, 1}, {40000, 1}},
			b:   []testutil.Sample{{10000, 2}, {20000, 2}, {30000, 2}, {40000, 2}},
			exp: []testutil.Sample{{10000, 1}, {20000, 1}, {40000, 1}},
		},
		{
			a:   []testutil.Sample{{10000, 1}, {20000, 1}, {40000, 1}},
			b:   []testutil.Sample{{15000, 2}, {25000, 2}, {35000, 2}, {45000, 2}},
			exp: []testutil.Sample{{10000, 1}, {20000, 1}, {40000, 1}},
		},
		{ // Once the gap gets bigger than 2 deltas, switch and stay with the new series.
			a:   []testutil.Sample{{10000, 1}, {20000, 1}, {30000, 1}, {60000, 1}, {70000, 1}},
			b:   []testutil.Sample{{10100, 2}, {20100, 2}, {30100, 2}, {40100, 2}, {50100, 2}, {60100, 2}},
			exp: []testutil.Sample{{10000, 1}, {20000, 1}, {30000, 1}, {50100, 2}, {60100, 2}},
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
	run := func(b *testing.B, s1, s2 []testutil.Sample) {
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
		var s1, s2 []testutil.Sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, testutil.Sample{T: int64(i * 10000), V: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, testutil.Sample{T: int64(i * 10000), V: 2})
		}
		run(b, s1, s2)
	})
	b.Run("fixed-delta", func(b *testing.B) {
		var s1, s2 []testutil.Sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, testutil.Sample{T: int64(i * 10000), V: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, testutil.Sample{T: int64(i*10000) + 10, V: 2})
		}
		run(b, s1, s2)
	})
	b.Run("minor-rand-delta", func(b *testing.B) {
		var s1, s2 []testutil.Sample

		for i := 0; i < b.N; i++ {
			s1 = append(s1, testutil.Sample{T: int64(i*10000) + rand.Int63n(5000), V: 1})
		}
		for i := 0; i < b.N; i++ {
			s2 = append(s2, testutil.Sample{T: int64(i*10000) + +rand.Int63n(5000), V: 2})
		}
		run(b, s1, s2)
	})
}

type SampleIterator struct {
	l []testutil.Sample
	i int
}

func (s *SampleIterator) Err() error {
	return nil
}

func (s *SampleIterator) At() (int64, float64) {
	return s.l[s.i].T, s.l[s.i].V
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
		if s.l[s.i].T >= t {
			return true
		}
		s.i++
	}
}
