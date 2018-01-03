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
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunkenc"
)

func TestQuerier_Series(t *testing.T) {
	testProxy := &storeServer{
		resps: []*storepb.SeriesResponse{
			storeSeriesResponse(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			storepb.NewWarnSeriesResponse(errors.New("partial error")),
			storeSeriesResponse(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}, []sample{{1, 1}, {2, 2}, {3, 3}}),
			storeSeriesResponse(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
		},
	}

	// Querier clamps the range to [1,300], which should drop some samples of the result above.
	// The store API allows endpoints to send more data then initially requested.
	q := newQuerier(context.Background(), nil, 1, 300, "", testProxy)
	defer q.Close()

	res, err := q.Select()
	testutil.Ok(t, err)

	expected := []struct {
		lset    labels.Labels
		samples []sample
	}{
		{
			lset:    labels.FromStrings("a", "a"),
			samples: []sample{{2, 1}, {3, 2}},
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

func expandSeries(t testing.TB, it storage.SeriesIterator) (res []sample) {
	for it.Next() {
		t, v := it.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, it.Err())
	return res
}

func TestDedupSeriesSet(t *testing.T) {
	input := []struct {
		lset []storepb.Label
		vals []sample
	}{
		{
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"replica", "replica-1"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"replica", "replica-2"}},
			vals: []sample{{60000, 3}, {70000, 4}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"replica", "replica-3"}},
			vals: []sample{{200000, 5}, {210000, 6}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}, {"d", "4"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "3"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "1"}, {"c", "4"}, {"replica", "replica-1"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "2"}, {"c", "3"}, {"replica", "replica-3"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		}, {
			lset: []storepb.Label{{"a", "2"}, {"c", "3"}, {"replica", "replica-3"}},
			vals: []sample{{60000, 3}, {70000, 4}},
		},
	}
	exp := []struct {
		lset labels.Labels
		vals []sample
	}{
		{
			lset: labels.Labels{{"a", "1"}, {"c", "3"}},
			vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}, {200000, 5}, {210000, 6}},
		},
		{
			lset: labels.Labels{{"a", "1"}, {"c", "3"}, {"d", "4"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		},
		{
			lset: labels.Labels{{"a", "1"}, {"c", "3"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		},
		{
			lset: labels.Labels{{"a", "1"}, {"c", "4"}},
			vals: []sample{{10000, 1}, {20000, 2}},
		},
		{
			lset: labels.Labels{{"a", "2"}, {"c", "3"}},
			vals: []sample{{10000, 1}, {20000, 2}, {60000, 3}, {70000, 4}},
		},
	}
	var series []storepb.Series
	for _, c := range input {
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
		s.Chunks = append(s.Chunks, storepb.AggrChunk{
			MinTime: smpls[0].t,
			MaxTime: smpls[len(smpls)-1].t,
			Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
		})
	}
	return storepb.NewSeriesResponse(&s)
}
