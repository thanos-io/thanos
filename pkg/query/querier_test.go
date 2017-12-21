package query

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"sort"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc"
)

func TestQuerier_LabelValues(t *testing.T) {
	a := &testStoreClient{
		values: map[string][]string{
			"test": []string{"a", "b", "c", "d"},
		},
	}
	b := &testStoreClient{
		values: map[string][]string{
			// The contract is that label values are sorted but we should be resilient
			// to misbehaving clients.
			"test": []string{"a", "out-of-order", "d", "x", "y"},
		},
	}
	c := &testStoreClient{
		values: map[string][]string{
			"test": []string{"e"},
		},
	}
	expected := []string{"a", "b", "c", "d", "e", "out-of-order", "x", "y"}

	q := newQuerier(context.Background(), nil, []*StoreInfo{
		&StoreInfo{Client: a},
		&StoreInfo{Client: b},
		&StoreInfo{Client: c},
	}, 0, 10000, "")
	defer q.Close()

	vals, err := q.LabelValues("test")
	testutil.Ok(t, err)
	testutil.Equals(t, expected, vals)
}

// TestQuerier_Series catches common edge cases encountered when querying multiple store nodes.
// It is not a subtitute for testing fanin/merge procedures in depth.
func TestQuerier_Series(t *testing.T) {
	a := &testStoreClient{
		series: []storepb.Series{
			testStoreSeries(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			testStoreSeries(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}),
		},
	}
	b := &testStoreClient{
		series: []storepb.Series{
			testStoreSeries(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
		},
	}
	c := &testStoreClient{
		series: []storepb.Series{
			testStoreSeries(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
		},
	}
	// Querier clamps the range to [1,300], which should drop some samples of the result above.
	// The store API allows endpoints to send more data then initially requested.
	q := newQuerier(context.Background(), nil, []*StoreInfo{
		&StoreInfo{Client: a},
		&StoreInfo{Client: b},
		&StoreInfo{Client: c},
	}, 1, 300, "")
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
			lset:    labels.FromStrings("a", "c"),
			samples: []sample{{100, 1}, {300, 3}},
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

func TestStoreSelectSingle(t *testing.T) {
	c := &testStoreClient{
		series: []storepb.Series{
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
		},
	}
	// Just verify we assembled the input data according to the store API contract.
	ok := sort.SliceIsSorted(c.series, func(i, j int) bool {
		return storepb.CompareLabels(c.series[i].Labels, c.series[j].Labels) < 0
	})
	testutil.Assert(t, ok, "input data unoreded")

	q := newQuerier(context.Background(), nil, nil, 0, 0, "b")

	res, err := q.selectSingle(context.Background(), c, true)
	testutil.Ok(t, err)

	exp := [][]storepb.Label{
		{
			{"a", "1"},
			{"c", "3"},
			{"b", "replica-1"},
		},
		{
			{"a", "1"},
			{"c", "3"},
			{"b", "replica-2"},
		},
		{
			{"a", "1"},
			{"c", "3"},
			{"d", "4"},
			{"b", "replica-1"},
		},
		{
			{"a", "1"},
			{"c", "4"},
			{"b", "replica-1"},
		},
	}
	var got [][]storepb.Label

	for res.Next() {
		lset, _ := res.At()
		got = append(got, lset)
	}
	testutil.Equals(t, exp, got)
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

func expandSeries(t testing.TB, it storage.SeriesIterator) (res []sample) {
	for it.Next() {
		t, v := it.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, it.Err())
	return res
}

func testStoreSeries(t testing.TB, lset labels.Labels, smpls []sample) (s storepb.Series) {
	for _, l := range lset {
		s.Labels = append(s.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}
	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	testutil.Ok(t, err)

	for _, smpl := range smpls {
		a.Append(smpl.t, smpl.v)
	}
	s.Chunks = append(s.Chunks, storepb.Chunk{
		Type:    storepb.Chunk_XOR,
		MinTime: smpls[0].t,
		MaxTime: smpls[len(smpls)-1].t,
		Data:    c.Bytes(),
	})
	return s
}

type testStoreClient struct {
	values map[string][]string
	series []storepb.Series
}

func (s *testStoreClient) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStoreClient) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return &testStoreSeriesClient{ctx: ctx, series: s.series}, nil
}

func (s *testStoreClient) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStoreClient) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{Values: s.values[req.Label]}, nil
}

type testStoreSeriesClient struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesClient
	ctx    context.Context
	series []storepb.Series
	i      int
}

func (c *testStoreSeriesClient) Recv() (*storepb.SeriesResponse, error) {
	if c.i >= len(c.series) {
		return nil, io.EOF
	}
	s := c.series[c.i]
	c.i++
	return &storepb.SeriesResponse{Series: s}, nil
}

func (c *testStoreSeriesClient) Context() context.Context {
	return c.ctx
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
			&sampleIterator{l: c.a, i: -1},
			&sampleIterator{l: c.b, i: -1},
		)
		res := expandSeries(t, it)
		testutil.Equals(t, c.exp, res)
	}
}

func BenchmarkDedupSeriesIterator(b *testing.B) {
	run := func(b *testing.B, s1, s2 []sample) {
		it := newDedupSeriesIterator(
			&sampleIterator{l: s1, i: -1},
			&sampleIterator{l: s2, i: -1},
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

type sampleIterator struct {
	l []sample
	i int
}

func (s *sampleIterator) Err() error {
	return nil
}

func (s *sampleIterator) At() (int64, float64) {
	return s.l[s.i].t, s.l[s.i].v
}

func (s *sampleIterator) Next() bool {
	if s.i >= len(s.l) {
		return false
	}
	s.i++
	return true
}

func (s *sampleIterator) Seek(t int64) bool {
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
