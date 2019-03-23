package store

import (
	"context"
	"io/ioutil"
	"math"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb/labels"
)

func TestBucketBlockSet_addGet(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	set := newBucketBlockSet(labels.Labels{})

	type resBlock struct {
		mint, maxt int64
		window     int64
	}
	input := []resBlock{
		// Blocks from 0 to 100 with raw resolution.
		{window: downsample.ResLevel0, mint: 0, maxt: 100},
		{window: downsample.ResLevel0, mint: 100, maxt: 200},
		{window: downsample.ResLevel0, mint: 200, maxt: 300},
		{window: downsample.ResLevel0, mint: 300, maxt: 400},
		{window: downsample.ResLevel0, mint: 400, maxt: 500},
		// Lower resolution data not covering last block.
		{window: downsample.ResLevel1, mint: 0, maxt: 100},
		{window: downsample.ResLevel1, mint: 100, maxt: 200},
		{window: downsample.ResLevel1, mint: 200, maxt: 300},
		{window: downsample.ResLevel1, mint: 300, maxt: 400},
		// Lower resolution data only covering middle blocks.
		{window: downsample.ResLevel2, mint: 100, maxt: 200},
		{window: downsample.ResLevel2, mint: 200, maxt: 300},
	}

	for _, in := range input {
		var m metadata.Meta
		m.Thanos.Downsample.Resolution = in.window
		m.MinTime = in.mint
		m.MaxTime = in.maxt

		testutil.Ok(t, set.add(&bucketBlock{meta: &m}))
	}

	cases := []struct {
		mint, maxt    int64
		minResolution int64
		res           []resBlock
	}{
		{
			mint:          -100,
			maxt:          1000,
			minResolution: 0,
			res: []resBlock{
				{window: downsample.ResLevel0, mint: 0, maxt: 100},
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 200, maxt: 300},
				{window: downsample.ResLevel0, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          100,
			maxt:          400,
			minResolution: downsample.ResLevel1 - 1,
			res: []resBlock{
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 200, maxt: 300},
				{window: downsample.ResLevel0, mint: 300, maxt: 400},
			},
		}, {
			mint:          100,
			maxt:          500,
			minResolution: downsample.ResLevel1,
			res: []resBlock{
				{window: downsample.ResLevel1, mint: 100, maxt: 200},
				{window: downsample.ResLevel1, mint: 200, maxt: 300},
				{window: downsample.ResLevel1, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          0,
			maxt:          500,
			minResolution: downsample.ResLevel2,
			res: []resBlock{
				{window: downsample.ResLevel1, mint: 0, maxt: 100},
				{window: downsample.ResLevel2, mint: 100, maxt: 200},
				{window: downsample.ResLevel2, mint: 200, maxt: 300},
				{window: downsample.ResLevel1, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		},
	}
	for i, c := range cases {
		t.Logf("case %d", i)

		var exp []*bucketBlock
		for _, b := range c.res {
			var m metadata.Meta
			m.Thanos.Downsample.Resolution = b.window
			m.MinTime = b.mint
			m.MaxTime = b.maxt
			exp = append(exp, &bucketBlock{meta: &m})
		}
		res := set.getFor(c.mint, c.maxt, c.minResolution)
		testutil.Equals(t, exp, res)
	}
}

func TestBucketBlockSet_remove(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	set := newBucketBlockSet(labels.Labels{})

	type resBlock struct {
		id         ulid.ULID
		mint, maxt int64
	}
	input := []resBlock{
		{id: ulid.MustNew(1, nil), mint: 0, maxt: 100},
		{id: ulid.MustNew(2, nil), mint: 100, maxt: 200},
		{id: ulid.MustNew(3, nil), mint: 200, maxt: 300},
	}

	for _, in := range input {
		var m metadata.Meta
		m.ULID = in.id
		m.MinTime = in.mint
		m.MaxTime = in.maxt
		testutil.Ok(t, set.add(&bucketBlock{meta: &m}))
	}
	set.remove(input[1].id)
	res := set.getFor(0, 300, 0)

	testutil.Equals(t, 2, len(res))
	testutil.Equals(t, input[0].id, res[0].meta.ULID)
	testutil.Equals(t, input[2].id, res[1].meta.ULID)
}

func TestBucketBlockSet_labelMatchers(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	set := newBucketBlockSet(labels.FromStrings("a", "b", "c", "d"))

	cases := []struct {
		in    []labels.Matcher
		res   []labels.Matcher
		match bool
	}{
		{
			in:    []labels.Matcher{},
			res:   []labels.Matcher{},
			match: true,
		},
		{
			in: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
				labels.NewEqualMatcher("c", "d"),
			},
			res:   []labels.Matcher{},
			match: true,
		},
		{
			in: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
				labels.NewEqualMatcher("c", "b"),
			},
			match: false,
		},
		{
			in: []labels.Matcher{
				labels.NewEqualMatcher("a", "b"),
				labels.NewEqualMatcher("e", "f"),
			},
			res: []labels.Matcher{
				labels.NewEqualMatcher("e", "f"),
			},
			match: true,
		},
		// Those are matchers mentioned here: https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
		// We want to provide explicit tests that says when Thanos supports its and when not. We don't support it here in
		// external labelset level.
		{
			in: []labels.Matcher{
				labels.Not(labels.NewEqualMatcher("", "x")),
			},
			res: []labels.Matcher{
				labels.Not(labels.NewEqualMatcher("", "x")),
			},
			match: true,
		},
		{
			in: []labels.Matcher{
				labels.Not(labels.NewEqualMatcher("", "d")),
			},
			res: []labels.Matcher{
				labels.Not(labels.NewEqualMatcher("", "d")),
			},
			match: true,
		},
	}
	for _, c := range cases {
		res, ok := set.labelMatchers(c.in...)
		testutil.Equals(t, c.match, ok)
		testutil.Equals(t, c.res, res)
	}
}

func TestGapBasedPartitioner_Partition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	const maxGapSize = 1024 * 512

	for _, c := range []struct {
		input    [][2]int
		expected []part
	}{
		{
			input:    [][2]int{{1, 10}},
			expected: []part{{start: 1, end: 10, elemRng: [2]int{0, 1}}},
		},
		{
			input:    [][2]int{{1, 2}, {3, 5}, {7, 10}},
			expected: []part{{start: 1, end: 10, elemRng: [2]int{0, 3}}},
		},
		{
			input: [][2]int{
				{1, 2},
				{3, 5},
				{20, 30},
				{maxGapSize + 31, maxGapSize + 32},
			},
			expected: []part{
				{start: 1, end: 30, elemRng: [2]int{0, 3}},
				{start: maxGapSize + 31, end: maxGapSize + 32, elemRng: [2]int{3, 4}},
			},
		},
		// Overlapping ranges.
		{
			input: [][2]int{
				{1, 30},
				{1, 4},
				{3, 28},
				{maxGapSize + 31, maxGapSize + 32},
				{maxGapSize + 31, maxGapSize + 40},
			},
			expected: []part{
				{start: 1, end: 30, elemRng: [2]int{0, 3}},
				{start: maxGapSize + 31, end: maxGapSize + 40, elemRng: [2]int{3, 5}},
			},
		},
		{
			input: [][2]int{
				// Mimick AllPostingsKey, where range specified whole range.
				{1, 15},
				{1, maxGapSize + 100},
				{maxGapSize + 31, maxGapSize + 40},
			},
			expected: []part{{start: 1, end: maxGapSize + 100, elemRng: [2]int{0, 3}}},
		},
	} {
		res := gapBasedPartitioner{maxGapSize: maxGapSize}.Partition(len(c.input), func(i int) (uint64, uint64) {
			return uint64(c.input[i][0]), uint64(c.input[i][1])
		})
		testutil.Equals(t, c.expected, res)
	}
}

func TestBucketStore_Info(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "prometheus-test")
	testutil.Ok(t, err)

	bucketStore, err := NewBucketStore(nil, nil, nil, dir, 2e5, 2e5, 0, 0, false, 20)
	testutil.Ok(t, err)

	resp, err := bucketStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, storepb.StoreType_STORE, resp.StoreType)
	testutil.Equals(t, int64(math.MaxInt64), resp.MinTime)
	testutil.Equals(t, int64(math.MinInt64), resp.MaxTime)
}
