package store

import (
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/improbable-eng/thanos/pkg/compact/downsample"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/testutil"
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
		var m block.Meta
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
			var m block.Meta
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
		var m block.Meta
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
	}
	for _, c := range cases {
		res, ok := set.labelMatchers(c.in...)
		testutil.Equals(t, c.match, ok)
		testutil.Equals(t, c.res, res)
	}
}

func TestPartitionRanges(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	const maxGapSize = 1024 * 512

	for _, c := range []struct {
		input    [][2]int
		expected [][2]int
	}{
		{
			input:    [][2]int{{1, 10}},
			expected: [][2]int{{0, 1}},
		},
		{
			input:    [][2]int{{1, 2}, {3, 5}, {7, 10}},
			expected: [][2]int{{0, 3}},
		},
		{
			input: [][2]int{
				{1, 2},
				{3, 5},
				{20, 30},
				{maxGapSize + 31, maxGapSize + 32},
			},
			expected: [][2]int{{0, 3}, {3, 4}},
		},
		// Overlapping ranges.
		{
			input: [][2]int{
				{1, 30},
				{3, 28},
				{1, 4},
				{maxGapSize + 31, maxGapSize + 32},
				{maxGapSize + 31, maxGapSize + 40},
			},
			expected: [][2]int{{0, 3}, {3, 5}},
		},
	} {
		res := partitionRanges(len(c.input), func(i int) (uint64, uint64) {
			return uint64(c.input[i][0]), uint64(c.input[i][1])
		}, maxGapSize)
		testutil.Equals(t, c.expected, res)
	}
}
