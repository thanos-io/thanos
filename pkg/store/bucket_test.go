package store

import (
	"context"
	"io/ioutil"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/oklog/ulid"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestBucketBlock_Property(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(2000)
	parameters.MinSuccessfulTests = 20000
	properties := gopter.NewProperties(parameters)

	set := newBucketBlockSet(labels.Labels{})

	type resBlock struct {
		mint, maxt int64
		window     int64
	}
	// This input resembles a typical production-level block layout
	// in remote object storage.
	input := []resBlock{
		{window: downsample.ResLevel0, mint: 0, maxt: 100},
		{window: downsample.ResLevel0, mint: 100, maxt: 200},
		// Compaction level 2 begins but not downsampling (8 hour block length)
		{window: downsample.ResLevel0, mint: 200, maxt: 600},
		{window: downsample.ResLevel0, mint: 600, maxt: 1000},
		// Compaction level 3 begins, Some of it is downsampled but still retained (48 hour block length)
		{window: downsample.ResLevel0, mint: 1000, maxt: 1750},
		{window: downsample.ResLevel1, mint: 1000, maxt: 1750},
		// Compaction level 4 begins, different downsampling levels cover the same (336 hour block length)
		{window: downsample.ResLevel0, mint: 1750, maxt: 7000},
		{window: downsample.ResLevel1, mint: 1750, maxt: 7000},
		{window: downsample.ResLevel2, mint: 1750, maxt: 7000},
		// Compaction level 4 already happened, raw samples have been deleted
		{window: downsample.ResLevel0, mint: 7000, maxt: 14000},
		{window: downsample.ResLevel1, mint: 7000, maxt: 14000},
		// Compaction level 4 already happened, raw and downsample res level 1 samples have been deleted
		{window: downsample.ResLevel2, mint: 14000, maxt: 21000},
	}

	for _, in := range input {
		var m metadata.Meta
		m.Thanos.Downsample.Resolution = in.window
		m.MinTime = in.mint
		m.MaxTime = in.maxt

		testutil.Ok(t, set.add(&bucketBlock{meta: &m}))
	}

	properties.Property("getFor always gets at least some data in range", prop.ForAllNoShrink(
		func(low, high, maxResolution int64) bool {
			// Bogus case.
			if low >= high {
				return true
			}

			res := set.getFor(low, high, maxResolution)

			// The data that we get must all encompass our requested range
			if len(res) == 1 && (res[0].meta.Thanos.Downsample.Resolution > maxResolution ||
				res[0].meta.MinTime > low) {
				return false
			} else if len(res) > 1 {
				mint := int64(21001)
				for i := 0; i < len(res)-1; i++ {
					if res[i].meta.Thanos.Downsample.Resolution > maxResolution {
						return false
					}
					if res[i+1].meta.MinTime != res[i].meta.MaxTime {
						return false
					}
					if res[i].meta.MinTime < mint {
						mint = res[i].meta.MinTime
					}
				}
				if res[len(res)-1].meta.MinTime < mint {
					mint = res[len(res)-1].meta.MinTime
				}
				if low < mint {
					return false
				}

			}
			return true
		}, gen.Int64Range(0, 21000), gen.Int64Range(0, 21000), gen.Int64Range(0, 60*60*1000)),
	)

	properties.Property("getFor always gets all data in range", prop.ForAllNoShrink(
		func(low, high int64) bool {
			// Bogus case.
			if low >= high {
				return true
			}

			maxResolution := downsample.ResLevel2
			res := set.getFor(low, high, maxResolution)

			// The data that we get must all encompass our requested range
			if len(res) == 1 && (res[0].meta.Thanos.Downsample.Resolution > maxResolution ||
				res[0].meta.MinTime > low || res[0].meta.MaxTime < high) {
				return false
			} else if len(res) > 1 {
				mint := int64(21001)
				maxt := int64(0)
				for i := 0; i < len(res)-1; i++ {
					if res[i+1].meta.MinTime != res[i].meta.MaxTime {
						return false
					}
					if res[i].meta.MinTime < mint {
						mint = res[i].meta.MinTime
					}
					if res[i].meta.MaxTime > maxt {
						maxt = res[i].meta.MaxTime
					}
				}
				if res[len(res)-1].meta.MinTime < mint {
					mint = res[len(res)-1].meta.MinTime
				}
				if res[len(res)-1].meta.MaxTime > maxt {
					maxt = res[len(res)-1].meta.MaxTime
				}
				if low < mint {
					return false
				}
				if high > maxt {
					return false
				}

			}
			return true
		}, gen.Int64Range(0, 21000), gen.Int64Range(0, 21000)),
	)

	properties.TestingRun(t)
}

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
		maxResolution int64
		res           []resBlock
	}{
		{
			mint:          -100,
			maxt:          1000,
			maxResolution: 0,
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
			maxResolution: downsample.ResLevel1 - 1,
			res: []resBlock{
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 200, maxt: 300},
				{window: downsample.ResLevel0, mint: 300, maxt: 400},
			},
		}, {
			mint:          100,
			maxt:          500,
			maxResolution: downsample.ResLevel1,
			res: []resBlock{
				{window: downsample.ResLevel1, mint: 100, maxt: 200},
				{window: downsample.ResLevel1, mint: 200, maxt: 300},
				{window: downsample.ResLevel1, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          0,
			maxt:          500,
			maxResolution: downsample.ResLevel2,
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
		res := set.getFor(c.mint, c.maxt, c.maxResolution)
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

	dir, err := ioutil.TempDir("", "bucketstore-test")
	testutil.Ok(t, err)

	bucketStore, err := NewBucketStore(nil, nil, nil, dir, noopCache{}, 2e5, 0, 0, false, 20, filterConf)
	testutil.Ok(t, err)

	resp, err := bucketStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, storepb.StoreType_STORE, resp.StoreType)
	testutil.Equals(t, int64(math.MaxInt64), resp.MinTime)
	testutil.Equals(t, int64(math.MinInt64), resp.MaxTime)
}

func TestBucketStore_isBlockInMinMaxRange(t *testing.T) {
	ctx := context.TODO()
	dir, err := ioutil.TempDir("", "block-min-max-test")
	testutil.Ok(t, err)

	series := []labels.Labels{labels.FromStrings("a", "1", "b", "1")}
	extLset := labels.FromStrings("ext1", "value1")

	// Create a block in range [-2w, -1w]
	id1, err := testutil.CreateBlock(ctx, dir, series, 10,
		timestamp.FromTime(time.Now().Add(-14*24*time.Hour)),
		timestamp.FromTime(time.Now().Add(-7*24*time.Hour)),
		extLset, 0)
	testutil.Ok(t, err)

	// Create a block in range [-1w, 0w]
	id2, err := testutil.CreateBlock(ctx, dir, series, 10,
		timestamp.FromTime(time.Now().Add(-7*24*time.Hour)),
		timestamp.FromTime(time.Now().Add(-0*24*time.Hour)),
		extLset, 0)
	testutil.Ok(t, err)

	// Create a block in range [+1w, +2w]
	id3, err := testutil.CreateBlock(ctx, dir, series, 10,
		timestamp.FromTime(time.Now().Add(7*24*time.Hour)),
		timestamp.FromTime(time.Now().Add(14*24*time.Hour)),
		extLset, 0)
	testutil.Ok(t, err)

	dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())
	meta1, err := metadata.Read(dir1)
	testutil.Ok(t, err)
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), dir1, meta1))

	meta2, err := metadata.Read(dir2)
	testutil.Ok(t, err)
	testutil.Ok(t, metadata.Write(log.NewNopLogger(), dir2, meta2))

	// Run actual test
	hourBeforeDur := prommodel.Duration(-1 * time.Hour)
	hourBefore := model.TimeOrDurationValue{Dur: &hourBeforeDur}

	// bucketStore accepts blocks in range [0, now-1h]
	bucketStore, err := NewBucketStore(nil, nil, inmem.NewBucket(), dir, noopCache{}, 0, 0, 20, false, 20,
		&FilterConfig{
			MinTime: minTimeDuration,
			MaxTime: hourBefore,
		})
	testutil.Ok(t, err)

	inRange, err := bucketStore.isBlockInMinMaxRange(context.TODO(), id1)
	testutil.Ok(t, err)
	testutil.Equals(t, true, inRange)

	inRange, err = bucketStore.isBlockInMinMaxRange(context.TODO(), id2)
	testutil.Ok(t, err)
	testutil.Equals(t, true, inRange)

	inRange, err = bucketStore.isBlockInMinMaxRange(context.TODO(), id3)
	testutil.Ok(t, err)
	testutil.Equals(t, false, inRange)
}
