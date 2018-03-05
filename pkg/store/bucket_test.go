package store

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/improbable-eng/thanos/pkg/compact/downsample"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/tsdb/labels"
)

// TODO(bplotka): This should go to the e2e tests package. Here should be mocked test.
// TODO(bplotka): Add leaktest when this is done: https://github.com/improbable-eng/thanos/issues/234
func TestBucketStore_e2e(t *testing.T) {
	bkt, cleanup, err := testutil.NewObjectStoreBucket(t)
	testutil.Ok(t, err)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_bucketstore_e2e")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "2", "b", "1"),
		labels.FromStrings("a", "2", "b", "2"),
		labels.FromStrings("a", "1", "c", "1"),
		labels.FromStrings("a", "1", "c", "2"),
		labels.FromStrings("a", "2", "c", "1"),
		labels.FromStrings("a", "2", "c", "2"),
	}
	start := time.Now()
	now := start

	minTime := int64(0)
	maxTime := int64(0)
	for i := 0; i < 3; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		if minTime == 0 {
			minTime = mint
		}
		maxTime = maxt

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := testutil.CreateBlock(dir, series[:4], 10, mint, maxt)
		testutil.Ok(t, err)
		id2, err := testutil.CreateBlock(dir, series[4:], 10, mint, maxt)
		testutil.Ok(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Add labels to the meta of the second block.
		meta, err := block.ReadMetaFile(dir2)
		testutil.Ok(t, err)
		meta.Thanos.Labels = map[string]string{"ext": "value"}
		testutil.Ok(t, block.WriteMetaFile(dir2, meta))

		// TODO(fabxc): remove the component dependency by factoring out the block interface.
		testutil.Ok(t, objstore.UploadDir(ctx, bkt, dir1, id1.String()))
		testutil.Ok(t, objstore.UploadDir(ctx, bkt, dir2, id2.String()))

		testutil.Ok(t, os.RemoveAll(dir1))
		testutil.Ok(t, os.RemoveAll(dir2))
	}

	store, err := NewBucketStore(nil, nil, bkt, dir, 100, 0)
	testutil.Ok(t, err)

	go func() {
		runutil.Repeat(100*time.Millisecond, ctx.Done(), func() error {
			return store.SyncBlocks(ctx)
		})
	}()

	ctx, _ = context.WithTimeout(ctx, 30*time.Second)

	err = runutil.Retry(100*time.Millisecond, ctx.Done(), func() error {
		if store.numBlocks() < 6 {
			return errors.New("not all blocks loaded")
		}
		return nil
	})
	testutil.Ok(t, err)

	mint, maxt := store.TimeRange()
	testutil.Equals(t, minTime, mint)
	testutil.Equals(t, maxTime, maxt)

	vals, err := store.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "a"})
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"1", "2"}, vals.Values)

	pbseries := [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
	}
	srv := newStoreSeriesServer(ctx)

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

	for i, s := range srv.SeriesSet {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	pbseries = [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
	}
	srv = newStoreSeriesServer(ctx)

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

	for i, s := range srv.SeriesSet {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	// Matching by external label should work as well.
	pbseries = [][]storepb.Label{
		{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}, {Name: "ext", Value: "value"}},
		{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}, {Name: "ext", Value: "value"}},
	}
	srv = newStoreSeriesServer(ctx)

	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
			{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "value"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, len(pbseries), len(srv.SeriesSet))

	for i, s := range srv.SeriesSet {
		testutil.Equals(t, pbseries[i], s.Labels)
		testutil.Equals(t, 3, len(s.Chunks))
	}

	srv = newStoreSeriesServer(ctx)
	err = store.Series(&storepb.SeriesRequest{
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
			{Type: storepb.LabelMatcher_EQ, Name: "ext", Value: "wrong-value"},
		},
		MinTime: timestamp.FromTime(start),
		MaxTime: timestamp.FromTime(now),
	}, srv)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(srv.SeriesSet))
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
		var m block.Meta
		m.Thanos.Downsample.Resolution = in.window
		m.MinTime = in.mint
		m.MaxTime = in.maxt
		set.add(&bucketBlock{meta: &m})
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
		set.add(&bucketBlock{meta: &m})
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
