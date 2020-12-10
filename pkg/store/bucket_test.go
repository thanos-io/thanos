// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/oklog/ulid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/pool"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

var emptyRelabelConfig = make([]*relabel.Config, 0)

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
		// Compaction level 2 begins but not downsampling (8 hour block length).
		{window: downsample.ResLevel0, mint: 200, maxt: 600},
		{window: downsample.ResLevel0, mint: 600, maxt: 1000},
		// Compaction level 3 begins, Some of it is downsampled but still retained (48 hour block length).
		{window: downsample.ResLevel0, mint: 1000, maxt: 1750},
		{window: downsample.ResLevel1, mint: 1000, maxt: 1750},
		// Compaction level 4 begins, different downsampling levels cover the same (336 hour block length).
		{window: downsample.ResLevel0, mint: 1750, maxt: 7000},
		{window: downsample.ResLevel1, mint: 1750, maxt: 7000},
		{window: downsample.ResLevel2, mint: 1750, maxt: 7000},
		// Compaction level 4 already happened, raw samples have been deleted.
		{window: downsample.ResLevel0, mint: 7000, maxt: 14000},
		{window: downsample.ResLevel1, mint: 7000, maxt: 14000},
		// Compaction level 4 already happened, raw and downsample res level 1 samples have been deleted.
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

			res := set.getFor(low, high, maxResolution, nil)

			// The data that we get must all encompass our requested range.
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
			res := set.getFor(low, high, maxResolution, nil)

			// The data that we get must all encompass our requested range.
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

func TestBucketBlock_matchLabels(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	dir, err := ioutil.TempDir("", "bucketblock-test")
	testutil.Ok(t, err)
	defer testutil.Ok(t, os.RemoveAll(dir))

	bkt, err := filesystem.NewBucket(dir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	blockID := ulid.MustNew(1, nil)
	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: blockID},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
	}

	b, err := newBucketBlock(context.Background(), log.NewNopLogger(), newBucketStoreMetrics(nil), meta, bkt, path.Join(dir, blockID.String()), nil, nil, nil, nil)
	testutil.Ok(t, err)

	cases := []struct {
		in    []*labels.Matcher
		match bool
	}{
		{
			in:    []*labels.Matcher{},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "c", Value: "d"},
			},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "c", Value: "b"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "e", Value: "f"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: block.BlockIDLabel, Value: blockID.String()},
			},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: block.BlockIDLabel, Value: "xxx"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: block.BlockIDLabel, Value: blockID.String()},
				{Type: labels.MatchEqual, Name: "c", Value: "b"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "x"},
			},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "d"},
			},
			match: true,
		},
	}
	for _, c := range cases {
		ok := b.matchRelabelLabels(c.in)
		testutil.Equals(t, c.match, ok)
	}

	// Ensure block's labels in the meta have not been manipulated.
	testutil.Equals(t, map[string]string{
		"a": "b",
		"c": "d",
	}, meta.Thanos.Labels)
}

func TestBucketBlockSet_addGet(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	set := newBucketBlockSet(labels.Labels{})

	type resBlock struct {
		mint, maxt int64
		window     int64
	}
	// Input is expected to be sorted. It is sorted in addBlock.
	input := []resBlock{
		// Blocks from 0 to 100 with raw resolution.
		{window: downsample.ResLevel0, mint: 0, maxt: 100},
		{window: downsample.ResLevel0, mint: 100, maxt: 200},
		{window: downsample.ResLevel0, mint: 100, maxt: 200}, // Same overlap.
		{window: downsample.ResLevel0, mint: 200, maxt: 299}, // Short overlap.
		{window: downsample.ResLevel0, mint: 200, maxt: 300},
		{window: downsample.ResLevel0, mint: 300, maxt: 400},
		{window: downsample.ResLevel0, mint: 300, maxt: 600}, // Long overlap.
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

	for _, c := range []struct {
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
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 200, maxt: 299},
				{window: downsample.ResLevel0, mint: 200, maxt: 300},
				{window: downsample.ResLevel0, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          100,
			maxt:          400,
			maxResolution: downsample.ResLevel1 - 1,
			res: []resBlock{
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 200, maxt: 299},
				{window: downsample.ResLevel0, mint: 200, maxt: 300},
				{window: downsample.ResLevel0, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
				// Block intervals are half-open: [b.MinTime, b.MaxTime), so 400-500 contains single sample.
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          100,
			maxt:          500,
			maxResolution: downsample.ResLevel1,
			res: []resBlock{
				{window: downsample.ResLevel1, mint: 100, maxt: 200},
				{window: downsample.ResLevel1, mint: 200, maxt: 300},
				{window: downsample.ResLevel1, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
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
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			var exp []*bucketBlock
			for _, b := range c.res {
				var m metadata.Meta
				m.Thanos.Downsample.Resolution = b.window
				m.MinTime = b.mint
				m.MaxTime = b.maxt
				exp = append(exp, &bucketBlock{meta: &m})
			}
			testutil.Equals(t, exp, set.getFor(c.mint, c.maxt, c.maxResolution, nil))
		})
	}
}

func TestBucketBlockSet_remove(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

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
	res := set.getFor(0, 300, 0, nil)

	testutil.Equals(t, 2, len(res))
	testutil.Equals(t, input[0].id, res[0].meta.ULID)
	testutil.Equals(t, input[2].id, res[1].meta.ULID)
}

func TestBucketBlockSet_labelMatchers(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	set := newBucketBlockSet(labels.FromStrings("a", "b", "c", "d"))

	cases := []struct {
		in    []*labels.Matcher
		res   []*labels.Matcher
		match bool
	}{
		{
			in:    []*labels.Matcher{},
			res:   []*labels.Matcher{},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "c", Value: "d"},
			},
			res:   []*labels.Matcher{},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "c", Value: "b"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "e", Value: "f"},
			},
			res: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "e", Value: "f"},
			},
			match: true,
		},
		// Those are matchers mentioned here: https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
		// We want to provide explicit tests that says when Thanos supports its and when not. We don't support it here in
		// external labelset level.
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "x"},
			},
			res: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "x"},
			},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "d"},
			},
			res: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "d"},
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
	defer testutil.TolerantVerifyLeak(t)

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
	defer testutil.TolerantVerifyLeak(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "bucketstore-test")
	testutil.Ok(t, err)

	defer testutil.Ok(t, os.RemoveAll(dir))

	bucketStore, err := NewBucketStore(
		nil,
		nil,
		nil,
		nil,
		dir,
		noopCache{},
		nil,
		2e5,
		NewChunksLimiterFactory(0),
		false,
		20,
		allowAllFilterConf,
		true,
		DefaultPostingOffsetInMemorySampling,
		false,
		false,
		0,
	)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bucketStore.Close()) }()

	resp, err := bucketStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, storepb.StoreType_STORE, resp.StoreType)
	testutil.Equals(t, int64(math.MaxInt64), resp.MinTime)
	testutil.Equals(t, int64(math.MinInt64), resp.MaxTime)
	testutil.Equals(t, []labelpb.ZLabelSet(nil), resp.LabelSets)
	testutil.Equals(t, []labelpb.ZLabel(nil), resp.Labels)
}

type recorder struct {
	mtx sync.Mutex
	objstore.Bucket

	getRangeTouched []string
	getTouched      []string
}

func (r *recorder) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.getTouched = append(r.getTouched, name)
	return r.Bucket.Get(ctx, name)
}

func (r *recorder) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.getRangeTouched = append(r.getRangeTouched, name)
	return r.Bucket.GetRange(ctx, name, off, length)
}

func TestBucketStore_Sharding(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	dir, err := ioutil.TempDir("", "test-sharding-prepare")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	bkt := objstore.NewInMemBucket()
	series := []labels.Labels{labels.FromStrings("a", "1", "b", "1")}

	id1, err := e2eutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r1"}}, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id1.String())))

	id2, err := e2eutil.CreateBlock(ctx, dir, series, 10, 1000, 2000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r1"}}, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id2.String())))

	id3, err := e2eutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "b"}, {Name: "region", Value: "r1"}}, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id3.String())))

	id4, err := e2eutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r2"}}, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id4.String())))

	if ok := t.Run("new_runs", func(t *testing.T) {
		testSharding(t, "", bkt, id1, id2, id3, id4)
	}); !ok {
		return
	}

	dir2, err := ioutil.TempDir("", "test-sharding2")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir2)) }()

	t.Run("reuse_disk", func(t *testing.T) {
		testSharding(t, dir2, bkt, id1, id2, id3, id4)
	})
}

func testSharding(t *testing.T, reuseDisk string, bkt objstore.Bucket, all ...ulid.ULID) {
	var cached []ulid.ULID

	logger := log.NewLogfmtLogger(os.Stderr)
	for _, sc := range []struct {
		name              string
		relabel           string
		expectedIDs       []ulid.ULID
		expectedAdvLabels []labelpb.ZLabelSet
	}{
		{
			name:        "no sharding",
			expectedIDs: all,
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r2"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "b"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "drop cluster=a sources",
			relabel: `
            - action: drop
              regex: "a"
              source_labels:
              - cluster
            `,
			expectedIDs: []ulid.ULID{all[2]},
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "b"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "keep only cluster=a sources",
			relabel: `
            - action: keep
              regex: "a"
              source_labels:
              - cluster
            `,
			expectedIDs: []ulid.ULID{all[0], all[1], all[3]},
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r2"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "keep only cluster=a without .*2 region sources",
			relabel: `
            - action: keep
              regex: "a"
              source_labels:
              - cluster
            - action: drop
              regex: ".*2"
              source_labels:
              - region
            `,
			expectedIDs: []ulid.ULID{all[0], all[1]},
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "drop all",
			relabel: `
            - action: drop
              regex: "a"
              source_labels:
              - cluster
            - action: drop
              regex: "r1"
              source_labels:
              - region
            `,
			expectedIDs:       []ulid.ULID{},
			expectedAdvLabels: []labelpb.ZLabelSet{},
		},
	} {
		t.Run(sc.name, func(t *testing.T) {
			dir := reuseDisk

			if dir == "" {
				var err error
				dir, err = ioutil.TempDir("", "test-sharding")
				testutil.Ok(t, err)
				defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()
			}
			relabelConf, err := block.ParseRelabelConfig([]byte(sc.relabel), block.SelectorSupportedRelabelActions)
			testutil.Ok(t, err)

			rec := &recorder{Bucket: bkt}
			metaFetcher, err := block.NewMetaFetcher(logger, 20, objstore.WithNoopInstr(bkt), dir, nil, []block.MetadataFilter{
				block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
				block.NewLabelShardedMetaFilter(relabelConf),
			}, nil)
			testutil.Ok(t, err)

			bucketStore, err := NewBucketStore(
				logger,
				nil,
				objstore.WithNoopInstr(rec),
				metaFetcher,
				dir,
				noopCache{},
				nil,
				0,
				NewChunksLimiterFactory(0),
				false,
				20,
				allowAllFilterConf,
				true,
				DefaultPostingOffsetInMemorySampling,
				false,
				false,
				0,
			)
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, bucketStore.Close()) }()

			testutil.Ok(t, bucketStore.InitialSync(context.Background()))

			// Check "stored" blocks.
			ids := make([]ulid.ULID, 0, len(bucketStore.blocks))
			for id := range bucketStore.blocks {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool {
				return ids[i].Compare(ids[j]) < 0
			})
			testutil.Equals(t, sc.expectedIDs, ids)

			// Check Info endpoint.
			resp, err := bucketStore.Info(context.Background(), &storepb.InfoRequest{})
			testutil.Ok(t, err)

			testutil.Equals(t, storepb.StoreType_STORE, resp.StoreType)
			testutil.Equals(t, []labelpb.ZLabel(nil), resp.Labels)
			testutil.Equals(t, sc.expectedAdvLabels, resp.LabelSets)

			// Make sure we don't download files we did not expect to.
			// Regression test: https://github.com/thanos-io/thanos/issues/1664

			// Sort records. We load blocks concurrently so operations might be not ordered.
			sort.Strings(rec.getRangeTouched)

			// With binary header nothing should be downloaded fully.
			testutil.Equals(t, []string(nil), rec.getTouched)
			if reuseDisk != "" {
				testutil.Equals(t, expectedTouchedBlockOps(all, sc.expectedIDs, cached), rec.getRangeTouched)
				cached = sc.expectedIDs
				return
			}

			testutil.Equals(t, expectedTouchedBlockOps(all, sc.expectedIDs, nil), rec.getRangeTouched)
		})
	}
}

func expectedTouchedBlockOps(all []ulid.ULID, expected []ulid.ULID, cached []ulid.ULID) []string {
	var ops []string
	for _, id := range all {
		blockCached := false
		for _, fid := range cached {
			if id.Compare(fid) == 0 {
				blockCached = true
				break
			}
		}
		if blockCached {
			continue
		}

		found := false
		for _, fid := range expected {
			if id.Compare(fid) == 0 {
				found = true
				break
			}
		}

		if found {
			ops = append(ops,
				// To create binary header we touch part of index few times.
				path.Join(id.String(), block.IndexFilename), // Version.
				path.Join(id.String(), block.IndexFilename), // TOC.
				path.Join(id.String(), block.IndexFilename), // Symbols.
				path.Join(id.String(), block.IndexFilename), // PostingOffsets.
			)
		}
	}
	sort.Strings(ops)
	return ops
}

// Regression tests against: https://github.com/thanos-io/thanos/issues/1983.
func TestReadIndexCache_LoadSeries(t *testing.T) {
	bkt := objstore.NewInMemBucket()

	s := newBucketStoreMetrics(nil)
	b := &bucketBlock{
		meta: &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: ulid.MustNew(1, nil),
			},
		},
		bkt:        bkt,
		logger:     log.NewNopLogger(),
		metrics:    s,
		indexCache: noopCache{},
	}

	buf := encoding.Encbuf{}
	buf.PutByte(0)
	buf.PutByte(0)
	buf.PutUvarint(10)
	buf.PutString("aaaaaaaaaa")
	buf.PutUvarint(10)
	buf.PutString("bbbbbbbbbb")
	buf.PutUvarint(10)
	buf.PutString("cccccccccc")
	testutil.Ok(t, bkt.Upload(context.Background(), filepath.Join(b.meta.ULID.String(), block.IndexFilename), bytes.NewReader(buf.Get())))

	r := bucketIndexReader{
		block:        b,
		stats:        &queryStats{},
		loadedSeries: map[uint64][]byte{},
	}

	// Success with no refetches.
	testutil.Ok(t, r.loadSeries(context.TODO(), []uint64{2, 13, 24}, false, 2, 100))
	testutil.Equals(t, map[uint64][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, r.loadedSeries)
	testutil.Equals(t, float64(0), promtest.ToFloat64(s.seriesRefetches))

	// Success with 2 refetches.
	r.loadedSeries = map[uint64][]byte{}
	testutil.Ok(t, r.loadSeries(context.TODO(), []uint64{2, 13, 24}, false, 2, 15))
	testutil.Equals(t, map[uint64][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, r.loadedSeries)
	testutil.Equals(t, float64(2), promtest.ToFloat64(s.seriesRefetches))

	// Success with refetch on first element.
	r.loadedSeries = map[uint64][]byte{}
	testutil.Ok(t, r.loadSeries(context.TODO(), []uint64{2}, false, 2, 5))
	testutil.Equals(t, map[uint64][]byte{
		2: []byte("aaaaaaaaaa"),
	}, r.loadedSeries)
	testutil.Equals(t, float64(3), promtest.ToFloat64(s.seriesRefetches))

	buf.Reset()
	buf.PutByte(0)
	buf.PutByte(0)
	buf.PutUvarint(10)
	buf.PutString("aaaaaaa")
	testutil.Ok(t, bkt.Upload(context.Background(), filepath.Join(b.meta.ULID.String(), block.IndexFilename), bytes.NewReader(buf.Get())))

	// Fail, but no recursion at least.
	testutil.NotOk(t, r.loadSeries(context.TODO(), []uint64{2, 13, 24}, false, 1, 15))
}

func TestBucketIndexReader_ExpandedPostings(t *testing.T) {
	tb := testutil.NewTB(t)

	tmpDir, err := ioutil.TempDir("", "test-expanded-postings")
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(tb, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(tb, bkt.Close()) }()

	id := uploadTestBlock(tb, tmpDir, bkt, 500)

	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, DefaultPostingOffsetInMemorySampling)
	testutil.Ok(tb, err)

	benchmarkExpandedPostings(tb, bkt, id, r, 500)
}

func BenchmarkBucketIndexReader_ExpandedPostings(b *testing.B) {
	tb := testutil.NewTB(b)

	tmpDir, err := ioutil.TempDir("", "bench-expanded-postings")
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(tb, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(tb, bkt.Close()) }()

	id := uploadTestBlock(tb, tmpDir, bkt, 50e5)
	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, DefaultPostingOffsetInMemorySampling)
	testutil.Ok(tb, err)

	benchmarkExpandedPostings(tb, bkt, id, r, 50e5)
}

func uploadTestBlock(t testing.TB, tmpDir string, bkt objstore.Bucket, series int) ulid.ULID {
	h, err := tsdb.NewHead(nil, nil, nil, 1000, tmpDir, nil, chunks.DefaultWriteBufferSize, tsdb.DefaultStripeSize, nil)
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, h.Close())
	}()

	logger := log.NewNopLogger()

	appendTestData(t, h.Appender(context.Background()), series)

	testutil.Ok(t, os.MkdirAll(filepath.Join(tmpDir, "tmp"), os.ModePerm))
	id := createBlockFromHead(t, filepath.Join(tmpDir, "tmp"), h)

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, "tmp", id.String()), metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, "tmp", id.String())))

	return id
}

func appendTestData(t testing.TB, app storage.Appender, series int) {
	addSeries := func(l labels.Labels) {
		_, err := app.Add(l, 0, 0)
		testutil.Ok(t, err)
	}

	series = series / 5
	for n := 0; n < 10; n++ {
		for i := 0; i < series/10; i++ {
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "foo"))
			// Have some series that won't be matched, to properly test inverted matches.
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "bar"))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", "0_"+strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "bar"))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", "1_"+strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "bar"))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", "2_"+strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "foo"))
		}
	}
	testutil.Ok(t, app.Commit())
}

func createBlockFromHead(t testing.TB, dir string, head *tsdb.Head) ulid.ULID {
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(t, err)

	testutil.Ok(t, os.MkdirAll(dir, 0777))

	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	ulid, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
	testutil.Ok(t, err)
	return ulid
}

// Very similar benchmark to ths: https://github.com/prometheus/prometheus/blob/1d1732bc25cc4b47f513cb98009a4eb91879f175/tsdb/querier_bench_test.go#L82,
// but with postings results check when run as test.
func benchmarkExpandedPostings(
	t testutil.TB,
	bkt objstore.BucketReader,
	id ulid.ULID,
	r indexheader.Reader,
	series int,
) {
	n1 := labels.MustNewMatcher(labels.MatchEqual, "n", "1"+storetestutil.LabelLongSuffix)

	jFoo := labels.MustNewMatcher(labels.MatchEqual, "j", "foo")
	jNotFoo := labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")

	iStar := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.*$")
	iPlus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")
	i1Plus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^1.+$")
	iEmptyRe := labels.MustNewMatcher(labels.MatchRegexp, "i", "^$")
	iNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "i", "")
	iNot2 := labels.MustNewMatcher(labels.MatchNotEqual, "n", "2"+storetestutil.LabelLongSuffix)
	iNot2Star := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")
	iRegexSet := labels.MustNewMatcher(labels.MatchRegexp, "i", "0"+storetestutil.LabelLongSuffix+"|1"+storetestutil.LabelLongSuffix+"|2"+storetestutil.LabelLongSuffix)

	series = series / 5
	cases := []struct {
		name     string
		matchers []*labels.Matcher

		expectedLen int
	}{
		{`n="1"`, []*labels.Matcher{n1}, int(float64(series) * 0.2)},
		{`n="1",j="foo"`, []*labels.Matcher{n1, jFoo}, int(float64(series) * 0.1)},
		{`j="foo",n="1"`, []*labels.Matcher{jFoo, n1}, int(float64(series) * 0.1)},
		{`n="1",j!="foo"`, []*labels.Matcher{n1, jNotFoo}, int(float64(series) * 0.1)},
		{`i=~".*"`, []*labels.Matcher{iStar}, 5 * series},
		{`i=~".+"`, []*labels.Matcher{iPlus}, 5 * series},
		{`i=~""`, []*labels.Matcher{iEmptyRe}, 0},
		{`i!=""`, []*labels.Matcher{iNotEmpty}, 5 * series},
		{`n="1",i=~".*",j="foo"`, []*labels.Matcher{n1, iStar, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".*",i!="2",j="foo"`, []*labels.Matcher{n1, iStar, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i!=""`, []*labels.Matcher{n1, iNotEmpty}, int(float64(series) * 0.2)},
		{`n="1",i!="",j="foo"`, []*labels.Matcher{n1, iNotEmpty, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".+",j="foo"`, []*labels.Matcher{n1, iPlus, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~"1.+",j="foo"`, []*labels.Matcher{n1, i1Plus, jFoo}, int(float64(series) * 0.011111)},
		{`n="1",i=~".+",i!="2",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".+",i!~"2.*",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2Star, jFoo}, int(1 + float64(series)*0.088888)},
		{`i=~"0|1|2"`, []*labels.Matcher{iRegexSet}, 150}, // 50 series for "1", 50 for "2" and 50 for "3".
	}

	for _, c := range cases {
		t.Run(c.name, func(t testutil.TB) {
			b := &bucketBlock{
				logger:            log.NewNopLogger(),
				metrics:           newBucketStoreMetrics(nil),
				indexHeaderReader: r,
				indexCache:        noopCache{},
				bkt:               bkt,
				meta:              &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}},
				partitioner:       gapBasedPartitioner{maxGapSize: partitionerMaxGapSize},
			}

			indexr := newBucketIndexReader(context.Background(), b)

			t.ResetTimer()
			for i := 0; i < t.N(); i++ {
				p, err := indexr.ExpandedPostings(c.matchers)
				testutil.Ok(t, err)
				testutil.Equals(t, c.expectedLen, len(p))
			}
		})
	}
}

func TestBucketSeries(t *testing.T) {
	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, false, samplesPerSeries, series, 1)
	})
}

func TestBucketSkipChunksSeries(t *testing.T) {
	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, true, samplesPerSeries, series, 1)
	})
}

func BenchmarkBucketSeries(b *testing.B) {
	tb := testutil.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, false, samplesPerSeries, series, 1/100e6, 1/10e4, 1)
	})
}

func BenchmarkBucketSkipChunksSeries(b *testing.B) {
	tb := testutil.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, true, samplesPerSeries, series, 1/100e6, 1/10e4, 1)
	})
}

func benchBucketSeries(t testutil.TB, skipChunk bool, samplesPerSeries, totalSeries int, requestedRatios ...float64) {
	const numOfBlocks = 4

	tmpDir, err := ioutil.TempDir("", "testorbench-bucketseries")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	var (
		logger = log.NewNopLogger()
		blocks []*bucketBlock
		series []*storepb.Series
		random = rand.New(rand.NewSource(120))
	)

	extLset := labels.Labels{{Name: "ext1", Value: "1"}}
	thanosMeta := metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	var chunkPool pool.BytesPool
	chunkPool, err = pool.NewBucketedBytesPool(maxChunkSize, 50e6, 2, 100e7)
	testutil.Ok(t, err)

	if !t.IsBenchmark() {
		chunkPool = &mockedPool{parent: chunkPool}
	}
	blockDir := filepath.Join(tmpDir, "tmp")

	samplesPerSeriesPerBlock := samplesPerSeries / numOfBlocks
	if samplesPerSeriesPerBlock == 0 {
		samplesPerSeriesPerBlock = 1
	}

	seriesPerBlock := totalSeries / numOfBlocks
	if seriesPerBlock == 0 {
		seriesPerBlock = 1
	}

	// Create 4 blocks. Each will have seriesPerBlock number of series that have samplesPerSeriesPerBlock samples.
	// Timestamp will be counted for each new series and new sample, so each each series will have unique timestamp.
	// This allows to pick time range that will correspond to number of series picked 1:1.
	for bi := 0; bi < numOfBlocks; bi++ {
		head, bSeries := storetestutil.CreateHeadWithSeries(t, bi, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", bi)),
			SamplesPerSeries: samplesPerSeriesPerBlock,
			Series:           seriesPerBlock,
			PrependLabels:    extLset,
			Random:           random,
			SkipChunks:       t.IsBenchmark() || skipChunk,
		})
		id := createBlockFromHead(t, blockDir, head)
		testutil.Ok(t, head.Close())
		series = append(series, bSeries...)

		meta, err := metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, id.String()), thanosMeta, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String())))

		m := newBucketStoreMetrics(nil)
		b := &bucketBlock{
			indexCache:  noopCache{},
			logger:      logger,
			metrics:     m,
			bkt:         bkt,
			meta:        meta,
			partitioner: gapBasedPartitioner{maxGapSize: partitionerMaxGapSize},
			chunkObjs:   []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:   chunkPool,
		}
		blocks = append(blocks, b)
	}

	store := &BucketStore{
		bkt:             objstore.WithNoopInstr(bkt),
		logger:          logger,
		indexCache:      noopCache{},
		indexReaderPool: indexheader.NewReaderPool(log.NewNopLogger(), false, 0, nil),
		metrics:         newBucketStoreMetrics(nil),
		blockSets: map[uint64]*bucketBlockSet{
			labels.Labels{{Name: "ext1", Value: "1"}}.Hash(): {blocks: [][]*bucketBlock{blocks}},
		},
		queryGate:            noopGate{},
		chunksLimiterFactory: NewChunksLimiterFactory(0),
	}

	for _, block := range blocks {
		block.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, block.meta.ULID, DefaultPostingOffsetInMemorySampling)
		testutil.Ok(t, err)
	}

	var bCases []*storetestutil.SeriesCase
	for _, p := range requestedRatios {
		expectedSamples := int(p * float64(totalSeries*samplesPerSeries))
		if expectedSamples == 0 {
			expectedSamples = 1
		}
		seriesCut := int(p * float64(numOfBlocks*seriesPerBlock))
		if seriesCut == 0 {
			seriesCut = 1
		} else if seriesCut == 1 {
			seriesCut = expectedSamples / samplesPerSeriesPerBlock
		}

		bCases = append(bCases, &storetestutil.SeriesCase{
			Name: fmt.Sprintf("%dof%d", expectedSamples, totalSeries*samplesPerSeries),
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: int64(expectedSamples) - 1,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
				SkipChunks: skipChunk,
			},
			// This does not cut chunks properly, but those are assured against for non benchmarks only, where we use 100% case only.
			ExpectedSeries: series[:seriesCut],
		})
	}
	storetestutil.TestServerSeries(t, store, bCases...)

	if !t.IsBenchmark() {
		if !skipChunk {
			// Make sure the pool is correctly used. This is expected for 200k numbers.
			testutil.Equals(t, numOfBlocks, int(chunkPool.(*mockedPool).gets.Load()))
			// TODO(bwplotka): This is wrong negative for large number of samples (1mln). Investigate.
			testutil.Equals(t, 0, int(chunkPool.(*mockedPool).balance.Load()))
			chunkPool.(*mockedPool).gets.Store(0)
		}

		for _, b := range blocks {
			// NOTE(bwplotka): It is 4 x 1.0 for 100mln samples. Kind of make sense: long series.
			testutil.Equals(t, 0.0, promtest.ToFloat64(b.metrics.seriesRefetches))
		}
	}
}

var _ = fakePool{}

type fakePool struct{}

func (m fakePool) Get(sz int) (*[]byte, error) {
	b := make([]byte, 0, sz)
	return &b, nil
}

func (m fakePool) Put(_ *[]byte) {}

type mockedPool struct {
	parent  pool.BytesPool
	balance atomic.Uint64
	gets    atomic.Uint64
}

func (m *mockedPool) Get(sz int) (*[]byte, error) {
	b, err := m.parent.Get(sz)
	if err != nil {
		return nil, err
	}
	m.balance.Add(uint64(cap(*b)))
	m.gets.Add(uint64(1))
	return b, nil
}

func (m *mockedPool) Put(b *[]byte) {
	m.balance.Sub(uint64(cap(*b)))
	m.parent.Put(b)
}

type noopGate struct{}

func (noopGate) Start(context.Context) error { return nil }
func (noopGate) Done()                       {}

// Regression test against: https://github.com/thanos-io/thanos/issues/2147.
func TestBucketSeries_OneBlock_InMemIndexCacheSegfault(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "segfault-series")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	logger := log.NewNopLogger()
	thanosMeta := metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	chunkPool, err := pool.NewBucketedBytesPool(maxChunkSize, 50e6, 2, 100e7)
	testutil.Ok(t, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{
		MaxItemSize: 3000,
		// This is the exact size of cache needed for our *single request*.
		// This is limited in order to make sure we test evictions.
		MaxSize: 8889,
	})
	testutil.Ok(t, err)

	var b1 *bucketBlock

	const numSeries = 100

	// Create 4 blocks. Each will have numSeriesPerBlock number of series that have 1 sample only.
	// Timestamp will be counted for each new series, so each series will have unique timestamp.
	// This allows to pick time range that will correspond to number of series picked 1:1.
	{
		// Block 1.
		h, err := tsdb.NewHead(nil, nil, nil, 1, tmpDir, nil, chunks.DefaultWriteBufferSize, tsdb.DefaultStripeSize, nil)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, h.Close()) }()

		app := h.Appender(context.Background())

		for i := 0; i < numSeries; i++ {
			ts := int64(i)
			lbls := labels.FromStrings("foo", "bar", "b", "1", "i", fmt.Sprintf("%07d%s", ts, storetestutil.LabelLongSuffix))

			_, err := app.Add(lbls, ts, 0)
			testutil.Ok(t, err)
		}
		testutil.Ok(t, app.Commit())

		blockDir := filepath.Join(tmpDir, "tmp")
		id := createBlockFromHead(t, blockDir, h)

		meta, err := metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, id.String()), thanosMeta, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String())))

		b1 = &bucketBlock{
			indexCache:  indexCache,
			logger:      logger,
			metrics:     newBucketStoreMetrics(nil),
			bkt:         bkt,
			meta:        meta,
			partitioner: gapBasedPartitioner{maxGapSize: partitionerMaxGapSize},
			chunkObjs:   []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:   chunkPool,
		}
		b1.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b1.meta.ULID, DefaultPostingOffsetInMemorySampling)
		testutil.Ok(t, err)
	}

	var b2 *bucketBlock
	{
		// Block 2, do not load this block yet.
		h, err := tsdb.NewHead(nil, nil, nil, 1, tmpDir, nil, chunks.DefaultWriteBufferSize, tsdb.DefaultStripeSize, nil)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, h.Close()) }()

		app := h.Appender(context.Background())

		for i := 0; i < numSeries; i++ {
			ts := int64(i)
			lbls := labels.FromStrings("foo", "bar", "b", "2", "i", fmt.Sprintf("%07d%s", ts, storetestutil.LabelLongSuffix))

			_, err := app.Add(lbls, ts, 0)
			testutil.Ok(t, err)
		}
		testutil.Ok(t, app.Commit())

		blockDir := filepath.Join(tmpDir, "tmp2")
		id := createBlockFromHead(t, blockDir, h)

		meta, err := metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, id.String()), thanosMeta, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String())))

		b2 = &bucketBlock{
			indexCache:  indexCache,
			logger:      logger,
			metrics:     newBucketStoreMetrics(nil),
			bkt:         bkt,
			meta:        meta,
			partitioner: gapBasedPartitioner{maxGapSize: partitionerMaxGapSize},
			chunkObjs:   []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:   chunkPool,
		}
		b2.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b2.meta.ULID, DefaultPostingOffsetInMemorySampling)
		testutil.Ok(t, err)
	}

	store := &BucketStore{
		bkt:             objstore.WithNoopInstr(bkt),
		logger:          logger,
		indexCache:      indexCache,
		indexReaderPool: indexheader.NewReaderPool(log.NewNopLogger(), false, 0, nil),
		metrics:         newBucketStoreMetrics(nil),
		blockSets: map[uint64]*bucketBlockSet{
			labels.Labels{{Name: "ext1", Value: "1"}}.Hash(): {blocks: [][]*bucketBlock{{b1, b2}}},
		},
		blocks: map[ulid.ULID]*bucketBlock{
			b1.meta.ULID: b1,
			b2.meta.ULID: b2,
		},
		queryGate:            noopGate{},
		chunksLimiterFactory: NewChunksLimiterFactory(0),
	}

	t.Run("invoke series for one block. Fill the cache on the way.", func(t *testing.T) {
		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: int64(numSeries) - 1,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
				// This bug shows only when we use lot's of symbols for matching.
				{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
			},
		}, srv))
		testutil.Equals(t, 0, len(srv.Warnings))
		testutil.Equals(t, numSeries, len(srv.SeriesSet))
	})
	t.Run("invoke series for second block. This should revoke previous cache.", func(t *testing.T) {
		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: int64(numSeries) - 1,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
				// This bug shows only when we use lot's of symbols for matching.
				{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
			},
		}, srv))
		testutil.Equals(t, 0, len(srv.Warnings))
		testutil.Equals(t, numSeries, len(srv.SeriesSet))
	})
	t.Run("remove second block. Cache stays. Ask for first again.", func(t *testing.T) {
		testutil.Ok(t, store.removeBlock(b2.meta.ULID))

		srv := newStoreSeriesServer(context.Background())
		testutil.Ok(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: int64(numSeries) - 1,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
				// This bug shows only when we use lot's of symbols for matching.
				{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
			},
		}, srv))
		testutil.Equals(t, 0, len(srv.Warnings))
		testutil.Equals(t, numSeries, len(srv.SeriesSet))
	})
}

func TestSeries_RequestAndResponseHints(t *testing.T) {
	tb, store, seriesSet1, seriesSet2, block1, block2, close := setupStoreForHintsTest(t)
	defer close()

	testCases := []*storetestutil.SeriesCase{
		{
			Name: "querying a range containing 1 block should return 1 block in the response hints",
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: 1,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			ExpectedSeries: seriesSet1,
			ExpectedHints: []hintspb.SeriesResponseHints{
				{
					QueriedBlocks: []hintspb.Block{
						{Id: block1.String()},
					},
				},
			},
		}, {
			Name: "querying a range containing multiple blocks should return multiple blocks in the response hints",
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			ExpectedSeries: append(append([]*storepb.Series{}, seriesSet1...), seriesSet2...),
			ExpectedHints: []hintspb.SeriesResponseHints{
				{
					QueriedBlocks: []hintspb.Block{
						{Id: block1.String()},
						{Id: block2.String()},
					},
				},
			},
		}, {
			Name: "querying a range containing multiple blocks but filtering a specific block should query only the requested block",
			Req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: 3,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
				Hints: mustMarshalAny(&hintspb.SeriesRequestHints{
					BlockMatchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: block1.String()},
					},
				}),
			},
			ExpectedSeries: seriesSet1,
			ExpectedHints: []hintspb.SeriesResponseHints{
				{
					QueriedBlocks: []hintspb.Block{
						{Id: block1.String()},
					},
				},
			},
		},
	}

	storetestutil.TestServerSeries(tb, store, testCases...)
}

func TestSeries_ErrorUnmarshallingRequestHints(t *testing.T) {
	tb := testutil.NewTB(t)

	tmpDir, err := ioutil.TempDir("", "test-series-hints-enabled")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
	)

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		logger,
		nil,
		instrBkt,
		fetcher,
		tmpDir,
		indexCache,
		nil,
		1000000,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		false,
		10,
		nil,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
	)
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(t, store.Close()) }()

	testutil.Ok(tb, store.SyncBlocks(context.Background()))

	// Create a request with invalid hints (uses response hints instead of request hints).
	req := &storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 3,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
		},
		Hints: mustMarshalAny(&hintspb.SeriesResponseHints{}),
	}

	srv := newStoreSeriesServer(context.Background())
	err = store.Series(req, srv)
	testutil.NotOk(t, err)
	testutil.Equals(t, true, regexp.MustCompile(".*unmarshal series request hints.*").MatchString(err.Error()))
}

func TestSeries_BlockWithMultipleChunks(t *testing.T) {
	tb := testutil.NewTB(t)

	tmpDir, err := ioutil.TempDir("", "test-block-with-multiple-chunks")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	// Create a block with 1 series but an high number of samples,
	// so that they will span across multiple chunks.
	blkDir := filepath.Join(tmpDir, "block")

	h, err := tsdb.NewHead(nil, nil, nil, 10000000000, blkDir, nil, chunks.DefaultWriteBufferSize, tsdb.DefaultStripeSize, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, h.Close()) }()

	series := labels.FromStrings("__name__", "test")
	for ts := int64(0); ts < 10000; ts++ {
		// Appending a single sample is very unoptimised, but guarantees each chunk is always MaxSamplesPerChunk
		// (except the last one, which could be smaller).
		app := h.Appender(context.Background())
		_, err := app.Add(series, ts, float64(ts))
		testutil.Ok(t, err)
		testutil.Ok(t, app.Commit())
	}

	blk := createBlockFromHead(t, blkDir, h)

	thanosMeta := metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blkDir, blk.String()), thanosMeta, nil)
	testutil.Ok(t, err)

	// Create a bucket and upload the block there.
	bktDir := filepath.Join(tmpDir, "bucket")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	instrBkt := objstore.WithNoopInstr(bkt)
	logger := log.NewNopLogger()
	testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blkDir, blk.String())))

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		logger,
		nil,
		instrBkt,
		fetcher,
		tmpDir,
		indexCache,
		nil,
		1000000,
		NewChunksLimiterFactory(100000/MaxSamplesPerChunk),
		false,
		10,
		nil,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
	)
	testutil.Ok(tb, err)
	testutil.Ok(tb, store.SyncBlocks(context.Background()))

	tests := map[string]struct {
		reqMinTime      int64
		reqMaxTime      int64
		expectedSamples int
	}{
		"query the entire block": {
			reqMinTime:      math.MinInt64,
			reqMaxTime:      math.MaxInt64,
			expectedSamples: 10000,
		},
		"query the beginning of the block": {
			reqMinTime:      0,
			reqMaxTime:      100,
			expectedSamples: MaxSamplesPerChunk,
		},
		"query the middle of the block": {
			reqMinTime:      4000,
			reqMaxTime:      4050,
			expectedSamples: MaxSamplesPerChunk,
		},
		"query the end of the block": {
			reqMinTime:      9800,
			reqMaxTime:      10000,
			expectedSamples: (MaxSamplesPerChunk * 2) + (10000 % MaxSamplesPerChunk),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &storepb.SeriesRequest{
				MinTime: testData.reqMinTime,
				MaxTime: testData.reqMaxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "test"},
				},
			}

			srv := newStoreSeriesServer(context.Background())
			err = store.Series(req, srv)
			testutil.Ok(t, err)
			testutil.Assert(t, len(srv.SeriesSet) == 1)

			// Count the number of samples in the returned chunks.
			numSamples := 0
			for _, rawChunk := range srv.SeriesSet[0].Chunks {
				decodedChunk, err := chunkenc.FromData(chunkenc.EncXOR, rawChunk.Raw.Data)
				testutil.Ok(t, err)

				numSamples += decodedChunk.NumSamples()
			}

			testutil.Assert(t, testData.expectedSamples == numSamples, "expected: %d, actual: %d", testData.expectedSamples, numSamples)
		})
	}
}

func mustMarshalAny(pb proto.Message) *types.Any {
	out, err := types.MarshalAny(pb)
	if err != nil {
		panic(err)
	}
	return out
}

func TestBigEndianPostingsCount(t *testing.T) {
	const count = 1000
	raw := make([]byte, count*4)

	for ix := 0; ix < count; ix++ {
		binary.BigEndian.PutUint32(raw[4*ix:], rand.Uint32())
	}

	p := newBigEndianPostings(raw)
	testutil.Equals(t, count, p.length())

	c := 0
	for p.Next() {
		c++
	}
	testutil.Equals(t, count, c)
}

func TestBlockWithLargeChunks(t *testing.T) {
	defer testutil.TolerantVerifyLeak(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "large-chunk-test")
	testutil.Ok(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	blockDir := filepath.Join(tmpDir, "block")
	b := createBlockWithLargeChunk(testutil.NewTB(t), blockDir, labels.FromStrings("__name__", "test"), rand.New(rand.NewSource(0)))

	thanosMeta := metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, b.String()), thanosMeta, nil)
	testutil.Ok(t, err)

	bucketDir := filepath.Join(os.TempDir(), "bkt")
	bkt, err := filesystem.NewBucket(bucketDir)
	testutil.Ok(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(bucketDir)
	})

	logger := log.NewNopLogger()
	instrBkt := objstore.WithNoopInstr(bkt)

	testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, b.String())))

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil, nil)
	testutil.Ok(t, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(t, err)

	store, err := NewBucketStore(
		logger,
		nil,
		instrBkt,
		fetcher,
		tmpDir,
		indexCache,
		nil,
		1000000,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		false,
		10,
		nil,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
	)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, store.Close()) }()

	testutil.Ok(t, store.SyncBlocks(context.Background()))

	req := &storepb.SeriesRequest{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "test"},
		},
	}
	srv := newStoreSeriesServer(context.Background())
	testutil.Ok(t, store.Series(req, srv))
	testutil.Equals(t, 1, len(srv.SeriesSet))
}

// This method relies on a bug in TSDB Compactor which will just merge overlapping chunks into one big chunk.
// If compactor is fixed in the future, we may need a different way of generating the block, or commit
// existing block to the repository.
func createBlockWithLargeChunk(t testutil.TB, dir string, lbls labels.Labels, random *rand.Rand) ulid.ULID {
	// Block covering time [0 ... 10000)
	b1 := createBlockWithOneSeriesWithStep(t, dir, lbls, 0, 10000, random, 1)

	// This block has only 11 samples that fit into one chunk, but it completely overlaps entire first block.
	// Last sample has higher timestamp than last sample in b1.
	// This will make compactor to merge all chunks into one.
	b2 := createBlockWithOneSeriesWithStep(t, dir, lbls, 0, 11, random, 1000)

	// Merge the blocks together.
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil)
	testutil.Ok(t, err)

	blocksToCompact := []string{filepath.Join(dir, b1.String()), filepath.Join(dir, b2.String())}
	newBlock, err := compactor.Compact(dir, blocksToCompact, nil)
	testutil.Ok(t, err)

	for _, b := range blocksToCompact {
		testutil.Ok(t, os.RemoveAll(b))
	}

	db, err := tsdb.Open(dir, nil, nil, tsdb.DefaultOptions())
	defer func() {
		testutil.Ok(t, db.Close())
	}()
	testutil.Ok(t, err)
	bs := db.Blocks()
	testutil.Equals(t, 1, len(bs))
	cr, err := bs[0].Chunks()
	defer func() {
		testutil.Ok(t, cr.Close())
	}()
	testutil.Ok(t, err)
	// Ref is (<segment file index> << 32 + offset in the file). In TSDB v1 first chunk is always at offset 8.
	c, err := cr.Chunk(8)
	testutil.Ok(t, err)

	// Make sure that this is really a big chunk, otherwise this method makes a false promise.
	testutil.Equals(t, 10001, c.NumSamples())

	return newBlock
}

func createBlockWithOneSeriesWithStep(t testutil.TB, dir string, lbls labels.Labels, blockIndex int, totalSamples int, random *rand.Rand, step int64) ulid.ULID {
	h, err := tsdb.NewHead(nil, nil, nil, int64(totalSamples)*step, dir, nil, chunks.DefaultWriteBufferSize, tsdb.DefaultStripeSize, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, h.Close()) }()

	app := h.Appender(context.Background())

	ts := int64(blockIndex * totalSamples)
	ref, err := app.Add(lbls, ts, random.Float64())
	testutil.Ok(t, err)
	for i := 1; i < totalSamples; i++ {
		testutil.Ok(t, app.AddFast(ref, ts+step*int64(i), random.Float64()))
	}
	testutil.Ok(t, app.Commit())

	return createBlockFromHead(t, dir, h)
}

func setupStoreForHintsTest(t *testing.T) (testutil.TB, *BucketStore, []*storepb.Series, []*storepb.Series, ulid.ULID, ulid.ULID, func()) {
	tb := testutil.NewTB(t)

	closers := []func(){}

	tmpDir, err := ioutil.TempDir("", "test-hints")
	testutil.Ok(t, err)
	closers = append(closers, func() { testutil.Ok(t, os.RemoveAll(tmpDir)) })

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	closers = append(closers, func() { testutil.Ok(t, bkt.Close()) })

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
		random   = rand.New(rand.NewSource(120))
	)

	extLset := labels.Labels{{Name: "ext1", Value: "1"}}
	// Inject the Thanos meta to each block in the storage.
	thanosMeta := metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	// Create TSDB blocks.
	head, seriesSet1 := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "0"),
		SamplesPerSeries: 1,
		Series:           2,
		PrependLabels:    extLset,
		Random:           random,
	})
	block1 := createBlockFromHead(t, bktDir, head)
	testutil.Ok(t, head.Close())
	head2, seriesSet2 := storetestutil.CreateHeadWithSeries(t, 1, storetestutil.HeadGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "1"),
		SamplesPerSeries: 1,
		Series:           2,
		PrependLabels:    extLset,
		Random:           random,
	})
	block2 := createBlockFromHead(t, bktDir, head2)
	testutil.Ok(t, head2.Close())

	for _, blockID := range []ulid.ULID{block1, block2} {
		_, err := metadata.InjectThanos(logger, filepath.Join(bktDir, blockID.String()), thanosMeta, nil)
		testutil.Ok(t, err)
	}

	// Instance a real bucket store we'll use to query back the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		logger,
		nil,
		instrBkt,
		fetcher,
		tmpDir,
		indexCache,
		nil,
		1000000,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		false,
		10,
		nil,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
	)
	testutil.Ok(tb, err)
	testutil.Ok(tb, store.SyncBlocks(context.Background()))

	closers = append(closers, func() { testutil.Ok(t, store.Close()) })

	return tb, store, seriesSet1, seriesSet2, block1, block2, func() {
		for _, close := range closers {
			close()
		}
	}
}

func TestLabelNamesAndValuesHints(t *testing.T) {
	_, store, seriesSet1, seriesSet2, block1, block2, close := setupStoreForHintsTest(t)
	defer close()

	type labelNamesValuesCase struct {
		name string

		labelNamesReq      *storepb.LabelNamesRequest
		expectedNames      []string
		expectedNamesHints hintspb.LabelNamesResponseHints

		labelValuesReq      *storepb.LabelValuesRequest
		expectedValues      []string
		expectedValuesHints hintspb.LabelValuesResponseHints
	}

	testCases := []labelNamesValuesCase{
		{
			name: "querying a range containing 1 block should return 1 block in the labels hints",

			labelNamesReq: &storepb.LabelNamesRequest{
				Start: 0,
				End:   1,
			},
			expectedNames: labelNamesFromSeriesSet(seriesSet1),
			expectedNamesHints: hintspb.LabelNamesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},

			labelValuesReq: &storepb.LabelValuesRequest{
				Label: "ext1",
				Start: 0,
				End:   1,
			},
			expectedValues: []string{"1"},
			expectedValuesHints: hintspb.LabelValuesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},
		},
		{
			name: "querying a range containing multiple blocks should return multiple blocks in the response hints",

			labelNamesReq: &storepb.LabelNamesRequest{
				Start: 0,
				End:   3,
			},
			expectedNames: labelNamesFromSeriesSet(
				append(append([]*storepb.Series{}, seriesSet1...), seriesSet2...),
			),
			expectedNamesHints: hintspb.LabelNamesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
					{Id: block2.String()},
				},
			},

			labelValuesReq: &storepb.LabelValuesRequest{
				Label: "ext1",
				Start: 0,
				End:   3,
			},
			expectedValues: []string{"1"},
			expectedValuesHints: hintspb.LabelValuesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
					{Id: block2.String()},
				},
			},
		}, {
			name: "querying a range containing multiple blocks but filtering a specific block should query only the requested block",

			labelNamesReq: &storepb.LabelNamesRequest{
				Start: 0,
				End:   3,
				Hints: mustMarshalAny(&hintspb.LabelNamesRequestHints{
					BlockMatchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: block1.String()},
					},
				}),
			},
			expectedNames: labelNamesFromSeriesSet(seriesSet1),
			expectedNamesHints: hintspb.LabelNamesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},

			labelValuesReq: &storepb.LabelValuesRequest{
				Label: "ext1",
				Start: 0,
				End:   3,
				Hints: mustMarshalAny(&hintspb.LabelValuesRequestHints{
					BlockMatchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: block1.String()},
					},
				}),
			},
			expectedValues: []string{"1"},
			expectedValuesHints: hintspb.LabelValuesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			namesResp, err := store.LabelNames(context.Background(), tc.labelNamesReq)
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expectedNames, namesResp.Names)

			var namesHints hintspb.LabelNamesResponseHints
			testutil.Ok(t, types.UnmarshalAny(namesResp.Hints, &namesHints))
			// The order is not determinate, so we are sorting them.
			sort.Slice(namesHints.QueriedBlocks, func(i, j int) bool {
				return namesHints.QueriedBlocks[i].Id < namesHints.QueriedBlocks[j].Id
			})
			testutil.Equals(t, tc.expectedNamesHints, namesHints)

			valuesResp, err := store.LabelValues(context.Background(), tc.labelValuesReq)
			testutil.Ok(t, err)
			testutil.Equals(t, tc.expectedValues, valuesResp.Values)

			var valuesHints hintspb.LabelValuesResponseHints
			testutil.Ok(t, types.UnmarshalAny(valuesResp.Hints, &valuesHints))
			// The order is not determinate, so we are sorting them.
			sort.Slice(valuesHints.QueriedBlocks, func(i, j int) bool {
				return valuesHints.QueriedBlocks[i].Id < valuesHints.QueriedBlocks[j].Id
			})
			testutil.Equals(t, tc.expectedValuesHints, valuesHints)
		})
	}
}

func labelNamesFromSeriesSet(series []*storepb.Series) []string {
	labelsMap := map[string]struct{}{}

	for _, s := range series {
		for _, label := range s.Labels {
			labelsMap[label.Name] = struct{}{}
		}
	}

	labels := make([]string, 0, len(labelsMap))
	for k := range labelsMap {
		labels = append(labels, k)
	}

	sort.Strings(labels)
	return labels
}

func TestInjectExtLabels(t *testing.T) {
	testInjectExtLabels(testutil.NewTB(t))
}

func BenchmarkInjectExtLabels(b *testing.B) {
	testInjectExtLabels(testutil.NewTB(b))
}

var x labels.Labels

func testInjectExtLabels(tb testutil.TB) {
	in := labels.FromStrings(
		"__name__", "subscription_labels",
		"_id", "0dfsdfsdsfdsffd1e96-4432-9abe-e33436ea969a",
		"account", "1afsdfsddsfsdfsdfsdfsdfs",
		"ebs_account", "1asdasdad45",
		"email_domain", "asdasddgfkw.example.com",
		"endpoint", "metrics",
		"external_organization", "dfsdfsdf",
		"instance", "10.128.4.231:8080",
		"job", "sdd-acct-mngr-metrics",
		"managed", "false",
		"namespace", "production",
		"organization", "dasdadasdasasdasaaFGDSG",
		"pod", "sdd-acct-mngr-6669c947c8-xjx7f",
		"prometheus", "telemeter-production/telemeter",
		"prometheus_replica", "prometheus-telemeter-1",
		"risk", "5",
		"service", "sdd-acct-mngr-metrics",
		"support", "Self-Support", // Should be overwritten.
	)
	extLset := labels.FromStrings(
		"support", "Host-Support",
		"replica", "1",
		"tenant", "2342",
	)
	tb.ResetTimer()
	for i := 0; i < tb.N(); i++ {
		x = injectLabels(in, extLset)

		if !tb.IsBenchmark() {
			testutil.Equals(tb, labels.FromStrings(
				"__name__", "subscription_labels",
				"_id", "0dfsdfsdsfdsffd1e96-4432-9abe-e33436ea969a",
				"account", "1afsdfsddsfsdfsdfsdfsdfs",
				"ebs_account", "1asdasdad45",
				"email_domain", "asdasddgfkw.example.com",
				"endpoint", "metrics",
				"external_organization", "dfsdfsdf",
				"instance", "10.128.4.231:8080",
				"job", "sdd-acct-mngr-metrics",
				"managed", "false",
				"namespace", "production",
				"organization", "dasdadasdasasdasaaFGDSG",
				"pod", "sdd-acct-mngr-6669c947c8-xjx7f",
				"prometheus", "telemeter-production/telemeter",
				"prometheus_replica", "prometheus-telemeter-1",
				"replica", "1",
				"risk", "5",
				"service", "sdd-acct-mngr-metrics",
				"support", "Host-Support",
				"tenant", "2342",
			), x)
		}
	}
	fmt.Fprint(ioutil.Discard, x)
}
