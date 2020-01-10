package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/oklog/ulid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"gopkg.in/yaml.v2"
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

			res := set.getFor(low, high, maxResolution)

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
			res := set.getFor(low, high, maxResolution)

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

	defer testutil.Ok(t, os.RemoveAll(dir))

	bucketStore, err := NewBucketStore(
		nil,
		nil,
		nil,
		nil,
		dir,
		noopCache{},
		2e5,
		0,
		0,
		false,
		20,
		allowAllFilterConf,
		true,
	)
	testutil.Ok(t, err)

	resp, err := bucketStore.Info(ctx, &storepb.InfoRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, storepb.StoreType_STORE, resp.StoreType)
	testutil.Equals(t, int64(math.MaxInt64), resp.MinTime)
	testutil.Equals(t, int64(math.MinInt64), resp.MaxTime)
	testutil.Equals(t, []storepb.LabelSet(nil), resp.LabelSets)
	testutil.Equals(t, []storepb.Label(nil), resp.Labels)
}

type recorder struct {
	mtx sync.Mutex
	objstore.Bucket

	touched []string
}

func (r *recorder) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.touched = append(r.touched, name)
	return r.Bucket.Get(ctx, name)
}

func TestBucketStore_Sharding(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	dir, err := ioutil.TempDir("", "test-sharding-prepare")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	bkt := inmem.NewBucket()
	series := []labels.Labels{labels.FromStrings("a", "1", "b", "1")}

	id1, err := testutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r1"}}, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id1.String())))

	id2, err := testutil.CreateBlock(ctx, dir, series, 10, 1000, 2000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r1"}}, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id2.String())))

	id3, err := testutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "b"}, {Name: "region", Value: "r1"}}, 0)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id3.String())))

	id4, err := testutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r2"}}, 0)
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

	if ok := t.Run("reuse_disk", func(t *testing.T) {
		testSharding(t, dir2, bkt, id1, id2, id3, id4)
	}); !ok {
		return
	}

}

func testSharding(t *testing.T, reuseDisk string, bkt objstore.Bucket, all ...ulid.ULID) {
	var cached []ulid.ULID

	logger := log.NewLogfmtLogger(os.Stderr)
	for _, sc := range []struct {
		name              string
		relabel           string
		expectedIDs       []ulid.ULID
		expectedAdvLabels []storepb.LabelSet
	}{
		{
			name:        "no sharding",
			expectedIDs: all,
			expectedAdvLabels: []storepb.LabelSet{
				{
					Labels: []storepb.Label{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []storepb.Label{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r2"},
					},
				},
				{
					Labels: []storepb.Label{
						{Name: "cluster", Value: "b"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []storepb.Label{
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
			expectedAdvLabels: []storepb.LabelSet{
				{
					Labels: []storepb.Label{
						{Name: "cluster", Value: "b"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []storepb.Label{
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
			expectedAdvLabels: []storepb.LabelSet{
				{
					Labels: []storepb.Label{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []storepb.Label{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r2"},
					},
				},
				{
					Labels: []storepb.Label{
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
			expectedAdvLabels: []storepb.LabelSet{
				{
					Labels: []storepb.Label{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []storepb.Label{
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
			expectedAdvLabels: []storepb.LabelSet(nil),
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

			var relabelConf []*relabel.Config
			testutil.Ok(t, yaml.Unmarshal([]byte(sc.relabel), &relabelConf))

			rec := &recorder{Bucket: bkt}
			metaFetcher, err := block.NewMetaFetcher(logger, 20, bkt, dir, nil,
				block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime).Filter,
				block.NewLabelShardedMetaFilter(relabelConf).Filter,
			)
			testutil.Ok(t, err)

			bucketStore, err := NewBucketStore(
				logger,
				nil,
				rec,
				metaFetcher,
				dir,
				noopCache{},
				0,
				0,
				99,
				false,
				20,
				allowAllFilterConf,
				true)
			testutil.Ok(t, err)

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
			testutil.Equals(t, []storepb.Label(nil), resp.Labels)
			testutil.Equals(t, sc.expectedAdvLabels, resp.LabelSets)

			// Make sure we don't download files we did not expect to.
			// Regression test: https://github.com/thanos-io/thanos/issues/1664

			// Sort records. We load blocks concurrently so operations might be not ordered.
			sort.Strings(rec.touched)

			fmt.Println(cached, sc.expectedIDs, all, rec.touched)
			if reuseDisk != "" {
				testutil.Equals(t, expectedTouchedBlockOps(all, sc.expectedIDs, cached), rec.touched)
				cached = sc.expectedIDs
				return
			}

			testutil.Equals(t, expectedTouchedBlockOps(all, sc.expectedIDs, nil), rec.touched)
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
				path.Join(id.String(), block.IndexCacheFilename),
				path.Join(id.String(), block.IndexFilename),
			)
		}
	}
	sort.Strings(ops)
	return ops
}

// Regression tests against: https://github.com/thanos-io/thanos/issues/1983.
func TestReadIndexCache_LoadSeries(t *testing.T) {
	bkt := inmem.NewBucket()

	s := newBucketStoreMetrics(nil)
	b := &bucketBlock{
		meta: &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(1, nil)},
		},
		bucket:          bkt,
		seriesRefetches: s.seriesRefetches,
		logger:          log.NewNopLogger(),
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
		cache:        noopCache{},
		logger:       log.NewNopLogger(),
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
