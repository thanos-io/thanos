// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/pool"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	storetestutil "github.com/thanos-io/thanos/pkg/store/storepb/testutil"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

var emptyRelabelConfig = make([]*relabel.Config, 0)

func TestRawChunkReset(t *testing.T) {
	t.Parallel()

	r := rawChunk([]byte{1, 2})
	r.Reset([]byte{3, 4})
	testutil.Equals(t, []byte(r), []byte{3, 4})
}

func TestBucketBlock_Property(t *testing.T) {
	t.Parallel()

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

func TestBucketFilterExtLabelsMatchers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
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
	b, _ := newBucketBlock(context.Background(), newBucketStoreMetrics(nil), meta, bkt, path.Join(dir, blockID.String()), nil, nil, nil, nil, nil, nil, nil)
	ms := []*labels.Matcher{
		{Type: labels.MatchNotEqual, Name: "a", Value: "b"},
	}
	res, _ := b.FilterExtLabelsMatchers(ms)
	testutil.Equals(t, len(res), 0)

	ms = []*labels.Matcher{
		{Type: labels.MatchNotEqual, Name: "a", Value: "a"},
	}
	_, ok := b.FilterExtLabelsMatchers(ms)
	testutil.Equals(t, ok, true)

	ms = []*labels.Matcher{
		{Type: labels.MatchNotEqual, Name: "a", Value: "a"},
		{Type: labels.MatchNotEqual, Name: "c", Value: "d"},
	}
	res, _ = b.FilterExtLabelsMatchers(ms)
	testutil.Equals(t, len(res), 0)

	ms = []*labels.Matcher{
		{Type: labels.MatchNotEqual, Name: "a2", Value: "a"},
	}
	res, _ = b.FilterExtLabelsMatchers(ms)
	testutil.Equals(t, len(res), 1)
	testutil.Equals(t, res, ms)

	// validate that it can filter out ext labels that match non-equal matchers
	ext, err := labels.NewMatcher(labels.MatchRegexp, "a", ".*")
	if err != nil {
		t.Error(err)
	}
	ms = []*labels.Matcher{
		{Type: labels.MatchNotEqual, Name: "a2", Value: "a"},
	}
	res, _ = b.FilterExtLabelsMatchers(append(ms, ext))
	testutil.Equals(t, len(res), 1)
	testutil.Equals(t, res, ms)
}

func TestBucketBlock_matchLabels(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

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

	b, err := newBucketBlock(context.Background(), newBucketStoreMetrics(nil), meta, bkt, path.Join(dir, blockID.String()), nil, nil, nil, nil, nil, nil, nil)
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	const maxGapSize = 1024 * 512

	for _, c := range []struct {
		input    [][2]int
		expected []Part
	}{
		{
			input:    [][2]int{{1, 10}},
			expected: []Part{{Start: 1, End: 10, ElemRng: [2]int{0, 1}}},
		},
		{
			input:    [][2]int{{1, 2}, {3, 5}, {7, 10}},
			expected: []Part{{Start: 1, End: 10, ElemRng: [2]int{0, 3}}},
		},
		{
			input: [][2]int{
				{1, 2},
				{3, 5},
				{20, 30},
				{maxGapSize + 31, maxGapSize + 32},
			},
			expected: []Part{
				{Start: 1, End: 30, ElemRng: [2]int{0, 3}},
				{Start: maxGapSize + 31, End: maxGapSize + 32, ElemRng: [2]int{3, 4}},
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
			expected: []Part{
				{Start: 1, End: 30, ElemRng: [2]int{0, 3}},
				{Start: maxGapSize + 31, End: maxGapSize + 40, ElemRng: [2]int{3, 5}},
			},
		},
		{
			input: [][2]int{
				// Mimic AllPostingsKey, where range specified whole range.
				{1, 15},
				{1, maxGapSize + 100},
				{maxGapSize + 31, maxGapSize + 40},
			},
			expected: []Part{{Start: 1, End: maxGapSize + 100, ElemRng: [2]int{0, 3}}},
		},
	} {
		res := gapBasedPartitioner{maxGapSize: maxGapSize}.Partition(len(c.input), func(i int) (uint64, uint64) {
			return uint64(c.input[i][0]), uint64(c.input[i][1])
		})
		testutil.Equals(t, c.expected, res)
	}
}

func TestBucketStoreConfig_validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config   *BucketStore
		expected error
	}{
		"should pass on valid config": {
			config: &BucketStore{
				blockSyncConcurrency: 1,
			},
			expected: nil,
		},
		"should fail on blockSyncConcurrency < 1": {
			config: &BucketStore{
				blockSyncConcurrency: 0,
			},
			expected: errBlockSyncConcurrencyNotValid,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			testutil.Equals(t, testData.expected, testData.config.validate())
		})
	}
}

func TestBucketStore_TSDBInfo(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := log.NewNopLogger()
	dir := t.TempDir()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	series := []labels.Labels{labels.FromStrings("a", "1", "b", "1")}

	for _, tt := range []struct {
		mint, maxt int64
		extLabels  labels.Labels
	}{
		{mint: 0, maxt: 1000, extLabels: labels.FromStrings("a", "b")},
		{mint: 1000, maxt: 2000, extLabels: labels.FromStrings("a", "b")},
		{mint: 3000, maxt: 4000, extLabels: labels.FromStrings("a", "b")},
		{mint: 3500, maxt: 5000, extLabels: labels.FromStrings("a", "b")},
		{mint: 0, maxt: 1000, extLabels: labels.FromStrings("a", "c")},
		{mint: 500, maxt: 2000, extLabels: labels.FromStrings("a", "c")},
		{mint: 0, maxt: 1000, extLabels: labels.FromStrings("a", "d")},
		{mint: 2000, maxt: 3000, extLabels: labels.FromStrings("a", "d")},
	} {
		id1, err := e2eutil.CreateBlock(ctx, dir, series, 10, tt.mint, tt.maxt, tt.extLabels, 0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id1.String()), metadata.NoneFunc))
	}

	baseBlockIDsFetcher := block.NewConcurrentLister(logger, bkt)
	metaFetcher, err := block.NewMetaFetcher(logger, 20, bkt, baseBlockIDsFetcher, dir, nil, []block.MetadataFilter{
		block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
	})
	testutil.Ok(t, err)

	chunkPool, err := NewDefaultChunkBytesPool(2e5)
	testutil.Ok(t, err)

	bucketStore, err := NewBucketStore(
		objstore.WithNoopInstr(bkt),
		metaFetcher,
		dir,
		NewChunksLimiterFactory(0),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		20,
		true,
		DefaultPostingOffsetInMemorySampling,
		false,
		false,
		0,
		WithChunkPool(chunkPool),
		WithFilterConfig(allowAllFilterConf),
	)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bucketStore.Close()) }()

	testutil.Ok(t, bucketStore.SyncBlocks(ctx))
	infos := bucketStore.TSDBInfos()
	slices.SortFunc(infos, func(a, b infopb.TSDBInfo) int {
		return strings.Compare(a.Labels.String(), b.Labels.String())
	})
	testutil.Equals(t, infos, []infopb.TSDBInfo{
		{
			Labels:  labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "b"}}},
			MinTime: 0,
			MaxTime: 2000,
		},
		{
			Labels:  labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "b"}}},
			MinTime: 3000,
			MaxTime: 5000,
		},
		{
			Labels:  labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "c"}}},
			MinTime: 0,
			MaxTime: 2000,
		},
		{
			Labels:  labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}}},
			MinTime: 0,
			MaxTime: 1000,
		},
		{
			Labels:  labelpb.ZLabelSet{Labels: []labelpb.ZLabel{{Name: "a", Value: "d"}}},
			MinTime: 2000,
			MaxTime: 3000,
		},
	})
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
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()

	dir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	series := []labels.Labels{labels.FromStrings("a", "1", "b", "1")}

	id1, err := e2eutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.FromStrings("cluster", "a", "region", "r1"), 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id1.String()), metadata.NoneFunc))

	id2, err := e2eutil.CreateBlock(ctx, dir, series, 10, 1000, 2000, labels.FromStrings("cluster", "a", "region", "r1"), 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id2.String()), metadata.NoneFunc))

	id3, err := e2eutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.FromStrings("cluster", "b", "region", "r1"), 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id3.String()), metadata.NoneFunc))

	id4, err := e2eutil.CreateBlock(ctx, dir, series, 10, 0, 1000, labels.FromStrings("cluster", "a", "region", "r2"), 0, metadata.NoneFunc)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id4.String()), metadata.NoneFunc))

	if ok := t.Run("new_runs", func(t *testing.T) {
		testSharding(t, "", bkt, id1, id2, id3, id4)
	}); !ok {
		return
	}

	dir2 := t.TempDir()

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
				dir = t.TempDir()
			}
			relabelConf, err := block.ParseRelabelConfig([]byte(sc.relabel), block.SelectorSupportedRelabelActions)
			testutil.Ok(t, err)

			rec := &recorder{Bucket: bkt}
			insBkt := objstore.WithNoopInstr(bkt)
			baseBlockIDsFetcher := block.NewConcurrentLister(logger, insBkt)
			metaFetcher, err := block.NewMetaFetcher(logger, 20, insBkt, baseBlockIDsFetcher, dir, nil, []block.MetadataFilter{
				block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
				block.NewLabelShardedMetaFilter(relabelConf),
			})
			testutil.Ok(t, err)

			bucketStore, err := NewBucketStore(
				objstore.WithNoopInstr(rec),
				metaFetcher,
				dir,
				NewChunksLimiterFactory(0),
				NewSeriesLimiterFactory(0),
				NewBytesLimiterFactory(0),
				NewGapBasedPartitioner(PartitionerMaxGapSize),
				20,
				true,
				DefaultPostingOffsetInMemorySampling,
				false,
				false,
				0,
				WithLogger(logger),
				WithFilterConfig(allowAllFilterConf),
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

func expectedTouchedBlockOps(all, expected, cached []ulid.ULID) []string {
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
	t.Parallel()

	bkt := objstore.NewInMemBucket()
	ctx := context.Background()

	s := newBucketStoreMetrics(nil)
	b := &bucketBlock{
		meta: &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: ulid.MustNew(1, nil),
			},
		},
		bkt:        bkt,
		metrics:    s,
		indexCache: noopCache{},
	}
	logger := log.NewNopLogger()

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
		loadedSeries: map[storage.SeriesRef][]byte{},
		logger:       logger,
	}

	// Success with no refetches.
	testutil.Ok(t, r.loadSeries(ctx, []storage.SeriesRef{2, 13, 24}, false, 2, 100, NewBytesLimiterFactory(0)(nil), tenancy.DefaultTenant))
	testutil.Equals(t, map[storage.SeriesRef][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, r.loadedSeries)
	testutil.Equals(t, float64(0), promtest.ToFloat64(s.seriesRefetches.WithLabelValues(tenancy.DefaultTenant)))

	// Success with 2 refetches.
	r.loadedSeries = map[storage.SeriesRef][]byte{}
	testutil.Ok(t, r.loadSeries(ctx, []storage.SeriesRef{2, 13, 24}, false, 2, 15, NewBytesLimiterFactory(0)(nil), tenancy.DefaultTenant))
	testutil.Equals(t, map[storage.SeriesRef][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, r.loadedSeries)
	testutil.Equals(t, float64(2), promtest.ToFloat64(s.seriesRefetches.WithLabelValues(tenancy.DefaultTenant)))

	// Success with refetch on first element.
	r.loadedSeries = map[storage.SeriesRef][]byte{}
	testutil.Ok(t, r.loadSeries(ctx, []storage.SeriesRef{2}, false, 2, 5, NewBytesLimiterFactory(0)(nil), tenancy.DefaultTenant))
	testutil.Equals(t, map[storage.SeriesRef][]byte{
		2: []byte("aaaaaaaaaa"),
	}, r.loadedSeries)
	testutil.Equals(t, float64(3), promtest.ToFloat64(s.seriesRefetches.WithLabelValues(tenancy.DefaultTenant)))

	buf.Reset()
	buf.PutByte(0)
	buf.PutByte(0)
	buf.PutUvarint(10)
	buf.PutString("aaaaaaa")
	testutil.Ok(t, bkt.Upload(ctx, filepath.Join(b.meta.ULID.String(), block.IndexFilename), bytes.NewReader(buf.Get())))

	// Fail, but no recursion at least.
	testutil.NotOk(t, r.loadSeries(ctx, []storage.SeriesRef{2, 13, 24}, false, 1, 15, NewBytesLimiterFactory(0)(nil), tenancy.DefaultTenant))
}

func TestBucketIndexReader_ExpandedPostings(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(tb, bkt.Close()) }()

	id := uploadTestBlock(tb, tmpDir, bkt, 500)

	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
	testutil.Ok(tb, err)

	benchmarkExpandedPostings(tb, bkt, id, r, 500)
}

func BenchmarkBucketIndexReader_ExpandedPostings(b *testing.B) {
	tb := testutil.NewTB(b)

	tmpDir := b.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(tb, bkt.Close()) }()

	id := uploadTestBlock(tb, tmpDir, bkt, 50e5)
	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
	testutil.Ok(tb, err)

	benchmarkExpandedPostings(tb, bkt, id, r, 50e5)
}

func uploadTestBlock(t testing.TB, tmpDir string, bkt objstore.Bucket, series int) ulid.ULID {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = tmpDir
	headOpts.ChunkRange = 1000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, h.Close())
	}()

	logger := log.NewNopLogger()
	ctx := context.Background()

	appendTestData(t, h.Appender(ctx), series)

	dir := filepath.Join(tmpDir, "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))
	id := storetestutil.CreateBlockFromHead(t, dir, h)
	bdir := filepath.Join(dir, id.String())
	meta, err := metadata.ReadFromDir(bdir)
	testutil.Ok(t, err)
	stats, err := block.GatherIndexHealthStats(ctx, logger, filepath.Join(bdir, block.IndexFilename), meta.MinTime, meta.MaxTime)
	testutil.Ok(t, err)

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, "tmp", id.String()), metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
		IndexStats: metadata.IndexStats{
			SeriesMaxSize:   stats.SeriesMaxSize,
			SeriesP90Size:   stats.SeriesP90Size,
			SeriesP99Size:   stats.SeriesP99Size,
			SeriesP999Size:  stats.SeriesP999Size,
			SeriesP9999Size: stats.SeriesP9999Size,
			ChunkMaxSize:    stats.ChunkMaxSize,
		},
	}, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, logger, bkt, bdir, metadata.NoneFunc))

	return id
}

func appendTestData(t testing.TB, app storage.Appender, series int) {
	addSeries := func(l labels.Labels) {
		_, err := app.Append(0, l, 0, 0)
		testutil.Ok(t, err)
	}

	series = series / 5
	uniq := 0
	for n := 0; n < 10; n++ {
		for i := 0; i < series/10; i++ {
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "foo", "uniq", fmt.Sprintf("%08d", uniq)))
			// Have some series that won't be matched, to properly test inverted matches.
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "bar", "uniq", fmt.Sprintf("%08d", uniq+1)))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", "0_"+strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "bar", "uniq", fmt.Sprintf("%08d", uniq+2)))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", "1_"+strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "bar", "uniq", fmt.Sprintf("%08d", uniq+3)))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+storetestutil.LabelLongSuffix, "n", "2_"+strconv.Itoa(n)+storetestutil.LabelLongSuffix, "j", "foo", "uniq", fmt.Sprintf("%08d", uniq+4)))
			uniq += 5
		}
	}
	testutil.Ok(t, app.Commit())
}

// Very similar benchmark to this: https://github.com/prometheus/prometheus/blob/1d1732bc25cc4b47f513cb98009a4eb91879f175/tsdb/querier_bench_test.go#L82,
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
	iNot2 := labels.MustNewMatcher(labels.MatchNotEqual, "i", "2"+storetestutil.LabelLongSuffix)
	iNot2Star := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")
	iRegexSet := labels.MustNewMatcher(labels.MatchRegexp, "i", "0"+storetestutil.LabelLongSuffix+"|1"+storetestutil.LabelLongSuffix+"|2"+storetestutil.LabelLongSuffix)
	bigValueSetSize := series / 10
	if bigValueSetSize > 50000 {
		bigValueSetSize = 50000
	}
	bigValueSet := make([]string, 0, bigValueSetSize)
	for i := 0; i < series; i += series / bigValueSetSize {
		bigValueSet = append(bigValueSet, fmt.Sprintf("%08d", i))
	}
	bigValueSetSize = len(bigValueSet)
	rand.New(rand.NewSource(time.Now().UnixNano())).Shuffle(len(bigValueSet), func(i, j int) {
		bigValueSet[i], bigValueSet[j] = bigValueSet[j], bigValueSet[i]
	})
	iRegexBigValueSet := labels.MustNewMatcher(labels.MatchRegexp, "uniq", strings.Join(bigValueSet, "|"))

	logger := log.NewNopLogger()
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
		{`n="1",i=~".*",i!="2",j="foo"`, []*labels.Matcher{n1, iStar, iNot2, jFoo}, int(float64(series)/10 - 1)},
		{`n="1",i!=""`, []*labels.Matcher{n1, iNotEmpty}, int(float64(series) * 0.2)},
		{`n="1",i!="",j="foo"`, []*labels.Matcher{n1, iNotEmpty, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".+",j="foo"`, []*labels.Matcher{n1, iPlus, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~"1.+",j="foo"`, []*labels.Matcher{n1, i1Plus, jFoo}, int(float64(series) * 0.011111)},
		{`n="1",i=~".+",i!="2",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2, jFoo}, int(float64(series)/10 - 1)},
		{`n="1",i=~".+",i!~"2.*",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2Star, jFoo}, int(1 + float64(series)*0.088888)},
		{`n="1",i=~".+",i=~".+",i=~".+",i=~".+",i=~".+",j="foo"`, []*labels.Matcher{n1, iPlus, iPlus, iPlus, iPlus, iPlus, jFoo}, int(float64(series) * 0.1)},
		{`i=~"0|1|2"`, []*labels.Matcher{iRegexSet}, 150}, // 50 series for "1", 50 for "2" and 50 for "3".
		{`uniq=~"9|random-shuffled-values|1"`, []*labels.Matcher{iRegexBigValueSet}, bigValueSetSize},
	}

	dummyCounter := promauto.NewCounter(prometheus.CounterOpts{Name: "test"})
	for _, c := range cases {
		t.Run(c.name, func(t testutil.TB) {
			b := &bucketBlock{
				metrics:           newBucketStoreMetrics(nil),
				indexHeaderReader: r,
				indexCache:        noopCache{},
				bkt:               bkt,
				meta:              &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}},
				partitioner:       NewGapBasedPartitioner(PartitionerMaxGapSize),
			}

			indexr := newBucketIndexReader(b, logger)

			t.ResetTimer()
			for i := 0; i < t.N(); i++ {
				p, err := indexr.ExpandedPostings(context.Background(), newSortedMatchers(c.matchers), NewBytesLimiterFactory(0)(nil), false, dummyCounter, tenancy.DefaultTenant)
				testutil.Ok(t, err)
				testutil.Equals(t, c.expectedLen, len(p.postings))
			}
		})
	}
}

func TestExpandedPostingsEmptyPostings(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	id := uploadTestBlock(t, tmpDir, bkt, 100)

	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
	testutil.Ok(t, err)
	b := &bucketBlock{
		metrics:           newBucketStoreMetrics(nil),
		indexHeaderReader: r,
		indexCache:        noopCache{},
		bkt:               bkt,
		meta:              &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}},
		partitioner:       NewGapBasedPartitioner(PartitionerMaxGapSize),
	}

	logger := log.NewNopLogger()
	indexr := newBucketIndexReader(b, logger)
	matcher1 := labels.MustNewMatcher(labels.MatchEqual, "j", "foo")
	// Match nothing.
	matcher2 := labels.MustNewMatcher(labels.MatchRegexp, "i", "500.*")
	ctx := context.Background()
	dummyCounter := promauto.With(prometheus.NewRegistry()).NewCounter(prometheus.CounterOpts{Name: "test"})
	ps, err := indexr.ExpandedPostings(ctx, newSortedMatchers([]*labels.Matcher{matcher1, matcher2}), NewBytesLimiterFactory(0)(nil), false, dummyCounter, tenancy.DefaultTenant)
	testutil.Ok(t, err)
	testutil.Equals(t, ps, (*lazyExpandedPostings)(nil))
	// Make sure even if a matcher doesn't match any postings, we still cache empty expanded postings.
	testutil.Equals(t, 1, indexr.stats.cachedPostingsCompressions)
}

func TestLazyExpandedPostingsEmptyPostings(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	id := uploadTestBlock(t, tmpDir, bkt, 100)

	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
	testutil.Ok(t, err)
	b := &bucketBlock{
		metrics:                newBucketStoreMetrics(nil),
		indexHeaderReader:      r,
		indexCache:             noopCache{},
		bkt:                    bkt,
		meta:                   &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}},
		partitioner:            NewGapBasedPartitioner(PartitionerMaxGapSize),
		estimatedMaxSeriesSize: 20,
	}

	logger := log.NewNopLogger()
	indexr := newBucketIndexReader(b, logger)
	// matcher1 and matcher2 will match nothing after intersection.
	matcher1 := labels.MustNewMatcher(labels.MatchEqual, "j", "foo")
	matcher2 := labels.MustNewMatcher(labels.MatchRegexp, "n", "1_.*")
	matcher3 := labels.MustNewMatcher(labels.MatchRegexp, "i", ".+")
	ctx := context.Background()
	dummyCounter := promauto.With(prometheus.NewRegistry()).NewCounter(prometheus.CounterOpts{Name: "test"})
	ps, err := indexr.ExpandedPostings(ctx, newSortedMatchers([]*labels.Matcher{matcher1, matcher2, matcher3}), NewBytesLimiterFactory(0)(nil), true, dummyCounter, tenancy.DefaultTenant)
	testutil.Ok(t, err)
	// We expect emptyLazyPostings rather than lazy postings with 0 length but with matchers.
	testutil.Equals(t, ps, emptyLazyPostings)
}

func TestBucketSeries(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, chunkenc.ValFloat, false, false, samplesPerSeries, series, 1)
	})
}

func TestBucketSeriesLazyExpandedPostings(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, chunkenc.ValFloat, false, true, samplesPerSeries, series, 1)
	})
}

func TestBucketHistogramSeries(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, chunkenc.ValHistogram, false, false, samplesPerSeries, series, 1)
	})
}

func TestBucketFloatHistogramSeries(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, chunkenc.ValFloatHistogram, false, false, samplesPerSeries, series, 1)
	})
}

func TestBucketSkipChunksSeries(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)
	storetestutil.RunSeriesInterestingCases(tb, 200e3, 200e3, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, chunkenc.ValFloat, true, false, samplesPerSeries, series, 1)
	})
}

func BenchmarkBucketSeries(b *testing.B) {
	tb := testutil.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, chunkenc.ValFloat, false, false, samplesPerSeries, series, 1/100e6, 1/10e4, 1)
	})
}

func BenchmarkBucketSkipChunksSeries(b *testing.B) {
	tb := testutil.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	storetestutil.RunSeriesInterestingCases(tb, 10e6, 10e5, func(t testutil.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, chunkenc.ValFloat, true, false, samplesPerSeries, series, 1/100e6, 1/10e4, 1)
	})
}

func benchBucketSeries(t testutil.TB, sampleType chunkenc.ValueType, skipChunk, lazyExpandedPostings bool, samplesPerSeries, totalSeries int, requestedRatios ...float64) {
	const numOfBlocks = 4

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	var (
		logger = log.NewNopLogger()
		series []*storepb.Series
		random = rand.New(rand.NewSource(120))
	)

	extLset := labels.FromStrings("ext1", "1")
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
		head, _ := storetestutil.CreateHeadWithSeries(t, bi, storetestutil.HeadGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", bi)),
			SamplesPerSeries: samplesPerSeriesPerBlock,
			Series:           seriesPerBlock,
			PrependLabels:    extLset,
			Random:           random,
			SkipChunks:       t.IsBenchmark() || skipChunk,
			SampleType:       sampleType,
		})
		id := storetestutil.CreateBlockFromHead(t, blockDir, head)
		testutil.Ok(t, head.Close())
		blockIDDir := filepath.Join(blockDir, id.String())
		meta, err := metadata.ReadFromDir(blockIDDir)
		testutil.Ok(t, err)
		stats, err := block.GatherIndexHealthStats(context.TODO(), logger, filepath.Join(blockIDDir, block.IndexFilename), meta.MinTime, meta.MaxTime)
		testutil.Ok(t, err)
		thanosMeta := metadata.Thanos{
			Labels:     extLset.Map(),
			Downsample: metadata.ThanosDownsample{Resolution: 0},
			Source:     metadata.TestSource,
			IndexStats: metadata.IndexStats{
				SeriesMaxSize:   stats.SeriesMaxSize,
				SeriesP90Size:   stats.SeriesP90Size,
				SeriesP99Size:   stats.SeriesP99Size,
				SeriesP999Size:  stats.SeriesP999Size,
				SeriesP9999Size: stats.SeriesP9999Size,
				ChunkMaxSize:    stats.ChunkMaxSize,
			},
		}

		// Histogram chunks are represented differently in memory and on disk. In order to
		// have a precise comparison, we need to use the on-disk representation as the expected value
		// instead of the in-memory one.
		diskBlock, err := tsdb.OpenBlock(logger, blockIDDir, nil)
		testutil.Ok(t, err)
		series = append(series, storetestutil.ReadSeriesFromBlock(t, diskBlock, extLset, skipChunk)...)

		meta, err = metadata.InjectThanos(logger, blockIDDir, thanosMeta, nil)
		testutil.Ok(t, err)

		testutil.Ok(t, meta.WriteToDir(logger, blockIDDir))
		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, blockIDDir, metadata.NoneFunc))
	}

	ibkt := objstore.WithNoopInstr(bkt)
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, ibkt)
	f, err := block.NewRawMetaFetcher(logger, ibkt, baseBlockIDsFetcher)
	testutil.Ok(t, err)

	chunkPool, err := pool.NewBucketedPool[byte](chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, 1e9) // 1GB.
	testutil.Ok(t, err)

	st, err := NewBucketStore(
		ibkt,
		f,
		tmpDir,
		NewChunksLimiterFactory(0),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		1,
		false,
		DefaultPostingOffsetInMemorySampling,
		false,
		false,
		0,
		WithLogger(logger),
		WithChunkPool(chunkPool),
		WithLazyExpandedPostings(lazyExpandedPostings),
	)
	testutil.Ok(t, err)

	if !t.IsBenchmark() {
		st.chunkPool = &mockedPool{parent: st.chunkPool}
	}

	testutil.Ok(t, st.SyncBlocks(context.Background()))

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

		matchersCase := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar"),
			labels.MustNewMatcher(labels.MatchEqual, "j", "0"),
			labels.MustNewMatcher(labels.MatchNotEqual, "j", "0"),
			labels.MustNewMatcher(labels.MatchRegexp, "j", "(0|1)"),
			labels.MustNewMatcher(labels.MatchRegexp, "j", "0|1"),
			labels.MustNewMatcher(labels.MatchNotRegexp, "j", "(0|1)"),
			labels.MustNewMatcher(labels.MatchNotRegexp, "j", "0|1"),
		}

		for _, lm := range matchersCase {
			var expectedSeries []*storepb.Series
			m, err := storepb.PromMatchersToMatchers(lm)
			testutil.Ok(t, err)

			// seriesCut does not cut chunks properly, but those are assured against for non benchmarks only, where we use 100% case only.
			for _, s := range series[:seriesCut] {
				for _, label := range s.Labels {
					if label.Name == lm.Name && lm.Matches(label.Value) {
						expectedSeries = append(expectedSeries, s)
						break
					}
				}
			}
			bCases = append(bCases, &storetestutil.SeriesCase{
				Name: fmt.Sprintf("%dof%d[%s]", expectedSamples, totalSeries*samplesPerSeries, lm.String()),
				Req: &storepb.SeriesRequest{
					MinTime:    0,
					MaxTime:    int64(expectedSamples) - 1,
					Matchers:   m,
					SkipChunks: skipChunk,
				},
				ExpectedSeries: expectedSeries,
			})
		}
	}
	storetestutil.TestServerSeries(t, st, bCases...)

	if !t.IsBenchmark() {
		if !skipChunk {
			// TODO(bwplotka): This is wrong negative for large number of samples (1mln). Investigate.
			testutil.Equals(t, 0, int(st.chunkPool.(*mockedPool).balance.Load()))
			st.chunkPool.(*mockedPool).gets.Store(0)
		}

		for _, b := range st.blocks {
			// NOTE(bwplotka): It is 4 x 1.0 for 100mln samples. Kind of make sense: long series.
			testutil.Equals(t, 0.0, promtest.ToFloat64(b.metrics.seriesRefetches.WithLabelValues(tenancy.DefaultTenant)))
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
	parent  pool.Pool[byte]
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

// Regression test against: https://github.com/thanos-io/thanos/issues/2147.
func TestBucketSeries_OneBlock_InMemIndexCacheSegfault(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	logger := log.NewLogfmtLogger(os.Stderr)
	thanosMeta := metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	chunkPool, err := pool.NewBucketedPool[byte](chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, 100e7)
	testutil.Ok(t, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{
		MaxItemSize: 3000,
		// This is the exact size of cache needed for our *single request*.
		// This is limited in order to make sure we test evictions.
		MaxSize: 8889,
	})
	testutil.Ok(t, err)

	var b1 *bucketBlock

	const numSeries = 100
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = tmpDir
	headOpts.ChunkRange = 1

	// Create 4 blocks. Each will have numSeriesPerBlock number of series that have 1 sample only.
	// Timestamp will be counted for each new series, so each series will have unique timestamp.
	// This allows to pick time range that will correspond to number of series picked 1:1.
	{
		// Block 1.
		h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, h.Close()) }()

		app := h.Appender(context.Background())

		for i := 0; i < numSeries; i++ {
			ts := int64(i)
			lbls := labels.FromStrings("foo", "bar", "b", "1", "i", fmt.Sprintf("%07d%s", ts, storetestutil.LabelLongSuffix))

			_, err := app.Append(0, lbls, ts, 0)
			testutil.Ok(t, err)
		}
		testutil.Ok(t, app.Commit())

		blockDir := filepath.Join(tmpDir, "tmp")
		id := storetestutil.CreateBlockFromHead(t, blockDir, h)

		meta, err := metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, id.String()), thanosMeta, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), metadata.NoneFunc))

		b1 = &bucketBlock{
			indexCache:             indexCache,
			metrics:                newBucketStoreMetrics(nil),
			bkt:                    bkt,
			meta:                   meta,
			partitioner:            NewGapBasedPartitioner(PartitionerMaxGapSize),
			chunkObjs:              []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:              chunkPool,
			estimatedMaxSeriesSize: EstimatedMaxSeriesSize,
			estimatedMaxChunkSize:  EstimatedMaxChunkSize,
		}
		b1.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b1.meta.ULID, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
		testutil.Ok(t, err)
	}

	var b2 *bucketBlock
	{
		// Block 2, do not load this block yet.
		h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, h.Close()) }()

		app := h.Appender(context.Background())

		for i := 0; i < numSeries; i++ {
			ts := int64(i)
			lbls := labels.FromStrings("foo", "bar", "b", "2", "i", fmt.Sprintf("%07d%s", ts, storetestutil.LabelLongSuffix))

			_, err := app.Append(0, lbls, ts, 0)
			testutil.Ok(t, err)
		}
		testutil.Ok(t, app.Commit())

		blockDir := filepath.Join(tmpDir, "tmp2")
		id := storetestutil.CreateBlockFromHead(t, blockDir, h)

		meta, err := metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, id.String()), thanosMeta, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), metadata.NoneFunc))

		b2 = &bucketBlock{
			indexCache:             indexCache,
			metrics:                newBucketStoreMetrics(nil),
			bkt:                    bkt,
			meta:                   meta,
			partitioner:            NewGapBasedPartitioner(PartitionerMaxGapSize),
			chunkObjs:              []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:              chunkPool,
			estimatedMaxSeriesSize: EstimatedMaxSeriesSize,
			estimatedMaxChunkSize:  EstimatedMaxChunkSize,
		}
		b2.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b2.meta.ULID, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
		testutil.Ok(t, err)
	}

	store := &BucketStore{
		bkt:             objstore.WithNoopInstr(bkt),
		logger:          logger,
		indexCache:      indexCache,
		indexReaderPool: indexheader.NewReaderPool(log.NewNopLogger(), false, 0, indexheader.NewReaderPoolMetrics(nil), indexheader.AlwaysEagerDownloadIndexHeader),
		metrics:         newBucketStoreMetrics(nil),
		blockSets: map[uint64]*bucketBlockSet{
			labels.FromStrings("ext1", "1").Hash(): {blocks: [][]*bucketBlock{{b1, b2}}},
		},
		blocks: map[ulid.ULID]*bucketBlock{
			b1.meta.ULID: b1,
			b2.meta.ULID: b2,
		},
		queryGate:            gate.NewNoop(),
		chunksLimiterFactory: NewChunksLimiterFactory(0),
		seriesLimiterFactory: NewSeriesLimiterFactory(0),
		bytesLimiterFactory:  NewBytesLimiterFactory(0),
		seriesBatchSize:      SeriesBatchSize,
		requestLoggerFunc:    NoopRequestLoggerFunc,
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
	t.Parallel()

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
		},
		{
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
		},
		{
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
		{
			Name: "Query Stats Enabled",
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
					EnableQueryStats: true,
				}),
			},
			ExpectedSeries: seriesSet1,
			ExpectedHints: []hintspb.SeriesResponseHints{
				{
					QueriedBlocks: []hintspb.Block{
						{Id: block1.String()},
					},
					QueryStats: &hintspb.QueryStats{
						BlocksQueried:     1,
						PostingsTouched:   1,
						PostingsFetched:   1,
						SeriesTouched:     2,
						SeriesFetched:     2,
						ChunksTouched:     2,
						ChunksFetched:     2,
						MergedSeriesCount: 2,
						MergedChunksCount: 2,
					},
				},
			},
			HintsCompareFunc: func(t testutil.TB, expected, actual hintspb.SeriesResponseHints) {
				testutil.Equals(t, expected.QueriedBlocks, actual.QueriedBlocks)
				testutil.Equals(t, expected.QueryStats.BlocksQueried, actual.QueryStats.BlocksQueried)
				testutil.Equals(t, expected.QueryStats.PostingsTouched, actual.QueryStats.PostingsTouched)
				testutil.Equals(t, expected.QueryStats.PostingsFetched, actual.QueryStats.PostingsFetched)
				testutil.Equals(t, expected.QueryStats.SeriesTouched, actual.QueryStats.SeriesTouched)
				testutil.Equals(t, expected.QueryStats.SeriesFetched, actual.QueryStats.SeriesFetched)
				testutil.Equals(t, expected.QueryStats.ChunksTouched, actual.QueryStats.ChunksTouched)
				testutil.Equals(t, expected.QueryStats.ChunksFetched, actual.QueryStats.ChunksFetched)
				testutil.Equals(t, expected.QueryStats.MergedSeriesCount, actual.QueryStats.MergedSeriesCount)
				testutil.Equals(t, expected.QueryStats.MergedChunksCount, actual.QueryStats.MergedChunksCount)
			},
		},
	}

	storetestutil.TestServerSeries(tb, store, testCases...)
}

func TestSeries_ErrorUnmarshallingRequestHints(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)

	tmpDir := t.TempDir()

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
	)

	// Instance a real bucket store we'll use to query the series.
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, instrBkt)
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, baseBlockIDsFetcher, tmpDir, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		10,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		WithLogger(logger),
		WithIndexCache(indexCache),
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
	t.Parallel()

	tb := testutil.NewTB(t)

	tmpDir := t.TempDir()

	// Create a block with 1 series but an high number of samples,
	// so that they will span across multiple chunks.
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(tmpDir, "block")
	headOpts.ChunkRange = 10000000000

	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, h.Close()) }()

	series := labels.FromStrings("__name__", "test")
	for ts := int64(0); ts < 10000; ts++ {
		// Appending a single sample is very unoptimised, but guarantees each chunk is always MaxSamplesPerChunk
		// (except the last one, which could be smaller).
		app := h.Appender(context.Background())
		_, err := app.Append(0, series, ts, float64(ts))
		testutil.Ok(t, err)
		testutil.Ok(t, app.Commit())
	}

	blk := storetestutil.CreateBlockFromHead(t, headOpts.ChunkDirRoot, h)

	thanosMeta := metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(headOpts.ChunkDirRoot, blk.String()), thanosMeta, nil)
	testutil.Ok(t, err)

	// Create a bucket and upload the block there.
	bktDir := filepath.Join(tmpDir, "bucket")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	instrBkt := objstore.WithNoopInstr(bkt)
	logger := log.NewNopLogger()
	testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(headOpts.ChunkDirRoot, blk.String()), metadata.NoneFunc))

	// Instance a real bucket store we'll use to query the series.
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, instrBkt)
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, baseBlockIDsFetcher, tmpDir, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(100000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		10,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		WithLogger(logger),
		WithIndexCache(indexCache),
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

func TestSeries_SeriesSortedWithoutReplicaLabels(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		series         [][]labels.Labels
		replicaLabels  []string
		expectedSeries []labels.Labels
	}{
		"use TSDB label as replica label": {
			series: [][]labels.Labels{
				{
					labels.FromStrings("a", "1", "replica", "1", "z", "1"),
					labels.FromStrings("a", "1", "replica", "1", "z", "2"),
					labels.FromStrings("a", "1", "replica", "2", "z", "1"),
					labels.FromStrings("a", "1", "replica", "2", "z", "2"),
					labels.FromStrings("a", "2", "replica", "1", "z", "1"),
					labels.FromStrings("a", "2", "replica", "2", "z", "1"),
				},
				{
					labels.FromStrings("a", "1", "replica", "3", "z", "1"),
					labels.FromStrings("a", "1", "replica", "3", "z", "2"),
					labels.FromStrings("a", "2", "replica", "3", "z", "1"),
				},
			},
			replicaLabels: []string{"replica"},
			expectedSeries: []labels.Labels{
				labels.FromStrings("a", "1", "ext1", "0", "z", "1"),
				labels.FromStrings("a", "1", "ext1", "0", "z", "2"),
				labels.FromStrings("a", "1", "ext1", "1", "z", "1"),
				labels.FromStrings("a", "1", "ext1", "1", "z", "2"),
				labels.FromStrings("a", "2", "ext1", "0", "z", "1"),
				labels.FromStrings("a", "2", "ext1", "1", "z", "1"),
			},
		},
		"use external label as replica label": {
			series: [][]labels.Labels{
				{
					labels.FromStrings("a", "1", "replica", "1", "z", "1"),
					labels.FromStrings("a", "1", "replica", "1", "z", "2"),
					labels.FromStrings("a", "1", "replica", "2", "z", "1"),
					labels.FromStrings("a", "1", "replica", "2", "z", "2"),
				},
				{
					labels.FromStrings("a", "1", "replica", "1", "z", "1"),
					labels.FromStrings("a", "1", "replica", "1", "z", "2"),
				},
			},
			replicaLabels: []string{"ext1"},
			expectedSeries: []labels.Labels{
				labels.FromStrings("a", "1", "replica", "1", "z", "1"),
				labels.FromStrings("a", "1", "replica", "1", "z", "2"),
				labels.FromStrings("a", "1", "replica", "2", "z", "1"),
				labels.FromStrings("a", "1", "replica", "2", "z", "2"),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tb := testutil.NewTB(t)

			tmpDir := t.TempDir()

			bktDir := filepath.Join(tmpDir, "bucket")
			bkt, err := filesystem.NewBucket(bktDir)
			testutil.Ok(t, err)
			defer testutil.Ok(t, bkt.Close())

			instrBkt := objstore.WithNoopInstr(bkt)
			logger := log.NewNopLogger()

			for i, series := range testData.series {
				replicaVal := strconv.Itoa(i)
				head := uploadSeriesToBucket(t, bkt, replicaVal, filepath.Join(tmpDir, replicaVal), series)
				defer testutil.Ok(t, head.Close())
			}

			// Instance a real bucket store we'll use to query the series.
			baseBlockIDsFetcher := block.NewConcurrentLister(logger, instrBkt)
			fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, baseBlockIDsFetcher, tmpDir, nil, nil)
			testutil.Ok(tb, err)

			indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{})
			testutil.Ok(tb, err)

			store, err := NewBucketStore(
				instrBkt,
				fetcher,
				tmpDir,
				NewChunksLimiterFactory(100000/MaxSamplesPerChunk),
				NewSeriesLimiterFactory(0),
				NewBytesLimiterFactory(0),
				NewGapBasedPartitioner(PartitionerMaxGapSize),
				10,
				false,
				DefaultPostingOffsetInMemorySampling,
				true,
				false,
				0,
				WithLogger(logger),
				WithIndexCache(indexCache),
			)
			testutil.Ok(tb, err)
			testutil.Ok(tb, store.SyncBlocks(context.Background()))

			req := &storepb.SeriesRequest{
				MinTime: math.MinInt,
				MaxTime: math.MaxInt64,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: ".+"},
				},
				WithoutReplicaLabels: testData.replicaLabels,
			}

			srv := newStoreSeriesServer(context.Background())
			err = store.Series(req, srv)
			testutil.Ok(t, err)
			testutil.Assert(t, len(srv.SeriesSet) == len(testData.expectedSeries))

			var response []labels.Labels
			for _, respSeries := range srv.SeriesSet {
				promLabels := labelpb.ZLabelsToPromLabels(respSeries.Labels)
				response = append(response, promLabels)
			}

			testutil.Equals(t, testData.expectedSeries, response)
		})
	}
}

func uploadSeriesToBucket(t *testing.T, bkt *filesystem.Bucket, replica string, path string, series []labels.Labels) *tsdb.Head {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(path, "block")

	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)

	for _, s := range series {
		for ts := int64(0); ts < 100; ts++ {
			// Appending a single sample is very unoptimised, but guarantees each chunk is always MaxSamplesPerChunk
			// (except the last one, which could be smaller).
			app := h.Appender(context.Background())
			_, err := app.Append(0, s, ts, float64(ts))
			testutil.Ok(t, err)
			testutil.Ok(t, app.Commit())
		}
	}

	blk := storetestutil.CreateBlockFromHead(t, headOpts.ChunkDirRoot, h)

	thanosMeta := metadata.Thanos{
		Labels:     labels.FromStrings("ext1", replica).Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(headOpts.ChunkDirRoot, blk.String()), thanosMeta, nil)
	testutil.Ok(t, err)

	testutil.Ok(t, block.Upload(context.Background(), log.NewNopLogger(), bkt, filepath.Join(headOpts.ChunkDirRoot, blk.String()), metadata.NoneFunc))
	testutil.Ok(t, err)

	return h
}

func mustMarshalAny(pb proto.Message) *types.Any {
	out, err := types.MarshalAny(pb)
	if err != nil {
		panic(err)
	}
	return out
}

func TestBigEndianPostingsCount(t *testing.T) {
	t.Parallel()

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

func createBlockWithOneSeriesWithStep(t testutil.TB, dir string, lbls labels.Labels, blockIndex, totalSamples int, random *rand.Rand, step int64) ulid.ULID {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = dir
	headOpts.ChunkRange = int64(totalSamples) * step
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, h.Close()) }()

	app := h.Appender(context.Background())

	ts := int64(blockIndex * totalSamples)
	ref, err := app.Append(0, lbls, ts, random.Float64())
	testutil.Ok(t, err)
	for i := 1; i < totalSamples; i++ {
		_, err := app.Append(ref, labels.EmptyLabels(), ts+step*int64(i), random.Float64())
		testutil.Ok(t, err)
	}
	testutil.Ok(t, app.Commit())

	return storetestutil.CreateBlockFromHead(t, dir, h)
}

func setupStoreForHintsTest(t *testing.T) (testutil.TB, *BucketStore, []*storepb.Series, []*storepb.Series, ulid.ULID, ulid.ULID, func()) {
	tb := testutil.NewTB(t)

	closers := []func(){}

	tmpDir := t.TempDir()

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	closers = append(closers, func() { testutil.Ok(t, bkt.Close()) })

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
		random   = rand.New(rand.NewSource(120))
	)

	extLset := labels.FromStrings("ext1", "1")
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
	block1 := storetestutil.CreateBlockFromHead(t, bktDir, head)
	testutil.Ok(t, head.Close())
	head2, seriesSet2 := storetestutil.CreateHeadWithSeries(t, 1, storetestutil.HeadGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "1"),
		SamplesPerSeries: 1,
		Series:           2,
		PrependLabels:    extLset,
		Random:           random,
	})
	block2 := storetestutil.CreateBlockFromHead(t, bktDir, head2)
	testutil.Ok(t, head2.Close())

	for _, blockID := range []ulid.ULID{block1, block2} {
		_, err := metadata.InjectThanos(logger, filepath.Join(bktDir, blockID.String()), thanosMeta, nil)
		testutil.Ok(t, err)
	}

	// Instance a real bucket store we'll use to query back the series.
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, instrBkt)
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, baseBlockIDsFetcher, tmpDir, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		10,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		WithLogger(logger),
		WithIndexCache(indexCache),
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
	t.Parallel()

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

func TestSeries_ChunksHaveHashRepresentation(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)

	tmpDir := t.TempDir()

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(tmpDir, "block")

	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, h.Close()) }()

	series := labels.FromStrings("__name__", "test")
	app := h.Appender(context.Background())
	for ts := int64(0); ts < 10_000; ts++ {
		_, err := app.Append(0, series, ts, float64(ts))
		testutil.Ok(t, err)
	}
	testutil.Ok(t, app.Commit())

	blk := storetestutil.CreateBlockFromHead(t, headOpts.ChunkDirRoot, h)

	thanosMeta := metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(headOpts.ChunkDirRoot, blk.String()), thanosMeta, nil)
	testutil.Ok(t, err)

	// Create a bucket and upload the block there.
	bktDir := filepath.Join(tmpDir, "bucket")
	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	instrBkt := objstore.WithNoopInstr(bkt)
	logger := log.NewNopLogger()
	testutil.Ok(t, block.Upload(context.Background(), logger, bkt, filepath.Join(headOpts.ChunkDirRoot, blk.String()), metadata.NoneFunc))

	// Instance a real bucket store we'll use to query the series.
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, instrBkt)
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, baseBlockIDsFetcher, tmpDir, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(100000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		10,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		WithLogger(logger),
		WithIndexCache(indexCache),
	)
	testutil.Ok(tb, err)
	testutil.Ok(tb, store.SyncBlocks(context.Background()))

	reqMinTime := math.MinInt64
	reqMaxTime := math.MaxInt64

	testCases := []struct {
		name              string
		calculateChecksum bool
	}{
		{
			name:              "calculate checksum",
			calculateChecksum: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &storepb.SeriesRequest{
				MinTime: int64(reqMinTime),
				MaxTime: int64(reqMaxTime),
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "test"},
				},
			}

			srv := newStoreSeriesServer(context.Background())
			err = store.Series(req, srv)
			testutil.Ok(t, err)
			testutil.Assert(t, len(srv.SeriesSet) == 1)

			for _, rawChunk := range srv.SeriesSet[0].Chunks {
				hash := rawChunk.Raw.Hash
				decodedChunk, err := chunkenc.FromData(chunkenc.EncXOR, rawChunk.Raw.Data)
				testutil.Ok(t, err)

				if tc.calculateChecksum {
					expectedHash := xxhash.Sum64(decodedChunk.Bytes())
					testutil.Equals(t, expectedHash, hash)
				} else {
					testutil.Equals(t, uint64(0), hash)
				}
			}
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

func BenchmarkBucketBlock_readChunkRange(b *testing.B) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()

		// Read chunks of different length. We're not using random to make the benchmark repeatable.
		readLengths = []int64{300, 500, 1000, 5000, 10000, 30000, 50000, 100000, 300000, 1500000}
	)

	tmpDir := b.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(b, err)
	b.Cleanup(func() {
		testutil.Ok(b, bkt.Close())
	})

	// Create a block.
	blockID := createBlockWithOneSeriesWithStep(testutil.NewTB(b), tmpDir, labels.FromStrings("__name__", "test"), 0, 100000, rand.New(rand.NewSource(0)), 5000)

	// Upload the block to the bucket.
	thanosMeta := metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	blockMeta, err := metadata.InjectThanos(logger, filepath.Join(tmpDir, blockID.String()), thanosMeta, nil)
	testutil.Ok(b, err)

	testutil.Ok(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	// Create a chunk pool with buckets between 8B and 32KB.
	chunkPool, err := pool.NewBucketedPool[byte](8, 32*1024, 2, 1e10)
	testutil.Ok(b, err)

	// Create a bucket block with only the dependencies we need for the benchmark.
	blk, err := newBucketBlock(context.Background(), newBucketStoreMetrics(nil), blockMeta, bkt, tmpDir, nil, chunkPool, nil, nil, nil, nil, nil)
	testutil.Ok(b, err)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		offset := int64(0)
		length := readLengths[n%len(readLengths)]

		_, err := blk.readChunkRange(ctx, 0, offset, length, byteRanges{{offset: 0, length: int(length)}}, logger)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkBlockSeries(b *testing.B) {
	blk, blockMeta := prepareBucket(b, compact.ResolutionLevelRaw)

	aggrs := []storepb.Aggr{storepb.Aggr_RAW}
	for _, concurrency := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("concurrency: %d", concurrency), func(b *testing.B) {
			benchmarkBlockSeriesWithConcurrency(b, concurrency, blockMeta, blk, aggrs)
		})
	}
}

func prepareBucket(b *testing.B, resolutionLevel compact.ResolutionLevel) (*bucketBlock, *metadata.Meta) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	tmpDir := b.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(b, err)
	b.Cleanup(func() {
		testutil.Ok(b, bkt.Close())
	})

	// Create a block.
	head, _ := storetestutil.CreateHeadWithSeries(b, 0, storetestutil.HeadGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "head"),
		SamplesPerSeries: 86400 / 15, // Simulate 1 day block with 15s scrape interval.
		ScrapeInterval:   15 * time.Second,
		Series:           1000,
		PrependLabels:    labels.EmptyLabels(),
		Random:           rand.New(rand.NewSource(120)),
		SkipChunks:       true,
	})
	blockID := storetestutil.CreateBlockFromHead(b, tmpDir, head)

	// Upload the block to the bucket.
	thanosMeta := metadata.Thanos{
		Labels:     labels.FromStrings("ext1", "1").Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	blockMeta, err := metadata.InjectThanos(logger, filepath.Join(tmpDir, blockID.String()), thanosMeta, nil)
	testutil.Ok(b, err)

	testutil.Ok(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	if resolutionLevel > 0 {
		// Downsample newly-created block.
		blockID, err = downsample.Downsample(ctx, logger, blockMeta, head, tmpDir, int64(resolutionLevel))
		testutil.Ok(b, err)
		blockMeta, err = metadata.ReadFromDir(filepath.Join(tmpDir, blockID.String()))
		testutil.Ok(b, err)

		testutil.Ok(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))
	}
	testutil.Ok(b, head.Close())

	// Create chunk pool and partitioner using the same production settings.
	chunkPool, err := NewDefaultChunkBytesPool(64 * 1024 * 1024 * 1024)
	testutil.Ok(b, err)

	partitioner := NewGapBasedPartitioner(PartitionerMaxGapSize)

	// Create an index header reader.
	indexHeaderReader, err := indexheader.NewBinaryReader(ctx, logger, bkt, tmpDir, blockMeta.ULID, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
	testutil.Ok(b, err)
	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.DefaultInMemoryIndexCacheConfig)
	testutil.Ok(b, err)

	// Create a bucket block with only the dependencies we need for the benchmark.
	blk, err := newBucketBlock(context.Background(), newBucketStoreMetrics(nil), blockMeta, bkt, tmpDir, indexCache, chunkPool, indexHeaderReader, partitioner, nil, nil, nil)
	testutil.Ok(b, err)
	return blk, blockMeta
}

func benchmarkBlockSeriesWithConcurrency(b *testing.B, concurrency int, blockMeta *metadata.Meta, blk *bucketBlock, aggrs []storepb.Aggr) {
	// Run the same number of queries per goroutine.
	queriesPerWorker := b.N / concurrency

	// No limits.
	chunksLimiter := NewChunksLimiterFactory(0)(nil)
	seriesLimiter := NewSeriesLimiterFactory(0)(nil)
	ctx := context.Background()

	// Run multiple workers to execute the queries.
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	dummyCounter := promauto.NewCounter(prometheus.CounterOpts{Name: "test"})
	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()

			for n := 0; n < queriesPerWorker; n++ {
				// Each query touches a subset of series. To make it reproducible and make sure
				// we just don't query consecutive series (as is in the real world), we do create
				// a label matcher which looks for a short integer within the label value.
				labelMatcher := fmt.Sprintf(".*%d.*", n%20)

				req := &storepb.SeriesRequest{
					MinTime: blockMeta.MinTime,
					MaxTime: blockMeta.MaxTime,
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: labelMatcher},
					},
					SkipChunks: false,
					Aggregates: aggrs,
				}

				matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
				// TODO FIXME! testutil.Ok calls b.Fatalf under the hood, which
				// must be called only from the goroutine running the Benchmark function.
				testutil.Ok(b, err)
				sortedMatchers := newSortedMatchers(matchers)

				dummyHistogram := promauto.NewHistogramVec(prometheus.HistogramOpts{}, []string{tenancy.MetricLabel})
				blockClient := newBlockSeriesClient(
					ctx,
					nil,
					blk,
					req,
					seriesLimiter,
					chunksLimiter,
					NewBytesLimiterFactory(0)(nil),
					matchers,
					nil,
					false,
					SeriesBatchSize,
					dummyHistogram,
					dummyHistogram,
					dummyHistogram,
					nil,
					false,
					dummyCounter,
					dummyCounter,
					dummyCounter,
					tenancy.DefaultTenant,
				)
				testutil.Ok(b, blockClient.ExpandPostings(sortedMatchers, seriesLimiter))
				defer blockClient.Close()

				// Ensure at least 1 series has been returned (as expected).
				_, err = blockClient.Recv()
				testutil.Ok(b, err)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkDownsampledBlockSeries(b *testing.B) {
	blk, blockMeta := prepareBucket(b, compact.ResolutionLevel5m)
	aggrs := []storepb.Aggr{}
	for i := 1; i < int(storepb.Aggr_COUNTER); i++ {
		aggrs = append(aggrs, storepb.Aggr(i))
		for _, concurrency := range []int{1, 2, 4, 8, 16, 32} {
			b.Run(fmt.Sprintf("aggregates: %v, concurrency: %d", aggrs, concurrency), func(b *testing.B) {
				benchmarkBlockSeriesWithConcurrency(b, concurrency, blockMeta, blk, aggrs)
			})
		}
	}
}

func TestExpandPostingsWithContextCancel(t *testing.T) {
	t.Parallel()

	// Not enough number of postings to check context cancellation.
	p := index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5, 6, 7, 8})
	ctx, cancel := context.WithCancel(context.Background())

	cancel()
	_, err := ExpandPostingsWithContext(ctx, p)
	testutil.Ok(t, err)

	refs := make([]storage.SeriesRef, 0)
	for i := 0; i < 128; i++ {
		refs = append(refs, storage.SeriesRef(i))
	}
	p = index.NewListPostings(refs)
	ctx, cancel = context.WithCancel(context.Background())

	cancel()
	res, err := ExpandPostingsWithContext(ctx, p)
	testutil.NotOk(t, err)
	testutil.Equals(t, context.Canceled, err)
	testutil.Equals(t, []storage.SeriesRef(nil), res)
}

func samePostingGroup(a, b *postingGroup) bool {
	if a.name != b.name || a.lazy != b.lazy || a.addAll != b.addAll || a.cardinality != b.cardinality || len(a.matchers) != len(b.matchers) {
		return false
	}

	if !reflect.DeepEqual(a.addKeys, b.addKeys) || !reflect.DeepEqual(a.removeKeys, b.removeKeys) {
		return false
	}

	for i := 0; i < len(a.matchers); i++ {
		if a.matchers[i].String() != b.matchers[i].String() {
			return false
		}
	}
	return true
}

func TestMatchersToPostingGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		matchers    []*labels.Matcher
		labelValues map[string][]string
		expected    []*postingGroup
	}{
		{
			name: "single equal matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
				},
			},
		},
		{
			name: "deduplicate two equal matchers",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
				},
			},
		},
		{
			name: "deduplicate multiple equal matchers",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
				},
			},
		},
		{
			name: "two equal matchers with different label name, merge",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "baz"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
				"bar": {"baz"},
			},
			expected: []*postingGroup{
				{
					name:     "bar",
					addAll:   false,
					addKeys:  []string{"baz"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "bar", "baz")},
				},
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
				},
			},
		},
		{
			name: "two different equal matchers with same label name, intersect",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "baz"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: nil,
		},
		{
			name: "Intersect equal and unequal matcher, no hit",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: nil,
		},
		{
			name: "Intersect equal and unequal matcher, has result",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "baz"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchNotEqual, "foo", "baz")},
				},
			},
		},
		{
			name: "Intersect equal and regex matcher, no result",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "x.*"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
		},
		{
			name: "Intersect equal and regex matcher, have result",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "b.*"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchRegexp, "foo", "b.*")},
				},
			},
		},
		{
			name: "Intersect equal and unequal inverse matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", ""),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchNotEqual, "foo", "")},
				},
			},
		},
		{
			name: "Intersect equal and regexp matching non empty matcher",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".+"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchRegexp, "foo", ".+")},
				},
			},
		},
		{
			name: "Intersect two regex matchers",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar|baz"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar|buzz"),
			},
			labelValues: map[string][]string{
				"foo": {"bar", "baz", "buzz"},
			},
			expected: []*postingGroup{
				{
					name:     "foo",
					addAll:   false,
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar|baz"), labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar|buzz")},
				},
			},
		},
		{
			name: "Merge inverse matchers",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "baz"),
			},
			labelValues: map[string][]string{
				"foo": {"buzz", "bar", "baz"},
			},
			expected: []*postingGroup{
				{
					name:       "foo",
					addAll:     true,
					removeKeys: []string{"bar", "baz"},
					matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "foo", "bar"), labels.MustNewMatcher(labels.MatchNotEqual, "foo", "baz")},
				},
			},
		},
		{
			name: "Dedup match all regex matchers",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"),
				labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"),
				labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"),
				labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"),
				labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"),
			},
			labelValues: map[string][]string{
				labels.MetricName: {"up", "go_info"},
			},
			expected: []*postingGroup{
				{
					name:     labels.MetricName,
					addAll:   true,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*")},
				},
			},
		},
		{
			name: "Multiple posting groups",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", ".*"),
				labels.MustNewMatcher(labels.MatchNotEqual, "cluster", ""),
			},
			labelValues: map[string][]string{
				labels.MetricName: {"up", "go_info"},
				"cluster":         {"us-east-1", "us-west-2"},
				"job":             {"prometheus", "thanos"},
			},
			expected: []*postingGroup{
				{
					name:     labels.MetricName,
					addAll:   false,
					addKeys:  []string{"up"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
				},
				{
					name:     "cluster",
					addAll:   false,
					addKeys:  []string{"us-east-1", "us-west-2"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "cluster", "")},
				},
				{
					name:     "job",
					addAll:   true,
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", ".*")},
				},
			},
		},
		{
			name: "Multiple unequal empty",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, ""),
				labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, ""),
				labels.MustNewMatcher(labels.MatchNotEqual, "job", ""),
				labels.MustNewMatcher(labels.MatchNotEqual, "job", ""),
				labels.MustNewMatcher(labels.MatchNotEqual, "cluster", ""),
				labels.MustNewMatcher(labels.MatchNotEqual, "cluster", ""),
			},
			labelValues: map[string][]string{
				labels.MetricName: {"up", "go_info"},
				"cluster":         {"us-east-1", "us-west-2"},
				"job":             {"prometheus", "thanos"},
			},
			expected: []*postingGroup{
				{
					name:     labels.MetricName,
					addAll:   false,
					addKeys:  []string{"go_info", "up"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "__name__", "")},
				},
				{
					name:     "cluster",
					addAll:   false,
					addKeys:  []string{"us-east-1", "us-west-2"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "cluster", "")},
				},
				{
					name:     "job",
					addAll:   false,
					addKeys:  []string{"prometheus", "thanos"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "job", "")},
				},
			},
		},
		{
			name: "Reproduce values shadow bug",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "name", "test.*"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "name", "testfoo"),
				labels.MustNewMatcher(labels.MatchNotEqual, "name", ""),
			},
			labelValues: map[string][]string{
				"name": {"testbar", "testfoo"},
			},
			expected: []*postingGroup{
				{
					name:    "name",
					addAll:  false,
					addKeys: []string{"testbar"},
					matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchNotEqual, "name", ""),
						labels.MustNewMatcher(labels.MatchRegexp, "name", "test.*"),
						labels.MustNewMatcher(labels.MatchNotRegexp, "name", "testfoo"),
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := matchersToPostingGroups(ctx, func(name string) ([]string, error) {
				sort.Strings(tc.labelValues[name])
				return tc.labelValues[name], nil
			}, tc.matchers)
			testutil.Ok(t, err)
			testutil.Equals(t, len(tc.expected), len(actual))
			for i := 0; i < len(tc.expected); i++ {
				testutil.Assert(t, samePostingGroup(tc.expected[i], actual[i]))
			}
		})
	}
}

func TestPostingGroupMerge(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		group1   *postingGroup
		group2   *postingGroup
		expected *postingGroup
	}{
		{
			name:     "empty group2",
			group1:   &postingGroup{},
			group2:   nil,
			expected: &postingGroup{},
		},
		{
			name:     "group different names, return group1",
			group1:   &postingGroup{name: "bar"},
			group2:   &postingGroup{name: "foo"},
			expected: nil,
		},
		{
			name:     "both same addKey",
			group1:   &postingGroup{addKeys: []string{"foo"}},
			group2:   &postingGroup{addKeys: []string{"foo"}},
			expected: &postingGroup{addKeys: []string{"foo"}},
		},
		{
			name:     "both same addKeys, but different order",
			group1:   &postingGroup{addKeys: []string{"foo", "bar"}},
			group2:   &postingGroup{addKeys: []string{"bar", "foo"}},
			expected: &postingGroup{addKeys: []string{"bar", "foo"}},
		},
		{
			name:     "different addKeys",
			group1:   &postingGroup{addKeys: []string{"foo"}},
			group2:   &postingGroup{addKeys: []string{"bar"}},
			expected: &postingGroup{addKeys: []string{}},
		},
		{
			name:     "intersect common add keys",
			group1:   &postingGroup{addKeys: []string{"foo", "bar"}},
			group2:   &postingGroup{addKeys: []string{"bar", "baz"}},
			expected: &postingGroup{addKeys: []string{"bar"}},
		},
		{
			name:     "both addAll, no remove keys",
			group1:   &postingGroup{addAll: true},
			group2:   &postingGroup{addAll: true},
			expected: &postingGroup{addAll: true},
		},
		{
			name:     "both addAll, one remove keys",
			group1:   &postingGroup{addAll: true},
			group2:   &postingGroup{addAll: true, removeKeys: []string{"foo"}},
			expected: &postingGroup{addAll: true, removeKeys: []string{"foo"}},
		},
		{
			name:     "both addAll, same remove keys",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"foo"}},
			group2:   &postingGroup{addAll: true, removeKeys: []string{"foo"}},
			expected: &postingGroup{addAll: true, removeKeys: []string{"foo"}},
		},
		{
			name:     "both addAll, merge different remove keys",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"foo"}},
			group2:   &postingGroup{addAll: true, removeKeys: []string{"bar"}},
			expected: &postingGroup{addAll: true, removeKeys: []string{"bar", "foo"}},
		},
		{
			name:     "both addAll, multiple remove keys",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"foo", "zoo"}},
			group2:   &postingGroup{addAll: true, removeKeys: []string{"a", "bar"}},
			expected: &postingGroup{addAll: true, removeKeys: []string{"a", "bar", "foo", "zoo"}},
		},
		{
			name:     "both addAll, multiple remove keys 2",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"a", "bar"}},
			group2:   &postingGroup{addAll: true, removeKeys: []string{"foo", "zoo"}},
			expected: &postingGroup{addAll: true, removeKeys: []string{"a", "bar", "foo", "zoo"}},
		},
		{
			name:     "one add and one remove, only keep addKeys",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"foo"}},
			group2:   &postingGroup{addKeys: []string{""}},
			expected: &postingGroup{addKeys: []string{""}},
		},
		{
			name:     "one add and one remove, subtract common keys to add and remove",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"foo"}},
			group2:   &postingGroup{addKeys: []string{"", "foo"}},
			expected: &postingGroup{addKeys: []string{""}},
		},
		{
			name:     "same add and remove key",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"foo"}},
			group2:   &postingGroup{addKeys: []string{"foo"}},
			expected: &postingGroup{addKeys: []string{}},
		},
		{
			name:     "same add and remove key, multiple keys",
			group1:   &postingGroup{addAll: true, removeKeys: []string{"2"}},
			group2:   &postingGroup{addKeys: []string{"1", "2", "3", "4", "5", "6"}},
			expected: &postingGroup{addKeys: []string{"1", "3", "4", "5", "6"}},
		},
		{
			name:     "addAll and non addAll posting group merge with empty keys",
			group1:   &postingGroup{addAll: true, removeKeys: nil},
			group2:   &postingGroup{addKeys: nil},
			expected: &postingGroup{addKeys: nil},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.group1 != nil {
				slices.Sort(tc.group1.addKeys)
				slices.Sort(tc.group1.removeKeys)
			}
			if tc.group2 != nil {
				slices.Sort(tc.group2.addKeys)
				slices.Sort(tc.group2.removeKeys)
			}
			res := tc.group1.mergeKeys(tc.group2)
			testutil.Equals(t, tc.expected, res)
		})
	}
}

// TestExpandedPostings is a test whether there is a race between multiple ExpandPostings() calls.
func TestExpandedPostingsRace(t *testing.T) {
	t.Parallel()

	const blockCount = 10

	tmpDir := t.TempDir()
	t.Cleanup(func() {
		testutil.Ok(t, os.RemoveAll(tmpDir))
	})

	bkt := objstore.NewInMemBucket()
	t.Cleanup(func() {
		testutil.Ok(t, bkt.Close())
	})

	logger := log.NewNopLogger()
	// Create a block.
	head, _ := storetestutil.CreateHeadWithSeries(t, 0, storetestutil.HeadGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "head"),
		SamplesPerSeries: 10,
		ScrapeInterval:   15 * time.Second,
		Series:           1000,
		PrependLabels:    labels.EmptyLabels(),
		Random:           rand.New(rand.NewSource(120)),
		SkipChunks:       true,
	})
	blockID := storetestutil.CreateBlockFromHead(t, tmpDir, head)

	bucketBlocks := make([]*bucketBlock, 0, blockCount)

	for i := 0; i < blockCount; i++ {
		ul := ulid.MustNew(uint64(i), rand.New(rand.NewSource(444)))

		// Upload the block to the bucket.
		thanosMeta := metadata.Thanos{
			Labels:     labels.FromStrings("ext1", fmt.Sprintf("%d", i)).Map(),
			Downsample: metadata.ThanosDownsample{Resolution: 0},
			Source:     metadata.TestSource,
		}
		m, err := metadata.ReadFromDir(filepath.Join(tmpDir, blockID.String()))
		testutil.Ok(t, err)

		m.Thanos = thanosMeta
		m.BlockMeta.ULID = ul

		e2eutil.Copy(t, filepath.Join(tmpDir, blockID.String()), filepath.Join(tmpDir, ul.String()))
		testutil.Ok(t, m.WriteToDir(log.NewLogfmtLogger(os.Stderr), filepath.Join(tmpDir, ul.String())))
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(context.Background(), log.NewLogfmtLogger(os.Stderr), bkt, filepath.Join(tmpDir, ul.String()), metadata.NoneFunc))

		r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, ul, DefaultPostingOffsetInMemorySampling, indexheader.NewBinaryReaderMetrics(nil))
		testutil.Ok(t, err)

		blk, err := newBucketBlock(
			context.Background(),
			newBucketStoreMetrics(nil),
			m,
			bkt,
			filepath.Join(tmpDir, ul.String()),
			noopCache{},
			nil,
			r,
			NewGapBasedPartitioner(PartitionerMaxGapSize),
			nil,
			nil,
			nil,
		)
		testutil.Ok(t, err)

		bucketBlocks = append(bucketBlocks, blk)
	}

	tm, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	l := sync.Mutex{}
	previousRefs := make(map[int][]storage.SeriesRef)
	dummyCounter := promauto.With(prometheus.NewRegistry()).NewCounter(prometheus.CounterOpts{Name: "test"})

	for {
		if tm.Err() != nil {
			break
		}

		m := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			labels.MustNewMatcher(labels.MatchRegexp, "j", ".+"),
			labels.MustNewMatcher(labels.MatchRegexp, "i", ".+"),
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
			labels.MustNewMatcher(labels.MatchRegexp, "j", ".+"),
			labels.MustNewMatcher(labels.MatchRegexp, "i", ".+"),
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
		}

		wg := &sync.WaitGroup{}
		for i, bb := range bucketBlocks {
			wg.Add(1)

			go func(i int, bb *bucketBlock) {
				refs, err := bb.indexReader(logger).ExpandedPostings(context.Background(), m, NewBytesLimiterFactory(0)(nil), false, dummyCounter, tenancy.DefaultTenant)
				testutil.Ok(t, err)
				defer wg.Done()

				l.Lock()
				defer l.Unlock()
				if previousRefs[i] != nil {
					testutil.Equals(t, previousRefs[i], refs.postings)
				} else {
					previousRefs[i] = refs.postings
				}
			}(i, bb)
		}
		wg.Wait()
	}
}

func TestBucketIndexReader_decodeCachedPostingsErrors(t *testing.T) {
	t.Parallel()

	bir := bucketIndexReader{stats: &queryStats{}}
	t.Run("should return error on broken cached postings without snappy prefix", func(t *testing.T) {
		_, _, err := bir.decodeCachedPostings([]byte("foo"))
		testutil.NotOk(t, err)
	})
	t.Run("should return error on broken cached postings with snappy prefix", func(t *testing.T) {
		_, _, err := bir.decodeCachedPostings(append([]byte(codecHeaderSnappy), []byte("foo")...))
		testutil.NotOk(t, err)
	})
}

func TestBucketStoreDedupOnBlockSeriesSet(t *testing.T) {
	t.Parallel()

	logger := log.NewNopLogger()
	tmpDir := t.TempDir()
	bktDir := filepath.Join(tmpDir, "bkt")
	auxDir := filepath.Join(tmpDir, "aux")
	metaDir := filepath.Join(tmpDir, "meta")
	extLset := labels.FromStrings("region", "eu-west")

	testutil.Ok(t, os.MkdirAll(metaDir, os.ModePerm))
	testutil.Ok(t, os.MkdirAll(auxDir, os.ModePerm))

	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bkt.Close()) })

	for i := 0; i < 2; i++ {
		headOpts := tsdb.DefaultHeadOptions()
		headOpts.ChunkDirRoot = tmpDir
		headOpts.ChunkRange = 1000
		h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
		testutil.Ok(t, err)
		t.Cleanup(func() { testutil.Ok(t, h.Close()) })

		app := h.Appender(context.Background())
		_, err = app.Append(0, labels.FromStrings("replica", "a", "z", "1"), 0, 1)
		testutil.Ok(t, err)
		_, err = app.Append(0, labels.FromStrings("replica", "a", "z", "2"), 0, 1)
		testutil.Ok(t, err)
		_, err = app.Append(0, labels.FromStrings("replica", "b", "z", "1"), 0, 1)
		testutil.Ok(t, err)
		_, err = app.Append(0, labels.FromStrings("replica", "b", "z", "2"), 0, 1)
		testutil.Ok(t, err)
		testutil.Ok(t, app.Commit())

		id := storetestutil.CreateBlockFromHead(t, auxDir, h)

		auxBlockDir := filepath.Join(auxDir, id.String())
		_, err = metadata.InjectThanos(log.NewNopLogger(), auxBlockDir, metadata.Thanos{
			Labels:     extLset.Map(),
			Downsample: metadata.ThanosDownsample{Resolution: 0},
			Source:     metadata.TestSource,
		}, nil)
		testutil.Ok(t, err)

		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, auxBlockDir, metadata.NoneFunc))
		testutil.Ok(t, block.Upload(context.Background(), logger, bkt, auxBlockDir, metadata.NoneFunc))
	}

	chunkPool, err := NewDefaultChunkBytesPool(2e5)
	testutil.Ok(t, err)

	insBkt := objstore.WithNoopInstr(bkt)
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, insBkt)
	metaFetcher, err := block.NewMetaFetcher(logger, 20, insBkt, baseBlockIDsFetcher, metaDir, nil, []block.MetadataFilter{
		block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
	})
	testutil.Ok(t, err)

	bucketStore, err := NewBucketStore(
		objstore.WithNoopInstr(bkt),
		metaFetcher,
		"",
		NewChunksLimiterFactory(10e6),
		NewSeriesLimiterFactory(10e6),
		NewBytesLimiterFactory(10e6),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		20,
		true,
		DefaultPostingOffsetInMemorySampling,
		false,
		false,
		1*time.Minute,
		WithChunkPool(chunkPool),
		WithFilterConfig(allowAllFilterConf),
	)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bucketStore.Close()) })

	testutil.Ok(t, bucketStore.SyncBlocks(context.Background()))

	srv := newStoreSeriesServer(context.Background())
	testutil.Ok(t, bucketStore.Series(&storepb.SeriesRequest{
		WithoutReplicaLabels: []string{"replica"},
		MinTime:              timestamp.FromTime(minTime),
		MaxTime:              timestamp.FromTime(maxTime),
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_NEQ, Name: "z", Value: ""},
		},
	}, srv))

	testutil.Equals(t, true, slices.IsSortedFunc(srv.SeriesSet, func(x, y storepb.Series) int {
		return labels.Compare(x.PromLabels(), y.PromLabels())
	}))
	testutil.Equals(t, 2, len(srv.SeriesSet))
}

func TestQueryStatsMerge(t *testing.T) {
	t.Parallel()

	s := &queryStats{
		blocksQueried:                      1,
		postingsTouched:                    1,
		PostingsTouchedSizeSum:             1,
		postingsToFetch:                    1,
		postingsFetched:                    1,
		PostingsFetchedSizeSum:             1,
		postingsFetchCount:                 1,
		PostingsFetchDurationSum:           1,
		cachedPostingsCompressions:         1,
		cachedPostingsCompressionErrors:    1,
		CachedPostingsOriginalSizeSum:      1,
		CachedPostingsCompressedSizeSum:    1,
		CachedPostingsCompressionTimeSum:   1,
		cachedPostingsDecompressions:       1,
		cachedPostingsDecompressionErrors:  1,
		CachedPostingsDecompressionTimeSum: 1,
		seriesTouched:                      1,
		SeriesTouchedSizeSum:               1,
		seriesFetched:                      1,
		SeriesFetchedSizeSum:               1,
		seriesFetchCount:                   1,
		SeriesFetchDurationSum:             1,
		SeriesDownloadLatencySum:           1,
		chunksTouched:                      1,
		ChunksTouchedSizeSum:               1,
		chunksFetched:                      1,
		ChunksFetchedSizeSum:               1,
		chunksFetchCount:                   1,
		ChunksFetchDurationSum:             1,
		ChunksDownloadLatencySum:           1,
		GetAllDuration:                     1,
		mergedSeriesCount:                  1,
		mergedChunksCount:                  1,
		MergeDuration:                      1,
		DataDownloadedSizeSum:              1,
	}

	o := &queryStats{
		blocksQueried:                      100,
		postingsTouched:                    100,
		PostingsTouchedSizeSum:             100,
		postingsToFetch:                    100,
		postingsFetched:                    100,
		PostingsFetchedSizeSum:             100,
		postingsFetchCount:                 100,
		PostingsFetchDurationSum:           100,
		cachedPostingsCompressions:         100,
		cachedPostingsCompressionErrors:    100,
		CachedPostingsOriginalSizeSum:      100,
		CachedPostingsCompressedSizeSum:    100,
		CachedPostingsCompressionTimeSum:   100,
		cachedPostingsDecompressions:       100,
		cachedPostingsDecompressionErrors:  100,
		CachedPostingsDecompressionTimeSum: 100,
		seriesTouched:                      100,
		SeriesTouchedSizeSum:               100,
		seriesFetched:                      100,
		SeriesFetchedSizeSum:               100,
		seriesFetchCount:                   100,
		SeriesFetchDurationSum:             100,
		SeriesDownloadLatencySum:           100,
		chunksTouched:                      100,
		ChunksTouchedSizeSum:               100,
		chunksFetched:                      100,
		ChunksFetchedSizeSum:               100,
		chunksFetchCount:                   100,
		ChunksFetchDurationSum:             100,
		ChunksDownloadLatencySum:           100,
		GetAllDuration:                     100,
		mergedSeriesCount:                  100,
		mergedChunksCount:                  100,
		MergeDuration:                      100,
		DataDownloadedSizeSum:              100,
	}

	// Expected stats.
	e := &queryStats{
		blocksQueried:                      101,
		postingsTouched:                    101,
		PostingsTouchedSizeSum:             101,
		postingsToFetch:                    101,
		postingsFetched:                    101,
		PostingsFetchedSizeSum:             101,
		postingsFetchCount:                 101,
		PostingsFetchDurationSum:           101,
		cachedPostingsCompressions:         101,
		cachedPostingsCompressionErrors:    101,
		CachedPostingsOriginalSizeSum:      101,
		CachedPostingsCompressedSizeSum:    101,
		CachedPostingsCompressionTimeSum:   101,
		cachedPostingsDecompressions:       101,
		cachedPostingsDecompressionErrors:  101,
		CachedPostingsDecompressionTimeSum: 101,
		seriesTouched:                      101,
		SeriesTouchedSizeSum:               101,
		seriesFetched:                      101,
		SeriesFetchedSizeSum:               101,
		seriesFetchCount:                   101,
		SeriesFetchDurationSum:             101,
		SeriesDownloadLatencySum:           101,
		chunksTouched:                      101,
		ChunksTouchedSizeSum:               101,
		chunksFetched:                      101,
		ChunksFetchedSizeSum:               101,
		chunksFetchCount:                   101,
		ChunksFetchDurationSum:             101,
		ChunksDownloadLatencySum:           101,
		GetAllDuration:                     101,
		mergedSeriesCount:                  101,
		mergedChunksCount:                  101,
		MergeDuration:                      101,
		DataDownloadedSizeSum:              101,
	}

	s.merge(o)
	testutil.Equals(t, e, s)
}

func TestBucketStoreStreamingSeriesLimit(t *testing.T) {
	t.Parallel()

	logger := log.NewNopLogger()
	tmpDir := t.TempDir()
	bktDir := filepath.Join(tmpDir, "bkt")
	auxDir := filepath.Join(tmpDir, "aux")
	metaDir := filepath.Join(tmpDir, "meta")
	extLset := labels.FromStrings("region", "eu-west")

	testutil.Ok(t, os.MkdirAll(metaDir, os.ModePerm))
	testutil.Ok(t, os.MkdirAll(auxDir, os.ModePerm))

	bkt, err := filesystem.NewBucket(bktDir)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bkt.Close()) })

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = tmpDir
	headOpts.ChunkRange = 1000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, h.Close()) })

	app := h.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("a", "1", "z", "1"), 0, 1)
	testutil.Ok(t, err)
	_, err = app.Append(0, labels.FromStrings("a", "1", "z", "2"), 0, 1)
	testutil.Ok(t, err)
	_, err = app.Append(0, labels.FromStrings("a", "1", "z", "3"), 0, 1)
	testutil.Ok(t, err)
	_, err = app.Append(0, labels.FromStrings("a", "1", "z", "4"), 0, 1)
	testutil.Ok(t, err)
	_, err = app.Append(0, labels.FromStrings("a", "1", "z", "5"), 0, 1)
	testutil.Ok(t, err)
	_, err = app.Append(0, labels.FromStrings("a", "1", "z", "6"), 0, 1)
	testutil.Ok(t, err)
	testutil.Ok(t, app.Commit())

	id := storetestutil.CreateBlockFromHead(t, auxDir, h)

	auxBlockDir := filepath.Join(auxDir, id.String())
	_, err = metadata.InjectThanos(log.NewNopLogger(), auxBlockDir, metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(context.Background(), logger, bkt, auxBlockDir, metadata.NoneFunc))

	chunkPool, err := NewDefaultChunkBytesPool(2e5)
	testutil.Ok(t, err)

	insBkt := objstore.WithNoopInstr(bkt)
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, insBkt)
	metaFetcher, err := block.NewMetaFetcher(logger, 20, insBkt, baseBlockIDsFetcher, metaDir, nil, []block.MetadataFilter{
		block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
	})
	testutil.Ok(t, err)

	firstBytesLimiterChecked := false
	secondBytesLimiterChecked := false

	// Set series limit to 2. Only pass if series limiter applies
	// for lazy postings only.
	bucketStore, err := NewBucketStore(
		objstore.WithNoopInstr(bkt),
		metaFetcher,
		"",
		NewChunksLimiterFactory(10e6),
		NewSeriesLimiterFactory(2),
		func(_ prometheus.Counter) BytesLimiter {
			return &compositeBytesLimiterMock{
				limiters: []BytesLimiter{
					&bytesLimiterMock{
						limitFunc: func(_ uint64, _ StoreDataType) error {
							firstBytesLimiterChecked = true
							return nil
						},
					},
					&bytesLimiterMock{
						limitFunc: func(_ uint64, _ StoreDataType) error {
							secondBytesLimiterChecked = true
							return nil
						},
					},
				},
			}
		},
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		20,
		true,
		DefaultPostingOffsetInMemorySampling,
		false,
		false,
		1*time.Minute,
		WithChunkPool(chunkPool),
		WithFilterConfig(allowAllFilterConf),
		WithLazyExpandedPostings(true),
		WithBlockEstimatedMaxSeriesFunc(func(_ metadata.Meta) uint64 {
			return 1
		}),
	)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, bucketStore.Close()) })

	testutil.Ok(t, bucketStore.SyncBlocks(context.Background()))

	req := &storepb.SeriesRequest{
		MinTime: timestamp.FromTime(minTime),
		MaxTime: timestamp.FromTime(maxTime),
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
			{Type: storepb.LabelMatcher_RE, Name: "z", Value: "1|2"},
		},
	}
	srv := newStoreSeriesServer(context.Background())
	testutil.Ok(t, bucketStore.Series(req, srv))
	testutil.Equals(t, 2, len(srv.SeriesSet))
	testutil.Equals(t, true, firstBytesLimiterChecked)
	testutil.Equals(t, true, secondBytesLimiterChecked)
}

type bytesLimiterMock struct {
	limitFunc func(uint64, StoreDataType) error
}

func (m *bytesLimiterMock) ReserveWithType(num uint64, dataType StoreDataType) error {
	return m.limitFunc(num, dataType)
}

type compositeBytesLimiterMock struct {
	limiters []BytesLimiter
}

func (m *compositeBytesLimiterMock) ReserveWithType(num uint64, dataType StoreDataType) error {
	for _, l := range m.limiters {
		if err := l.ReserveWithType(num, dataType); err != nil {
			return err
		}
	}
	return nil
}

func TestBucketStoreMetadataLimit(t *testing.T) {
	t.Parallel()

	tb := testutil.NewTB(t)

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(tb, err)
	defer func() { testutil.Ok(tb, bkt.Close()) }()

	uploadTestBlock(tb, tmpDir, bkt, 30000)

	instrBkt := objstore.WithNoopInstr(bkt)
	logger := log.NewNopLogger()

	// Instance a real bucket store we'll use to query the series.
	baseBlockIDsFetcher := block.NewConcurrentLister(logger, instrBkt)
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, baseBlockIDsFetcher, tmpDir, nil, nil)
	testutil.Ok(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, nil, storecache.InMemoryIndexCacheConfig{})
	testutil.Ok(tb, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(0),
		NewSeriesLimiterFactory(0),
		NewBytesLimiterFactory(0),
		NewGapBasedPartitioner(PartitionerMaxGapSize),
		10,
		false,
		DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		WithLogger(logger),
		WithIndexCache(indexCache),
	)
	testutil.Ok(tb, err)
	testutil.Ok(tb, store.SyncBlocks(context.Background()))

	seriesTests := map[string]struct {
		limit           int64
		expectedResults int
	}{
		"series without limit": {
			expectedResults: 12000,
		},
		"series with limit": {
			limit:           11000,
			expectedResults: 11000,
		},
	}

	for testName, testData := range seriesTests {
		t.Run(testName, func(t *testing.T) {
			req := &storepb.SeriesRequest{
				MinTime: timestamp.FromTime(minTime),
				MaxTime: timestamp.FromTime(maxTime),
				Limit:   testData.limit,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "j", Value: "foo"},
				},
			}

			srv := newStoreSeriesServer(context.Background())
			err = store.Series(req, srv)
			testutil.Ok(t, err)
			testutil.Assert(t, len(srv.SeriesSet) == testData.expectedResults)
		})
	}

	labelNamesTests := map[string]struct {
		limit           int64
		expectedResults []string
	}{
		"label names without limit": {
			expectedResults: []string{"ext1", "i", "j", "n", "uniq"},
		},
		"label names with limit": {
			limit:           3,
			expectedResults: []string{"ext1", "i", "j"},
		},
	}

	for testName, testData := range labelNamesTests {
		t.Run(testName, func(t *testing.T) {
			req := &storepb.LabelNamesRequest{
				Start: timestamp.FromTime(minTime),
				End:   timestamp.FromTime(maxTime),
				Limit: testData.limit,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "j", Value: "foo"},
				},
			}

			resp, err := store.LabelNames(context.Background(), req)
			testutil.Ok(t, err)
			testutil.Equals(t, testData.expectedResults, resp.Names)
		})
	}

	labelValuesTests := map[string]struct {
		limit           int64
		expectedResults []string
	}{
		"label values without limit": {
			expectedResults: []string{"bar", "foo"},
		},
		"label values with limit": {
			limit:           1,
			expectedResults: []string{"bar"},
		},
	}

	for testName, testData := range labelValuesTests {
		t.Run(testName, func(t *testing.T) {
			req := &storepb.LabelValuesRequest{
				Start: timestamp.FromTime(minTime),
				End:   timestamp.FromTime(maxTime),
				Label: "j",
				Limit: testData.limit,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "j", Value: "(foo|bar)"},
				},
			}

			resp, err := store.LabelValues(context.Background(), req)
			testutil.Ok(t, err)
			testutil.Equals(t, testData.expectedResults, resp.Values)
		})
	}
}
