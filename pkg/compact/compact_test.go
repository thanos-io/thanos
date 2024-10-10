// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
)

func TestHaltError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsHaltError(err), "halt error")

	err = halt(errors.New("test"))
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(halt(errors.New("test")), "something")
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(errors.Wrap(halt(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error")
}

func TestHaltMultiError(t *testing.T) {
	haltErr := halt(errors.New("halt error"))
	nonHaltErr := errors.New("not a halt error")

	errs := errutil.MultiError{nonHaltErr}
	testutil.Assert(t, !IsHaltError(errs.Err()), "should not be a halt error")

	errs.Add(haltErr)
	testutil.Assert(t, IsHaltError(errs.Err()), "if any halt errors are present this should return true")
	testutil.Assert(t, IsHaltError(errors.Wrap(errs.Err(), "wrap")), "halt error with wrap")

}

func TestRetryMultiError(t *testing.T) {
	retryErr := retry(errors.New("retry error"))
	nonRetryErr := errors.New("not a retry error")

	errs := errutil.MultiError{nonRetryErr}
	testutil.Assert(t, !IsRetryError(errs.Err()), "should not be a retry error")

	errs = errutil.MultiError{retryErr}
	testutil.Assert(t, IsRetryError(errs.Err()), "if all errors are retriable this should return true")

	testutil.Assert(t, IsRetryError(errors.Wrap(errs.Err(), "wrap")), "retry error with wrap")

	errs = errutil.MultiError{nonRetryErr, retryErr}
	testutil.Assert(t, !IsRetryError(errs.Err()), "mixed errors should return false")
}

func TestRetryError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsRetryError(err), "retry error")

	err = retry(errors.New("test"))
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.New("test")), "something")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(errors.Wrap(retry(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.Wrap(halt(errors.New("test")), "something")), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error. Retry should not hide halt error")
}

func TestGroupKey(t *testing.T) {
	for _, tcase := range []struct {
		input    metadata.Thanos
		expected string
	}{
		{
			input:    metadata.Thanos{},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{"foo": "bar", "foo1": "bar2"},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@2124638872457683483",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{`foo/some..thing/some.thing/../`: `a_b_c/bar-something-a\metric/a\x`},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@16590761456214576373",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			testutil.Equals(t, tcase.expected, tcase.input.GroupKey())
		}); !ok {
			return
		}
	}
}

func TestGroupMaxMinTime(t *testing.T) {
	g := &Group{
		metasByMinTime: []*metadata.Meta{
			{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 10}},
			{BlockMeta: tsdb.BlockMeta{MinTime: 1, MaxTime: 20}},
			{BlockMeta: tsdb.BlockMeta{MinTime: 2, MaxTime: 30}},
		},
	}

	testutil.Equals(t, int64(0), g.MinTime())
	testutil.Equals(t, int64(30), g.MaxTime())
}

func BenchmarkGatherNoCompactionMarkFilter_Filter(b *testing.B) {
	ctx := context.TODO()
	logger := log.NewLogfmtLogger(io.Discard)

	m := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})

	for blocksNum := 10; blocksNum <= 10000; blocksNum *= 10 {
		bkt := objstore.NewInMemBucket()

		metas := make(map[ulid.ULID]*metadata.Meta, blocksNum)

		for i := 0; i < blocksNum; i++ {
			var meta metadata.Meta
			meta.Version = 1
			meta.ULID = ulid.MustNew(uint64(i), nil)
			metas[meta.ULID] = &meta

			var buf bytes.Buffer
			testutil.Ok(b, json.NewEncoder(&buf).Encode(&meta))
			testutil.Ok(b, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
		}

		for i := 10; i <= 60; i += 10 {
			b.Run(fmt.Sprintf("Bench-%d-%d", blocksNum, i), func(b *testing.B) {
				b.ResetTimer()

				for n := 0; n <= b.N; n++ {
					slowBucket := objstore.WithNoopInstr(objstore.WithDelay(bkt, time.Millisecond*2))
					f := NewGatherNoCompactionMarkFilter(logger, slowBucket, i)
					testutil.Ok(b, f.Filter(ctx, metas, m, nil))
				}
			})
		}
	}

}

func createBlockMeta(id uint64, minTime, maxTime int64, labels map[string]string, resolution int64, sources []uint64) *metadata.Meta {
	sourceBlocks := make([]ulid.ULID, len(sources))
	for ind, source := range sources {
		sourceBlocks[ind] = ulid.MustNew(source, nil)
	}

	m := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustNew(id, nil),
			MinTime: minTime,
			MaxTime: maxTime,
			Compaction: tsdb.BlockMetaCompaction{
				Sources: sourceBlocks,
			},
		},
		Thanos: metadata.Thanos{
			Labels: labels,
			Downsample: metadata.ThanosDownsample{
				Resolution: resolution,
			},
		},
	}

	return m
}

func TestRetentionProgressCalculate(t *testing.T) {
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()

	var bkt objstore.Bucket
	temp := promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_metric_for_group", Help: "this is a test metric for compact progress tests"})
	grouper := NewDefaultGrouper(logger, bkt, false, false, reg, temp, temp, temp, "", 1, 1)

	type retInput struct {
		meta   []*metadata.Meta
		resMap map[ResolutionLevel]time.Duration
	}

	keys := make([]string, 3)
	m := make([]metadata.Meta, 3)
	m[0].Thanos.Labels = map[string]string{"a": "1"}
	m[0].Thanos.Downsample.Resolution = downsample.ResLevel0
	m[1].Thanos.Labels = map[string]string{"b": "2"}
	m[1].Thanos.Downsample.Resolution = downsample.ResLevel1
	m[2].Thanos.Labels = map[string]string{"a": "1", "b": "2"}
	m[2].Thanos.Downsample.Resolution = downsample.ResLevel2
	for ind, meta := range m {
		keys[ind] = meta.Thanos.GroupKey()
	}

	ps := NewRetentionProgressCalculator(reg, nil)

	for _, tcase := range []struct {
		testName string
		input    retInput
		expected float64
	}{
		{
			// In this test case, blocks belonging to multiple groups are tested. All blocks in the first group and the first block in the second group are beyond their retention period. In the second group, the second block still has some time before its retention period and hence, is not marked to be deleted.
			testName: "multi_group_test",
			input: retInput{
				meta: []*metadata.Meta{
					createBlockMeta(6, 1, int64(time.Now().Add(-6*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{}),
					createBlockMeta(9, 1, int64(time.Now().Add(-9*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{}),
					createBlockMeta(7, 1, int64(time.Now().Add(-4*30*24*time.Hour).Unix()*1000), map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{}),
					createBlockMeta(8, 1, int64(time.Now().Add(-1*30*24*time.Hour).Unix()*1000), map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{}),
					createBlockMeta(10, 1, int64(time.Now().Add(-4*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1", "b": "2"}, downsample.ResLevel2, []uint64{}),
				},
				resMap: map[ResolutionLevel]time.Duration{
					ResolutionLevel(downsample.ResLevel0): 5 * 30 * 24 * time.Hour, // 5 months retention.
					ResolutionLevel(downsample.ResLevel1): 3 * 30 * 24 * time.Hour, // 3 months retention.
					ResolutionLevel(downsample.ResLevel2): 6 * 30 * 24 * time.Hour, // 6 months retention.
				},
			},
			expected: 3.0,
		}, {
			// In this test case, all the blocks are retained since they have not yet crossed their retention period.
			testName: "retain_test",
			input: retInput{
				meta: []*metadata.Meta{
					createBlockMeta(6, 1, int64(time.Now().Add(-6*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{}),
					createBlockMeta(7, 1, int64(time.Now().Add(-4*30*24*time.Hour).Unix()*1000), map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{}),
					createBlockMeta(8, 1, int64(time.Now().Add(-7*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1", "b": "2"}, downsample.ResLevel2, []uint64{}),
				},
				resMap: map[ResolutionLevel]time.Duration{
					ResolutionLevel(downsample.ResLevel0): 10 * 30 * 24 * time.Hour, // 10 months retention.
					ResolutionLevel(downsample.ResLevel1): 12 * 30 * 24 * time.Hour, // 12 months retention.
					ResolutionLevel(downsample.ResLevel2): 16 * 30 * 24 * time.Hour, // 6 months retention.
				},
			},
			expected: 0.0,
		},
		{
			// In this test case, all the blocks are deleted since they are past their retention period.
			testName: "delete_test",
			input: retInput{
				meta: []*metadata.Meta{
					createBlockMeta(6, 1, int64(time.Now().Add(-6*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{}),
					createBlockMeta(7, 1, int64(time.Now().Add(-4*30*24*time.Hour).Unix()*1000), map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{}),
					createBlockMeta(8, 1, int64(time.Now().Add(-7*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1", "b": "2"}, downsample.ResLevel2, []uint64{}),
				},
				resMap: map[ResolutionLevel]time.Duration{
					ResolutionLevel(downsample.ResLevel0): 3 * 30 * 24 * time.Hour, // 3 months retention.
					ResolutionLevel(downsample.ResLevel1): 1 * 30 * 24 * time.Hour, // 1 months retention.
					ResolutionLevel(downsample.ResLevel2): 6 * 30 * 24 * time.Hour, // 6 months retention.
				},
			},
			expected: 3.0,
		},
		{
			// In this test case, none of the blocks are marked for deletion since the retention period is 0d i.e. indefinitely long retention.
			testName: "zero_day_test",
			input: retInput{
				meta: []*metadata.Meta{
					createBlockMeta(6, 1, int64(time.Now().Add(-6*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{}),
					createBlockMeta(7, 1, int64(time.Now().Add(-4*30*24*time.Hour).Unix()*1000), map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{}),
					createBlockMeta(8, 1, int64(time.Now().Add(-7*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1", "b": "2"}, downsample.ResLevel2, []uint64{}),
				},
				resMap: map[ResolutionLevel]time.Duration{
					ResolutionLevel(downsample.ResLevel0): 0,
					ResolutionLevel(downsample.ResLevel1): 0,
					ResolutionLevel(downsample.ResLevel2): 0,
				},
			},
			expected: 0.0,
		},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			blocks := make(map[ulid.ULID]*metadata.Meta, len(tcase.input.meta))
			for _, meta := range tcase.input.meta {
				blocks[meta.ULID] = meta
			}
			groups, err := grouper.Groups(blocks)
			testutil.Ok(t, err)
			ps.retentionByResolution = tcase.input.resMap
			err = ps.ProgressCalculate(context.Background(), groups)
			testutil.Ok(t, err)
			metrics := ps.RetentionProgressMetrics
			testutil.Ok(t, err)
			testutil.Equals(t, tcase.expected, promtestutil.ToFloat64(metrics.NumberOfBlocksToDelete))
		}); !ok {
			return
		}
	}
}

func TestCompactProgressCalculate(t *testing.T) {
	type planResult struct {
		compactionBlocks, compactionRuns float64
	}

	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	planner := NewTSDBBasedPlanner(logger, []int64{
		int64(1 * time.Hour / time.Millisecond),
		int64(2 * time.Hour / time.Millisecond),
		int64(4 * time.Hour / time.Millisecond),
		int64(8 * time.Hour / time.Millisecond),
	})

	keys := make([]string, 3)
	m := make([]metadata.Meta, 3)
	m[0].Thanos.Labels = map[string]string{"a": "1"}
	m[1].Thanos.Labels = map[string]string{"b": "2"}
	m[2].Thanos.Labels = map[string]string{"a": "1", "b": "2"}
	m[2].Thanos.Downsample.Resolution = 1
	for ind, meta := range m {
		keys[ind] = meta.Thanos.GroupKey()
	}

	ps := NewCompactionProgressCalculator(reg, planner)

	var bkt objstore.Bucket
	temp := promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_metric_for_group", Help: "this is a test metric for compact progress tests"})
	grouper := NewDefaultGrouper(logger, bkt, false, false, reg, temp, temp, temp, "", 1, 1)

	for _, tcase := range []struct {
		testName string
		input    []*metadata.Meta
		expected planResult
	}{
		{
			// This test has a single compaction run with two blocks from the second group compacted.
			testName: "single_run_test",
			input: []*metadata.Meta{
				createBlockMeta(0, 0, int64(time.Duration(2)*time.Hour/time.Millisecond), map[string]string{"a": "1"}, 0, []uint64{}),
				createBlockMeta(1, int64(time.Duration(2)*time.Hour/time.Millisecond), int64(time.Duration(4)*time.Hour/time.Millisecond), map[string]string{"a": "1"}, 0, []uint64{}),
				createBlockMeta(2, int64(time.Duration(4)*time.Hour/time.Millisecond), int64(time.Duration(6)*time.Hour/time.Millisecond), map[string]string{"b": "2"}, 0, []uint64{}),
				createBlockMeta(3, int64(time.Duration(6)*time.Hour/time.Millisecond), int64(time.Duration(8)*time.Hour/time.Millisecond), map[string]string{"b": "2"}, 0, []uint64{}),
				createBlockMeta(4, int64(time.Duration(8)*time.Hour/time.Millisecond), int64(time.Duration(10)*time.Hour/time.Millisecond), map[string]string{"b": "2"}, 0, []uint64{}),
				createBlockMeta(5, int64(time.Duration(10)*time.Hour/time.Millisecond), int64(time.Duration(12)*time.Hour/time.Millisecond), map[string]string{"a": "1", "b": "2"}, 1, []uint64{}),
				createBlockMeta(6, int64(time.Duration(12)*time.Hour/time.Millisecond), int64(time.Duration(20)*time.Hour/time.Millisecond), map[string]string{"a": "1", "b": "2"}, 1, []uint64{}),
				createBlockMeta(7, int64(time.Duration(20)*time.Hour/time.Millisecond), int64(time.Duration(28)*time.Hour/time.Millisecond), map[string]string{"a": "1", "b": "2"}, 1, []uint64{}),
			},
			expected: planResult{
				compactionRuns:   1.0,
				compactionBlocks: 2.0,
			},
		},
		{
			// This test has three compaction runs, with blocks from the first group getting compacted.
			testName: "three_runs_test",
			input: []*metadata.Meta{
				createBlockMeta(0, 0, int64(time.Duration(2)*time.Hour/time.Millisecond), map[string]string{"a": "1"}, 0, []uint64{}),
				createBlockMeta(3, int64(time.Duration(2)*time.Hour/time.Millisecond), int64(time.Duration(4)*time.Hour/time.Millisecond), map[string]string{"a": "1"}, 0, []uint64{}),
				createBlockMeta(4, int64(time.Duration(4)*time.Hour/time.Millisecond), int64(time.Duration(6)*time.Hour/time.Millisecond), map[string]string{"a": "1"}, 0, []uint64{}),
				createBlockMeta(5, int64(time.Duration(6)*time.Hour/time.Millisecond), int64(time.Duration(8)*time.Hour/time.Millisecond), map[string]string{"a": "1"}, 0, []uint64{}),
				createBlockMeta(6, int64(time.Duration(8)*time.Hour/time.Millisecond), int64(time.Duration(10)*time.Hour/time.Millisecond), map[string]string{"a": "1"}, 0, []uint64{}),
				createBlockMeta(1, int64(time.Duration(2)*time.Hour/time.Millisecond), int64(time.Duration(4)*time.Hour/time.Millisecond), map[string]string{"b": "2"}, 0, []uint64{}),
				createBlockMeta(2, int64(time.Duration(4)*time.Hour/time.Millisecond), int64(time.Duration(6)*time.Hour/time.Millisecond), map[string]string{"b": "2"}, 0, []uint64{}),
			},
			expected: planResult{
				compactionRuns:   3.0,
				compactionBlocks: 6.0,
			},
		},
		{
			// This test case has 4 2-hour blocks, which are non consecutive.
			// Hence, only the first two blocks are compacted.
			testName: "non_consecutive_blocks_test",
			input: []*metadata.Meta{
				createBlockMeta(1, int64(time.Duration(2)*time.Hour/time.Millisecond), int64(time.Duration(4)*time.Hour/time.Millisecond), map[string]string{"a": "1", "b": "2"}, 1, []uint64{}),
				createBlockMeta(2, int64(time.Duration(4)*time.Hour/time.Millisecond), int64(time.Duration(6)*time.Hour/time.Millisecond), map[string]string{"a": "1", "b": "2"}, 1, []uint64{}),
				createBlockMeta(3, int64(time.Duration(6)*time.Hour/time.Millisecond), int64(time.Duration(8)*time.Hour/time.Millisecond), map[string]string{"a": "1", "b": "2"}, 1, []uint64{}),
				createBlockMeta(4, int64(time.Duration(10)*time.Hour/time.Millisecond), int64(time.Duration(12)*time.Hour/time.Millisecond), map[string]string{"a": "1", "b": "2"}, 1, []uint64{}),
			},
			expected: planResult{
				compactionRuns:   1.0,
				compactionBlocks: 2.0,
			},
		},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			blocks := make(map[ulid.ULID]*metadata.Meta, len(tcase.input))
			for _, meta := range tcase.input {
				blocks[meta.ULID] = meta
			}
			groups, err := grouper.Groups(blocks)
			testutil.Ok(t, err)
			err = ps.ProgressCalculate(context.Background(), groups)
			testutil.Ok(t, err)
			metrics := ps.CompactProgressMetrics
			testutil.Ok(t, err)
			testutil.Equals(t, tcase.expected.compactionBlocks, promtestutil.ToFloat64(metrics.NumberOfCompactionBlocks))
			testutil.Equals(t, tcase.expected.compactionRuns, promtestutil.ToFloat64(metrics.NumberOfCompactionRuns))
		}); !ok {
			return
		}
	}
}

func TestDownsampleProgressCalculate(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	keys := make([]string, 3)
	m := make([]metadata.Meta, 3)
	m[0].Thanos.Labels = map[string]string{"a": "1"}
	m[0].Thanos.Downsample.Resolution = downsample.ResLevel0
	m[1].Thanos.Labels = map[string]string{"b": "2"}
	m[1].Thanos.Downsample.Resolution = downsample.ResLevel1
	m[2].Thanos.Labels = map[string]string{"a": "1", "b": "2"}
	m[2].Thanos.Downsample.Resolution = downsample.ResLevel2
	for ind, meta := range m {
		keys[ind] = meta.Thanos.GroupKey()
	}

	ds := NewDownsampleProgressCalculator(reg)

	var bkt objstore.Bucket
	temp := promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_metric_for_group", Help: "this is a test metric for downsample progress tests"})
	grouper := NewDefaultGrouper(logger, bkt, false, false, reg, temp, temp, temp, "", 1, 1)

	for _, tcase := range []struct {
		testName string
		input    []*metadata.Meta
		expected float64
	}{
		{
			// This test case has blocks from multiple groups and resolution levels. Only the blocks in the second group should be downsampled since the others either have time differences not in the range for their resolution, or a resolution which should not be downsampled.
			testName: "multi_group_test",
			input: []*metadata.Meta{
				createBlockMeta(6, 1, downsample.ResLevel1DownsampleRange, map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{7, 8}),
				createBlockMeta(7, 0, downsample.ResLevel2DownsampleRange, map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{8, 9}),
				createBlockMeta(9, 0, downsample.ResLevel2DownsampleRange, map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{8, 11}),
				createBlockMeta(8, 0, downsample.ResLevel2DownsampleRange, map[string]string{"a": "1", "b": "2"}, downsample.ResLevel2, []uint64{9, 10}),
			},
			expected: 2.0,
		}, {
			// This is a test case for resLevel0, with the correct time difference threshold.
			// This block should be downsampled.
			testName: "res_level0_test",
			input: []*metadata.Meta{
				createBlockMeta(9, 0, downsample.ResLevel1DownsampleRange, map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{10, 11}),
			},
			expected: 1.0,
		}, {
			// This is a test case for resLevel1, with the correct time difference threshold.
			// This block should be downsampled.
			testName: "res_level1_test",
			input: []*metadata.Meta{
				createBlockMeta(9, 0, downsample.ResLevel2DownsampleRange, map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{10, 11}),
			},
			expected: 1.0,
		},
		{
			// This is a test case for resLevel2.
			// Blocks with this resolution should not be downsampled.
			testName: "res_level2_test",
			input: []*metadata.Meta{
				createBlockMeta(10, 0, downsample.ResLevel2DownsampleRange, map[string]string{"a": "1", "b": "2"}, downsample.ResLevel2, []uint64{11, 12}),
			},
			expected: 0.0,
		}, {
			// This is a test case for resLevel0, with incorrect time difference, below the threshold.
			// This block should be downsampled.
			testName: "res_level0_test_incorrect",
			input: []*metadata.Meta{
				createBlockMeta(9, 1, downsample.ResLevel1DownsampleRange, map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{10, 11}),
			},
			expected: 0.0,
		},
		{
			// This is a test case for resLevel1, with incorrect time difference, below the threshold.
			// This block should be downsampled.
			testName: "res_level1_test",
			input: []*metadata.Meta{
				createBlockMeta(9, 1, downsample.ResLevel2DownsampleRange, map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{10, 11}),
			},
			expected: 0.0,
		},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			blocks := make(map[ulid.ULID]*metadata.Meta, len(tcase.input))
			for _, meta := range tcase.input {
				blocks[meta.ULID] = meta
			}
			groups, err := grouper.Groups(blocks)
			testutil.Ok(t, err)

			err = ds.ProgressCalculate(context.Background(), groups)
			testutil.Ok(t, err)
			metrics := ds.DownsampleProgressMetrics
			testutil.Equals(t, tcase.expected, promtestutil.ToFloat64(metrics.NumberOfBlocksDownsampled))
		}); !ok {
			return
		}
	}
}

func TestNoMarkFilterAtomic(t *testing.T) {
	ctx := context.TODO()
	logger := log.NewLogfmtLogger(io.Discard)

	m := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})

	blocksNum := 200
	bkt := objstore.NewInMemBucket()

	metas := make(map[ulid.ULID]*metadata.Meta, blocksNum)

	noMarkCounter := promauto.NewCounter(prometheus.CounterOpts{
		Name: "coolcounter",
	})

	for i := 0; i < blocksNum; i++ {
		var meta metadata.Meta
		meta.Version = 1
		meta.ULID = ulid.MustNew(uint64(i), nil)
		metas[meta.ULID] = &meta

		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
		if i%2 == 0 {
			testutil.Ok(
				t,
				block.MarkForNoCompact(ctx, logger, bkt, meta.ULID, metadata.NoCompactReason("test"), "nodetails", noMarkCounter),
			)
		}
	}

	slowBucket := objstore.WithNoopInstr(objstore.WithDelay(bkt, time.Millisecond*200))
	f := NewGatherNoCompactionMarkFilter(logger, slowBucket, 10)

	ctx, cancel := context.WithCancel(ctx)

	g := &run.Group{}

	// Fill the map initially.
	testutil.Ok(t, f.Filter(ctx, metas, m, nil))
	testutil.Assert(t, len(f.NoCompactMarkedBlocks()) > 0, "expected to always have not compacted blocks")

	g.Add(func() error {
		for {
			if ctx.Err() != nil {
				return nil
			}
			if err := f.Filter(ctx, metas, m, nil); err != nil && !errors.Is(err, context.Canceled) {
				testutil.Ok(t, err)
			}
		}
	}, func(err error) {
		cancel()
	})

	g.Add(func() error {
		for {
			if ctx.Err() != nil {
				return nil
			}

			if len(f.NoCompactMarkedBlocks()) == 0 {
				return fmt.Errorf("expected to always have not compacted blocks")
			}
		}
	}, func(err error) {
		cancel()
	})

	time.AfterFunc(10*time.Second, func() {
		cancel()
	})
	testutil.Ok(t, g.Run())
}
