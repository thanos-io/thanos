// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/testutil"
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
			testutil.Equals(t, tcase.expected, DefaultGroupKey(tcase.input))
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
	logger := log.NewLogfmtLogger(ioutil.Discard)

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
					testutil.Ok(b, f.Filter(ctx, metas, m))
				}
			})
		}
	}

}

func TestCompactProgressCalculate(t *testing.T) {
	type planResult struct {
		compactionBlocks, compactionRuns float64
	}
	type groupedResult map[string]planResult

	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	unRegisterer := &receive.UnRegisterer{Registerer: reg}
	planner := NewTSDBBasedPlanner(logger, []int64{
		int64(1 * time.Hour / time.Millisecond),
		int64(2 * time.Hour / time.Millisecond),
		int64(4 * time.Hour / time.Millisecond),
		int64(8 * time.Hour / time.Millisecond),
	})

	// pre calculating group keys
	keys := make([]string, 3)
	m := make([]metadata.Meta, 3)
	m[0].Thanos.Labels = map[string]string{"a": "1"}
	m[1].Thanos.Labels = map[string]string{"b": "2"}
	m[2].Thanos.Labels = map[string]string{"a": "1", "b": "2"}
	m[2].Thanos.Downsample.Resolution = 1
	for ind, meta := range m {
		keys[ind] = DefaultGroupKey(meta.Thanos)
	}

	var bkt objstore.Bucket
	temp := prometheus.NewCounter(prometheus.CounterOpts{})
	grouper := NewDefaultGrouper(logger, bkt, false, false, reg, temp, temp, temp, "")

	for _, tcase := range []struct {
		testName string
		input    []*metadata.Meta
		expected groupedResult
	}{
		{
			testName: "first_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(0, nil),
						MinTime: 0,
						MaxTime: int64(2 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(1, nil),
						MinTime: int64(2 * time.Hour / time.Millisecond),
						MaxTime: int64(4 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(2, nil),
						MinTime: int64(4 * time.Hour / time.Millisecond),
						MaxTime: int64(6 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(3, nil),
						MinTime: int64(6 * time.Hour / time.Millisecond),
						MaxTime: int64(8 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(4, nil),
						MinTime: int64(8 * time.Hour / time.Millisecond),
						MaxTime: int64(10 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(5, nil),
						MinTime: int64(10 * time.Hour / time.Millisecond),
						MaxTime: int64(12 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(6, nil),
						MinTime: int64(12 * time.Hour / time.Millisecond),
						MaxTime: int64(20 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(7, nil),
						MinTime: int64(20 * time.Hour / time.Millisecond),
						MaxTime: int64(28 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
			},
			expected: map[string]planResult{
				keys[0]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
				keys[1]: planResult{
					compactionRuns:   1.0,
					compactionBlocks: 2.0,
				},
				keys[2]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
			},
		},
		{
			testName: "second_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(0, nil),
						MinTime: 0,
						MaxTime: int64(2 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(1, nil),
						MinTime: int64(2 * time.Hour / time.Millisecond),
						MaxTime: int64(4 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(2, nil),
						MinTime: int64(4 * time.Hour / time.Millisecond),
						MaxTime: int64(6 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(3, nil),
						MinTime: int64(6 * time.Hour / time.Millisecond),
						MaxTime: int64(10 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(4, nil),
						MinTime: int64(10 * time.Hour / time.Millisecond),
						MaxTime: int64(14 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(5, nil),
						MinTime: int64(14 * time.Hour / time.Millisecond),
						MaxTime: int64(16 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
			},
			expected: map[string]planResult{
				keys[0]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
				keys[1]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
				keys[2]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
			},
		},
		{
			testName: "third_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(0, nil),
						MinTime: 0,
						MaxTime: int64(2 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(1, nil),
						MinTime: int64(2 * time.Hour / time.Millisecond),
						MaxTime: int64(4 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(2, nil),
						MinTime: int64(4 * time.Hour / time.Millisecond),
						MaxTime: int64(6 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(3, nil),
						MinTime: int64(6 * time.Hour / time.Millisecond),
						MaxTime: int64(8 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(4, nil),
						MinTime: int64(8 * time.Hour / time.Millisecond),
						MaxTime: int64(10 * time.Hour / time.Millisecond),
					},
					Thanos: metadata.Thanos{
						Version:    1,
						Labels:     map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{Resolution: 1},
					},
				},
			},
			expected: map[string]planResult{
				keys[0]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
				keys[1]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
				keys[2]: planResult{
					compactionRuns:   0.0,
					compactionBlocks: 0.0,
				},
			},
		},
		{},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			groups := make([]*Group, 3)

			blocks := make(map[ulid.ULID]*metadata.Meta, len(tcase.input))
			for _, meta := range tcase.input {
				blocks[meta.ULID] = meta
			}
			// form groups from the input metadata - do not hardcode groups. hence, grouper.Groups should stay
			groups, _ = grouper.Groups(blocks)
			ps := NewCompactionProgressCalculator(unRegisterer, planner)
			err := ps.ProgressCalculate(context.Background(), groups)
			metrics := ps.CompactProgressMetrics
			testutil.Ok(t, err)
			for _, key := range keys {
				a, err := metrics.NumberOfCompactionBlocks.GetMetricWithLabelValues(key)
				if err != nil {
					level.Warn(logger).Log("msg", "could not get number of blocks")
				}
				b, err := metrics.NumberOfCompactionRuns.GetMetricWithLabelValues(key)
				if err != nil {
					level.Warn(logger).Log("msg", "could not get number of runs")
				}

				testutil.Equals(t, tcase.expected[key].compactionBlocks, promtestutil.ToFloat64(a))
				testutil.Equals(t, tcase.expected[key].compactionRuns, promtestutil.ToFloat64(b))
			}
		}); !ok {
			return
		}
	}
}

func TestDownsampleProgressCalculate(t *testing.T) {
	reg := prometheus.NewRegistry()
	unRegisterer := &receive.UnRegisterer{Registerer: reg}
	logger := log.NewNopLogger()
	type groupedResult map[string]float64

	// pre calculating group keys
	keys := make([]string, 3)
	m := make([]metadata.Meta, 3)
	m[0].Thanos.Labels = map[string]string{"a": "1"}
	m[0].Thanos.Downsample.Resolution = downsample.ResLevel0
	m[1].Thanos.Labels = map[string]string{"b": "2"}
	m[1].Thanos.Downsample.Resolution = downsample.ResLevel1
	m[2].Thanos.Labels = map[string]string{"a": "1", "b": "2"}
	m[2].Thanos.Downsample.Resolution = downsample.ResLevel2
	for ind, meta := range m {
		keys[ind] = DefaultGroupKey(meta.Thanos)
	}

	var bkt objstore.Bucket
	temp := prometheus.NewCounter(prometheus.CounterOpts{})
	grouper := NewDefaultGrouper(logger, bkt, false, false, reg, temp, temp, temp, "")

	for _, tcase := range []struct {
		testName string
		input    []*metadata.Meta
		expected groupedResult
	}{
		{
			testName: "first_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(0, nil),
						MinTime: 0,
						MaxTime: downsample.DownsampleRange0,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(1, nil), ulid.MustNew(2, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel0,
						},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(3, nil),
						MinTime: 1,
						MaxTime: downsample.DownsampleRange1,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(4, nil), ulid.MustNew(5, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel1,
						},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(4, nil),
						MinTime: 1,
						MaxTime: downsample.DownsampleRange1,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(4, nil), ulid.MustNew(5, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel2,
						},
					},
				},
			},
			expected: map[string]float64{
				keys[0]: 1.0,
				keys[1]: 0.0,
				keys[2]: 0.0,
			},
		},
		{
			testName: "second_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(6, nil),
						MinTime: 1,
						MaxTime: downsample.DownsampleRange0,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(7, nil), ulid.MustNew(8, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel0,
						},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(7, nil),
						MinTime: 1,
						MaxTime: downsample.DownsampleRange1,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(7, nil), ulid.MustNew(8, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel1,
						},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(8, nil),
						MinTime: 1,
						MaxTime: downsample.DownsampleRange1,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(7, nil), ulid.MustNew(8, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1", "b": "2"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel2,
						},
					},
				},
			},
			expected: map[string]float64{
				keys[0]: 0.0,
				keys[1]: 0.0,
				keys[2]: 0.0,
			},
		}, {
			testName: "third_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(9, nil),
						MinTime: 0,
						MaxTime: downsample.DownsampleRange0,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(10, nil), ulid.MustNew(11, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel0,
						},
					},
				},
			},
			expected: map[string]float64{
				keys[0]: 1.0,
				keys[1]: 0.0,
				keys[2]: 0.0,
			},
		}, {
			testName: "fourth_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(10, nil),
						MinTime: 0,
						MaxTime: downsample.DownsampleRange1,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(11, nil), ulid.MustNew(12, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel1,
						},
					},
				},
			},
			expected: map[string]float64{
				keys[0]: 0.0,
				keys[1]: 0.0,
				keys[2]: 0.0,
			},
		},
		{
			testName: "fifth_test",
			input: []*metadata.Meta{
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(0, nil),
						MinTime: 0,
						MaxTime: downsample.DownsampleRange1,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(1, nil), ulid.MustNew(2, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"a": "1"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel1,
						},
					},
				},
				{
					BlockMeta: tsdb.BlockMeta{
						ULID:    ulid.MustNew(3, nil),
						MinTime: 0,
						MaxTime: downsample.DownsampleRange0,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: []ulid.ULID{ulid.MustNew(4, nil), ulid.MustNew(5, nil)},
						},
					},
					Thanos: metadata.Thanos{
						Version: 1,
						Labels:  map[string]string{"b": "2"},
						Downsample: metadata.ThanosDownsample{
							Resolution: downsample.ResLevel0,
						},
					},
				},
			},
			expected: map[string]float64{
				keys[0]: 0.0,
				keys[1]: 0.0,
				keys[2]: 0.0,
			},
		},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			groups := make([]*Group, 3)

			blocks := make(map[ulid.ULID]*metadata.Meta, len(tcase.input))
			for _, meta := range tcase.input {
				blocks[meta.ULID] = meta
			}
			groups, _ = grouper.Groups(blocks)

			ds := NewDownsampleProgressCalculator(unRegisterer)
			err := ds.ProgressCalculate(context.Background(), groups)
			testutil.Ok(t, err)
			metrics := ds.DownsampleProgressMetrics
			for _, key := range keys {
				a, err := metrics.NumberOfBlocksDownsampled.GetMetricWithLabelValues(key)
				if err != nil {
					level.Warn(logger).Log("msg", "could not get number of blocks")
				}
				testutil.Equals(t, tcase.expected[key], promtestutil.ToFloat64(a))
			}
		}); !ok {
			return
		}
	}
}
