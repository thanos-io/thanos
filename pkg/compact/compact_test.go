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
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
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

func TestPlanSimulate(t *testing.T) {
	logger := log.NewNopLogger()
	planner := NewTSDBBasedPlanner(logger, []int64{
		int64(1 * time.Hour / time.Millisecond),
		int64(2 * time.Hour / time.Millisecond),
		int64(8 * time.Hour / time.Millisecond),
		int64(2 * 24 * time.Hour / time.Millisecond),
	})
	reg := prometheus.NewRegistry()
	ps := NewCompactionSimulator(reg, planner)

	metas := []*metadata.Meta{
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
				Labels:  map[string]string{"a": "1"},
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
				Labels:  map[string]string{"a": "1"},
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
				Labels:  map[string]string{"a": "1"},
			},
		},
		{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(5, nil),
				MinTime: int64(10 * time.Hour / time.Millisecond),
				MaxTime: int64(12 * time.Hour / time.Millisecond),
			},
			Thanos: metadata.Thanos{
				Version: 1,
				Labels:  map[string]string{"a": "1"},
			},
		},
		{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(6, nil),
				MinTime: int64(12 * time.Hour / time.Millisecond),
				MaxTime: int64(20 * time.Hour / time.Millisecond),
			},
			Thanos: metadata.Thanos{
				Version: 1,
				Labels:  map[string]string{"a": "1"},
			},
		},
		{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(7, nil),
				MinTime: int64(20 * time.Hour / time.Millisecond),
				MaxTime: int64(28 * time.Hour / time.Millisecond),
			},
			Thanos: metadata.Thanos{
				Version: 1,
				Labels:  map[string]string{"a": "1"},
			},
		},
	}

	extLabels := labels.FromMap(map[string]string{"a": "1"})
	groups := []*Group{
		{
			labels:         extLabels,
			resolution:     0,
			metasByMinTime: metas,
		},
	}

	err := ps.ProgressCalculate(context.Background(), groups)
	testutil.Ok(t, err)
	metrics := ps.ProgressMetrics
	testutil.Equals(t, 2.0, promtestutil.ToFloat64(metrics.NumberOfCompactionRuns))
	testutil.Equals(t, 6.0, promtestutil.ToFloat64(metrics.NumberOfCompactionBlocks))
}
