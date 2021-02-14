// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
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

func TestConcurrentGroups(t *testing.T) {
	labels1 := metadata.Thanos{Labels: map[string]string{"foo": "bar"}, Downsample: metadata.ThanosDownsample{Resolution: 0}}
	labels2 := metadata.Thanos{Labels: map[string]string{"foo2": "bar2"}, Downsample: metadata.ThanosDownsample{Resolution: 0}}
	metas := map[ulid.ULID]*metadata.Meta{
		// Metas for first tenant.
		ULID(1): {BlockMeta: tsdb.BlockMeta{ULID: ULID(1), MinTime: 0, MaxTime: 1000}, Thanos: labels1},
		ULID(2): {BlockMeta: tsdb.BlockMeta{ULID: ULID(2), MinTime: 0, MaxTime: 1000}, Thanos: labels1},
		ULID(3): {BlockMeta: tsdb.BlockMeta{ULID: ULID(3), MinTime: 0, MaxTime: 1000}, Thanos: labels1},
		ULID(4): {BlockMeta: tsdb.BlockMeta{ULID: ULID(4), MinTime: 1000, MaxTime: 2000}, Thanos: labels1},
		ULID(5): {BlockMeta: tsdb.BlockMeta{ULID: ULID(5), MinTime: 1000, MaxTime: 2000}, Thanos: labels1},
		ULID(6): {BlockMeta: tsdb.BlockMeta{ULID: ULID(6), MinTime: 1000, MaxTime: 2000}, Thanos: labels1},
		ULID(7): {BlockMeta: tsdb.BlockMeta{ULID: ULID(7), MinTime: 2000, MaxTime: 3000}, Thanos: labels1},
		ULID(8): {BlockMeta: tsdb.BlockMeta{ULID: ULID(8), MinTime: 2000, MaxTime: 3000}, Thanos: labels1},
		ULID(9): {BlockMeta: tsdb.BlockMeta{ULID: ULID(9), MinTime: 2000, MaxTime: 3000}, Thanos: labels1},
		// Metas for second tenant.
		ULID(10): {BlockMeta: tsdb.BlockMeta{ULID: ULID(10), MinTime: 0, MaxTime: 1000}, Thanos: labels2},
		ULID(11): {BlockMeta: tsdb.BlockMeta{ULID: ULID(11), MinTime: 1000, MaxTime: 2000}, Thanos: labels2},
		ULID(12): {BlockMeta: tsdb.BlockMeta{ULID: ULID(12), MinTime: 2000, MaxTime: 3000}, Thanos: labels2},
		ULID(13): {BlockMeta: tsdb.BlockMeta{ULID: ULID(13), MinTime: 3000, MaxTime: 4000}, Thanos: labels2},
		ULID(14): {BlockMeta: tsdb.BlockMeta{ULID: ULID(14), MinTime: 3000, MaxTime: 4000}, Thanos: labels2},
		ULID(15): {BlockMeta: tsdb.BlockMeta{ULID: ULID(15), MinTime: 3000, MaxTime: 4000}, Thanos: labels2},
		ULID(16): {BlockMeta: tsdb.BlockMeta{ULID: ULID(16), MinTime: 4000, MaxTime: 5000}, Thanos: labels2},
	}
	logger := log.NewNopLogger()
	blocksMarkedForDeletion := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	garbageCollectedBlocks := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	planner := NewTSDBBasedPlanner(logger, []int64{1000, 3000})
	bkt := objstore.NewInMemBucket()
	grouper := NewDefaultGrouper(logger, bkt, false, true, true, prometheus.NewRegistry(), blocksMarkedForDeletion, garbageCollectedBlocks, metadata.NoneFunc, planner)
	groups, err := grouper.Groups(metas)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, len(groups))
	testutil.Equals(t, ULIDs(13, 14, 15), groups[0].IDs())
	testutil.Equals(t, ULIDs(10, 11, 12), groups[1].IDs())
	testutil.Equals(t, ULIDs(1, 2, 3), groups[2].IDs())
	testutil.Equals(t, ULIDs(4, 5, 6), groups[3].IDs())
	testutil.Equals(t, ULIDs(7, 8, 9), groups[4].IDs())
}

func ULID(i int) ulid.ULID { return ulid.MustNew(uint64(i), nil) }

func ULIDs(is ...int) []ulid.ULID {
	ret := []ulid.ULID{}
	for _, i := range is {
		ret = append(ret, ulid.MustNew(uint64(i), nil))
	}

	return ret
}
