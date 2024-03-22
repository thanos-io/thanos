// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestFilterNilCompact(t *testing.T) {
	blocks := []*metadata.Meta{nil, nil}
	filtered := FilterRemovedBlocks(blocks)
	testutil.Equals(t, 0, len(filtered))

	meta := []*metadata.Meta{
		createBlockMeta(6, 1, int64(time.Now().Add(-6*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1"}, downsample.ResLevel0, []uint64{}),
		nil,
		createBlockMeta(7, 1, int64(time.Now().Add(-4*30*24*time.Hour).Unix()*1000), map[string]string{"b": "2"}, downsample.ResLevel1, []uint64{}),
		createBlockMeta(8, 1, int64(time.Now().Add(-7*30*24*time.Hour).Unix()*1000), map[string]string{"a": "1", "b": "2"}, downsample.ResLevel2, []uint64{}),
		nil,
	}
	testutil.Equals(t, 3, len(FilterRemovedBlocks(meta)))
}

func TestPreCompactionCallback(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()
	temp := promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_metric_for_group", Help: "this is a test metric for overlapping blocks"})
	group := &Group{
		logger:            log.NewNopLogger(),
		bkt:               bkt,
		overlappingBlocks: temp,
	}
	labels := map[string]string{"a": "1"}
	callback := NewOverlappingCompactionLifecycleCallback()
	for _, tcase := range []struct {
		testName                 string
		input                    []*metadata.Meta
		enableVerticalCompaction bool
		expectedSize             int
		expectedBlocks           []*metadata.Meta
		err                      error
	}{
		{
			testName: "empty blocks",
		},
		{
			testName: "no overlapping blocks",
			input: []*metadata.Meta{
				createBlockMeta(6, 1, 3, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(7, 3, 5, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(8, 5, 10, labels, downsample.ResLevel0, []uint64{}),
			},
			expectedSize: 3,
		},
		{
			testName: "duplicated blocks",
			input: []*metadata.Meta{
				createBlockMeta(6, 1, 7, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(7, 1, 7, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(8, 1, 7, labels, downsample.ResLevel0, []uint64{}),
			},
			expectedSize: 3,
		},
		{
			testName: "receive blocks",
			input: []*metadata.Meta{
				createReceiveBlockMeta(6, 1, 7, labels),
				createReceiveBlockMeta(7, 1, 7, labels),
				createReceiveBlockMeta(8, 1, 7, labels),
			},
			expectedSize: 3,
		},
		{
			testName: "receive + compactor blocks",
			input: []*metadata.Meta{
				createReceiveBlockMeta(6, 1, 7, labels),
				createBlockMeta(7, 2, 7, labels, downsample.ResLevel0, []uint64{}),
				createReceiveBlockMeta(8, 2, 8, labels),
			},
			expectedSize: 2,
			expectedBlocks: []*metadata.Meta{
				createReceiveBlockMeta(6, 1, 7, labels),
				createReceiveBlockMeta(8, 2, 8, labels),
			},
		},
		{
			testName: "full overlapping blocks",
			input: []*metadata.Meta{
				createBlockMeta(6, 1, 10, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(7, 3, 6, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(8, 5, 8, labels, downsample.ResLevel0, []uint64{}),
			},
			expectedSize: 1,
			expectedBlocks: []*metadata.Meta{
				createBlockMeta(6, 1, 10, labels, downsample.ResLevel0, []uint64{}),
			},
		},
		{
			testName: "part overlapping blocks",
			input: []*metadata.Meta{
				createBlockMeta(1, 1, 2, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(2, 1, 6, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(3, 6, 8, labels, downsample.ResLevel0, []uint64{}),
			},
			expectedSize: 2,
			expectedBlocks: []*metadata.Meta{
				createBlockMeta(2, 1, 6, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(3, 6, 8, labels, downsample.ResLevel0, []uint64{}),
			},
		},
		{
			testName: "out of order blocks",
			input: []*metadata.Meta{
				createBlockMeta(6, 2, 3, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(7, 0, 5, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(8, 5, 8, labels, downsample.ResLevel0, []uint64{}),
			},
			err: halt(errors.Errorf("expect halt error")),
		},
		{
			testName: "partially overlapping blocks with vertical compaction off",
			input: []*metadata.Meta{
				createBlockMeta(6, 2, 4, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(7, 3, 5, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(8, 5, 8, labels, downsample.ResLevel0, []uint64{}),
			},
			err: halt(errors.Errorf("expect halt error")),
		},
		{
			testName: "partially overlapping blocks with vertical compaction on",
			input: []*metadata.Meta{
				createBlockMeta(6, 2, 4, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(7, 3, 6, labels, downsample.ResLevel0, []uint64{}),
				createBlockMeta(8, 5, 8, labels, downsample.ResLevel0, []uint64{}),
			},
			enableVerticalCompaction: true,
			expectedSize:             3,
		},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			group.enableVerticalCompaction = tcase.enableVerticalCompaction
			err := callback.PreCompactionCallback(context.Background(), logger, group, tcase.input)
			if tcase.err != nil {
				testutil.NotOk(t, err)
				if IsHaltError(tcase.err) {
					testutil.Assert(t, IsHaltError(err), "expected halt error")
				} else if IsRetryError(tcase.err) {
					testutil.Assert(t, IsRetryError(err), "expected retry error")
				}
				return
			}
			testutil.Equals(t, tcase.expectedSize, len(FilterRemovedBlocks(tcase.input)))
			if tcase.expectedSize != len(tcase.input) {
				testutil.Equals(t, tcase.expectedBlocks, FilterRemovedBlocks(tcase.input))
			}
		}); !ok {
			return
		}
	}
}

func createReceiveBlockMeta(id uint64, minTime, maxTime int64, labels map[string]string) *metadata.Meta {
	m := createBlockMeta(id, minTime, maxTime, labels, downsample.ResLevel0, []uint64{})
	m.Thanos.Source = metadata.ReceiveSource
	return m
}
