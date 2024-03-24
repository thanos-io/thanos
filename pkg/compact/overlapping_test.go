// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestFilterNilCompact(t *testing.T) {
	blocks := []*metadata.Meta{nil, nil}
	filtered := FilterRemovedBlocks(blocks)
	testutil.Equals(t, 0, len(filtered))

	meta := []*metadata.Meta{
		createCustomBlockMeta(6, 1, 3, metadata.CompactorSource, 1),
		nil,
		createCustomBlockMeta(7, 3, 5, metadata.CompactorSource, 2),
		createCustomBlockMeta(8, 5, 10, metadata.CompactorSource, 3),
		nil,
	}
	testutil.Equals(t, 3, len(FilterRemovedBlocks(meta)))
}

func TestPreCompactionCallback(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()
	group := &Group{
		logger: log.NewNopLogger(),
		bkt:    bkt,
	}
	callback := NewOverlappingCompactionLifecycleCallback(reg, true)
	for _, tcase := range []struct {
		testName       string
		input          []*metadata.Meta
		expectedSize   int
		expectedBlocks []*metadata.Meta
		err            error
	}{
		{
			testName: "empty blocks",
		},
		{
			testName: "no overlapping blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 3, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 3, 5, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 5, 10, metadata.CompactorSource, 1),
			},
			expectedSize: 3,
		},
		{
			testName: "duplicated blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 1, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 1, 7, metadata.CompactorSource, 1),
			},
			expectedSize: 1,
			expectedBlocks: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.CompactorSource, 1),
			},
		},
		{
			testName: "overlap non dup blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 1, 7, metadata.CompactorSource, 2),
				createCustomBlockMeta(8, 1, 7, metadata.CompactorSource, 2),
			},
			expectedSize: 2,
			expectedBlocks: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 1, 7, metadata.CompactorSource, 2),
			},
		},
		{
			testName: "receive blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.ReceiveSource, 1),
				createCustomBlockMeta(7, 1, 7, metadata.ReceiveSource, 2),
				createCustomBlockMeta(8, 1, 7, metadata.ReceiveSource, 3),
			},
			expectedSize: 3,
		},
		{
			testName: "receive + compactor blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.ReceiveSource, 1),
				createCustomBlockMeta(7, 2, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 2, 8, metadata.ReceiveSource, 1),
			},
			expectedSize: 2,
			expectedBlocks: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.ReceiveSource, 1),
				createCustomBlockMeta(8, 2, 8, metadata.ReceiveSource, 1),
			},
		},
		{
			testName: "full overlapping blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 10, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 3, 6, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 5, 8, metadata.CompactorSource, 1),
			},
			expectedSize: 1,
			expectedBlocks: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 10, metadata.CompactorSource, 1),
			},
		},
		{
			testName: "part overlapping blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(1, 1, 2, metadata.CompactorSource, 1),
				createCustomBlockMeta(2, 1, 6, metadata.CompactorSource, 1),
				createCustomBlockMeta(3, 6, 8, metadata.CompactorSource, 1),
			},
			expectedSize: 2,
			expectedBlocks: []*metadata.Meta{
				createCustomBlockMeta(2, 1, 6, metadata.CompactorSource, 1),
				createCustomBlockMeta(3, 6, 8, metadata.CompactorSource, 1),
			},
		},
		{
			testName: "out of order blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 2, 3, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 0, 5, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 5, 8, metadata.CompactorSource, 1),
			},
			err: halt(errors.Errorf("later blocks has smaller minTime than previous block: %s -- %s",
				createCustomBlockMeta(6, 2, 3, metadata.CompactorSource, 1).String(),
				createCustomBlockMeta(7, 0, 5, metadata.CompactorSource, 1).String(),
			)),
		},
		{
			testName: "partially overlapping blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 2, 4, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 3, 6, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 5, 8, metadata.CompactorSource, 1),
			},
			err: retry(errors.Errorf("found partially overlapping block: %s -- %s",
				createCustomBlockMeta(6, 2, 4, metadata.CompactorSource, 1).String(),
				createCustomBlockMeta(7, 3, 6, metadata.CompactorSource, 1).String(),
			)),
		},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			err := callback.PreCompactionCallback(context.Background(), logger, group, tcase.input)
			if tcase.err != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.err.Error(), err.Error())
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

func createCustomBlockMeta(id uint64, minTime, maxTime int64, source metadata.SourceType, numSeries uint64) *metadata.Meta {
	labels := map[string]string{"a": "1"}
	m := createBlockMeta(id, minTime, maxTime, labels, downsample.ResLevel0, []uint64{})
	m.Thanos.Source = source
	m.Stats.NumSeries = numSeries
	return m
}
