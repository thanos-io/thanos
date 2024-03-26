// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func TestPreCompactionCallback(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()
	callback := NewOverlappingCompactionLifecycleCallback(reg, true)
	for _, tcase := range []struct {
		testName      string
		input         []*metadata.Meta
		expectedMarks map[int]int
		expectedErr   error
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
		},
		{
			testName: "duplicated blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 1, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 1, 7, metadata.CompactorSource, 1),
			},
			expectedMarks: map[int]int{7: 2, 8: 2},
		},
		{
			testName: "overlap non dup blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 1, 7, metadata.CompactorSource, 2),
				createCustomBlockMeta(8, 1, 7, metadata.CompactorSource, 2),
			},
			expectedMarks: map[int]int{8: 2},
		},
		{
			testName: "receive blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.ReceiveSource, 1),
				createCustomBlockMeta(7, 1, 7, metadata.ReceiveSource, 2),
				createCustomBlockMeta(8, 1, 7, metadata.ReceiveSource, 3),
			},
		},
		{
			testName: "receive + compactor blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 7, metadata.ReceiveSource, 1),
				createCustomBlockMeta(7, 2, 7, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 2, 8, metadata.ReceiveSource, 1),
			},
			expectedMarks: map[int]int{7: 2},
		},
		{
			testName: "full overlapping blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 1, 10, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 3, 6, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 5, 8, metadata.CompactorSource, 1),
			},
			expectedMarks: map[int]int{7: 2, 8: 2},
		},
		{
			testName: "part overlapping blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(1, 1, 2, metadata.CompactorSource, 1),
				createCustomBlockMeta(2, 1, 6, metadata.CompactorSource, 1),
				createCustomBlockMeta(3, 6, 8, metadata.CompactorSource, 1),
			},
			expectedMarks: map[int]int{1: 2},
		},
		{
			testName: "out of order blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 2, 3, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 0, 5, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 5, 8, metadata.CompactorSource, 1),
			},
			expectedErr: halt(errors.Errorf("some errors")),
		},
		{
			testName: "partially overlapping blocks",
			input: []*metadata.Meta{
				createCustomBlockMeta(6, 2, 4, metadata.CompactorSource, 1),
				createCustomBlockMeta(7, 3, 6, metadata.CompactorSource, 1),
				createCustomBlockMeta(8, 5, 8, metadata.CompactorSource, 1),
			},
			expectedMarks: map[int]int{6: 1, 7: 1},
		},
	} {
		if ok := t.Run(tcase.testName, func(t *testing.T) {
			ctx := context.Background()
			bkt := objstore.NewInMemBucket()
			group := &Group{logger: log.NewNopLogger(), bkt: bkt}
			err := callback.PreCompactionCallback(ctx, logger, group, tcase.input)
			if tcase.expectedErr != nil || len(tcase.expectedMarks) != 0 {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
			}
			objs := bkt.Objects()
			expectedSize := 0
			for id, file := range tcase.expectedMarks {
				expectedSize += file
				_, noCompaction := objs[getFile(id, metadata.NoCompactMarkFilename)]
				_, noDownsampling := objs[getFile(id, metadata.NoDownsampleMarkFilename)]
				if file <= 2 {
					testutil.Assert(t, noCompaction, fmt.Sprintf("expect %d has no compaction", id))
				}
				if file == 2 {
					testutil.Assert(t, noDownsampling, fmt.Sprintf("expect %d has no downsampling", id))
				}
			}
			testutil.Equals(t, expectedSize, len(objs))
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

func getFile(id int, mark string) string {
	return path.Join(fmt.Sprintf("%010d", id)+fmt.Sprintf("%016d", 0), mark)
}
