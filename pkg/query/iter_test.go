// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"fmt"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestRemoveExactDuplicates(t *testing.T) {
	for _, tc := range []struct {
		name string
		chks []storepb.AggrChunk
	}{
		{name: "empty slice", chks: []storepb.AggrChunk{}},
		{name: "single element slice", chks: aggrChunkForMinTimes(0)},
		{name: "slice with duplicates", chks: aggrChunkForMinTimes(0, 0, 2, 30, 31, 31, 40)},
		{name: "slice without duplicates", chks: aggrChunkForMinTimes(0, 1, 2, 3, 5, 7, 8)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalChks := make([]storepb.AggrChunk, len(tc.chks))
			copy(originalChks, tc.chks)
			chks := removeExactDuplicates(tc.chks)
			missingChunk := isChunksSubset(originalChks, chks)
			testutil.Assert(t, missingChunk == nil, fmt.Sprintf("chunk %q missing in output", missingChunk.String()))
			unexpectedChunk := isChunksSubset(chks, originalChks)
			testutil.Assert(t, unexpectedChunk == nil, fmt.Sprintf("unexpected chunk %q does not appear in the input", unexpectedChunk.String()))

			if len(chks) > 0 {
				chk1 := chks[0]
				for _, chk2 := range chks[1:] {
					testutil.Assert(t, chk2.Compare(chk1) != 0, fmt.Sprintf("chunk %q appears twice in output", chk1.String()))
					chk1 = chk2
				}
			}
		})
	}
}

func aggrChunkForMinTimes(minTimes ...int64) []storepb.AggrChunk {
	chks := make([]storepb.AggrChunk, len(minTimes))
	for i, minTime := range minTimes {
		chks[i] = storepb.AggrChunk{MinTime: minTime}
	}
	return chks
}

// isChunksSubset returns nil if all chunks in chks1 also appear in chks2,
// otherwise returns a chunk in chks1 that does not apper in chks2.
func isChunksSubset(chks1, chks2 []storepb.AggrChunk) *storepb.AggrChunk {
	for _, chk1 := range chks1 {
		found := false
		for _, chk2 := range chks2 {
			if chk2.Compare(chk1) == 0 {
				found = true
				break
			}
		}
		if !found {
			return &chk1
		}
	}
	return nil
}
