// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

func TestSeriesByteAlignedPostings_NextAndAt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		align    bool
		input    index.Postings
		expected []storage.SeriesRef
	}{
		{
			name:  "empty",
			input: index.EmptyPostings(),
		},
		{
			name:     "align",
			align:    true,
			input:    index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}),
			expected: []storage.SeriesRef{16, 32, 48, 64, 80},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := newSeriesByteAlignedPostings(tc.input)
			var refs []storage.SeriesRef
			for output.Next() {
				refs = append(refs, output.At())
			}
			testutil.Equals(t, tc.expected, refs)
		})
	}
}

func TestSeriesByteAlignedPostings_Seek(t *testing.T) {
	alignedPosting := newSeriesByteAlignedPostings(index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}))
	testutil.Equals(t, true, alignedPosting.Seek(40))
	// 40 / 16 is round to 2 so aligned posting at 40 instead of 48.
	// This behavior is different from ListPostings but we don't expect
	// Seek to be called with a number that cannot be multiplied by 16
	// so it should be ok.
	testutil.Equals(t, storage.SeriesRef(32), alignedPosting.At())
	testutil.Equals(t, true, alignedPosting.Seek(48))
	testutil.Equals(t, storage.SeriesRef(48), alignedPosting.At())
	testutil.Equals(t, true, alignedPosting.Seek(80))
	testutil.Equals(t, storage.SeriesRef(80), alignedPosting.At())
	// 90 / 16 is round to 5.
	testutil.Equals(t, true, alignedPosting.Seek(90))
	// 100 / 16 is round to 6.
	testutil.Equals(t, false, alignedPosting.Seek(100))
}
