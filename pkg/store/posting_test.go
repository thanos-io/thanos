package store

import (
	"context"
	"testing"

	"sync"

	"sort"

	"fmt"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/index"
)

func Test_PreloadPostings(t *testing.T) {
	maxPostingGap := int(1024 * 512)
	for _, tcase := range []struct {
		ranges         [][]int
		expectedRanges [][]int
	}{
		{
			ranges:         [][]int{{1, 10}},
			expectedRanges: [][]int{{1, 10}},
		},
		{
			ranges:         [][]int{{1, 2}, {3, 5}, {7, 10}},
			expectedRanges: [][]int{{1, 10}},
		},
		{
			ranges:         [][]int{{1, 2}, {3, 5}, {20, 30}, {maxPostingGap + 30, maxPostingGap + 32}},
			expectedRanges: [][]int{{1, 30}, {maxPostingGap + 30, maxPostingGap + 32}},
		},
		// Overlapping ranges.
		{
			ranges:         [][]int{{1, 30}, {3, 28}, {1, 4}, {maxPostingGap + 31, maxPostingGap + 32}, {maxPostingGap + 31, maxPostingGap + 40}},
			expectedRanges: [][]int{{1, 30}, {maxPostingGap + 31, maxPostingGap + 40}},
		},
	} {
		var p []*lazyPostings
		for _, r := range tcase.ranges {
			p = append(p, &lazyPostings{ptr: index.Range{
				Start: int64(r[0]),
				End:   int64(r[1]),
			}})
		}

		sort.Slice(p, func(i, j int) bool {
			// Invert order of the postings to make sure preload handles that.
			return p[i].ptr.End > p[j].ptr.End
		})

		var foundRanges [][]int
		var foundPostings [][]*lazyPostings
		var mu sync.Mutex
		testutil.Ok(t, preloadPostings(context.Background(), p, func(_ context.Context, p []*lazyPostings, start int64, length int64) error {
			mu.Lock()
			defer mu.Unlock()

			foundRanges = append(foundRanges, []int{int(start), int(start + length)})
			foundPostings = append(foundPostings, p)
			return nil
		}))

		// Assert results.
		for i, found := range foundRanges {
			var match bool
			for _, r := range tcase.expectedRanges {
				if r[0] == found[0] && r[1] == found[1] {
					match = true
					break
				}
			}

			testutil.Assert(t, match, "Unexpected range; [%v:%v]", found[0], found[1])

			// Double check if postings ranges are within found range.
			for _, p := range foundPostings[i] {
				testutil.Assert(t, int(p.ptr.Start) >= found[0], fmt.Sprintf("given posting start ptr %v should be greater or equal to given start %v", p.ptr.Start, found[0]))
				testutil.Assert(t, int(p.ptr.End) <= found[1], fmt.Sprintf("given posting end ptr %v should be smaller or equal to given start+length %v", p.ptr.End, found[1]))
			}
		}
		testutil.Equals(t, len(tcase.expectedRanges), len(foundRanges))
	}
}
