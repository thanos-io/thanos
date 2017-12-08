package store

import (
	"context"
	"testing"

	"sync"

	"sort"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/tsdb/index"
)

func Test_PreloadPostings(t *testing.T) {
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
			ranges:         [][]int{{1, 2}, {3, 5}, {7, 10}},
			expectedRanges: [][]int{{1, 10}},
		},
		{
			ranges:         [][]int{{1, 2}, {3, 5}, {20, 30}, {1054, 1055}},
			expectedRanges: [][]int{{1, 30}, {1054, 1055}},
		},
		// Overlapping ranges.
		{
			ranges:         [][]int{{1, 30}, {3, 28}, {1, 4}, {1054, 1055}, {1054, 1085}},
			expectedRanges: [][]int{{1, 30}, {1054, 1085}},
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
		var mu sync.Mutex
		testutil.Ok(t, preloadPostings(context.Background(), p, func(_ context.Context, _ []*lazyPostings, start int64, length int64) error {
			mu.Lock()
			defer mu.Unlock()

			foundRanges = append(foundRanges, []int{int(start), int(start + length)})
			return nil
		}))

		// Assert results.
		for _, found := range foundRanges {
			var match bool
			for _, r := range tcase.expectedRanges {
				if r[0] == found[0] && r[1] == found[1] {
					match = true
					break
				}
			}

			testutil.Assert(t, match, "Unexpected range; [%v:%v]", found[0], found[1])
		}
		testutil.Equals(t, len(tcase.expectedRanges), len(foundRanges))
	}
}
