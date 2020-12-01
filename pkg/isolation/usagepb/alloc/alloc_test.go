package alloc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMemProfileSince(t *testing.T) {
	for i := 0; i < 2; i++ {
		// Run at least twice so we can ensure determinism.
		t.Run("", func(t *testing.T) {
			r, err := recordMemoryProfiles(0, func() {})
			testutil.Ok(t, err)
			testutil.Equals(t, []memProfileRecord(nil), r)

			var x1 []string
			r, err = recordMemoryProfiles(0, func() {
				x1 = make([]string, 100)
			})
			testutil.Ok(t, err)
			testutil.Equals(t, 1, len(r))
			testutil.Equals(t, int64(1), r[0].AllocObjects)
			testutil.Equals(t, int64(1792), r[0].AllocBytes)

			// Use fprint to ensure compiler does not optimize above allocs away.
			fmt.Fprint(ioutil.Discard, x1)
		})
	}
}

type testStruct struct {
	yolo      int64
	something float64
	ptr       *testStruct
}

func testWithAllocations_NoTracking() {
	x1 := make([]string, 100)

	x2 := uint64(1244)
	x3 := &testStruct{}

	// Use fprint to ensure compiler does not optimize those allocs away.
	fmt.Fprint(ioutil.Discard, x1, x2, x3)

}

func testWithAllocations_InaccurateTracking(tracker Tracker) {
	x1 := make([]string, 100)
	tracker.MemoryBytesAllocated(1792)

	x2 := uint64(1244)
	tracker.MemoryBytesAllocated(8) // Wrong, this is on stack.

	x3 := &testStruct{}
	tracker.MemoryBytesAllocated(32)

	// Use fprint to ensure compiler does not optimize those allocs away.
	fmt.Fprint(ioutil.Discard, x1, x2, x3)
	tracker.MemoryBytesAllocated(3672)
}

func testWithAllocations_InaccurateTracking2(tracker Tracker) {
	x1 := make([]string, 100)
	tracker.MemoryBytesAllocated(1891) // Wrong of 99.

	x2 := uint64(1244)

	x3 := &testStruct{}
	tracker.MemoryBytesAllocated(32)

	// Use fprint to ensure compiler does not optimize those allocs away.
	fmt.Fprint(ioutil.Discard, x1, x2, x3)
	tracker.MemoryBytesAllocated(3672)
}

func testWithAllocations_AccurateTracking(tracker Tracker) {
	x1 := make([]string, 100)
	tracker.MemoryBytesAllocated(1792)

	x2 := uint64(1244)

	x3 := &testStruct{}
	tracker.MemoryBytesAllocated(32)

	// Use fprint to ensure compiler does not optimize those allocs away.
	fmt.Fprint(ioutil.Discard, x1, x2, x3)
	tracker.MemoryBytesAllocated(3672)
}

func TestResourceTracker_MemoryBytesAllocated(t *testing.T) {
	for i := 0; i < 2; i++ {
		// Run at least twice so we can ensure determinism.
		// This is especially relevant for checking same functions couple of times. Frame allocations are shared between
		// traces/stacks (:
		t.Run("", func(t *testing.T) {
			t.Run("no tracking, no allocations", func(t *testing.T) {
				testutil.Ok(t, ValidateTracking(0, 0, func(_ Tracker) {}))
			})
			t.Run("something tracked, but no allocations", func(t *testing.T) {
				shallowCompare(t, []Problem{
					{TrackedBytes: 124, File: "pkg/isolation/usagepb/alloc/alloc_test.go", Line: 108},
				}, ValidateTracking(0, 0, func(tr Tracker) {
					tr.MemoryBytesAllocated(124)
				}))
			})
			t.Run("something tracked, but no allocations, with tolerance", func(t *testing.T) {
				shallowCompare(t, []Problem{
					{TrackedBytes: 124, File: "pkg/isolation/usagepb/alloc/alloc_test.go", Line: 115},
				}, ValidateTracking(0, 200, func(tr Tracker) {
					tr.MemoryBytesAllocated(124)
				}))
			})
			t.Run("no tracking, with allocations", func(t *testing.T) {
				var x1 []byte
				shallowCompare(t, []Problem{
					{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: 112}},
				}, ValidateTracking(0, 0, func(tr Tracker) {
					x1 = make([]byte, 100)
				}))
				// Use fprint to ensure compiler does not optimize above allocs away.
				fmt.Fprint(ioutil.Discard, x1)
			})
			t.Run("no tracking, with allocations and tolerance", func(t *testing.T) {
				var x1 []byte
				testutil.Ok(t, ValidateTracking(0, 113, func(tr Tracker) {
					x1 = make([]byte, 100)
				}))
				// Use fprint to ensure compiler does not optimize above allocs away.
				fmt.Fprint(ioutil.Discard, x1)
			})
			t.Run("inside function; tolerance 100", func(t *testing.T) {
				t.Run("no tracking, with allocations", func(t *testing.T) {
					shallowCompare(t, []Problem{
						{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: 232}},
						{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: 208}},
						{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: 1792}},
						{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: 1600}},
						{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: 1536}},
						{TrackedBytes: 0, Allocated: MemProfileFramedRecords{AllocatedBytes: 128}},
					}, ValidateTracking(1, 100, func(tr Tracker) {
						testWithAllocations_NoTracking()
					}))
				})
				t.Run("inaccurate tracking", func(t *testing.T) {
					shallowCompare(t, []Problem{
						{TrackedBytes: 8, File: "pkg/isolation/usagepb/alloc/alloc_test.go", Line: 59},
					}, ValidateTracking(1, 100, func(tr Tracker) {
						testWithAllocations_InaccurateTracking(tr)
					}))
				})
				t.Run("inaccurate tracking 2; lower tolerance", func(t *testing.T) {
					shallowCompare(t, []Problem{
						{TrackedBytes: 1891, Allocated: MemProfileFramedRecords{AllocatedBytes: 1792}, File: "pkg/isolation/usagepb/alloc/alloc_test.go", Line: 71},
					}, ValidateTracking(1, 98, func(tr Tracker) {
						testWithAllocations_InaccurateTracking2(tr)
					}))
				})
				t.Run("inaccurate tracking 2", func(t *testing.T) {
					testutil.Ok(t, ValidateTracking(1, 100, func(tr Tracker) {
						testWithAllocations_InaccurateTracking2(tr)
					}))
				})
				t.Run("accurate tracking", func(t *testing.T) {
					testutil.Ok(t, ValidateTracking(1, 100, func(tr Tracker) {
						testWithAllocations_AccurateTracking(tr)
					}))
				})
			})
		})
	}
}

func shallowCompare(t *testing.T, expected []Problem, err error) {
	t.Helper()

	testutil.NotOk(t, err)
	v := ValidationFailedError{}
	testutil.Assert(t, errors.As(err, &v))
	sort.Slice(v.Problems, func(i, j int) bool {
		return strings.Compare(v.Problems[i].Error(), v.Problems[j].Error()) > 0
	})

	for i := range v.Problems {
		// Don't compare frames, they are likely different.
		v.Problems[i].Allocated.records = nil
		// Trim as everyone has this code in different place.
		if v.Problems[i].File != "" {
			v.Problems[i].File = v.Problems[i].File[strings.Index(v.Problems[i].File, "pkg/isolation/"):]
		}
	}
	testutil.Equals(t, expected, v.Problems)
}
