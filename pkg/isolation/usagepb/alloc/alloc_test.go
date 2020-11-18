package alloc_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/thanos-io/thanos/pkg/isolation/usagepb/alloc"
	"github.com/thanos-io/thanos/pkg/testutil"
)

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

func testWithAllocations_InaccurateTracking(tracker alloc.Tracker) {
	x1 := make([]string, 100)
	tracker.MemoryBytesAllocated(1792)

	x2 := uint64(1244)
	tracker.MemoryBytesAllocated(8) // Wrong, this is on stack.

	x3 := &testStruct{}
	tracker.MemoryBytesAllocated(1214)

	// Use fprint to ensure compiler does not optimize those allocs away.
	fmt.Fprint(ioutil.Discard, x1, x2, x3)
	tracker.MemoryBytesAllocated(32 + 128 + 1600 + 32)
}

func testWithAllocations_InaccurateTracking2(tracker alloc.Tracker) {
	x1 := make([]string, 100)
	tracker.MemoryBytesAllocated(1791) // Wrong.

	x2 := uint64(1244)

	x3 := &testStruct{}
	tracker.MemoryBytesAllocated(1214)

	// Use fprint to ensure compiler does not optimize those allocs away.
	fmt.Fprint(ioutil.Discard, x1, x2, x3)
	tracker.MemoryBytesAllocated(32 + 128 + 1600 + 32)
}

func testWithAllocations_AccurateTracking(tracker alloc.Tracker) {
	x1 := make([]string, 100)
	tracker.MemoryBytesAllocated(1792)

	x2 := uint64(1244)

	x3 := &testStruct{}
	tracker.MemoryBytesAllocated(1214)

	// Use fprint to ensure compiler does not optimize those allocs away.
	fmt.Fprint(ioutil.Discard, x1, x2, x3)
	tracker.MemoryBytesAllocated(32 + 128 + 1600 + 32)
}

func TestResourceTracker_MemoryBytesAllocated(t *testing.T) {
	t.Run("no tracking", func(t *testing.T) {
		err := alloc.ValidateTracking(1, 125, func(_ alloc.Tracker) {
			testWithAllocations_NoTracking()
		})
		testutil.NotOk(t, err)
		testutil.Equals(t, `3 errors: Untracked allocations of 128 Bytes for: 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:86 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:867 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:716 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:1161 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:232 
    /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc_test.go:25 

; Untracked allocations of 1600 Bytes for: 
    /home/bwplotka/.gvm/gos/go1.15/src/reflect/value.go:119 
    /home/bwplotka/.gvm/gos/go1.15/src/reflect/value.go:1045 
    /home/bwplotka/.gvm/gos/go1.15/src/reflect/value.go:1015 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:726 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:869 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:716 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:1161 
    /home/bwplotka/.gvm/gos/go1.15/src/fmt/print.go:232 
    /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc_test.go:25 

; Untracked allocations of 1792 Bytes for: 
    /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc_test.go:19 

`, err.Error())

	})
	t.Run("inaccurate tracking", func(t *testing.T) {
		err := alloc.ValidateTracking(1, 125, func(tr alloc.Tracker) {
			testWithAllocations_InaccurateTracking(tr)
		})
		testutil.NotOk(t, err)
		testutil.Equals(t, `5 errors: /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc.go:59 Tracked 208 Bytes, but noting was actually allocated in near location; /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc_test.go:31 Tracked 1792 Bytes, but noting was actually allocated in near location; /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc_test.go:34 Tracked 8 Bytes, but noting was actually allocated in near location; /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc_test.go:37 Tracked 1214 Bytes, but noting was actually allocated in near location; /home/bwplotka/Repos/thanos/pkg/isolation/usagepb/alloc/alloc_test.go:41 Tracked 1792 Bytes, but noting was actually allocated in near location"`, err.Error())
	})
	t.Run("inaccurate tracking 2", func(t *testing.T) {
		err := alloc.ValidateTracking(1, 125, func(tr alloc.Tracker) {
			testWithAllocations_InaccurateTracking2(tr)
		})
		testutil.NotOk(t, err)
		testutil.Equals(t, `6 errors: Untracked allocations of 32 Bytes for: `, err.Error())
	})
	t.Run("accurate tracking", func(t *testing.T) {
		testutil.Ok(t, alloc.ValidateTracking(1, 125, func(tr alloc.Tracker) {
			testWithAllocations_AccurateTracking(tr)
		}))
	})
}
