package alloc

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/errutil"
)

type Tracker interface {
	MemoryBytesAllocated(b int)
}

type codeRef struct {
	file string
	line int
}

type tracker struct {
	mtx            sync.Mutex
	allocatedBytes map[codeRef]uint64
}

func (t *tracker) MemoryBytesAllocated(b int) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	var (
		c  = codeRef{}
		ok bool
	)
	_, c.file, c.line, ok = runtime.Caller(1)
	if !ok {
		panic("could not get caller")
	}
	t.allocatedBytes[c] += uint64(b)
}

// ValidateTracking...
// Not go-routine safe due to global memory profiling.
func ValidateTracking(skip int, toleranceBytes uint64, f func(tr Tracker)) (err error) {
	// Setting 1 to include every allocated block in the profile. (512 * 1024 by default).
	oldProfileRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	defer func() { runtime.MemProfileRate = oldProfileRate }()

	var (
		tracked  = &tracker{allocatedBytes: map[codeRef]uint64{}}
		callerPC uintptr
		ok       bool
	)

	// Anonymous function to get scoped PC.
	// NOTE: PC means program [stack] counter.
	func() {
		callerPC, _, _, ok = runtime.Caller(1)
		tracked.MemoryBytesAllocated(208)
		if !ok {
			err = errors.New("failed to get caller info")
			return
		}
		f(tracked)
	}()
	if err != nil {
		return err
	}

	var (
		foundAllocs     = make(map[codeRef]memProfileFramedRecords, len(tracked.allocatedBytes))
		errs            errutil.MultiError
		fr              runtime.Frame
		untrackedAllocs []memProfileFramedRecord
	)

RecordsLoop:
	for _, r := range getRelatedMemRecords(callerPC) {
		frames := runtime.CallersFrames(r.relevantPCs)
		var (
			frs []runtime.Frame
			ok  = true
		)
		for ok {
			fr, ok = frames.Next()
			if _, isTrackerInvocation := tracked.allocatedBytes[codeRef{file: fr.File, line: fr.Line}]; isTrackerInvocation {
				// Ignore tracker allocated bytes.
				continue RecordsLoop
			}
			frs = append(frs, fr)
		}
		framed := memProfileFramedRecord{
			MemProfileRecord: r.MemProfileRecord,
			// -1 so we ignore f call.
			frames: frs[:len(frs)-skip-1],
		}
		for _, fr := range frs {
			// Hacky: Assume for now tracker is right after allocation.
			fcodeRef := codeRef{file: fr.File, line: fr.Line + 1}
			_, ok = tracked.allocatedBytes[fcodeRef]
			if ok {
				found := foundAllocs[fcodeRef]
				found.allocatedBytes += uint64(r.AllocBytes)
				found.records = append(found.records, framed)
				foundAllocs[fcodeRef] = found
				break
			}
		}
		if !ok {
			untrackedAllocs = append(untrackedAllocs, framed)
		}
	}

	for cr, b := range tracked.allocatedBytes {
		if found, ok := foundAllocs[cr]; ok {
			if b+toleranceBytes >= found.allocatedBytes && b-toleranceBytes <= found.allocatedBytes {
				continue
			}
			errs = append(errs, errors.Errorf("%v:%v Tracked %v Bytes, but actually nearby allocated %v\n", cr.file, cr.line, b, found.String()))
			continue
		}
		errs = append(errs, errors.Errorf("%v:%v Tracked %v Bytes, but noting was actually allocated in near location\n", cr.file, cr.line, b))
	}

	var toleratedBytes uint64
	var toleratedErrs errutil.MultiError
	for _, u := range untrackedAllocs {
		err := errors.Errorf("Untracked allocations of %v\n", u.String())
		if uint64(u.AllocBytes) <= toleranceBytes {
			toleratedBytes += uint64(u.AllocBytes)
			toleratedErrs = append(toleratedErrs, err)
			continue
		}
		errs = append(errs, err)
	}

	if toleratedBytes > toleranceBytes {
		errs = append(errs, errors.Wrapf(toleratedErrs.Err(), "Untracked tiny allocations accumulated above tolerance: %v\n", toleratedBytes))
	}
	return errs.Err()
}

type memProfileFramedRecords struct {
	allocatedBytes uint64
	records        []memProfileFramedRecord
}

func (r memProfileFramedRecords) String() string {
	msg := fmt.Sprintf("%v Details:\n", r.allocatedBytes)
	for _, rec := range r.records {
		msg += "  " + rec.String()
	}
	return msg
}

type memProfileFramedRecord struct {
	runtime.MemProfileRecord
	frames []runtime.Frame
}

func (r memProfileFramedRecord) String() string {
	msg := fmt.Sprintf("%v Bytes for: \n", r.AllocBytes)
	for _, f := range r.frames {
		msg += fmt.Sprintf("    %v:%v \n", f.File, f.Line)
	}
	return msg
}

type memProfileRecord struct {
	runtime.MemProfileRecord
	relevantPCs []uintptr
}

func getRelatedMemRecords(basePC uintptr) (relatedRecords []memProfileRecord) {
	records := make([]runtime.MemProfileRecord, 100)
	for {
		l, ok := runtime.MemProfile(records, true)
		if ok {
			break
		}
		records = make([]runtime.MemProfileRecord, l)
	}

	for _, r := range records {
		if r.AllocBytes == 0 && r.AllocObjects == 0 {
			break
		}

		s := r.Stack()

		// Get records only downstream of this PC.
		for i, pc := range s {
			if pc == basePC+1 {
				// Copy as runtime reuse all arrays.
				relevantPCs := make([]uintptr, i)
				copy(relevantPCs, s[:i])
				relatedRecords = append(relatedRecords, memProfileRecord{
					MemProfileRecord: r,
					relevantPCs:      relevantPCs,
				})
				break
			}
		}
	}
	return relatedRecords
}
