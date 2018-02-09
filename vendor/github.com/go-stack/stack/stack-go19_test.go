// +build go1.9

package stack_test

import (
	"runtime"
	"testing"

	"github.com/go-stack/stack"
)

func TestCallerInlinedPanic(t *testing.T) {
	t.Parallel()

	var line int

	defer func() {
		if recover() != nil {
			var pcs [32]uintptr
			n := runtime.Callers(1, pcs[:])
			frames := runtime.CallersFrames(pcs[:n])
			// count frames to runtime.sigpanic
			panicIdx := 0
			for {
				f, more := frames.Next()
				if f.Function == "runtime.sigpanic" {
					break
				}
				panicIdx++
				if !more {
					t.Fatal("no runtime.sigpanic entry on the stack")
				}
			}

			c := stack.Caller(panicIdx)
			if got, want := c.Frame().Function, "runtime.sigpanic"; got != want {
				t.Errorf("sigpanic frame: got name == %v, want name == %v", got, want)
			}

			c1 := stack.Caller(panicIdx + 1)
			if got, want := c1.Frame().Function, "github.com/go-stack/stack_test.inlinablePanic"; got != want {
				t.Errorf("TestCallerInlinedPanic frame: got name == %v, want name == %v", got, want)
			}
			if got, want := c1.Frame().Line, line; got != want {
				t.Errorf("TestCallerInlinedPanic frame: got line == %v, want line == %v", got, want)
			}
		}
	}()

	doPanic(t, &line)
	t.Fatal("failed to panic")
}

func doPanic(t *testing.T, panicLine *int) {
	_, _, line, ok := runtime.Caller(0)
	*panicLine = line + 11 // adjust to match line of panic below
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	inlinablePanic()
}

func inlinablePanic() {
	// Initiate a sigpanic.
	var x *uintptr
	_ = *x
}
