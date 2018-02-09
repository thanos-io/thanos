package stack_test

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/go-stack/stack"
)

func TestCaller(t *testing.T) {
	t.Parallel()

	c := stack.Caller(0)
	_, file, line, ok := runtime.Caller(0)
	line--
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	if got, want := c.Frame().File, file; got != want {
		t.Errorf("got file == %v, want file == %v", got, want)
	}

	if got, want := c.Frame().Line, line; got != want {
		t.Errorf("got line == %v, want line == %v", got, want)
	}
}

func f3(f1 func() stack.Call) stack.Call {
	return f2(f1)
}

func f2(f1 func() stack.Call) stack.Call {
	return f1()
}

func TestCallerMidstackInlined(t *testing.T) {
	t.Parallel()

	_, _, line, ok := runtime.Caller(0)
	line -= 10 // adjust to return f1() line inside f2()
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	c := f3(func() stack.Call {
		return stack.Caller(2)
	})

	if got, want := c.Frame().Line, line; got != want {
		t.Errorf("got line == %v, want line == %v", got, want)
	}
	if got, want := c.Frame().Function, "github.com/go-stack/stack_test.f3"; got != want {
		t.Errorf("got func name == %v, want func name == %v", got, want)
	}
}

func TestCallerPanic(t *testing.T) {
	t.Parallel()

	var (
		line int
		ok   bool
	)

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
			if got, want := c1.Frame().Function, "github.com/go-stack/stack_test.TestCallerPanic"; got != want {
				t.Errorf("TestCallerPanic frame: got name == %v, want name == %v", got, want)
			}
			if got, want := c1.Frame().Line, line; got != want {
				t.Errorf("TestCallerPanic frame: got line == %v, want line == %v", got, want)
			}
		}
	}()

	_, _, line, ok = runtime.Caller(0)
	line += 7 // adjust to match line of panic below
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	// Initiate a sigpanic.
	var x *uintptr
	_ = *x
}

type tholder struct {
	trace func() stack.CallStack
}

func (th *tholder) traceLabyrinth() stack.CallStack {
	for {
		return th.trace()
	}
}

func TestTrace(t *testing.T) {
	t.Parallel()

	_, _, line, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	fh := tholder{
		trace: func() stack.CallStack {
			cs := stack.Trace()
			return cs
		},
	}

	cs := fh.traceLabyrinth()

	lines := []int{line + 7, line - 7, line + 12}

	for i, line := range lines {
		if got, want := cs[i].Frame().Line, line; got != want {
			t.Errorf("got line[%d] == %v, want line[%d] == %v", i, got, i, want)
		}
	}
}

// Test stack handling originating from a sigpanic.
func TestTracePanic(t *testing.T) {
	t.Parallel()

	var (
		line int
		ok   bool
	)

	defer func() {
		if recover() != nil {
			trace := stack.Trace()

			// find runtime.sigpanic
			panicIdx := -1
			for i, c := range trace {
				if c.Frame().Function == "runtime.sigpanic" {
					panicIdx = i
					break
				}
			}
			if panicIdx == -1 {
				t.Fatal("no runtime.sigpanic entry on the stack")
			}
			if got, want := trace[panicIdx].Frame().Function, "runtime.sigpanic"; got != want {
				t.Errorf("sigpanic frame: got name == %v, want name == %v", got, want)
			}
			if got, want := trace[panicIdx+1].Frame().Function, "github.com/go-stack/stack_test.TestTracePanic"; got != want {
				t.Errorf("TestTracePanic frame: got name == %v, want name == %v", got, want)
			}
			if got, want := trace[panicIdx+1].Frame().Line, line; got != want {
				t.Errorf("TestTracePanic frame: got line == %v, want line == %v", got, want)
			}
		}
	}()

	_, _, line, ok = runtime.Caller(0)
	line += 7 // adjust to match line of panic below
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	// Initiate a sigpanic.
	var x *uintptr
	_ = *x
}

const importPath = "github.com/go-stack/stack"

type testType struct{}

func (tt testType) testMethod() (c stack.Call, file string, line int, ok bool) {
	c = stack.Caller(0)
	_, file, line, ok = runtime.Caller(0)
	line--
	return
}

func TestCallFormat(t *testing.T) {
	t.Parallel()

	c := stack.Caller(0)
	_, file, line, ok := runtime.Caller(0)
	line--
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	relFile := path.Join(importPath, filepath.Base(file))

	c2, file2, line2, ok2 := testType{}.testMethod()
	if !ok2 {
		t.Fatal("runtime.Caller(0) failed")
	}
	relFile2 := path.Join(importPath, filepath.Base(file2))

	data := []struct {
		c    stack.Call
		desc string
		fmt  string
		out  string
	}{
		{stack.Call{}, "error", "%s", "%!s(NOFUNC)"},

		{c, "func", "%s", path.Base(file)},
		{c, "func", "%+s", relFile},
		{c, "func", "%#s", file},
		{c, "func", "%d", fmt.Sprint(line)},
		{c, "func", "%n", "TestCallFormat"},
		{c, "func", "%+n", "github.com/go-stack/stack_test.TestCallFormat"},
		{c, "func", "%k", "stack_test"},
		{c, "func", "%+k", "github.com/go-stack/stack_test"},
		{c, "func", "%v", fmt.Sprint(path.Base(file), ":", line)},
		{c, "func", "%+v", fmt.Sprint(relFile, ":", line)},
		{c, "func", "%#v", fmt.Sprint(file, ":", line)},

		{c2, "meth", "%s", path.Base(file2)},
		{c2, "meth", "%+s", relFile2},
		{c2, "meth", "%#s", file2},
		{c2, "meth", "%d", fmt.Sprint(line2)},
		{c2, "meth", "%n", "testType.testMethod"},
		{c2, "meth", "%+n", "github.com/go-stack/stack_test.testType.testMethod"},
		{c2, "meth", "%k", "stack_test"},
		{c2, "meth", "%+k", "github.com/go-stack/stack_test"},
		{c2, "meth", "%v", fmt.Sprint(path.Base(file2), ":", line2)},
		{c2, "meth", "%+v", fmt.Sprint(relFile2, ":", line2)},
		{c2, "meth", "%#v", fmt.Sprint(file2, ":", line2)},
	}

	for _, d := range data {
		got := fmt.Sprintf(d.fmt, d.c)
		if got != d.out {
			t.Errorf("fmt.Sprintf(%q, Call(%s)) = %s, want %s", d.fmt, d.desc, got, d.out)
		}
	}
}

func TestCallString(t *testing.T) {
	t.Parallel()

	c := stack.Caller(0)
	_, file, line, ok := runtime.Caller(0)
	line--
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	c2, file2, line2, ok2 := testType{}.testMethod()
	if !ok2 {
		t.Fatal("runtime.Caller(0) failed")
	}

	data := []struct {
		c    stack.Call
		desc string
		out  string
	}{
		{stack.Call{}, "error", "%!v(NOFUNC)"},
		{c, "func", fmt.Sprint(path.Base(file), ":", line)},
		{c2, "meth", fmt.Sprint(path.Base(file2), ":", line2)},
	}

	for _, d := range data {
		got := d.c.String()
		if got != d.out {
			t.Errorf("got %s, want %s", got, d.out)
		}
	}
}

func TestCallMarshalText(t *testing.T) {
	t.Parallel()

	c := stack.Caller(0)
	_, file, line, ok := runtime.Caller(0)
	line--
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	c2, file2, line2, ok2 := testType{}.testMethod()
	if !ok2 {
		t.Fatal("runtime.Caller(0) failed")
	}

	data := []struct {
		c    stack.Call
		desc string
		out  []byte
		err  error
	}{
		{stack.Call{}, "error", nil, stack.ErrNoFunc},
		{c, "func", []byte(fmt.Sprint(path.Base(file), ":", line)), nil},
		{c2, "meth", []byte(fmt.Sprint(path.Base(file2), ":", line2)), nil},
	}

	for _, d := range data {
		text, err := d.c.MarshalText()
		if got, want := err, d.err; got != want {
			t.Errorf("%s: got err %v, want err %v", d.desc, got, want)
		}
		if got, want := text, d.out; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %s, want %s", d.desc, got, want)
		}
	}
}

func TestCallStackString(t *testing.T) {
	cs, line0 := getTrace(t)
	_, file, line1, ok := runtime.Caller(0)
	line1--
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	file = path.Base(file)
	if got, want := cs.String(), fmt.Sprintf("[%s:%d %s:%d]", file, line0, file, line1); got != want {
		t.Errorf("\n got %v\nwant %v", got, want)
	}
}

func TestCallStackMarshalText(t *testing.T) {
	cs, line0 := getTrace(t)
	_, file, line1, ok := runtime.Caller(0)
	line1--
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	file = path.Base(file)
	text, _ := cs.MarshalText()
	if got, want := text, []byte(fmt.Sprintf("[%s:%d %s:%d]", file, line0, file, line1)); !reflect.DeepEqual(got, want) {
		t.Errorf("\n got %v\nwant %v", got, want)
	}
}

func getTrace(t *testing.T) (stack.CallStack, int) {
	cs := stack.Trace().TrimRuntime()
	_, _, line, ok := runtime.Caller(0)
	line--
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	return cs, line
}

func TestTrimAbove(t *testing.T) {
	trace := trimAbove()
	if got, want := len(trace), 2; got != want {
		t.Fatalf("got len(trace) == %v, want %v, trace: %n", got, want, trace)
	}
	if got, want := fmt.Sprintf("%n", trace[1]), "TestTrimAbove"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func trimAbove() stack.CallStack {
	call := stack.Caller(1)
	trace := stack.Trace()
	return trace.TrimAbove(call)
}

func TestTrimBelow(t *testing.T) {
	trace := trimBelow()
	if got, want := fmt.Sprintf("%n", trace[0]), "TestTrimBelow"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func trimBelow() stack.CallStack {
	call := stack.Caller(1)
	trace := stack.Trace()
	return trace.TrimBelow(call)
}

func TestTrimRuntime(t *testing.T) {
	trace := stack.Trace().TrimRuntime()
	if got, want := len(trace), 1; got != want {
		t.Errorf("got len(trace) == %v, want %v, goroot: %q, trace: %#v", got, want, runtime.GOROOT(), trace)
	}
}

func BenchmarkCallVFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprint(ioutil.Discard, c)
	}
}

func BenchmarkCallPlusVFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%+v", c)
	}
}

func BenchmarkCallSharpVFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%#v", c)
	}
}

func BenchmarkCallSFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%s", c)
	}
}

func BenchmarkCallPlusSFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%+s", c)
	}
}

func BenchmarkCallSharpSFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%#s", c)
	}
}

func BenchmarkCallDFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%d", c)
	}
}

func BenchmarkCallNFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%n", c)
	}
}

func BenchmarkCallPlusNFmt(b *testing.B) {
	c := stack.Caller(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(ioutil.Discard, "%+n", c)
	}
}

func BenchmarkCaller(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stack.Caller(0)
	}
}

func BenchmarkTrace(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stack.Trace()
	}
}

func deepStack(depth int, b *testing.B) stack.CallStack {
	if depth > 0 {
		return deepStack(depth-1, b)
	}
	b.StartTimer()
	s := stack.Trace()
	return s
}

func BenchmarkTrace10(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		deepStack(10, b)
	}
}

func BenchmarkTrace50(b *testing.B) {
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		deepStack(50, b)
	}
}

func BenchmarkTrace100(b *testing.B) {
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		deepStack(100, b)
	}
}

////////////////
// Benchmark functions followed by formatting
////////////////

func BenchmarkCallerAndVFmt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fmt.Fprint(ioutil.Discard, stack.Caller(0))
	}
}

func BenchmarkTraceAndVFmt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fmt.Fprint(ioutil.Discard, stack.Trace())
	}
}

func BenchmarkTrace10AndVFmt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fmt.Fprint(ioutil.Discard, deepStack(10, b))
	}
}

////////////////
// Baseline against package runtime.
////////////////

func BenchmarkRuntimeCaller(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runtime.Caller(0)
	}
}

func BenchmarkRuntimeCallerAndFmt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, file, line, _ := runtime.Caller(0)
		const sep = "/"
		if i := strings.LastIndex(file, sep); i != -1 {
			file = file[i+len(sep):]
		}
		fmt.Fprint(ioutil.Discard, file, ":", line)
	}
}

func BenchmarkFuncForPC(b *testing.B) {
	pc, _, _, _ := runtime.Caller(0)
	pc--
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.FuncForPC(pc)
	}
}

func BenchmarkFuncFileLine(b *testing.B) {
	pc, _, _, _ := runtime.Caller(0)
	pc--
	fn := runtime.FuncForPC(pc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn.FileLine(pc)
	}
}
