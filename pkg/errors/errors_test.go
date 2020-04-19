package errors

import (
	"fmt"
	"os"
	"testing"

	"github.com/thanos-io/thanos/pkg/errors/internal/stack"
	// TODO(bwplotka): When in need to export this package, make sure to copy testuils.
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestNewAndCause(t *testing.T) {
	testNewAndCause(t, New)
}

func TestWrapAndCause(t *testing.T) {
	testWrapAndCause(t, Wrap)
}

func TestNewWrap_UnsupportedFormattingVerb(t *testing.T) {
	err := New("error1")
	testutil.Equals(t, "(not supported \"d\" verb used) for string value. ('s' 'v' '+v' allowed only for errFrame value \"error1\")", fmt.Sprintf("%d", err))
	testutil.Equals(t, "(not supported \"d\" verb used) for string value. ('s' 'v' '+v' allowed only for errFrame value \"wrapping: error1\")", fmt.Sprintf("%d", Wrap(err, "wrapping")))
}

func testNewAndCause(t *testing.T, newFn func(format string, args ...interface{}) error) {
	for _, tcase := range []struct {
		name   string
		format string
		args   []interface{}

		expectedErrMsg string
	}{
		{
			name:   "new empty",
			format: "", args: nil,

			// TODO(bwplotka) Should we mention empty err?
			expectedErrMsg: "",
		},
		{
			name:   "new 1",
			format: "1", args: nil,

			expectedErrMsg: "1",
		},
		{
			name:   "new with args",
			format: "%s %d %v", args: []interface{}{"lmao", 1, []int{0, 1, 2}},

			expectedErrMsg: "lmao 1 [0 1 2]",
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := newFn(tcase.format, tcase.args...)
			testutil.NotOk(t, err)
			testutil.Equals(t, tcase.expectedErrMsg, err.Error())
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%s", err))
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%v", err))

			testutil.Equals(t, err, Cause(err))
		})
	}
}

func testWrapAndCause(t *testing.T, wrapFn func(err error, format string, args ...interface{}) error) {
	for _, tcase := range []struct {
		name       string
		err        error
		wrapFormat string
		wrapArgs   []interface{}

		expectedErrMsg string
		expectedCause  error
	}{
		{
			name: "nil wrap should wrap NilErrorWrappedErr",
			err:  nil, wrapFormat: "", wrapArgs: nil,

			expectedErrMsg: ": unknown error (nil error wrapped)",
			expectedCause:  NilErrorWrappedErr,
		},
		{
			name: "nil wrap with wrap fmt should wrap NilErrorWrappedErr",
			err:  nil, wrapFormat: "wrapping error", wrapArgs: nil,

			expectedErrMsg: "wrapping error: unknown error (nil error wrapped)",
			expectedCause:  NilErrorWrappedErr,
		},
		{
			name: "error(nil) wrap with wrap fmt should wrap NilErrorWrappedErr",
			err:  error(nil), wrapFormat: "wrapping error", wrapArgs: nil,

			expectedErrMsg: "wrapping error: unknown error (nil error wrapped)",
			expectedCause:  NilErrorWrappedErr,
		},
		{
			name: "certain error",
			err:  New("some error1"), wrapFormat: "", wrapArgs: nil,

			expectedErrMsg: ": some error1",
			expectedCause:  New("some error1"),
		},
		{
			name: "certain error with single wrap",
			err:  New("some error2"), wrapFormat: "wrapping error", wrapArgs: nil,

			expectedErrMsg: "wrapping error: some error2",
			expectedCause:  New("some error2"),
		},
		{
			name: "multiple wraps",
			err:  wrapFn(wrapFn(wrapFn(New("some error3"), "wrap1"), "wrap2"), "wrap3"), wrapFormat: "wrap4", wrapArgs: nil,

			expectedErrMsg: "wrap4: wrap3: wrap2: wrap1: some error3",
			expectedCause:  New("some error3"),
		},
		{
			name: "multiple wraps + mix + custom error",
			err:  wrapFn(Wrap(wrapFn(&someErr{message: "some error4"}, "wrap1"), "wrap2"), "wrap3"), wrapFormat: "wrap4", wrapArgs: nil,

			expectedErrMsg: "wrap4: wrap3: wrap2: wrap1: some error4",
			expectedCause:  &someErr{message: "some error4"},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			err := wrapFn(tcase.err, tcase.wrapFormat, tcase.wrapArgs...)
			testutil.NotOk(t, err)
			testutil.Equals(t, tcase.expectedErrMsg, err.Error())
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%s", err))
			testutil.Equals(t, tcase.expectedErrMsg, fmt.Sprintf("%v", err))

			testutil.Equals(t, tcase.expectedCause.Error(), Cause(err).Error())
		})
	}
}

func TestStackTrace(t *testing.T) {
	dir, err := os.Getwd()
	testutil.Ok(t, err)

	tests := []struct {
		err  error
		want []string
	}{{
		New("ooh"), []string{
			fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors.TestStackTrace\n"+
				"\t%s/errors_test.go:146", dir),
		},
	}, {
		Wrap(New("ooh"), "ahh"), []string{
			fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors.TestStackTrace\n"+
				"\t%s/errors_test.go:151", dir), // This is the stack of Wrap, not New.
		},
	}, {
		Cause(Wrap(New("ooh"), "ahh")), []string{
			fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors.TestStackTrace\n"+
				"\t%s/errors_test.go:156", dir), // This is the stack of New.
		},
	}, {
		func() error { return New("ooh") }(), []string{
			fmt.Sprintf(`github.com/thanos-io/thanos/pkg/errors.TestStackTrace.func1`+
				"\n\t%s/errors_test.go:161", dir), // This is the stack of New.
			fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors.TestStackTrace\n"+
				"\t%s/errors_test.go:161", dir), // This is the stack of New's caller.
		},
	}, {
		Cause(func() error {
			return func() error {
				return New("hello %s", fmt.Sprintf("world"))
			}()
		}()), []string{
			fmt.Sprintf(`github.com/thanos-io/thanos/pkg/errors.TestStackTrace.func2.1`+
				"\n\t%s/errors_test.go:170", dir), // This is the stack of New.
			fmt.Sprintf(`github.com/thanos-io/thanos/pkg/errors.TestStackTrace.func2`+
				"\n\t%s/errors_test.go:171", dir), // This is the stack of New's caller.
			fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors.TestStackTrace\n"+
				"\t%s/errors_test.go:172", dir), // This is the stack of New's caller's caller.
		},
	}}
	for _, tt := range tests {
		x, ok := tt.err.(interface {
			Trace() stack.Trace
		})
		if !ok {
			t.Errorf("expected %#v to implement Trace() Trace", tt.err)
			continue
		}
		st := x.Trace()
		for j, want := range tt.want {
			testutil.Equals(t, want, fmt.Sprintf("%+v", st[j]))
		}
	}
}
