package stack

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	// TODO(bwplotka): When in need to export this package, make sure to copy testuils.
	"github.com/thanos-io/thanos/pkg/testutil"
)

var initpc = New(2).Trace()[0]

type X struct{}

// val returns a Frame pointing to itself.
func (x X) val() Frame {
	return New(2).Trace()[0]
}

// ptr returns a Frame pointing to itself.
func (x *X) ptr() Frame {
	return New(2).Trace()[0]
}

func TestFrameFormat(t *testing.T) {
	dir, err := os.Getwd()
	testutil.Ok(t, err)

	var tests = []struct {
		Frame
		format string
		want   string
	}{{
		initpc,
		"%s",
		"stack_test.go",
	}, {
		initpc,
		"%+s",
		fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors/internal/stack.init\n"+
			"\t%s/stack_test.go", dir),
	}, {
		0,
		"%s",
		"unknown",
	}, {
		0,
		"%+s",
		"unknown",
	}, {
		initpc,
		"%d",
		"12",
	}, {
		0,
		"%d",
		"0",
	}, {
		initpc,
		"%n",
		"init",
	}, {
		func() Frame {
			var x X
			return x.ptr()
		}(),
		"%n",
		`(*X).ptr`,
	}, {
		func() Frame {
			var x X
			return x.val()
		}(),
		"%n",
		"X.val",
	}, {
		0,
		"%n",
		"unknown",
	}, {
		initpc,
		"%v",
		"stack_test.go:12",
	}, {
		initpc,
		"%+v",
		fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors/internal/stack.init\n"+
			"\t%s/stack_test.go:12", dir),
	}, {
		0,
		"%v",
		"unknown:0",
	}}

	for _, tt := range tests {
		testutil.Equals(t, tt.want, fmt.Sprintf(tt.format, tt.Frame))
	}
}

func TestFuncname(t *testing.T) {
	tests := []struct {
		name, want string
	}{
		{"", ""},
		{"runtime.main", "main"},
		{"github.com/thanos-io/thanos/pkg/errors.funcname", "funcname"},
		{"funcname", "funcname"},
		{"io.copyBuffer", "copyBuffer"},
		{"main.(*R).Write", "(*R).Write"},
	}

	for _, tt := range tests {
		testutil.Equals(t, tt.want, funcname(tt.name))
	}
}

func stackTrace() Trace {
	const depth = 8
	var pcs [depth]uintptr
	n := runtime.Callers(1, pcs[:])
	var st Stack = pcs[0:n]
	return st.Trace()
}

func TestStackTraceFormat(t *testing.T) {
	dir, err := os.Getwd()
	testutil.Ok(t, err)

	tests := []struct {
		Trace
		format string
		want   string
	}{{
		nil,
		"%s",
		`[]`,
	}, {
		nil,
		"%v",
		`[]`,
	}, {
		nil,
		"%+v",
		"",
	}, {
		nil,
		"%#v",
		`[]stack.Frame(nil)`,
	}, {
		make(Trace, 0),
		"%s",
		`[]`,
	}, {
		make(Trace, 0),
		"%v",
		`[]`,
	}, {
		make(Trace, 0),
		"%+v",
		"",
	}, {
		make(Trace, 0),
		"%#v",
		`[]stack.Frame{}`,
	}, {
		stackTrace()[:2],
		"%s",
		`[stack_test.go stack_test.go]`,
	}, {
		stackTrace()[:2],
		"%v",
		`[stack_test.go:121 stack_test.go:171]`,
	}, {
		stackTrace()[:2],
		"%+v",
		"\n" +
			fmt.Sprintf("github.com/thanos-io/thanos/pkg/errors/internal/stack.stackTrace\n"+
				"\t%s/stack_test.go:121\n"+
				"github.com/thanos-io/thanos/pkg/errors/internal/stack.TestStackTraceFormat\n"+
				"\t%s/stack_test.go:175", dir, dir),
	}, {
		stackTrace()[:2],
		"%#v",
		`[]stack.Frame{stack_test.go:121, stack_test.go:183}`,
	}}

	for _, tt := range tests {
		testutil.Equals(t, tt.want, fmt.Sprintf(tt.format, tt.Trace))
	}
}
