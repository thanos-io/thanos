// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package errors

import (
	"fmt"
	"runtime"
	"strings"
)

// stack holds the address of
type stacktrace []uintptr

// New captures a stack trace. skip specifies the number of frames to skip from
// a stack trace. skip=0 records stack.New call as the innermost frame.
func newStackTrace() stacktrace {
	const stackDepth = 16 // record maximum 16 frames (if available)

	pc := make([]uintptr, stackDepth+1)
	// using skip=3 for not to count the program counter address of
	// 1. the respective function from errors package (eg. errors.New)
	// 2. newStacktrace itself
	// 3. the function used in runtime.Callers
	n := runtime.Callers(3, pc)
	return stacktrace(pc[:n])
}

// String implements the fmt.Stringer interface to provide formatted text output.
func (s stacktrace) String() string {
	var buf strings.Builder

	// CallersFrames takes the slice of Program Counter addresses returned by Callers to
	// retrieve function/file/line information.
	cf := runtime.CallersFrames(s)
	for {
		// more indicates if the next call will be successful or not.
		frame, more := cf.Next()
		// used formatting scheme <`>`space><function name><newline><tab><filepath><:><line><newline>
		buf.WriteString(fmt.Sprintf("> %s\t%s:%d\n", frame.Func.Name(), frame.File, frame.Line))
		if !more {
			break
		}
	}
	return buf.String()
}
