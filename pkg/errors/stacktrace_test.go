// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package errors

import (
	"strings"
	"testing"
)

func caller() stacktrace {
	return newStackTrace()
}

func TestStacktraceOutput(t *testing.T) {
	st := caller()
	expectedPhrase := "/pkg/errors/stacktrace_test.go:16"
	if !strings.Contains(st.String(), expectedPhrase) {
		t.Fatalf("expected %v phrase into the stacktrace, received stacktrace: \n%v", expectedPhrase, st.String())
	}
}

func TestStacktraceProgramCounterLen(t *testing.T) {
	st := caller()
	output := st.String()
	lines := len(strings.Split(strings.TrimSuffix(output, "\n"), "\n"))
	if len(st) != lines {
		t.Fatalf("output lines vs program counter size mismatch: program counter size %v, output lines %v", len(st), lines)
	}
}
