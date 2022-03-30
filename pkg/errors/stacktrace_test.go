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
	expectedPhrase := "thanos/pkg/errors/stacktrace_test.go:13"
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
