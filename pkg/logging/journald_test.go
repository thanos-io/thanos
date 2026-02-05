// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"errors"
	"testing"

	"github.com/coreos/go-systemd/v22/journal"
	"github.com/go-kit/log/level"
	"github.com/stretchr/testify/require"
)

func TestNewLogger_JournaldError(t *testing.T) {
	// Skip test if journald is actually available (e.g., on Ubuntu with systemd).
	// This test is specifically for checking the error behavior when journald is NOT available.
	if journal.Enabled() {
		t.Skip("journald is enabled on this system, skipping error test")
	}

	_, err := NewLogger("info", "journald", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "journald log format requested but systemd journal is not available")
}

func TestToJournalField(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"ts", "THANOS_TS"},
		{"caller", "THANOS_CALLER"},
		{"http.method", "THANOS_HTTP_METHOD"},
		{"request-id", "THANOS_REQUEST_ID"},
		{"foo_bar", "THANOS_FOO_BAR"},
		{"123", "THANOS_123"},
		{"", "THANOS_"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.expected, toJournalField(tc.input))
		})
	}
}

// mockJournalWriter is a mock implementation for testing journaldLogger.
type mockJournalWriter struct {
	messages   []string
	priorities []journal.Priority
	vars       []map[string]string
	sendError  error
}

func (m *mockJournalWriter) Send(message string, priority journal.Priority, vars map[string]string) error {
	if m.sendError != nil {
		return m.sendError
	}
	m.messages = append(m.messages, message)
	m.priorities = append(m.priorities, priority)
	m.vars = append(m.vars, vars)
	return nil
}

func TestJournaldLogger_PriorityMapping(t *testing.T) {
	tests := []struct {
		level            string
		expectedPriority journal.Priority
	}{
		{"error", journal.PriErr},
		{"warn", journal.PriWarning},
		{"info", journal.PriInfo},
		{"debug", journal.PriDebug},
		{"unknown", journal.PriInfo}, // default
	}

	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			mock := &mockJournalWriter{}
			logger := &journaldLogger{writer: mock}

			level.Info(logger).Log("level", tc.level, "msg", "test message")

			require.Len(t, mock.priorities, 1)
			require.Equal(t, tc.expectedPriority, mock.priorities[0])
		})
	}
}

func TestJournaldLogger_FieldExtraction(t *testing.T) {
	tests := []struct {
		name          string
		keyvals       []interface{}
		expectedMsg   string
		expectedVars  map[string]string
		expectedLevel journal.Priority
	}{
		{
			name:          "basic fields",
			keyvals:       []interface{}{"level", "info", "msg", "hello world", "caller", "main.go:10"},
			expectedMsg:   "hello world",
			expectedVars:  map[string]string{"THANOS_CALLER": "main.go:10"},
			expectedLevel: journal.PriInfo,
		},
		{
			name:          "multiple custom fields",
			keyvals:       []interface{}{"level", "error", "msg", "failed", "component", "receiver", "tenant", "acme"},
			expectedMsg:   "failed",
			expectedVars:  map[string]string{"THANOS_COMPONENT": "receiver", "THANOS_TENANT": "acme"},
			expectedLevel: journal.PriErr,
		},
		{
			name:          "special characters in field names",
			keyvals:       []interface{}{"level", "warn", "msg", "warning", "http.method", "GET", "trace-id", "abc123"},
			expectedMsg:   "warning",
			expectedVars:  map[string]string{"THANOS_HTTP_METHOD": "GET", "THANOS_TRACE_ID": "abc123"},
			expectedLevel: journal.PriWarning,
		},
		{
			name:          "empty message",
			keyvals:       []interface{}{"level", "info", "msg", ""},
			expectedMsg:   "",
			expectedVars:  map[string]string{},
			expectedLevel: journal.PriInfo,
		},
		{
			name:          "no custom fields",
			keyvals:       []interface{}{"level", "debug", "msg", "debug info"},
			expectedMsg:   "debug info",
			expectedVars:  map[string]string{},
			expectedLevel: journal.PriDebug,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockJournalWriter{}
			logger := &journaldLogger{writer: mock}

			err := logger.Log(tc.keyvals...)
			require.NoError(t, err)

			require.Len(t, mock.messages, 1)
			require.Equal(t, tc.expectedMsg, mock.messages[0])
			require.Equal(t, tc.expectedLevel, mock.priorities[0])
			require.Equal(t, tc.expectedVars, mock.vars[0])
		})
	}
}

func TestJournaldLogger_OddNumberOfKeyvals(t *testing.T) {
	mock := &mockJournalWriter{}
	logger := &journaldLogger{writer: mock}

	// Orphan keys without values should be dropped.
	err := logger.Log("level", "info", "msg", "test", "orphan")
	require.NoError(t, err)

	require.Len(t, mock.messages, 1)
	require.Equal(t, "test", mock.messages[0])
	require.NotContains(t, mock.vars[0], "THANOS_ORPHAN")
	require.Empty(t, mock.vars[0], "vars should be empty since orphan key was dropped")
}

func TestJournaldLogger_SendError(t *testing.T) {
	mock := &mockJournalWriter{
		sendError: errors.New("mock error"),
	}
	logger := &journaldLogger{writer: mock}

	err := logger.Log("level", "info", "msg", "test")

	require.Error(t, err)
	require.EqualError(t, err, "mock error")
}
