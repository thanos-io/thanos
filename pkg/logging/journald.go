// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"strings"

	"github.com/coreos/go-systemd/v22/journal"
)

// journalWriter is an interface for sending logs to systemd journal.
type journalWriter interface {
	Send(message string, priority journal.Priority, vars map[string]string) error
}

// systemdJournalWriter is the actual implementation that writes to systemd journal.
type systemdJournalWriter struct{}

func (s *systemdJournalWriter) Send(message string, priority journal.Priority, vars map[string]string) error {
	return journal.Send(message, priority, vars)
}

// journaldLogger implements go-kit/log.Logger interface for systemd journal integration.
type journaldLogger struct {
	writer journalWriter
}

// newJournaldLogger creates a new journald logger that writes to systemd journal.
func newJournaldLogger() *journaldLogger {
	return &journaldLogger{writer: &systemdJournalWriter{}}
}

// Log implements log.Logger. It maps "level" field to journald priority,
// "msg" to MESSAGE, and other fields to THANOS_<KEY> structured fields.
// It ensures all field keys are valid journald field names.
func (l *journaldLogger) Log(keyvals ...interface{}) error {
	var (
		lvl     string
		msg     string
		vars    = make(map[string]string)
		varsLen = len(keyvals)
	)

	for i := 0; i < varsLen; i += 2 {
		key := fmt.Sprintf("%v", keyvals[i])
		var val interface{}
		if i+1 < varsLen {
			val = keyvals[i+1]
		} else {
			// Drop orphan keys without values
			continue
		}

		switch key {
		case "level":
			lvl = fmt.Sprintf("%v", val)
		case "msg":
			msg = fmt.Sprintf("%v", val)
		default:
			vars[toJournalField(key)] = fmt.Sprintf("%v", val)
		}
	}

	priority := journal.PriInfo
	switch lvl {
	case "error":
		priority = journal.PriErr
	case "warn":
		priority = journal.PriWarning
	case "debug":
		priority = journal.PriDebug
	}

	return l.writer.Send(msg, priority, vars)
}

func toJournalField(key string) string {
	key = strings.ToUpper(key)
	return "THANOS_" + strings.Map(func(r rune) rune {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, key)
}
