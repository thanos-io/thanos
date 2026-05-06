// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/coreos/go-systemd/v22/journal"
)

type journalWriter interface {
	Send(message string, priority journal.Priority, vars map[string]string) error
}

type systemdJournalWriter struct{}

func (s *systemdJournalWriter) Send(message string, priority journal.Priority, vars map[string]string) error {
	return journal.Send(message, priority, vars)
}

// journaldLogger implements go-kit/log.Logger interface for systemd journal integration.
type journaldLogger struct {
	writer journalWriter
}

func newJournaldLogger() *journaldLogger {
	return &journaldLogger{writer: &systemdJournalWriter{}}
}

// Log implements log.Logger. It maps "level" field to journald priority,
// "msg" to MESSAGE, and other fields to THANOS_<KEY> structured fields.
// It ensures all field keys are valid journald field names.
// If no "msg" is provided, falls back to "err" value for MESSAGE to comply
// with journald specification requiring a non-empty MESSAGE field.
func (l *journaldLogger) Log(keyvals ...interface{}) error {
	var (
		lvl     string
		msg     string
		errVal  string
		vars    = make(map[string]string)
		varsLen = len(keyvals)
	)

	for i := 0; i < varsLen; i += 2 {
		key := toString(keyvals[i])
		var val interface{}
		if i+1 < varsLen {
			val = keyvals[i+1]
		} else {
			// Drop orphan keys without values
			continue
		}

		switch key {
		case "level":
			lvl = toString(val)
		case "msg":
			msg = toString(val)
		case "err":
			errVal = toString(val)
			vars[toJournalField(key)] = errVal
		default:
			vars[toJournalField(key)] = toString(val)
		}
	}

	// Ensure MESSAGE field is not empty per journald spec.
	// Use error value as fallback if no explicit message is provided.
	if msg == "" {
		if errVal != "" {
			msg = errVal
		} else {
			msg = "(no message)"
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

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case error:
		return val.Error()
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}

func toJournalField(key string) string {
	var b strings.Builder
	b.Grow(len(key) + 7)
	b.WriteString("THANOS_")

	mapped := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return '_'
	}, key)

	b.WriteString(strings.ToUpper(mapped))
	return b.String()
}
