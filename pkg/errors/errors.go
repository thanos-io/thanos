// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// The idea of writing errors package in thanos is highly motivated from the Tast project of Chromium OS Authors. However, instead of
// copying the package, we end up writing our own simplified logic borrowing some ideas from the errors and github.com/pkg/errors.
// A big thanks to all of them.

// Package errors provides basic utilities to manipulate errors with a useful stacktrace. It combines the
// benefits of errors.New and fmt.Errorf world into a single package.
package errors

import (
	"errors"
	"fmt"
	"strings"
)

// base is the fundamental struct that implements the error interface and the acts as the backbone of this errors package.
type base struct {
	info  string     // error message passed through calls like errors.Wrap, errors.Errorf
	stack stacktrace // stacktrace where this error was generated
	err   error      // actual error which
}

// Error implements the error interface.
func (b *base) Error() string {
	if b.err != nil {
		return fmt.Sprintf("%s: %s", b.info, b.err.Error())
	}
	return b.info
}

// Unwrap implements the error Unwrap interface.
func (b *base) Unwrap() error {
	return b.err
}

// Format implements the fmt.Formatter interface to support the formatting of an error chain with "%+v" verb.
func (b *base) Format(s fmt.State, verb rune) {
	if verb == 'v' && s.Flag('+') {
		s.Write([]byte(formatErrorChain(b)))
		return
	}

	s.Write([]byte(b.Error()))
}

// Public functions that can be directly used through the package

// New creates a new error with the given message and a stacktrace in details.
// An alternative to errors.New function.
func New(msg string) error {
	return &base{
		info:  msg,
		stack: newStackTrace(),
		err:   nil,
	}
}

// Errorf creates a new error with the given message and a stacktrace in details.
// An alternative to fmt.Errorf function.
func Errorf(format string, args ...interface{}) error {
	return &base{
		info:  fmt.Sprintf(format, args...),
		stack: newStackTrace(),
		err:   nil,
	}
}

// Wrap creates a new error with the given message, wrapping another error and a stacktrace in details.
// If cause is nil, this is the same as New.
func Wrap(cause error, msg string) error {
	return &base{
		info:  msg,
		stack: newStackTrace(),
		err:   cause,
	}
}

// Wrapf creates a new error with the given message, wrapping another error and a stacktrace in details.
// If cause is nil, this is the same as Errorf.
func Wrapf(cause error, format string, args ...interface{}) error {
	return &base{
		info:  fmt.Sprintf(format, args...),
		stack: newStackTrace(),
		err:   cause,
	}
}

// Unwrap is a wrapper of built-in errors.Unwrap. Unwrap returns the result of
// calling the Unwrap method on err, if err's type contains an Unwrap method
// returning error. Otherwise, Unwrap returns nil.
func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// Is is a wrapper of built-in errors.Is. It reports whether any error in err's
// chain matches target. The chain consists of err itself followed by the sequence
// of errors obtained by repeatedly calling Unwrap.
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As is a wrapper of built-in errors.As. It finds the first error in err's
// chain that matches target, and if one is found, sets target to that error
// value and returns true. Otherwise, it returns false.
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

// formatErrorChain formats an error chain.
func formatErrorChain(err error) string {
	var buf strings.Builder
	for err != nil {
		if e, ok := err.(*base); ok {
			buf.WriteString(fmt.Sprintf("%s\n%v", e.info, e.stack))
			err = e.err
		} else {
			buf.WriteString(fmt.Sprintf("%s\n", err.Error()))
			err = nil
		}
	}
	return buf.String()
}
