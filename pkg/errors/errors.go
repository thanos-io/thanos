// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

//nolint
// The idea of writing errors package in thanos is highly motivated from the Tast project of Chromium OS Authors. However, instead of
// copying the package, we end up writing our own simplified logic borrowing some ideas from the errors and github.com/pkg/errors.
// A big thanks to all of them.

// Package errors provides basic utilities to manipulate errors with a useful stacktrace. It combines the
// benefits of errors.New and fmt.Errorf world into a single package.
package errors

import (
	//lint:ignore faillint Custom errors package needs to import standard library errors.
	"errors"
	"fmt"
	"strings"
)

// base is the fundamental struct that implements the error interface and the acts as the backbone of this errors package.
type base struct {
	// info contains the error message passed through calls like errors.Wrap, errors.New.
	info string
	// stacktrace stores information about the program counters - i.e. where this error was generated.
	stack stacktrace
	// err is the actual error which is being wrapped with a stacktrace and message information.
	err error
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
// Whenever error is printed with %+v format verb, stacktrace info gets dumped to the output.
func (b *base) Format(s fmt.State, verb rune) {
	if verb == 'v' && s.Flag('+') {
		s.Write([]byte(formatErrorChain(b)))
		return
	}

	s.Write([]byte(b.Error()))
}

// Newf formats according to a format specifier and returns a new error with a stacktrace
// with recent call frames. Each call to New returns a distinct error value even if the text is
// identical. An alternative of the errors.New function.
//
// If no args have been passed, it is same as `New` function without formatting. Character like
// '%' still has to be escaped in that scenario.
func Newf(format string, args ...interface{}) error {
	return &base{
		info:  fmt.Sprintf(format, args...),
		stack: newStackTrace(),
		err:   nil,
	}
}

// Wrapf returns a new error by formatting the error message with the supplied format specifier
// and wrapping another error with a stacktrace containing recent call frames.
//
// If cause is nil, this is the same as fmt.Errorf. If no args have been passed, it is same as `Wrap`
// function without formatting. Character like '%' still has to be escaped in that scenario.
func Wrapf(cause error, format string, args ...interface{}) error {
	return &base{
		info:  fmt.Sprintf(format, args...),
		stack: newStackTrace(),
		err:   cause,
	}
}

// Cause returns the result of repeatedly calling the Unwrap method on err, if err's
// type implements an Unwrap method. Otherwise, Cause returns the last encountered error.
// The difference between Unwrap and Cause is the first one performs unwrapping of one level
// but Cause returns the last err (whether it's nil or not) where it failed to assert
// the interface containing the Unwrap method.
// This is a replacement of errors.Cause without the causer interface from pkg/errors which
// actually can be sufficed through the errors.Is function. But considering some use cases
// where we need to peel off all the external layers applied through errors.Wrap family,
// it is useful ( where external SDK doesn't use errors.Is internally).
func Cause(err error) error {
	for err != nil {
		e, ok := err.(interface {
			Unwrap() error
		})
		if !ok {
			return err
		}
		err = e.Unwrap()
	}
	return nil
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

// The functions `Is`, `As` & `Unwrap` provides a thin wrapper around the builtin errors
// package in go. Just for sake of completeness and correct autocompletion behaviors from
// IDEs they have been wrapped using functions instead of using variable to reference them
// as first class functions (eg: var Is = errros.Is ).

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

// Unwrap is a wrapper of built-in errors.Unwrap. Unwrap returns the result of
// calling the Unwrap method on err, if err's type contains an Unwrap method
// returning error. Otherwise, Unwrap returns nil.
func Unwrap(err error) error {
	return errors.Unwrap(err)
}
