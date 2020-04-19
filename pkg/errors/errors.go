// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// TODO(bwplotka): This package is meant to be available at some point as separate module for easier consume
// once we use it fully in Thanos. Potentially via modularize.

package errors

import (
	"fmt"
	"io"

	"github.com/thanos-io/thanos/pkg/errors/internal/stack"
)

// NilErrorWrappedErr is a sentinel error representing case of wrapping nil error. This helps to avoid a popular case
// of hard to spot bugs when user wraps error and accidentally does not pass error properly, but nil instead.
// All invokes of `errors.Wrap` with nil error are considered a bug.
var NilErrorWrappedErr = New("unknown error (nil error wrapped)")

// New returns an error with the supplied message formatted as in Sprintf.
// It also records the stack trace at the point it was called.
func New(format string, args ...interface{}) error {
	return &errFrame{
		msg:   fmt.Sprintf(format, args...),
		Stack: stack.New(3),
	}
}

// Wrap returns an error annotating err with a stack trace
// at the point Wrap is called, and the format specifier.
// If err is nil, Wrap wraps NilErrorWrappedErr.
// TODO(bwplotka): If requested, we can add ability to disable stack trace tracking
// for better performance, probably via package-level switch.
func Wrap(err error, format string, args ...interface{}) error {
	if err == nil {
		return Wrap(NilErrorWrappedErr, format, args...)
	}
	return &errFrame{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
		Stack: stack.New(3),
	}
}

type causer interface {
	// We keep cause to have exactly same "cause" method name as in Go1.13 for compatibility.
	Unwrap() error
}

// Cause returns the underlying cause of the error, if possible.
// An error value has a cause if it implements the causer interface.
// If the error does not implement Cause or the returned error from Cause's cause interface is nil,
// the original error will be returned.
//
// If the err error is nil, nil will be returned without further
// investigation.
func Cause(err error) error {
	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		cerr := cause.Unwrap()
		if cerr == nil {
			break
		}
		err = cerr
	}
	return err
}

type errFrame struct {
	*stack.Stack

	cause error
	msg   string
}

// Cause provides compatibility for pkg/go error chains.
func (e *errFrame) Cause() error { return e.cause }

// Unwrap provides compatibility for Go 1.13 error chains.
func (e *errFrame) Unwrap() error { return e.cause }

func (e *errFrame) Error() string {
	r := e.msg
	if e.cause != nil {
		return r + ": " + e.cause.Error()
	}
	return r
}

func (e *errFrame) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = io.WriteString(s, e.Error())
			e.Stack.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, e.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.Error())
	default:
		_, _ = fmt.Fprintf(s, "(not supported %q verb used) for string value. ('s' 'v' '+v' allowed only for errFrame value %q)", string(verb), e.Error())
	}
}
