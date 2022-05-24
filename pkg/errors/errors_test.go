// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package errors

import (
	//lint:ignore faillint Custom errors package tests need to import standard library errors.
	stderrors "errors"
	"fmt"
	"regexp"
	"strconv"
	"testing"
)

const msg = "test_error_message"
const wrapper = "test_wrapper"

func TestNewf(t *testing.T) {
	err := Newf(msg)
	if err.Error() != msg {
		t.Fatalf("failed to match the root error message: %+v", err)
	}

	reg := regexp.MustCompile(msg + `[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestNewf	.*\/pkg\/errors\/errors_test\.go:\d+`)
	if !reg.MatchString(fmt.Sprintf("%+v", err)) {
		t.Fatalf("failed to match stacktrace in errors.New: %+v", err)
	}
}

func TestNewfFormatted(t *testing.T) {
	fmtMsg := msg + " key=%v"
	expectedMsg := msg + " key=value"

	err := Newf(fmtMsg, "value")
	if err.Error() != expectedMsg {
		t.Fatalf("failed to match the root error message: %+v", err)
	}
	reg := regexp.MustCompile(expectedMsg + `[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestNewfFormatted	.*\/pkg\/errors\/errors_test\.go:\d+`)
	if !reg.MatchString(fmt.Sprintf("%+v", err)) {
		t.Fatalf("failed to match stacktrace in errors.New with format string: %+v", err)
	}
}

func TestWrapf(t *testing.T) {
	err := Newf(msg)
	err = Wrapf(err, wrapper)

	expectedMsg := wrapper + ": " + msg
	if err.Error() != expectedMsg {
		t.Fatalf("failed to match the root error message: %+v", err)
	}

	reg := regexp.MustCompile(`test_wrapper[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestWrapf	.*\/pkg\/errors\/errors_test\.go:\d+
[[:ascii:]]+test_error_message[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestWrapf	.*\/pkg\/errors\/errors_test\.go:\d+`)

	if !reg.MatchString(fmt.Sprintf("%+v", err)) {
		t.Fatalf("failed to match stacktrace in errors.Wrapf: %+v", err)
	}
}

func TestUnwrap(t *testing.T) {
	// test with base error
	err := Newf(msg)

	for i, tc := range []struct {
		err      error
		expected string
		isNil    bool
	}{
		{
			// no wrapping
			err:   err,
			isNil: true,
		},
		{
			err:      Wrapf(err, wrapper),
			expected: "test_error_message",
		},
		{
			err:      Wrapf(Wrapf(err, wrapper), wrapper),
			expected: "test_wrapper: test_error_message",
		},
		// check primitives errors
		{
			err:   stderrors.New("std-error"),
			isNil: true,
		},
		{
			err:      Wrapf(stderrors.New("std-error"), wrapper),
			expected: "std-error",
		},
		{
			err:   nil,
			isNil: true,
		},
	} {
		t.Run("TestCase"+strconv.Itoa(i), func(t *testing.T) {
			unwrapped := Unwrap(tc.err)
			if tc.isNil {
				if unwrapped != nil {
					t.Fatalf("expected nil, received %+v", unwrapped)
				}
				return
			}
			if unwrapped.Error() != tc.expected {
				t.Fatalf("failed to match 'Unwrapped' output with expected error output: %+v", unwrapped)
			}
		})
	}
}

func TestCause(t *testing.T) {
	// test with base error that implements interface containing Unwrap method
	err := Newf(msg)

	for i, tc := range []struct {
		err      error
		expected string
		isNil    bool
	}{
		{
			// no wrapping
			err:   err,
			isNil: true,
		},
		{
			err:   Wrapf(err, wrapper),
			isNil: true,
		},
		{
			err:   Wrapf(Wrapf(err, wrapper), wrapper),
			isNil: true,
		},
		// check primitives errors
		{
			err:      stderrors.New("std-error"),
			expected: "std-error",
		},
		{
			err:      Wrapf(stderrors.New("std-error"), wrapper),
			expected: "std-error",
		},
		{
			err:   nil,
			isNil: true,
		},
	} {
		t.Run("TestCase"+strconv.Itoa(i), func(t *testing.T) {
			cause := Cause(tc.err)
			if tc.isNil {
				if cause != nil {
					t.Fatalf("expected nil, received %+v", cause)
				}
				return
			}
			if cause.Error() != tc.expected {
				t.Fatalf("failed to match 'Cause' output with expected error output: %+v", cause)
			}
		})
	}
}
