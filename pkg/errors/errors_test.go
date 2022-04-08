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

	"github.com/thanos-io/thanos/pkg/testutil"
)

const msg = "test_error_message"
const wrapper = "test_wrapper"

func TestNew(t *testing.T) {
	err := New(msg)
	testutil.Equals(t, err.Error(), msg, "the root error message must match")

	reg := regexp.MustCompile(msg + `[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestNew	.*\/pkg\/errors\/errors_test\.go:\d+`)
	testutil.Equals(t, reg.MatchString(fmt.Sprintf("%+v", err)), true, "matching stacktrace in errors.New")
}

func TestErrorf(t *testing.T) {
	fmtMsg := msg + " key=%v"
	expectedMsg := msg + " key=value"

	err := Errorf(fmtMsg, "value")
	testutil.Equals(t, err.Error(), expectedMsg, "the root error message must match")
	reg := regexp.MustCompile(expectedMsg + `[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestErrorf	.*\/pkg\/errors\/errors_test\.go:\d+`)
	testutil.Equals(t, reg.MatchString(fmt.Sprintf("%+v", err)), true, "matching stacktrace in errors.New with format string")
}

func TestWrap(t *testing.T) {
	err := New(msg)
	err = Wrap(err, wrapper)

	expectedMsg := wrapper + ": " + msg
	testutil.Equals(t, err.Error(), expectedMsg, "the root error message must match")

	reg := regexp.MustCompile(`test_wrapper[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestWrap	.*\/pkg\/errors\/errors_test\.go:\d+
[[:ascii:]]+test_error_message[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestWrap	.*\/pkg\/errors\/errors_test\.go:\d+`)

	t.Logf("%+v", err)
	testutil.Equals(t, reg.MatchString(fmt.Sprintf("%+v", err)), true, "matching stacktrace in errors.Wrap")
}

func TestUnwrap(t *testing.T) {
	// test with base error
	err := New(msg)

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
			err:      Wrap(err, wrapper),
			expected: "test_error_message",
		},
		{
			err:      Wrap(Wrap(err, wrapper), wrapper),
			expected: "test_wrapper: test_error_message",
		},
		// check primitives errors
		{
			err:   stderrors.New("std-error"),
			isNil: true,
		},
		{
			err:      Wrap(stderrors.New("std-error"), wrapper),
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
				testutil.Equals(t, unwrapped, nil)
				return
			}
			testutil.Equals(t, unwrapped.Error(), tc.expected, "Unwrap must match expected output")
		})
	}
}

func TestCause(t *testing.T) {
	// test with base error that implements interface containing Unwrap method
	err := New(msg)

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
			err:   Wrap(err, wrapper),
			isNil: true,
		},
		{
			err:   Wrap(Wrap(err, wrapper), wrapper),
			isNil: true,
		},
		// check primitives errors
		{
			err:      stderrors.New("std-error"),
			expected: "std-error",
		},
		{
			err:      Wrap(stderrors.New("std-error"), wrapper),
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
				testutil.Equals(t, cause, nil)
				return
			}
			testutil.Equals(t, cause.Error(), tc.expected, "Cause must match expected output")
		})
	}
}
