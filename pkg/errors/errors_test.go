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

	"github.com/efficientgo/core/testutil"
)

const msg = "test_error_message"
const wrapper = "test_wrapper"

func TestNewf(t *testing.T) {
	err := Newf(msg)
	testutil.Equals(t, err.Error(), msg, "the root error message must match")

	reg := regexp.MustCompile(msg + `[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestNewf	.*\/pkg\/errors\/errors_test\.go:\d+`)
	testutil.Equals(t, reg.MatchString(fmt.Sprintf("%+v", err)), true, "matching stacktrace in errors.New")
}

func TestNewfFormatted(t *testing.T) {
	fmtMsg := msg + " key=%v"
	expectedMsg := msg + " key=value"

	err := Newf(fmtMsg, "value")
	testutil.Equals(t, err.Error(), expectedMsg, "the root error message must match")
	reg := regexp.MustCompile(expectedMsg + `[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestNewfFormatted	.*\/pkg\/errors\/errors_test\.go:\d+`)
	testutil.Equals(t, reg.MatchString(fmt.Sprintf("%+v", err)), true, "matching stacktrace in errors.New with format string")
}

func TestWrapf(t *testing.T) {
	err := Newf(msg)
	err = Wrapf(err, wrapper)

	expectedMsg := wrapper + ": " + msg
	testutil.Equals(t, err.Error(), expectedMsg, "the root error message must match")

	reg := regexp.MustCompile(`test_wrapper[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestWrapf	.*\/pkg\/errors\/errors_test\.go:\d+
[[:ascii:]]+test_error_message[ \n]+> github\.com\/thanos-io\/thanos\/pkg\/errors\.TestWrapf	.*\/pkg\/errors\/errors_test\.go:\d+`)

	testutil.Equals(t, reg.MatchString(fmt.Sprintf("%+v", err)), true, "matching stacktrace in errors.Wrap")
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
				testutil.Equals(t, unwrapped, nil)
				return
			}
			testutil.Equals(t, unwrapped.Error(), tc.expected, "Unwrap must match expected output")
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
				testutil.Equals(t, cause, nil)
				return
			}
			testutil.Equals(t, cause.Error(), tc.expected, "Cause must match expected output")
		})
	}
}
