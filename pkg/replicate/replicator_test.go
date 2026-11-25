// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package replicate

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/oklog/run"
)

func TestRunReplicateInvalidConcurrency(t *testing.T) {
	var g run.Group
	err := RunReplicate(
		&g,
		nil,
		nil,
		nil,
		"",
		"",
		0,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		0,
		nil,
		nil,
		nil,
		false,
	)
	testutil.Assert(t, err != nil, "expected error for concurrency < 1")
	testutil.Equals(t, "min concurrency is 1, but got 0", err.Error())

	errNegative := RunReplicate(
		&g,
		nil,
		nil,
		nil,
		"",
		"",
		0,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		-5,
		nil,
		nil,
		nil,
		false,
	)
	testutil.Assert(t, errNegative != nil, "expected error for concurrency < 1")
	testutil.Equals(t, "min concurrency is 1, but got -5", errNegative.Error())
}
