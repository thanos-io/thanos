// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package grpc

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
)

func TestWithKeepaliveParams(t *testing.T) {
	opts := &options{}

	opt := WithKeepaliveParams(15*time.Second, 10*time.Second, true)
	opt.apply(opts)

	testutil.Equals(t, 15*time.Second, opts.keepaliveTime)
	testutil.Equals(t, 10*time.Second, opts.keepaliveTimeout)
	testutil.Equals(t, true, opts.keepalivePermitWithoutStream)
}

func TestWithKeepaliveEnforcementPolicy(t *testing.T) {
	opts := &options{}

	opt := WithKeepaliveEnforcementPolicy(5*time.Second, true)
	opt.apply(opts)

	testutil.Equals(t, 5*time.Second, opts.keepaliveMinTime)
}

func TestWithInitialWindowSize(t *testing.T) {
	opts := &options{}

	opt := WithInitialWindowSize(1024, 2048)
	opt.apply(opts)

	testutil.Equals(t, int32(1024), opts.initialWindowSize)
	testutil.Equals(t, int32(2048), opts.initialConnWindowSize)
}
