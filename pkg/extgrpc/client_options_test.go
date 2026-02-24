// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extgrpc

import (
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
)

func TestDefaultClientOptions(t *testing.T) {
	opts := defaultClientOptions()

	testutil.Equals(t, 10*time.Second, opts.keepaliveTime)
	testutil.Equals(t, 5*time.Second, opts.keepaliveTimeout)
	testutil.Equals(t, true, opts.keepalivePermitWithoutStream)
	testutil.Equals(t, int32(4<<20), opts.initialWindowSize)
	testutil.Equals(t, int32(4<<20), opts.initialConnWindowSize)
}

func TestWithKeepaliveParams(t *testing.T) {
	opts := defaultClientOptions()

	opt := WithKeepaliveParams(20*time.Second, 10*time.Second, false)
	opt(opts)

	testutil.Equals(t, 20*time.Second, opts.keepaliveTime)
	testutil.Equals(t, 10*time.Second, opts.keepaliveTimeout)
	testutil.Equals(t, false, opts.keepalivePermitWithoutStream)
}

func TestWithInitialWindowSize(t *testing.T) {
	opts := defaultClientOptions()

	opt := WithInitialWindowSize(1024, 2048)
	opt(opts)

	testutil.Equals(t, int32(1024), opts.initialWindowSize)
	testutil.Equals(t, int32(2048), opts.initialConnWindowSize)
}
