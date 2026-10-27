// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"testing"

	"capnproto.org/go/capnp/v3/exp/bufferpool"
	"github.com/stretchr/testify/require"
)

func TestBufferPoolReusesLargeMessages(t *testing.T) {
	for _, tc := range []struct {
		size, cap int
	}{
		{600 << 10, 1 << 20},
		{5 << 20, 8 << 20},
	} {
		buf := bufferpool.Default.Get(tc.size)
		require.Equal(t, tc.cap, cap(buf), "size %d", tc.size)
		bufferpool.Default.Put(buf)
	}
}
