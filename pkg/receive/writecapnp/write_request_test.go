// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/stretchr/testify/require"
)

func TestNewRequest(t *testing.T) {
	arena := capnp.SingleSegment(nil)

	_, seg, err := capnp.NewMessage(arena)
	require.NoError(t, err)

	wr, err := NewRootWriteRequest(seg)
	require.NoError(t, err)

	symbols, err := NewSymbols(seg)
	require.NoError(t, err)

	require.NoError(t, symbols.SetData([]byte(`foobar`)))
	list, err := capnp.NewUInt32List(
		seg, 2,
	)
	require.NoError(t, err)
	list.Set(0, 3)
	list.Set(1, 6)

	require.NoError(t, symbols.SetOffsets(list))
	require.NoError(t, wr.SetSymbols(symbols))

	req, err := NewRequest(wr)
	require.NoError(t, err)

	require.Equal(t, "foo", (*req.symbols)[0])
	require.Equal(t, "bar", (*req.symbols)[1])

}
