// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package otlptranslator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEveryNTimes(t *testing.T) {
	const n = 128
	ctx, cancel := context.WithCancel(context.Background())
	e := &everyNTimes{
		n: n,
	}

	for i := 0; i < n; i++ {
		require.NoError(t, e.checkContext(ctx))
	}

	cancel()
	for i := 0; i < n-1; i++ {
		require.NoError(t, e.checkContext(ctx))
	}
	require.EqualError(t, e.checkContext(ctx), context.Canceled.Error())
	// e should remember the error.
	require.EqualError(t, e.checkContext(ctx), context.Canceled.Error())
}
