// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package gate

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestGateAllowsDisablingLimits(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := New(reg, 0)

	require.NoError(t, g.Start(context.Background()))
}
