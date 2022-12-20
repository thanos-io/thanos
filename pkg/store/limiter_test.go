// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
)

func TestLimiter(t *testing.T) {
	c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	l := NewLimiter(10, c)

	testutil.Ok(t, l.Reserve(5))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.Ok(t, l.Reserve(5))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l.Reserve(1))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l.Reserve(2))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))
}
