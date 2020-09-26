// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/thanos/pkg/testutil"
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

func TestNewWithFailedCounterFrom(t *testing.T) {
	c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	expectedLimit := uint64(42)
	l := NewLimiter(expectedLimit, c)

	t.Run("should return error when from limiter is nil", func(t *testing.T) {
		var nilLimiter *Limiter
		_, err := l.NewWithFailedCounterFrom(nilLimiter)
		testutil.NotOk(t, err)
	})

	t.Run("should return error when from limiter is of different type", func(t *testing.T) {
		unknownLimiter := struct {
			ChunksLimiter
		}{}
		_, err := l.NewWithFailedCounterFrom(unknownLimiter)
		testutil.NotOk(t, err)
	})

	t.Run("should create new limiter with given counter", func(t *testing.T) {
		c2 := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
		limit := uint64(1)
		counterSource := NewLimiter(limit, c2)

		err := l.Reserve(expectedLimit - 1)
		testutil.Ok(t, err)

		res, err := l.NewWithFailedCounterFrom(counterSource)
		testutil.Ok(t, err)

		resLimiter, ok := res.(*Limiter)
		if !ok || resLimiter == nil {
			t.Fatalf("unexpected limiter in result %#v", resLimiter)
		}

		testutil.Equals(t, resLimiter.failedCounter, counterSource.failedCounter)
		testutil.Equals(t, resLimiter.failedOnce, counterSource.failedOnce)
		testutil.Equals(t, resLimiter.limit, expectedLimit)
		testutil.Equals(t, resLimiter.reserved, l.reserved)
	})
}

func TestLimiterFailedOnce(t *testing.T) {
	c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	factoryWithLimit1 := NewChunksLimiterFactory(1)
	factoryWithLimit2 := NewChunksLimiterFactory(2)

	l1 := factoryWithLimit1(c)
	l2, err := factoryWithLimit2(c).NewWithFailedCounterFrom(l1)
	testutil.Ok(t, err)

	testutil.Ok(t, l1.Reserve(1))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.Ok(t, l2.Reserve(2))
	testutil.Equals(t, float64(0), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l1.Reserve(1))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))

	testutil.NotOk(t, l2.Reserve(1))
	testutil.Equals(t, float64(1), prom_testutil.ToFloat64(c))
}
