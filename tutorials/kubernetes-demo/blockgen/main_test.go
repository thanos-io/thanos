package main

import (
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestCounterGen(t *testing.T) {
	g := &counterGen{
		minTime:      100,
		maxTime:      int64((24 * time.Hour).Seconds()) * 1000,
		interval:     15 * time.Second,
		rateInterval: 5 * time.Minute,
		min:          100,
		max:          400,
		jitter:       300,
	}

	lastV := float64(0)
	lastT := int64(0)

	init := false
	samples := int64(0)
	for g.Next() {
		samples++
		if init {
			testutil.Assert(t, lastV <= g.Value(), "")
			testutil.Assert(t, lastT <= g.Ts(), "")
			init = true
		}
		lastV = g.Value()
		lastT = g.Ts()
	}
	testutil.Equals(t, int64((24*time.Hour)/(15*time.Second)), samples)
}
