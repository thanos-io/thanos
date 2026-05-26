// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package p2c

import (
	"encoding/json"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

// fakeSubConn implements balancer.SubConn just well enough for the picker
// tests. We never call Connect/Shutdown on these in this file.
type fakeSubConn struct {
	balancer.SubConn
	id string
}

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := builder{}.ParseConfig(nil)
	require.NoError(t, err)
	c := cfg.(*LBConfig)
	require.Equal(t, uint32(defaultPickCount), c.PickCount)
	require.Equal(t, defaultProbeInterval, c.probeInterval)
	require.Equal(t, defaultProbeTimeout, c.probeTimeout)
	require.Equal(t, defaultLatencyHalfLife, c.latencyHalfLife)
	require.Equal(t, uint32(defaultConsecutiveFailureThreshold), c.ConsecutiveFailureThreshold)
}

func TestParseConfigCustom(t *testing.T) {
	raw := json.RawMessage(`{"pickCount":3,"probeInterval":"15s","probeTimeout":"2s","latencyHalfLife":"1m","consecutiveFailureThreshold":5}`)
	cfg, err := builder{}.ParseConfig(raw)
	require.NoError(t, err)
	c := cfg.(*LBConfig)
	require.Equal(t, uint32(3), c.PickCount)
	require.Equal(t, 15*time.Second, c.probeInterval)
	require.Equal(t, 2*time.Second, c.probeTimeout)
	require.Equal(t, time.Minute, c.latencyHalfLife)
	require.Equal(t, uint32(5), c.ConsecutiveFailureThreshold)
}

func TestParseConfigClampsAndRejects(t *testing.T) {
	// pickCount=20 clamps to maxPickCount.
	cfg, err := builder{}.ParseConfig(json.RawMessage(`{"pickCount":20}`))
	require.NoError(t, err)
	require.Equal(t, uint32(maxPickCount), cfg.(*LBConfig).PickCount)

	// pickCount=1 is rejected.
	_, err = builder{}.ParseConfig(json.RawMessage(`{"pickCount":1}`))
	require.Error(t, err)

	// Bogus duration is rejected.
	_, err = builder{}.ParseConfig(json.RawMessage(`{"probeInterval":"notaduration"}`))
	require.Error(t, err)

	// Non-positive duration is rejected.
	_, err = builder{}.ParseConfig(json.RawMessage(`{"probeTimeout":"0s"}`))
	require.Error(t, err)
}

func TestPickerEmptyReturnsNoSubConn(t *testing.T) {
	p := &picker{rng: rand.New(rand.NewPCG(1, 2))}
	_, err := p.Pick(balancer.PickInfo{})
	require.ErrorIs(t, err, balancer.ErrNoSubConnAvailable)
}

func TestPickerSingleReturnsThatSubConn(t *testing.T) {
	sc := &fakeSubConn{id: "only"}
	p := &picker{
		ready:     []readySubConn{{sc: sc, addr: "a", ewma: time.Millisecond, probed: true}},
		pickCount: 2,
		rng:       rand.New(rand.NewPCG(1, 2)),
	}
	res, err := p.Pick(balancer.PickInfo{})
	require.NoError(t, err)
	require.Same(t, sc, res.SubConn)
}

// TestPickerDistribution verifies P2C behaviour with two equally-sized
// populations (6 fast, 6 slow). The picker samples N *distinct* SubConns from
// the Ready set, so the chance that fast loses equals C(6,N)/C(12,N). That
// yields ~0.773 (N=2), ~0.909 (N=3), ~0.970 (N=4) traffic going to fast.
func TestPickerDistribution(t *testing.T) {
	const iterations = 20000
	const tolerance = 0.03

	cases := []struct {
		pickCount int
		want      float64
	}{
		{2, 0.773},
		{3, 0.909},
		{4, 0.970},
	}

	for _, tc := range cases {
		ready := make([]readySubConn, 0, 12)
		fastIDs := map[string]struct{}{}
		for i := 0; i < 6; i++ {
			sc := &fakeSubConn{id: "fast" + string(rune('0'+i))}
			ready = append(ready, readySubConn{sc: sc, addr: "fast" + string(rune('0'+i)), ewma: time.Millisecond, probed: true})
			fastIDs[sc.id] = struct{}{}
		}
		for i := 0; i < 6; i++ {
			sc := &fakeSubConn{id: "slow" + string(rune('0'+i))}
			ready = append(ready, readySubConn{sc: sc, addr: "slow" + string(rune('0'+i)), ewma: 100 * time.Millisecond, probed: true})
		}
		p := &picker{ready: ready, pickCount: tc.pickCount, rng: rand.New(rand.NewPCG(uint64(tc.pickCount), 42))}
		fast := 0
		for i := 0; i < iterations; i++ {
			res, err := p.Pick(balancer.PickInfo{})
			require.NoError(t, err)
			if _, ok := fastIDs[res.SubConn.(*fakeSubConn).id]; ok {
				fast++
			}
		}
		got := float64(fast) / float64(iterations)
		require.InDeltaf(t, tc.want, got, tolerance,
			"pickCount=%d: want ~%.3f got %.3f", tc.pickCount, tc.want, got)
	}
}

// TestPickerTieBreakDeterministic ensures that when sampled EWMAs match the
// pick is reproducible across runs given the same RNG (lowest address wins).
func TestPickerTieBreakDeterministic(t *testing.T) {
	ready := []readySubConn{
		{sc: &fakeSubConn{id: "b"}, addr: "b", ewma: time.Millisecond, probed: true},
		{sc: &fakeSubConn{id: "a"}, addr: "a", ewma: time.Millisecond, probed: true},
		{sc: &fakeSubConn{id: "c"}, addr: "c", ewma: time.Millisecond, probed: true},
	}
	// ready is unsorted on purpose; the balancer sorts before constructing
	// the picker, mirror that here.
	sorted := []readySubConn{ready[1], ready[0], ready[2]}
	p := &picker{ready: sorted, pickCount: 3, rng: rand.New(rand.NewPCG(1, 2))}
	res, err := p.Pick(balancer.PickInfo{})
	require.NoError(t, err)
	require.Equal(t, "a", res.SubConn.(*fakeSubConn).id)
}

// TestEWMAFirstSampleAdoptedWithLargeDt verifies that with a zero-valued
// lastProbe, the very large dt yields alpha=1 (saturated), so the first
// sample fully replaces the previous EWMA value. probed is also flipped.
func TestEWMAFirstSampleAdoptedWithLargeDt(t *testing.T) {
	b := &p2cBalancer{}
	st := &addrState{}
	sample := 10 * time.Millisecond
	now := time.Unix(1_000, 0)
	b.applyEWMASampleLocked(st, sample, now, 30*time.Second)
	require.True(t, st.probed)
	require.Equal(t, now, st.lastProbe)
	require.InDelta(t, float64(sample), float64(st.ewma), float64(time.Microsecond))
}

// TestEWMADecayMovesTowardSample exercises the decay formula: with halfLife
// equal to dt, a single sample should move the EWMA exactly halfway from the
// old value to the new one.
func TestEWMADecayMovesTowardSample(t *testing.T) {
	b := &p2cBalancer{}
	st := &addrState{ewma: 10 * time.Millisecond, probed: true, lastProbe: time.Unix(0, 0)}
	// dt == halfLife → α = 0.5, EWMA should land at the midpoint.
	b.applyEWMASampleLocked(st, 20*time.Millisecond, time.Unix(30, 0), 30*time.Second)
	require.InDelta(t, float64(15*time.Millisecond), float64(st.ewma), float64(100*time.Microsecond))
}

// TestEWMATimeoutSamplePoisons verifies that feeding a large timeout sample
// pulls the EWMA upward significantly, simulating the graduated penalty path.
func TestEWMATimeoutSamplePoisons(t *testing.T) {
	b := &p2cBalancer{}
	st := &addrState{ewma: 5 * time.Millisecond, probed: true, lastProbe: time.Unix(0, 0)}
	// Feed a 5s timeout sample after one half-life → EWMA must move halfway.
	b.applyEWMASampleLocked(st, 5*time.Second, time.Unix(30, 0), 30*time.Second)
	require.Greater(t, st.ewma, 2*time.Second, "EWMA must be pulled up by the timeout sample")
}

// TestRegeneratePickerEligibilityAfterReady verifies that a Ready SubConn
// is pickable immediately (before any probe lands) as long as it isn't
// soft-evicted or marked probeUnimplemented.
func TestRegeneratePickerEligibilityAfterReady(t *testing.T) {
	cfg := &LBConfig{PickCount: 2, ConsecutiveFailureThreshold: 3}
	b := &p2cBalancer{
		cfg:   cfg,
		addrs: resolver.NewAddressMapV2[*addrState](),
	}
	a := resolver.Address{Addr: "x:1"}
	st := &addrState{
		addr:                a,
		sc:                  &fakeSubConn{id: "x"},
		state:               connectivity.Ready,
		probed:              false,
		ewma:                0,
		consecutiveFailures: 0,
		probeUnimplemented:  false,
	}
	b.addrs.Set(a, st)
	b.regeneratePickerLocked()
	p, ok := b.picker.(*picker)
	require.True(t, ok, "expected *picker, got %T", b.picker)
	require.Len(t, p.ready, 1)
	require.Equal(t, "x:1", p.ready[0].addr)
	require.False(t, p.ready[0].probed)
}

// TestPickerProbedBeatsUnprobed verifies that the picker always prefers a
// probed SubConn over an unprobed one regardless of EWMA values.
func TestPickerProbedBeatsUnprobed(t *testing.T) {
	newcomer := readySubConn{sc: &fakeSubConn{id: "new"}, addr: "new", ewma: 0, probed: false}
	veteran := readySubConn{sc: &fakeSubConn{id: "vet"}, addr: "vet", ewma: 100 * time.Millisecond, probed: true}
	p := &picker{ready: []readySubConn{newcomer, veteran}, pickCount: 2, rng: rand.New(rand.NewPCG(1, 2))}
	for i := 0; i < 100; i++ {
		res, err := p.Pick(balancer.PickInfo{})
		require.NoError(t, err)
		require.Equal(t, "vet", res.SubConn.(*fakeSubConn).id, "veteran must always beat newcomer")
	}
}
