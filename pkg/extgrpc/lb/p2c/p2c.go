// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package p2c implements a gRPC load-balancing policy that selects SubConns
// using Power-of-N Choices ranked by an exponentially-weighted moving average
// (EWMA) of gRPC health-check probe latency. On every Pick we sample N
// (default 2) Ready SubConns with a recorded EWMA and return the one with the
// lowest latency. The aim is to bias traffic away from slow network paths and
// slow servers (e.g. IPv4/IPv6 asymmetry, partially-degraded backends) without
// imposing a hard cutoff.
//
// Each Ready SubConn runs its own probe goroutine that periodically issues a
// grpc.health.v1.Health/Check against the SubConn (via the per-SubConn
// producer mechanism). The first probe fires immediately on Ready so the
// EWMA is bootstrapped before the SubConn becomes pickable. Subsequent probes
// run at probeInterval with ±20% jitter. Timeouts feed the EWMA with the
// timeout duration (graduated penalty) and increment a consecutive-failure
// counter; once the counter crosses the threshold the SubConn is excluded
// from picks until the next successful probe.
package p2c

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/status"
)

// Name is the name under which the policy registers with gRPC.
const Name = "p2c"

const (
	defaultPickCount                   = 2
	minPickCount                       = 2
	maxPickCount                       = 16
	defaultProbeInterval               = 10 * time.Second
	defaultProbeTimeout                = 5 * time.Second
	defaultLatencyHalfLife             = 30 * time.Second
	defaultConsecutiveFailureThreshold = 3
	probeJitterFraction                = 0.2 // ±20%
)

// RegisterBalancer registers the p2c policy with gRPC, wiring the
// provided logger into the balancer for runtime diagnostics. Safe to call
// multiple times; subsequent calls overwrite the previously registered
// builder (gRPC's registry is keyed by Name).
func RegisterBalancer(logger log.Logger) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	balancer.Register(builder{logger: logger})
}

// LBConfig is the service-config representation of the policy's options.
type LBConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`

	// PickCount is the number of Ready SubConns to sample per Pick. Defaults
	// to 2, clamped to [2, 16].
	PickCount uint32 `json:"pickCount,omitempty"`
	// ProbeInterval is the period between health-check probes per SubConn.
	// Parsed as a duration string (e.g. "10s"). Defaults to 10s.
	ProbeInterval string `json:"probeInterval,omitempty"`
	// ProbeTimeout caps each individual probe RPC. Defaults to 5s.
	ProbeTimeout string `json:"probeTimeout,omitempty"`
	// LatencyHalfLife controls EWMA decay; smaller values are more reactive.
	// Defaults to 30s.
	LatencyHalfLife string `json:"latencyHalfLife,omitempty"`
	// ConsecutiveFailureThreshold is the number of consecutive probe failures
	// (timeout or error) after which a SubConn is excluded from picks until
	// the next successful probe. Defaults to 3.
	ConsecutiveFailureThreshold uint32 `json:"consecutiveFailureThreshold,omitempty"`

	// Parsed values, populated by ParseConfig.
	probeInterval   time.Duration `json:"-"`
	probeTimeout    time.Duration `json:"-"`
	latencyHalfLife time.Duration `json:"-"`
}

type builder struct {
	logger log.Logger
}

func (builder) Name() string { return Name }

func (builder) ParseConfig(s json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	cfg := &LBConfig{}
	if len(s) > 0 {
		if err := json.Unmarshal(s, cfg); err != nil {
			return nil, fmt.Errorf("p2c: unable to unmarshal LBConfig: %v", err)
		}
	}
	if cfg.PickCount == 0 {
		cfg.PickCount = defaultPickCount
	}
	if cfg.PickCount < minPickCount {
		return nil, fmt.Errorf("p2c: pickCount %d must be >= %d", cfg.PickCount, minPickCount)
	}
	if cfg.PickCount > maxPickCount {
		cfg.PickCount = maxPickCount
	}
	if err := parseDuration(cfg.ProbeInterval, defaultProbeInterval, "probeInterval", &cfg.probeInterval); err != nil {
		return nil, err
	}
	if err := parseDuration(cfg.ProbeTimeout, defaultProbeTimeout, "probeTimeout", &cfg.probeTimeout); err != nil {
		return nil, err
	}
	if err := parseDuration(cfg.LatencyHalfLife, defaultLatencyHalfLife, "latencyHalfLife", &cfg.latencyHalfLife); err != nil {
		return nil, err
	}
	if cfg.ConsecutiveFailureThreshold == 0 {
		cfg.ConsecutiveFailureThreshold = defaultConsecutiveFailureThreshold
	}
	return cfg, nil
}

func parseDuration(raw string, defValue time.Duration, name string, out *time.Duration) error {
	if raw == "" {
		*out = defValue
		return nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("p2c: invalid %s %q: %v", name, raw, err)
	}
	if d <= 0 {
		return fmt.Errorf("p2c: %s must be > 0, got %s", name, raw)
	}
	*out = d
	return nil
}

func (bb builder) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	logger := bb.logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	b := &p2cBalancer{
		cc:     cc,
		logger: logger,
		addrs:  resolver.NewAddressMapV2[*addrState](),
		bySC:   make(map[balancer.SubConn]*addrState),
	}
	// Until any SubConn is Ready and has a probe sample, Pick returns
	// ErrNoSubConnAvailable.
	b.picker = &errPicker{err: balancer.ErrNoSubConnAvailable}
	return b
}

// addrState tracks a single resolver address and its SubConn. The balancer
// mutex protects all mutable fields. `addr` and `sc` are set at construction
// time and never mutated thereafter, so they may be read without the lock.
type addrState struct {
	// addr is immutable after construction; safe to read without b.mu.
	addr resolver.Address

	// sc is set once in UpdateClientConnState and not mutated thereafter;
	// safe to read without b.mu.
	sc    balancer.SubConn
	state connectivity.State

	// EWMA bookkeeping.
	ewma time.Duration
	// probed is true once at least one probe has completed for the
	// current Ready cycle. The picker uses it to rank newcomers below
	// already-probed peers regardless of EWMA value.
	probed              bool
	lastProbe           time.Time
	consecutiveFailures uint32
	probeUnimplemented  bool

	// Per-SubConn probe goroutine cancellation. Set when state is Ready,
	// cleared (after invocation) when state leaves Ready or SubConn is shut
	// down.
	cancelProbe context.CancelFunc

	// Cached health producer for this SubConn. Built on Ready and released
	// when the SubConn leaves Ready (or shuts down). Reusing the producer
	// across probes avoids rebuilding the HealthClient on every interval.
	healthClient healthpb.HealthClient
	healthClose  func()
}

type p2cBalancer struct {
	cc     balancer.ClientConn
	logger log.Logger

	mu     sync.Mutex
	cfg    *LBConfig
	addrs  *resolver.AddressMapV2[*addrState]
	bySC   map[balancer.SubConn]*addrState
	picker balancer.Picker
	state  connectivity.State

	// probersWG tracks runProber goroutines so Close can wait for them to
	// exit after cancelling their contexts.
	probersWG sync.WaitGroup
}

// newRand returns a fresh RNG seeded from the math/rand/v2 global source.
// The global source is concurrency-safe and already well-mixed, so each
// caller gets independent jitter / pick sequences without any explicit
// pointer or time mixing.
func newRand() *rand.Rand {
	return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
}

func (b *p2cBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	cfg, ok := s.BalancerConfig.(*LBConfig)
	if !ok || cfg == nil {
		// Fall back to defaults if no config was supplied.
		parsed, err := builder{}.ParseConfig(nil)
		if err != nil {
			return err
		}
		parsedCfg, ok := parsed.(*LBConfig)
		if !ok {
			return fmt.Errorf("p2c: unexpected LoadBalancingConfig type %T", parsed)
		}
		cfg = parsedCfg
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg = cfg

	// Reconcile resolver addresses against existing SubConns.
	seen := resolver.NewAddressMapV2[bool]()
	for _, a := range s.ResolverState.Addresses {
		seen.Set(a, true)
		if _, ok := b.addrs.Get(a); ok {
			continue
		}
		st := &addrState{addr: a, state: connectivity.Idle}
		sc, err := b.newSubConn(a, st)
		if err != nil {
			level.Warn(b.logger).Log("msg", "failed to create SubConn", "component", "p2c", "target", a.Addr, "err", err)
			continue
		}
		level.Debug(b.logger).Log("msg", "created SubConn", "component", "p2c", "target", a.Addr)
		st.sc = sc
		b.addrs.Set(a, st)
		b.bySC[sc] = st
		sc.Connect()
	}
	// Tear down addresses no longer in the resolver state.
	for _, a := range b.addrs.Keys() {
		if _, ok := seen.Get(a); ok {
			continue
		}
		st, _ := b.addrs.Get(a)
		if st.cancelProbe != nil {
			st.cancelProbe()
			st.cancelProbe = nil
		}
		b.releaseHealthLocked(st)
		if st.sc != nil {
			st.sc.Shutdown()
		}
		b.addrs.Delete(a)
		// SubConn entries in bySC are cleared on the final Shutdown state.
	}

	if b.addrs.Len() == 0 {
		// Return ErrBadResolverState and let gRPC manage state transitions;
		// do not publish a picker here to avoid double state churn.
		b.state = connectivity.TransientFailure
		b.picker = &errPicker{err: fmt.Errorf("p2c: produced zero addresses")}
		return balancer.ErrBadResolverState
	}

	b.regeneratePickerLocked()
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	return nil
}

// newSubConn allocates a SubConn and wires its state listener.
func (b *p2cBalancer) newSubConn(a resolver.Address, st *addrState) (balancer.SubConn, error) {
	var sc balancer.SubConn
	sc, err := b.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{
		StateListener: func(scs balancer.SubConnState) { b.handleSubConnState(sc, st, scs) },
	})
	return sc, err
}

func (b *p2cBalancer) handleSubConnState(sc balancer.SubConn, st *addrState, scs balancer.SubConnState) {
	b.mu.Lock()
	if _, ok := b.bySC[sc]; !ok {
		b.bySC[sc] = st
	}

	switch scs.ConnectivityState {
	case connectivity.Ready:
		st.state = connectivity.Ready
		// Make pickable immediately with a neutral EWMA=0. The picker
		// uses `probed` to rank newcomers below probed peers until the
		// first probe lands. gRPC's built-in health check already
		// verified the server before reporting Ready.
		st.ewma = 0
		st.probed = false
		st.lastProbe = time.Time{}
		st.consecutiveFailures = 0
		st.probeUnimplemented = false
		// Build/refresh the cached health producer for this SubConn so the
		// probe loop can reuse it across probes.
		if st.healthClose != nil {
			st.healthClose()
			st.healthClient = nil
			st.healthClose = nil
		}
		p, closeFn := sc.GetOrBuildProducer(healthProducerBuilderSingleton)
		hp, ok := p.(*healthProducer)
		if !ok {
			// gRPC produced an unexpected producer type; this should never
			// happen. Bail out cleanly: release the producer, mark the
			// SubConn as TransientFailure, refresh state, and release the
			// lock before returning so we don't strand b.mu.
			closeFn()
			level.Error(b.logger).Log("msg", "unexpected producer type from gRPC", "component", "p2c", "target", st.addr.Addr, "type", fmt.Sprintf("%T", p))
			st.state = connectivity.TransientFailure
			st.probed = false
			b.regeneratePickerLocked()
			picker, state := b.picker, b.state
			b.mu.Unlock()
			b.cc.UpdateState(balancer.State{ConnectivityState: state, Picker: picker})
			return
		}
		st.healthClient = hp.client
		st.healthClose = closeFn
		// Start a probe goroutine; first probe fires immediately to
		// bootstrap the EWMA.
		if st.cancelProbe != nil {
			st.cancelProbe()
		}
		ctx, cancel := context.WithCancel(context.Background())
		st.cancelProbe = cancel
		b.probersWG.Add(1)
		go b.runProber(ctx, st)
	case connectivity.Idle:
		b.stopProbeLocked(st)
		b.releaseHealthLocked(st)
		st.state = connectivity.Idle
		st.probed = false
		// Re-trigger the connection attempt; mirrors the base balancer.
		sc.Connect()
	case connectivity.Connecting:
		// Reflect the actual gRPC state for accurate aggregate reporting.
		st.state = connectivity.Connecting
		if scs.ConnectionError != nil {
			level.Debug(b.logger).Log("msg", "SubConn connecting (previous attempt failed)", "component", "p2c", "target", st.addr.Addr, "err", scs.ConnectionError)
		}
		b.mu.Unlock()
		return
	case connectivity.TransientFailure:
		b.stopProbeLocked(st)
		b.releaseHealthLocked(st)
		st.state = connectivity.TransientFailure
		st.probed = false
		// Surface the underlying transport error (TLS, TCP, ALPN, etc.).
		level.Debug(b.logger).Log("msg", "SubConn transitioned to TransientFailure", "component", "p2c", "target", st.addr.Addr, "err", scs.ConnectionError)
	case connectivity.Shutdown:
		b.stopProbeLocked(st)
		b.releaseHealthLocked(st)
		delete(b.bySC, sc)
		b.regeneratePickerLocked()
		picker, state := b.picker, b.state
		b.mu.Unlock()
		b.cc.UpdateState(balancer.State{ConnectivityState: state, Picker: picker})
		return
	}

	b.regeneratePickerLocked()
	picker, state := b.picker, b.state
	b.mu.Unlock()
	b.cc.UpdateState(balancer.State{ConnectivityState: state, Picker: picker})
}

func (b *p2cBalancer) stopProbeLocked(st *addrState) {
	if st.cancelProbe != nil {
		st.cancelProbe()
		st.cancelProbe = nil
	}
}

// releaseHealthLocked drops the cached health producer for st, if any.
// Caller holds b.mu.
func (b *p2cBalancer) releaseHealthLocked(st *addrState) {
	if st.healthClose != nil {
		st.healthClose()
		st.healthClose = nil
	}
	st.healthClient = nil
}

// probeResult enumerates the outcome of a single probe attempt; used both for
// metrics and for selecting how the EWMA / failure counter update.
type probeResult int

const (
	// probeUnknown is a sentinel zero value (per RFC-021 guidance) so a
	// default-constructed probeResult is recognisable as "no result yet"
	// rather than silently meaning success. probeOnce always returns one
	// of the explicit results below; runProber's switch ignores
	// probeUnknown, which is the safe degradation if it ever leaks in.
	probeUnknown probeResult = iota
	probeSuccess
	probeTimeout
	probeError
	probeUnimplemented
)

// runProber loops health.Check calls against sc until ctx is cancelled. The
// first probe is issued immediately; subsequent probes wait probeInterval
// ±20% jitter. EWMA / failure bookkeeping happens under b.mu so the picker
// observes consistent snapshots.
func (b *p2cBalancer) runProber(ctx context.Context, st *addrState) {
	defer b.probersWG.Done()
	rng := newRand()
	for {
		b.mu.Lock()
		cfg := b.cfg
		if cfg == nil {
			b.mu.Unlock()
			return
		}
		interval, timeout := cfg.probeInterval, cfg.probeTimeout
		halfLife := cfg.latencyHalfLife
		threshold := cfg.ConsecutiveFailureThreshold
		unimpl := st.probeUnimplemented
		hc := st.healthClient
		b.mu.Unlock()
		if unimpl {
			// Server doesn't speak the health service; stop probing and
			// leave the SubConn non-pickable until it cycles.
			return
		}

		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		latency, result := probeOnce(probeCtx, hc)
		cancel()

		if ctx.Err() != nil {
			return
		}

		target := st.addr.Addr

		b.mu.Lock()
		// The SubConn may have left Ready while we were probing; bail out.
		if st.state != connectivity.Ready {
			b.mu.Unlock()
			return
		}
		now := time.Now()
		switch result {
		case probeSuccess:
			b.applyEWMASampleLocked(st, latency, now, halfLife)
			st.consecutiveFailures = 0
		case probeTimeout:
			// Penalise with the timeout duration as the sample; still
			// counts as a failure for the consecutive-failure gate.
			b.applyEWMASampleLocked(st, timeout, now, halfLife)
			st.consecutiveFailures++
		case probeError:
			// Don't poison the EWMA on transient errors; just count toward
			// the failure threshold.
			st.consecutiveFailures++
		case probeUnimplemented:
			st.probeUnimplemented = true
			level.Warn(b.logger).Log("msg", "server does not implement health service, SubConn will not be pickable", "component", "p2c", "target", target)
		}
		b.regeneratePickerLocked()
		picker, state := b.picker, b.state
		fails := st.consecutiveFailures
		b.mu.Unlock()
		b.cc.UpdateState(balancer.State{ConnectivityState: state, Picker: picker})

		if result == probeUnimplemented {
			return
		}
		if fails > 0 && fails == threshold {
			// We just crossed the threshold; log once per crossing.
			level.Warn(b.logger).Log("msg", "SubConn ejected due to consecutive probe failures", "component", "p2c", "target", target, "failures", fails)
		}

		// Sleep with jitter, but wake immediately on context cancellation.
		if interval <= 0 {
			interval = time.Second
		}
		spread := (1 - probeJitterFraction) + 2*probeJitterFraction*rng.Float64()
		t := time.NewTimer(time.Duration(float64(interval) * spread))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// applyEWMASampleLocked updates the EWMA with a new sample and marks the
// SubConn as probed. Caller holds b.mu.
func (b *p2cBalancer) applyEWMASampleLocked(st *addrState, sample time.Duration, now time.Time, halfLife time.Duration) {
	dt := now.Sub(st.lastProbe)
	if dt <= 0 {
		// Clock-skew or sub-nanosecond resolution: adopt sample wholesale.
		st.ewma = sample
		st.lastProbe = now
		st.probed = true
		return
	}
	// α = 1 - exp(-Δt / (halfLife / ln 2)) = 1 - 2^(-Δt/halfLife).
	exponent := float64(dt) / float64(halfLife)
	alpha := 1 - math.Pow(2, -exponent)
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}
	// Round (don't truncate) so that very small alpha*delta values still
	// nudge the EWMA toward the sample instead of getting stuck at 0.
	delta := alpha * float64(sample-st.ewma)
	st.ewma = st.ewma + time.Duration(math.Round(delta))
	st.lastProbe = now
	st.probed = true
}

// probeOnce issues one health.Check RPC against the supplied HealthClient,
// which is bound to a specific SubConn via the per-SubConn producer
// mechanism. Reusing a cached client across probes avoids rebuilding the
// underlying gRPC stub on every interval.
func probeOnce(ctx context.Context, hc healthpb.HealthClient) (time.Duration, probeResult) {
	if hc == nil {
		return 0, probeError
	}
	start := time.Now()
	_, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
	elapsed := time.Since(start)
	if err == nil {
		return elapsed, probeSuccess
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		return elapsed, probeTimeout
	case codes.Unimplemented:
		return elapsed, probeUnimplemented
	}
	return elapsed, probeError
}

// healthProducer implements balancer.Producer to expose a HealthClient that
// dispatches calls on the owning SubConn. The producer mechanism is the
// gRPC-recommended way to issue RPCs against a specific SubConn from a
// balancer.
type healthProducer struct {
	client healthpb.HealthClient
}

type healthProducerBuilder struct{}

func (healthProducerBuilder) Build(cci any) (balancer.Producer, func()) {
	cc, ok := cci.(grpc.ClientConnInterface)
	if !ok {
		// Should be unreachable: gRPC's producer mechanism always passes a
		// ClientConnInterface. Degrade gracefully by returning a producer
		// with a nil client; probeOnce treats a nil HealthClient as
		// probeError so the SubConn just never becomes pickable.
		return &healthProducer{}, func() {}
	}
	return &healthProducer{client: healthpb.NewHealthClient(cc)}, func() {}
}

var healthProducerBuilderSingleton = healthProducerBuilder{}

// regeneratePickerLocked builds a fresh picker from the current Ready set.
// SubConns are eligible if they have not breached the consecutive-failure
// threshold and the server speaks the health protocol. Probed status is
// passed through so the picker can rank newcomers below probed peers.
func (b *p2cBalancer) regeneratePickerLocked() {
	ready := make([]readySubConn, 0, b.addrs.Len())
	var connecting, tf int
	threshold := uint32(defaultConsecutiveFailureThreshold)
	pickCount := defaultPickCount
	if b.cfg != nil {
		threshold = b.cfg.ConsecutiveFailureThreshold
		pickCount = int(b.cfg.PickCount)
	}
	for _, a := range b.addrs.Keys() {
		st, _ := b.addrs.Get(a)
		switch st.state {
		case connectivity.Ready:
			if st.consecutiveFailures < threshold && !st.probeUnimplemented {
				ready = append(ready, readySubConn{sc: st.sc, addr: a.Addr, ewma: st.ewma, probed: st.probed})
			} else {
				// Ready transport but soft-ejected or excluded; count as
				// "connecting" so the balancer state reflects recoverable.
				connecting++
			}
		case connectivity.TransientFailure:
			tf++
		case connectivity.Connecting, connectivity.Idle:
			connecting++
		}
	}
	switch {
	case len(ready) > 0:
		b.state = connectivity.Ready
	case connecting > 0:
		b.state = connectivity.Connecting
	case tf > 0:
		b.state = connectivity.TransientFailure
	default:
		b.state = connectivity.Connecting
	}

	if len(ready) == 0 {
		if b.state == connectivity.TransientFailure {
			b.picker = &errPicker{err: fmt.Errorf("p2c: all SubConns in TransientFailure")}
		} else {
			b.picker = &errPicker{err: balancer.ErrNoSubConnAvailable}
		}
		return
	}
	// Sort by address for deterministic tie-breaking in the picker.
	sort.Slice(ready, func(i, j int) bool { return ready[i].addr < ready[j].addr })
	b.picker = &picker{
		ready:     ready,
		pickCount: pickCount,
		rng:       newRand(),
	}
}

func (b *p2cBalancer) ResolverError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	level.Warn(b.logger).Log("msg", "resolver error", "component", "p2c", "addrs", b.addrs.Len(), "err", err)
	if b.addrs.Len() == 0 {
		b.state = connectivity.TransientFailure
		b.picker = &errPicker{err: fmt.Errorf("p2c: resolver error: %v", err)}
		b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	}
}

// UpdateSubConnState is a nop; StateListener is always set in NewSubConn.
func (b *p2cBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	level.Error(b.logger).Log("msg", "UpdateSubConnState called unexpectedly", "component", "p2c", "sc", fmt.Sprintf("%p", sc), "state", state.ConnectivityState.String())
}

func (b *p2cBalancer) Close() {
	b.mu.Lock()
	for _, a := range b.addrs.Keys() {
		st, _ := b.addrs.Get(a)
		if st.cancelProbe != nil {
			st.cancelProbe()
			st.cancelProbe = nil
		}
		b.releaseHealthLocked(st)
		if st.sc != nil {
			st.sc.Shutdown()
		}
	}
	b.mu.Unlock()
	// Wait for all probe goroutines to exit. They re-acquire b.mu on every
	// iteration, so we must release the lock before waiting to avoid a
	// deadlock.
	b.probersWG.Wait()
}

// ExitIdle is a nop; we keep all SubConns continuously connecting.
func (b *p2cBalancer) ExitIdle() {}

// readySubConn is the per-Ready entry the picker samples from.
type readySubConn struct {
	sc     balancer.SubConn
	addr   string
	ewma   time.Duration
	probed bool
}

type picker struct {
	// ready and pickCount are immutable after the picker is constructed;
	// safe to read without holding p.mu.
	ready     []readySubConn
	pickCount int

	// mu protects only the RNG, which is the lone mutable field.
	mu  sync.Mutex
	rng *rand.Rand
}

func (p *picker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	switch len(p.ready) {
	case 0:
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	case 1:
		return balancer.PickResult{SubConn: p.ready[0].sc}, nil
	}
	n := p.pickCount
	if n > len(p.ready) {
		n = len(p.ready)
	}
	// Sample n distinct indices into a fixed-size stack array via rejection
	// sampling. Since n <= maxPickCount (16) and the duplicate check is a
	// linear scan over the already-chosen indices, this is O(n^2) in the
	// worst case but with a tiny constant. No heap allocation per Pick.
	var chosen [maxPickCount]int
	p.mu.Lock()
	for i := 0; i < n; i++ {
		for {
			c := p.rng.IntN(len(p.ready))
			dup := false
			for j := 0; j < i; j++ {
				if chosen[j] == c {
					dup = true
					break
				}
			}
			if !dup {
				chosen[i] = c
				break
			}
		}
	}
	p.mu.Unlock()

	best := chosen[0]
	for i := 1; i < n; i++ {
		k := chosen[i]
		// Probed beats unprobed; then lower EWMA wins; address (ready is
		// sorted by addr) breaks exact ties.
		a, b := p.ready[k], p.ready[best]
		if a.probed != b.probed {
			if a.probed {
				best = k
			}
			continue
		}
		if a.ewma < b.ewma || (a.ewma == b.ewma && a.addr < b.addr) {
			best = k
		}
	}
	return balancer.PickResult{SubConn: p.ready[best].sc}, nil
}

type errPicker struct{ err error }

func (e *errPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{}, e.err
}
