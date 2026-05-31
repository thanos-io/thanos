// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package fanout

import (
	"context"
	"sort"
	"sync"
	"time"
)

// StoreFanout describes a single endpoint that took part in a fan-out
// for one promql-engine operator, along with telemetry collected while
// streaming its response.
type StoreFanout struct {
	EndpointAddr   string
	Duration       time.Duration
	BytesProcessed int64
	NumResponses   int64
	Series         int64
	Chunks         int64
	Samples        int64
}

// Tracker captures, per promql-engine operator ID, the list of stores that
// were fanned out to while serving Series() calls for that operator and the
// per-store telemetry of those fan-outs.
type Tracker struct {
	mu     sync.Mutex
	byOpID map[uint64][]StoreFanout
}

// NewTracker returns a new empty Tracker.
func NewTracker() *Tracker {
	return &Tracker{byOpID: make(map[uint64][]StoreFanout)}
}

// AddStore registers a single store and its telemetry under the given
// operator ID.
func (t *Tracker) AddStore(opID uint64, s StoreFanout) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byOpID[opID] = append(t.byOpID[opID], s)
}

// Get returns the stores fanned out to for the given operator ID.
func (t *Tracker) Get(opID *uint64) []StoreFanout {
	if t == nil || opID == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	src := t.byOpID[*opID]
	if len(src) == 0 {
		return nil
	}
	out := make([]StoreFanout, len(src))
	copy(out, src)
	sort.Slice(out, func(i, j int) bool {
		return out[i].EndpointAddr < out[j].EndpointAddr
	})
	return out
}

type ctxKey struct{}

// NewContext returns a copy of ctx that carries the given Tracker so it can be
// retrieved by downstream code with FromContext.
func NewContext(ctx context.Context, t *Tracker) context.Context {
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, t)
}

// FromContext returns the Tracker stored in ctx, or nil if none is present.
func FromContext(ctx context.Context) *Tracker {
	if ctx == nil {
		return nil
	}
	t, _ := ctx.Value(ctxKey{}).(*Tracker)
	return t
}
