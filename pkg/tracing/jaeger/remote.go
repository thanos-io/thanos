// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// This file contains utility functions required to implement a rate limiting sampler.
// Since these are part of the 'internal' package folder in the OpenTelemetry-go repo, I have added
// these functions here to be used by the parent 'tracing/jaeger' package.
// Ref: https://github.com/open-telemetry/opentelemetry-go-contrib/blob/main/samplers/jaegerremote/internal/utils/rate_limiter.go

package jaeger

import (
	"math"
	"sync"
	"time"

	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type RateLimiter struct {
	lock sync.Mutex

	creditsPerSecond float64
	balance          float64
	maxBalance       float64
	lastTick         time.Time

	timeNow func() time.Time
}

type rateLimitingSampler struct {
	rateLimiter        *RateLimiter
	maxTracesPerSecond float64
}

// NewRateLimiter creates a new RateLimiter.
func NewRateLimiter(creditsPerSecond, maxBalance float64) *RateLimiter {
	return &RateLimiter{
		creditsPerSecond: creditsPerSecond,
		balance:          maxBalance,
		maxBalance:       maxBalance,
		lastTick:         time.Now(),
		timeNow:          time.Now,
	}
}

// CheckCredit tries to reduce the current balance by itemCost provided that the current balance
// is not lest than itemCost.
func (rl *RateLimiter) CheckCredit(itemCost float64) bool {
	rl.lock.Lock()
	defer rl.lock.Unlock()

	// if we have enough credits to pay for current item, then reduce balance and allow
	if rl.balance >= itemCost {
		rl.balance -= itemCost
		return true
	}
	// otherwise check if balance can be increased due to time elapsed, and try again
	rl.updateBalance()
	if rl.balance >= itemCost {
		rl.balance -= itemCost
		return true
	}
	return false
}

// updateBalance recalculates current balance based on time elapsed. Must be called while holding a lock.
func (rl *RateLimiter) updateBalance() {
	// calculate how much time passed since the last tick, and update current tick
	currentTime := rl.timeNow()
	elapsedTime := currentTime.Sub(rl.lastTick)
	rl.lastTick = currentTime
	// calculate how much credit have we accumulated since the last tick
	rl.balance += elapsedTime.Seconds() * rl.creditsPerSecond
	if rl.balance > rl.maxBalance {
		rl.balance = rl.maxBalance
	}
}

// Update changes the main parameters of the rate limiter in-place, while retaining
// the current accumulated balance (pro-rated to the new maxBalance value). Using this method
// instead of creating a new rate limiter helps to avoid thundering herd when sampling
// strategies are updated.
func (rl *RateLimiter) Update(creditsPerSecond, maxBalance float64) {
	rl.lock.Lock()
	defer rl.lock.Unlock()

	rl.updateBalance() // get up to date balance
	rl.balance = rl.balance * maxBalance / rl.maxBalance
	rl.creditsPerSecond = creditsPerSecond
	rl.maxBalance = maxBalance
}

func (r *rateLimitingSampler) Description() string {
	return "rateLimitingSampler{}"
}

func (r *rateLimitingSampler) ShouldSample(p tracesdk.SamplingParameters) tracesdk.SamplingResult {
	psc := oteltrace.SpanContextFromContext(p.ParentContext)
	if r.rateLimiter.CheckCredit(1.0) {
		return tracesdk.SamplingResult{
			Decision:   tracesdk.RecordAndSample,
			Tracestate: psc.TraceState(),
		}
	}
	return tracesdk.SamplingResult{
		Decision:   tracesdk.Drop,
		Tracestate: psc.TraceState(),
	}
}

func (r *rateLimitingSampler) init(rateLimit float64) {
	if r.rateLimiter == nil {
		r.rateLimiter = NewRateLimiter(rateLimit, math.Max(rateLimit, 1.0))
	} else {
		r.rateLimiter.Update(rateLimit, math.Max(rateLimit, 1.0))
	}
}

func (r *rateLimitingSampler) Update(maxTracesPerSecond float64) {
	if r.maxTracesPerSecond != maxTracesPerSecond {
		r.init(maxTracesPerSecond)
	}
}
