// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
)

type limiter struct {
	requestLimiter requestLimiter
	writeGate      gate.Gate
	// TODO: extract active series limiting logic into a self-contained type and
	// move it here.
}

type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
}

func newLimiter(root *RootLimitsConfig, reg prometheus.Registerer) *limiter {
	limiter := &limiter{
		writeGate:      gate.NewNoop(),
		requestLimiter: &noopRequestLimiter{},
	}
	if root == nil {
		return limiter
	}

	maxWriteConcurrency := root.WriteLimits.GlobalLimits.MaxConcurrency
	if maxWriteConcurrency > 0 {
		limiter.writeGate = gate.New(
			extprom.WrapRegistererWithPrefix(
				"thanos_receive_write_request_concurrent_",
				reg,
			),
			int(maxWriteConcurrency),
		)
	}
	limiter.requestLimiter = newConfigRequestLimiter(reg, &root.WriteLimits)

	return limiter
}
