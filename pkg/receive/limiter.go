// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"github.com/prometheus/client_golang/prometheus"
)

type limiter struct {
	requestLimiter requestLimiter
	// activeSeriesLimiter *activeSeriesLimiter `yaml:"active_series"`
}

type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
}

func newLimiter(root *RootLimitsConfig, reg prometheus.Registerer) *limiter {
	if root == nil {
		return &limiter{
			requestLimiter: &noopRequestLimiter{},
		}
	}

	return &limiter{
		requestLimiter: newConfigRequestLimiter(
			reg,
			&root.WriteLimits,
		),
	}
}
