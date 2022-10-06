// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
)

type limiter struct {
	requestLimiter    requestLimiter
	writeGate         gate.Gate
	HeadSeriesLimiter headSeriesLimiter
}

// requestLimiter encompasses logic for limiting remote write requests.
type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
}

// headSeriesLimiter encompasses active/head series limiting logic.
type headSeriesLimiter interface {
	QueryMetaMonitoring(context.Context) error
	isUnderLimit(tenant string) (bool, error)
}

func NewLimiter(root *RootLimitsConfig, reg prometheus.Registerer, r ReceiverMode, logger log.Logger) *limiter {
	limiter := &limiter{
		writeGate:         gate.NewNoop(),
		requestLimiter:    &noopRequestLimiter{},
		HeadSeriesLimiter: NewNopSeriesLimit(),
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

	// Impose active series limit only if Receiver is in Router or RouterIngestor mode, and config is provided.
	seriesLimitSupported := (r == RouterOnly || r == RouterIngestor) && (len(root.WriteLimits.TenantsLimits) != 0 || root.WriteLimits.DefaultLimits.HeadSeriesLimit != 0)
	if seriesLimitSupported {
		limiter.HeadSeriesLimiter = NewHeadSeriesLimit(root.WriteLimits, reg, logger)
	}

	return limiter
}
