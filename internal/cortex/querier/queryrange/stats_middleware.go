// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"context"

	"github.com/thanos-io/thanos/internal/cortex/querier/stats"
)

type statsMiddleware struct {
	next       Handler
	forceStats bool
}

func NewStatsMiddleware(forceStats bool) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return statsMiddleware{
			next:       next,
			forceStats: forceStats,
		}
	})
}

func (s statsMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	if s.forceStats {
		r = r.WithStats("all")
	}
	resp, err := s.next.Do(ctx, r)
	if err != nil {
		return resp, err
	}

	if resp.GetStats() != nil {
		if sts := stats.FromContext(ctx); sts != nil {
			sts.SetPeakSamples(max(sts.LoadPeakSamples(), resp.GetStats().Samples.PeakSamples))
			sts.AddTotalSamples(resp.GetStats().Samples.TotalQueryableSamples)
		}
	}

	return resp, err
}
