// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"context"
	"strconv"
)

const (
	StatsTotalSamplesHeader = "X-Thanos-Stats-Total-Samples"
	StatsPeakSamplesHeader  = "X-Thanos-Stats-Peak-Samples"
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
		if pr, ok := resp.(*PrometheusResponse); ok {
			pr.Headers = addStatsHeaders(pr.Headers, resp)
		}
		if pir, ok := resp.(*PrometheusInstantQueryResponse); ok {
			pir.Headers = addStatsHeaders(pir.Headers, resp)
		}
	}

	return resp, err
}

func addStatsHeaders(h []*PrometheusResponseHeader, resp Response) []*PrometheusResponseHeader {
	return append(
		h,
		&PrometheusResponseHeader{
			Name:   StatsTotalSamplesHeader,
			Values: []string{strconv.FormatInt(resp.GetStats().Samples.TotalQueryableSamples, 10)},
		},
		&PrometheusResponseHeader{
			Name:   StatsPeakSamplesHeader,
			Values: []string{strconv.FormatInt(int64(resp.GetStats().Samples.PeakSamples), 10)},
		},
	)
}
