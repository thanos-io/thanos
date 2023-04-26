// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"context"
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// DownsampledMiddleware creates a new Middleware that requests downsampled data
// should response to original request with auto max_source_resolution not contain data points.
func DownsampledMiddleware(merger queryrange.Merger, registerer prometheus.Registerer) queryrange.Middleware {
	return queryrange.MiddlewareFunc(func(next queryrange.Handler) queryrange.Handler {
		return downsampled{
			next:   next,
			merger: merger,
			additionalQueriesCount: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
				Namespace: "thanos",
				Name:      "frontend_downsampled_extra_queries_total",
				Help:      "Total number of additional queries for downsampled data",
			}),
		}
	})
}

type downsampled struct {
	next   queryrange.Handler
	merger queryrange.Merger

	// Metrics.
	additionalQueriesCount prometheus.Counter
}

var resolutions = []int64{downsample.ResLevel1, downsample.ResLevel2}

func (d downsampled) Do(ctx context.Context, req queryrange.Request) (queryrange.Response, error) {
	tqrr, ok := req.(*ThanosQueryRangeRequest)
	if !ok || !tqrr.AutoDownsampling {
		return d.next.Do(ctx, req)
	}

	var (
		resps = make([]queryrange.Response, 0)
		resp  queryrange.Response
		err   error
		i     int
	)

forLoop:
	for i < len(resolutions) {
		if i > 0 {
			d.additionalQueriesCount.Inc()
		}
		r := *tqrr
		resp, err = d.next.Do(ctx, &r)
		if err != nil {
			return nil, err
		}
		resps = append(resps, resp)
		// Set MaxSourceResolution for next request, if any.
		for i < len(resolutions) {
			if tqrr.MaxSourceResolution < resolutions[i] {
				tqrr.AutoDownsampling = false
				tqrr.MaxSourceResolution = resolutions[i]
				break
			}
			i++
		}
		m := minResponseTime(resp)
		switch m {
		case tqrr.Start: // Response not impacted by retention policy.
			break forLoop
		case -1: // Empty response, retry with higher MaxSourceResolution.
			continue
		default: // Data partially present, query for empty part with higher MaxSourceResolution.
			tqrr.End = m - tqrr.Step
		}
		if tqrr.Start > tqrr.End {
			break forLoop
		}
	}
	response, err := d.merger.MergeResponse(req, resps...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// minResponseTime returns earliest timestamp in r.Data.Result.
// -1 is returned if r contains no data points.
// Each SampleStream within r.Data.Result must be sorted by timestamp.
func minResponseTime(r queryrange.Response) int64 {
	var res = r.(*queryrange.PrometheusResponse).Data.Result
	if len(res) == 0 || (len(res[0].Samples) == 0 && len(res[0].Histograms) == 0) {
		return -1
	}

	minTs := int64(math.MaxInt64)

	for _, sampleStream := range res {
		if len(sampleStream.Samples) > 0 {
			if ts := sampleStream.Samples[0].TimestampMs; ts < minTs {
				minTs = ts
			}
		}

		if len(sampleStream.Histograms) > 0 {
			if ts := sampleStream.Histograms[0].Timestamp; ts < minTs {
				minTs = ts
			}
		}
	}

	return minTs
}
