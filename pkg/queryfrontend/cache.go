// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"

	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// thanosCacheKeyGenerator is a utility for using split interval when determining cache keys.
type thanosCacheKeyGenerator struct {
	interval    time.Duration
	resolutions []int64
}

func newThanosCacheKeyGenerator(interval time.Duration) thanosCacheKeyGenerator {
	return thanosCacheKeyGenerator{
		interval:    interval,
		resolutions: []int64{downsample.ResLevel2, downsample.ResLevel1, downsample.ResLevel0},
	}
}

// TODO(yeya24): Add other request params as request key.
// GenerateCacheKey generates a cache key based on the Request and interval.
func (t thanosCacheKeyGenerator) GenerateCacheKey(_ string, r queryrange.Request) string {
	currentInterval := r.GetStart() / t.interval.Milliseconds()
	switch tr := r.(type) {
	case *ThanosQueryRangeRequest:
		i := 0
		for ; i < len(t.resolutions) && t.resolutions[i] > tr.MaxSourceResolution; i++ {
		}
		return fmt.Sprintf("%s:%d:%d:%d", tr.Query, tr.Step, currentInterval, i)
	case *ThanosLabelsRequest:
		return fmt.Sprintf("%s:%d", tr.Label, currentInterval)
	case *ThanosSeriesRequest:
		return fmt.Sprintf("%s:%d", tr.Matchers, currentInterval)
	}
	return fmt.Sprintf("%s:%d:%d", r.GetQuery(), r.GetStep(), currentInterval)
}
