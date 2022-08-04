// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"
	"time"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
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

// GenerateCacheKey generates a cache key based on the Request and interval.
// TODO(yeya24): Add other request params as request key.
func (t thanosCacheKeyGenerator) GenerateCacheKey(userID string, r queryrange.Request) string {
	currentInterval := r.GetStart() / t.interval.Milliseconds()
	switch tr := r.(type) {
	case *ThanosQueryRangeRequest:
		i := 0
		for ; i < len(t.resolutions) && t.resolutions[i] > tr.MaxSourceResolution; i++ {
		}
		return fmt.Sprintf("fe:%s:%s:%d:%d:%d", userID, tr.Query, tr.Step, currentInterval, i)
	case *ThanosLabelsRequest:
		return fmt.Sprintf("fe:%s:%s:%s:%d", userID, tr.Label, tr.Matchers, currentInterval)
	case *ThanosSeriesRequest:
		return fmt.Sprintf("fe:%s:%s:%d", userID, tr.Matchers, currentInterval)
	}
	return fmt.Sprintf("fe:%s:%s:%d:%d", userID, r.GetQuery(), r.GetStep(), currentInterval)
}
