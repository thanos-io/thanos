// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// thanosCacheKeyGenerator is a utility for using split interval when determining cache keys.
type thanosCacheKeyGenerator struct {
	interval    queryrange.IntervalFn
	resolutions []int64
}

func newThanosCacheKeyGenerator(intervalFn queryrange.IntervalFn) thanosCacheKeyGenerator {
	return thanosCacheKeyGenerator{
		interval:    intervalFn,
		resolutions: []int64{downsample.ResLevel2, downsample.ResLevel1, downsample.ResLevel0},
	}
}

// GenerateCacheKey generates a cache key based on the Request and interval.
// TODO(yeya24): Add other request params as request key.
func (t thanosCacheKeyGenerator) GenerateCacheKey(userID string, r queryrange.Request) string {
	currentInterval := r.GetStart() / t.interval(r).Milliseconds()
	switch tr := r.(type) {
	case *ThanosQueryRangeRequest:
		i := 0
		for ; i < len(t.resolutions) && t.resolutions[i] > tr.MaxSourceResolution; i++ {
		}
		shardInfoKey := generateShardInfoKey(tr)
		return fmt.Sprintf("fe:%s:%s:%d:%d:%d:%s:%d", userID, tr.Query, tr.Step, currentInterval, i, shardInfoKey, tr.LookbackDelta)
	case *ThanosLabelsRequest:
		return fmt.Sprintf("fe:%s:%s:%s:%d", userID, tr.Label, tr.Matchers, currentInterval)
	case *ThanosSeriesRequest:
		return fmt.Sprintf("fe:%s:%s:%d", userID, tr.Matchers, currentInterval)
	}
	return fmt.Sprintf("fe:%s:%s:%d:%d", userID, r.GetQuery(), r.GetStep(), currentInterval)
}

func generateShardInfoKey(r *ThanosQueryRangeRequest) string {
	if r.ShardInfo == nil {
		return "-"
	}
	return fmt.Sprintf("%d:%d", r.ShardInfo.TotalShards, r.ShardInfo.ShardIndex)
}
