// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"
	"sort"
	"strings"

	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// thanosCacheKeyGenerator is a utility for using split interval when determining cache keys.
type thanosCacheKeyGenerator struct {
	resolutions []int64
}

func newThanosCacheKeyGenerator() thanosCacheKeyGenerator {
	return thanosCacheKeyGenerator{
		resolutions: []int64{downsample.ResLevel2, downsample.ResLevel1, downsample.ResLevel0},
	}
}

// GenerateCacheKey generates a cache key based on the Request and interval.
func (t thanosCacheKeyGenerator) GenerateCacheKey(userID string, r queryrange.Request) string {
	if sr, ok := r.(SplitRequest); ok {
		splitInterval := sr.GetSplitInterval().Milliseconds()
		currentInterval := r.GetStart() / splitInterval

		switch tr := r.(type) {
		case *ThanosQueryRangeRequest:
			i := 0
			for ; i < len(t.resolutions) && t.resolutions[i] > tr.MaxSourceResolution; i++ {
			}
			shardInfoKey := generateShardInfoKey(tr)
			sort.Strings(tr.ReplicaLabels)
			return fmt.Sprintf("fe:%s:%s:%d:%d:%d:%d:%s:%d:%s:%t:%s:%t",
				userID, tr.Query, tr.Step, splitInterval, currentInterval, i, shardInfoKey,
				tr.LookbackDelta, tr.Engine, tr.PartialResponse, strings.Join(tr.ReplicaLabels, ","), tr.Analyze)
		case *ThanosLabelsRequest:
			return fmt.Sprintf("fe:%s:%s:%s:%d:%d", userID, tr.Label, tr.Matchers, splitInterval, currentInterval)
		case *ThanosSeriesRequest:
			return fmt.Sprintf("fe:%s:%s:%d:%d", userID, tr.Matchers, splitInterval, currentInterval)
		}
	}

	// all possible request types are already covered
	panic("request type not supported")
}

func generateShardInfoKey(r *ThanosQueryRangeRequest) string {
	if r.ShardInfo == nil {
		return "-"
	}
	return fmt.Sprintf("%d:%d", r.ShardInfo.TotalShards, r.ShardInfo.ShardIndex)
}
