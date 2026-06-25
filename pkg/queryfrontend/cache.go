// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"
	"sort"
	"strings"
	"time"

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
			return t.generateQueryRangeCacheKey(userID, tr, tr.Step, splitInterval, currentInterval)
		case *ThanosLabelsRequest:
			return fmt.Sprintf("fe:%s:%s:%s:%d:%d", userID, tr.Label, tr.Matchers, splitInterval, currentInterval)
		case *ThanosSeriesRequest:
			return fmt.Sprintf("fe:%s:%s:%d:%d", userID, tr.Matchers, splitInterval, currentInterval)
		}
	}

	// all possible request types are already covered
	panic("request type not supported")
}

func (t thanosCacheKeyGenerator) GenerateCacheKeyAlternatives(userID string, r queryrange.Request) []string {
	sr, ok := r.(SplitRequest)
	if !ok {
		return nil
	}
	tr, ok := r.(*ThanosQueryRangeRequest)
	if !ok {
		return nil
	}

	splitInterval := sr.GetSplitInterval().Milliseconds()
	currentInterval := r.GetStart() / splitInterval
	steps := lowerStepCacheCandidates(tr.Step)
	if len(steps) == 0 {
		return nil
	}

	keys := make([]string, 0, len(steps))
	for _, step := range steps {
		if tr.Start%step != 0 {
			continue
		}
		keys = append(keys, t.generateQueryRangeCacheKey(userID, tr, step, splitInterval, currentInterval))
	}
	if len(keys) == 0 {
		return nil
	}

	return keys
}

func (t thanosCacheKeyGenerator) generateQueryRangeCacheKey(userID string, tr *ThanosQueryRangeRequest, step, splitInterval, currentInterval int64) string {
	i := 0
	for ; i < len(t.resolutions) && t.resolutions[i] > tr.MaxSourceResolution; i++ {
	}
	shardInfoKey := generateShardInfoKey(tr)
	replicaLabels := append([]string(nil), tr.ReplicaLabels...)
	sort.Strings(replicaLabels)
	return fmt.Sprintf("fe:%s:%s:%d:%d:%d:%d:%s:%d:%s:%t:%s:%t",
		userID, tr.Query, step, splitInterval, currentInterval, i, shardInfoKey,
		tr.LookbackDelta, tr.Engine, tr.PartialResponse, strings.Join(replicaLabels, ","), tr.Analyze)
}

// commonQuerySteps bounds alternative cache lookups to common dashboard query steps.
var commonQuerySteps = []int64{
	(12 * time.Hour).Milliseconds(),
	(6 * time.Hour).Milliseconds(),
	(3 * time.Hour).Milliseconds(),
	(2 * time.Hour).Milliseconds(),
	time.Hour.Milliseconds(),
	(30 * time.Minute).Milliseconds(),
	(15 * time.Minute).Milliseconds(),
	(10 * time.Minute).Milliseconds(),
	(5 * time.Minute).Milliseconds(),
	(2 * time.Minute).Milliseconds(),
	time.Minute.Milliseconds(),
	(30 * time.Second).Milliseconds(),
	(20 * time.Second).Milliseconds(),
	(15 * time.Second).Milliseconds(),
	(10 * time.Second).Milliseconds(),
	(5 * time.Second).Milliseconds(),
	time.Second.Milliseconds(),
}

func lowerStepCacheCandidates(step int64) []int64 {
	if !isCommonQueryStep(step) {
		return nil
	}

	candidates := make([]int64, 0, len(commonQuerySteps))
	for _, candidate := range commonQuerySteps {
		if candidate >= step || step%candidate != 0 {
			continue
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func isCommonQueryStep(step int64) bool {
	for _, commonStep := range commonQuerySteps {
		if commonStep == step {
			return true
		}
	}
	return false
}

func generateShardInfoKey(r *ThanosQueryRangeRequest) string {
	if r.ShardInfo == nil {
		return "-"
	}
	return fmt.Sprintf("%d:%d", r.ShardInfo.TotalShards, r.ShardInfo.ShardIndex)
}
