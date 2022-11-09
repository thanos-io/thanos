// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package querysharding

var excludedLabels = []string{"le"}

type QueryAnalysis struct {
	// Labels to shard on
	shardingLabels []string

	// When set to true, sharding is `by` shardingLabels,
	// otherwise it is `without` shardingLabels.
	shardBy bool
}

func nonShardableQuery() QueryAnalysis {
	return QueryAnalysis{
		shardingLabels: nil,
	}
}

func (q *QueryAnalysis) scopeToLabels(labels []string, by bool) QueryAnalysis {
	labels = without(labels, excludedLabels)

	if q.shardingLabels == nil {
		return QueryAnalysis{
			shardBy:        by,
			shardingLabels: labels,
		}
	}

	if q.shardBy && by {
		return QueryAnalysis{
			shardBy:        true,
			shardingLabels: intersect(q.shardingLabels, labels),
		}
	}

	if !q.shardBy && !by {
		return QueryAnalysis{
			shardBy:        false,
			shardingLabels: union(q.shardingLabels, labels),
		}
	}

	// If we are sharding by and without the same time,
	// keep the sharding by labels that are not in the without labels set.
	labelsBy, labelsWithout := q.shardingLabels, labels
	if !q.shardBy {
		labelsBy, labelsWithout = labelsWithout, labelsBy
	}
	return QueryAnalysis{
		shardBy:        true,
		shardingLabels: without(labelsBy, labelsWithout),
	}
}

func (q *QueryAnalysis) IsShardable() bool {
	return len(q.shardingLabels) > 0
}

func (q *QueryAnalysis) ShardingLabels() []string {
	if len(q.shardingLabels) == 0 {
		return nil
	}

	return q.shardingLabels
}

func (q *QueryAnalysis) ShardBy() bool {
	return q.shardBy
}

func intersect(sliceA, sliceB []string) []string {
	if len(sliceA) == 0 || len(sliceB) == 0 {
		return []string{}
	}

	mapA := make(map[string]struct{}, len(sliceA))
	for _, s := range sliceA {
		mapA[s] = struct{}{}
	}

	mapB := make(map[string]struct{}, len(sliceB))
	for _, s := range sliceB {
		mapB[s] = struct{}{}
	}

	result := make([]string, 0)
	for k := range mapA {
		if _, ok := mapB[k]; ok {
			result = append(result, k)
		}
	}

	return result
}

func without(sliceA, sliceB []string) []string {
	if sliceA == nil {
		return nil
	}

	if len(sliceA) == 0 || len(sliceB) == 0 {
		return []string{}
	}

	keyMap := make(map[string]struct{}, len(sliceA))
	for _, s := range sliceA {
		keyMap[s] = struct{}{}
	}
	for _, s := range sliceB {
		delete(keyMap, s)
	}

	result := make([]string, 0, len(keyMap))
	for k := range keyMap {
		result = append(result, k)
	}

	return result
}

func union(sliceA, sliceB []string) []string {
	if len(sliceA) == 0 || len(sliceB) == 0 {
		return []string{}
	}

	keyMap := make(map[string]struct{}, len(sliceA))
	for _, s := range sliceA {
		keyMap[s] = struct{}{}
	}
	for _, s := range sliceB {
		keyMap[s] = struct{}{}
	}

	result := make([]string, 0, len(keyMap))
	for k := range keyMap {
		result = append(result, k)
	}

	return result
}
