// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryprojection

type QueryAnalysis struct {
	// Labels to project.
	projectionLabels []string

	// When set to true, projection labels are kept.
	// Otherwise they are dropped.
	by bool

	// Grouping means whether we apply projection or not.
	grouping bool
}

func nonProjectableQuery() QueryAnalysis {
	return QueryAnalysis{
		projectionLabels: nil,
	}
}

func (q *QueryAnalysis) scopeToLabels(labels []string, by bool) QueryAnalysis {
	if q.projectionLabels == nil {
		return QueryAnalysis{
			by:               by,
			grouping:         true,
			projectionLabels: labels,
		}
	}

	if q.by && by {
		return QueryAnalysis{
			by:               true,
			grouping:         true,
			projectionLabels: union(q.projectionLabels, labels),
		}
	}

	if !q.by && !by {
		return QueryAnalysis{
			by:               false,
			grouping:         true,
			projectionLabels: intersect(q.projectionLabels, labels),
		}
	}

	labelsBy, labelsWithout := q.projectionLabels, labels
	if !q.by {
		labelsBy, labelsWithout = labelsWithout, labelsBy
	}
	return QueryAnalysis{
		by:               true,
		projectionLabels: without(labelsBy, labelsWithout),
	}
}

func (q *QueryAnalysis) Labels() []string {
	if len(q.projectionLabels) == 0 {
		return nil
	}

	return q.projectionLabels
}

func (q *QueryAnalysis) By() bool {
	return q.by
}

func (q *QueryAnalysis) Grouping() bool {
	return q.grouping
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
