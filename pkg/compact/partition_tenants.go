// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"encoding/json"
	"os"
	"sort"
)

type TenantWeight struct {
	TenantName string
	Weight     int
}

func readTenantWeights(path string) ([]TenantWeight, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Parse JSON as map of tenant names to weights
	var tenantWeightMap map[string]int
	if err := json.Unmarshal(content, &tenantWeightMap); err != nil {
		return nil, err
	}

	tenantWeightList := make([]TenantWeight, 0, len(tenantWeightMap))
	for tenantName, weight := range tenantWeightMap {
		tenantWeightList = append(tenantWeightList, TenantWeight{
			TenantName: tenantName,
			Weight:     weight,
		})
	}

	return tenantWeightList, nil
}

// Partition tenants for multitenant compaction.
// Returns two maps: one mapping shard ordinal to list of assigned tenant names, and one mapping shard ordinal to total weight.
func computeTenantAssignments(numShards int, tenantWeightList []TenantWeight) (map[int][]string, map[int]int) {
	// Initialize buckets and weights
	bucketTenantAssignments := make(map[int][]string)
	bucketWeights := make(map[int]int)
	for i := 0; i < numShards; i++ {
		bucketTenantAssignments[i] = []string{}
		bucketWeights[i] = 0
	}

	// Sort by weight descending, then by tenant name ascending
	sort.Slice(tenantWeightList, func(i, j int) bool {
		if tenantWeightList[i].Weight != tenantWeightList[j].Weight {
			return tenantWeightList[i].Weight > tenantWeightList[j].Weight
		}
		return tenantWeightList[i].TenantName < tenantWeightList[j].TenantName
	})

	// Distribute tenants into buckets using greedy algorithm
	for _, tenant := range tenantWeightList {
		// Find the minimum weight among all buckets
		minWeight := bucketWeights[0]
		for i := 1; i < numShards; i++ {
			if bucketWeights[i] < minWeight {
				minWeight = bucketWeights[i]
			}
		}

		// Select the highest ordinal bucket among those with the minimum weight
		minBucket := -1
		for i := numShards - 1; i >= 0; i-- {
			if bucketWeights[i] == minWeight {
				minBucket = i
				break
			}
		}

		// Assign tenant to the selected bucket
		bucketTenantAssignments[minBucket] = append(bucketTenantAssignments[minBucket], tenant.TenantName)
		bucketWeights[minBucket] += tenant.Weight
	}

	return bucketTenantAssignments, bucketWeights
}
