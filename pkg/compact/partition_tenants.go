// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"os"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"
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

// Discover tenants from S3 bucket by listing all tenant directories under the path prefix before tenant.
// e.g., if the path prefix is "v1/raw/", it will list all tenant directories under "v1/raw/".
func discoverTenantsFromBucket(ctx context.Context, bkt objstore.BucketReader, logger log.Logger, commonPathPrefix string, knownTenants []TenantWeight) ([]string, error) {
	discoveredTenants := []string{}
	knownTenantSet := make(map[string]bool)

	// Build set of known tenants from tenant weight config
	for _, tw := range knownTenants {
		knownTenantSet[tw.TenantName] = true
		level.Debug(logger).Log("msg", "marking tenant as known", "tenant", tw.TenantName)
	}

	// List all tenant directories under prefix before tenant
	err := bkt.Iter(ctx, commonPathPrefix, func(name string) error {
		// name will be like "v1/raw/tenant-a/"
		if !strings.HasSuffix(name, "/") {
			return nil // Not a directory
		}

		tenantName := strings.TrimSuffix(strings.TrimPrefix(name, commonPathPrefix), "/")

		// Skip if already active tenant shown in tenant weight config
		if knownTenantSet[tenantName] {
			level.Debug(logger).Log("msg", "skipping known tenant from discovery", "tenant", tenantName)
			return nil
		}

		level.Debug(logger).Log("msg", "discovered new tenant", "tenant", tenantName, "full_path", name)
		discoveredTenants = append(discoveredTenants, tenantName)

		return nil
	})

	if err != nil {
		return nil, err
	}

	level.Info(logger).Log("msg", "tenant discovery complete", "discovered tenants", strings.Join(discoveredTenants, ", "))

	return discoveredTenants, nil
}

func tenantToShard(tenantName string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(tenantName))
	return int(h.Sum32()) % numShards
}

func SetupTenantPartitioning(ctx context.Context, bkt objstore.Bucket, logger log.Logger, configPath string, commonPathPrefix string, numShards int) (map[int][]string, error) {
	// Get active tenants from tenant weight config
	activeTenants, err := readTenantWeights(configPath)
	if err != nil {
		return nil, err
	}

	// Compute tenant assignments for active tenants
	tenantAssignments, tenantWeights := computeTenantAssignments(numShards, activeTenants)
	level.Debug(logger).Log("msg", "computed assignments for active tenants", "count", len(activeTenants))

	// Discover additional tenants from S3
	discoveredTenants, err := discoverTenantsFromBucket(ctx, bkt, logger, commonPathPrefix, activeTenants)
	if err != nil {
		return nil, err
	}

	// Assign discovered tenants to pods with hashmod algorithm
	for _, discoveredTenant := range discoveredTenants {
		shard := tenantToShard(discoveredTenant, numShards)
		level.Debug(logger).Log("msg", "assigning discovered tenant", "tenant", discoveredTenant, "shard", shard)
		tenantAssignments[shard] = append(tenantAssignments[shard], discoveredTenant)
	}

	level.Info(logger).Log("msg", "tenant assignment complete",
		"total_tenants", len(tenantAssignments),
		"active_tenants", len(activeTenants),
		"discovered_tenants", len(discoveredTenants))

	// Log assignments
	for shardID, tenants := range tenantAssignments {
		level.Info(logger).Log("msg", "shard assignment",
			"shard", shardID,
			"tenants", len(tenants),
			"total_weight", tenantWeights[shardID])
	}

	return tenantAssignments, nil
}
