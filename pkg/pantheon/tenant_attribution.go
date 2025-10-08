// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pantheon

import (
	"fmt"
	"strings"

	"github.com/cespare/xxhash"
)

// GetMetricScope returns the MetricScope for a given scope name from the cluster configuration.
// Returns nil if scope is not found.
// Caller must ensure cluster is not nil.
func GetMetricScope(scope string, cluster *PantheonCluster) *MetricScope {
	for i := range cluster.MetricScopes {
		if cluster.MetricScopes[i].ScopeName == scope {
			return &cluster.MetricScopes[i]
		}
	}

	return nil
}

// GetTenantFromScope computes the tenant for a metric based on the metric scope.
// It returns the computed tenant string in the format: <scope>_<group> or <scope>_<n>-of-<m>.
func GetTenantFromScope(metricName string, metricScope *MetricScope) string {
	// Check if the metric belongs to any special group.
	for _, group := range metricScope.SpecialMetricGroups {
		if matchesSpecialGroup(metricName, &group) {
			return fmt.Sprintf("%s_%s", metricScope.ScopeName, group.GroupName)
		}
	}

	// If not in any special group, compute the hashmod tenant.
	return GetHashmodTenant(metricName, metricScope)
}

// matchesSpecialGroup checks if a metric name matches any pattern in the special group.
// Returns true on the first match found.
func matchesSpecialGroup(metricName string, group *SpecialMetricGroup) bool {
	// Check exact metric names.
	for _, name := range group.MetricNames {
		if metricName == name {
			return true
		}
	}

	// Check metric name prefixes and suffixes.
	for _, prefix := range group.MetricNamePrefixes {
		if strings.HasPrefix(metricName, prefix) {
			return true
		}
	}
	for _, suffix := range group.MetricNameSuffixes {
		if strings.HasSuffix(metricName, suffix) {
			return true
		}
	}
	return false
}

// computeMetricShard computes the shard number for a metric name using xxhash.
func computeMetricShard(metricName string, totalShards int) int {
	if totalShards <= 0 {
		return 0
	}

	h := xxhash.Sum64String(metricName)
	return int(h % uint64(totalShards))
}

// GetHashmodTenant computes the hashmod-based tenant for a metric name within a metric scope.
// It returns the tenant in format: <scope>_<n>-of-<m> where n is the computed shard.
// This is used for metrics that don't belong to any special group.
func GetHashmodTenant(metricName string, metricScope *MetricScope) string {
	shard := computeMetricShard(metricName, metricScope.Shards)
	return fmt.Sprintf("%s_%d-of-%d", metricScope.ScopeName, shard, metricScope.Shards)
}
