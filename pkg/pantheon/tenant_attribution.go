// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pantheon

import (
	"fmt"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
)

// GetTenantFromScope computes the tenant for a metric based on the scope and Pantheon configuration.
// It returns the computed tenant string in the format: <scope>_<group> or <scope>_<n>-of-<m>.
func GetTenantFromScope(scope string, metricName string, cluster *PantheonCluster) (string, error) {
	if cluster == nil {
		return "", errors.New("pantheon cluster configuration is nil")
	}

	// Find the metric scope in the configuration.
	var metricScope *MetricScope
	for i := range cluster.MetricScopes {
		if cluster.MetricScopes[i].ScopeName == scope {
			metricScope = &cluster.MetricScopes[i]
			break
		}
	}

	if metricScope == nil {
		return "", errors.Errorf("scope '%s' not found in pantheon configuration", scope)
	}

	// Check if the metric belongs to any special group.
	for _, group := range metricScope.SpecialMetricGroups {
		if matchesSpecialGroup(metricName, &group) {
			return fmt.Sprintf("%s_%s", scope, group.GroupName), nil
		}
	}

	// If not in any special group, compute the shard using hash.
	shard := computeMetricShard(metricName, metricScope.Shards)
	return fmt.Sprintf("%s_%d-of-%d", scope, shard, metricScope.Shards), nil
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

	// Check metric name prefixes.
	for _, prefix := range group.MetricNamePrefixes {
		if strings.HasPrefix(metricName, prefix) {
			return true
		}
	}

	// Check metric name suffixes.
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
