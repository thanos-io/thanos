// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pantheon

import (
	"fmt"
	"regexp"
	"strings"
)

// PantheonCluster represents the configuration for a Pantheon cluster.
// It includes semantically three types of metadata:
// - Data partitioning schema
// - Replicas and other metadata of DB groups
// - Tenant/partition to DB group assignment.
type PantheonCluster struct {
	// EffectiveDate is the date when this configuration becomes effective.
	// The version with the latest effective date is the latest version.
	// Format: YYYY-MM-DD
	EffectiveDate Date `json:"effective_date" yaml:"effective_date"`

	// Metric data is partitioned at two levels: the metric scope and the metric name shard.
	// A metric scope indicates where the time series comes from (e.g., HGCP, Infra2.0).
	MetricScopes []MetricScope `json:"metric_scopes" yaml:"metric_scopes"`

	// Each DB group contains a group of StatefulSets with the same number of replicas.
	// DB groups are dynamically scaled up/down according to demand.
	DBGroups []DbGroup `json:"db_groups" yaml:"db_groups"`
}

// PantheonClusterVersions contains multiple versions of PantheonCluster configurations.
// Multiple versions are maintained to support data repartitioning scenarios.
type PantheonClusterVersions struct {
	// Versions is the list of Pantheon cluster versions ordered by effective date.
	// The latest version is Versions[0].
	// The control plane uses only the latest version for scaling operations.
	// Older versions are kept for data migration and query fanout during repartitioning.
	Versions []PantheonCluster `json:"versions" yaml:"versions"`
}

type MetricScope struct {
	// ScopeName is the name of the metric scope that indicates the time series source.
	// The character set is [a-zA-Z0-9_-].
	// Examples: "az-eastus2", "az-eastus2-c2", "az-eastus2-meta", "az-eastus2-dataplane"
	ScopeName string `json:"scope_name" yaml:"scope_name"`

	// Shards is the number of metric name shards for this scope.
	// Time series are partitioned by: hash(metric_name) % shards.
	// Used for tenant calculation: <scope>_<shard_number>-of-<total_shards>
	// Must be >= 1
	Shards int `json:"shards" yaml:"shards"`

	// SpecialMetricGroups contains metrics with high cardinality or heavey reads.
	// These metrics get dedicated tenants to avoid skewed data partitions.
	// If a metric is in a special group, its tenant becomes: <scope>_<group_name>
	SpecialMetricGroups []SpecialMetricGroup `json:"special_metric_groups,omitempty" yaml:"special_metric_groups,omitempty"`
}

type SpecialMetricGroup struct {
	// Name is the identifier for this special metric group.
	// Used to create dedicated tenant: <scope>_<group_name>
	// Examples: "kube-metrics", "rpc-metrics", "recording-rules"
	// Character set: [a-zA-Z0-9_-]
	GroupName string `json:"group_name" yaml:"group_name"`

	// MetricNames is the list of metric names that belong to this special group.
	// Common examples: "container_cpu_usage_seconds_total", "container_memory_working_set_bytes"
	MetricNames []string `json:"metric_names" yaml:"metric_names"`

	// MetricNamePrefixes is the list of metric name prefixes that belong to this special group.
	// Examples: "autoscaling__", "kube_state_".
	MetricNamePrefixes []string `json:"metric_name_prefixes" yaml:"metric_name_prefixes"`

	// MetricNameSuffixes is the list of metric name suffixes that belong to this special group.
	// Examples: ":recording_rules", ":aggr_sum".
	MetricNameSuffixes []string `json:"metric_name_suffixes" yaml:"metric_name_suffixes"`
}

// This struct defines a set of tenants.
type TenantSet struct {
	// This refers to ScopeName in MetricScope.
	MetricScopeName string `json:"metric_scope_name" yaml:"metric_scope_name"`

	// This refers to GroupName in SpecialMetricGroup.
	// All the special groups in this scope belong to this tenant set.
	SpecialGroupNames []string `json:"special_group_names" yaml:"special_group_names"`

	// All the shards in this scope belong to this tenant set.
	Shards []int `json:"shards" yaml:"shards"`
}

type DbGroup struct {
	// StatefulSets are named: <db_group_name>-rep0, <db_group_name>-rep1, <db_group_name>-rep2
	// Example: "pantheon-db-a0" creates pantheon-db-a0-rep0, pantheon-db-a0-rep1, pantheon-db-a0-rep2
	DbGroupName string `json:"db_group_name" yaml:"db_group_name"`

	// Replicas is the number of replicas per StatefulSet in this DB group.
	// Total pods = Replicas * 3 (for 3 StatefulSets)
	// Range: 1-15 to avoid long release times
	// Must be >= 1 for production use
	Replicas int `json:"replicas" yaml:"replicas"`

	// DbHpa configures horizontal pod autoscaling for this DB group.
	// Automatically scales replicas based on CPU/memory/disk utilization.
	// Triggers tenant reassignment when scaling beyond limits (>45 total pods or <3 total pods).
	DbHpa DbHpaConfig `json:"db_hpa" yaml:"db_hpa"`

	// A tenant string has two formats:
	//   1. "<scope>_<special_group>" e.g., "az-eastus2_kube-metrics"
	//   2. "<scope>_<shard>-of-<total_shards>" e.g., "az-eastus2_0-of-20"
	// Character set: [a-zA-Z0-9_-]
	// All the tenant sets here are served by this DB group.
	TenantSets []TenantSet `json:"tenant_sets" yaml:"tenant_sets"`
}

type DbHpaConfig struct {
	// Enabled indicates whether horizontal pod autoscaling is active.
	// When disabled, DB group maintains fixed replica count.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// MaxReplicas is the maximum number of replicas per StatefulSet during autoscaling.
	// Total max pods = MaxReplicas * 3 (for 3 StatefulSets per DB group)
	// Constraint: MaxReplicas >= MinReplicas >= 0
	// Recommended: <= 15 to avoid long release times
	MaxReplicas int `json:"max_replicas" yaml:"max_replicas"`

	// MinReplicas is the minimum number of replicas per StatefulSet during autoscaling.
	// Total min pods = MinReplicas * 3 (for 3 StatefulSets per DB group)
	// Constraint: MinReplicas >= 0 (can be 0 to allow scaling to zero)
	// When scaling below this triggers tenant reassignment to other DB groups
	MinReplicas int `json:"min_replicas" yaml:"min_replicas"`
}

// ValidationError represents a validation error with details.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error in field '%s': %s", e.Field, e.Message)
}

// ValidationErrors represents multiple validation errors.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "no validation errors"
	}
	if len(e) == 1 {
		return e[0].Error()
	}

	var messages []string
	for _, err := range e {
		messages = append(messages, err.Error())
	}
	return fmt.Sprintf("multiple validation errors: [%s]", strings.Join(messages, "; "))
}

// Regular expressions for validation.
var (
	scopeNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// Validate validates a PantheonClusterVersions instance according to all constraints.
func (pcv *PantheonClusterVersions) Validate() error {
	var errors ValidationErrors

	// Validate that we have at least one version
	if len(pcv.Versions) == 0 {
		errors = append(errors, ValidationError{
			Field:   "versions",
			Message: "must have at least one version",
		})
		return errors
	}

	// Validate versions are ordered by effective date (latest first)
	if err := pcv.validateVersionOrdering(); err != nil {
		errors = append(errors, err...)
	}

	// Validate each cluster version
	for i, cluster := range pcv.Versions {
		if clusterErrors := cluster.validate(fmt.Sprintf("versions[%d]", i)); clusterErrors != nil {
			errors = append(errors, clusterErrors...)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// validateVersionOrdering validates that versions are ordered by effective date (latest first).
func (pcv *PantheonClusterVersions) validateVersionOrdering() ValidationErrors {
	var errors ValidationErrors

	for i := 0; i < len(pcv.Versions)-1; i++ {
		current := pcv.Versions[i]
		next := pcv.Versions[i+1]

		// Current should be after or equal to next (latest first ordering)
		if !current.EffectiveDate.After(next.EffectiveDate) {
			errors = append(errors, ValidationError{
				Field: fmt.Sprintf("versions[%d].effective_date", i),
				Message: fmt.Sprintf("versions must be ordered by effective date (latest first) decreasingly, but %s is not after %s",
					current.EffectiveDate.String(), next.EffectiveDate.String()),
			})
		}
	}

	return errors
}

// validate validates a single PantheonCluster.
func (pc *PantheonCluster) validate(prefix string) ValidationErrors {
	var errors ValidationErrors

	// Validate effective date is not zero
	if pc.EffectiveDate.IsZero() {
		errors = append(errors, ValidationError{
			Field:   prefix + ".effective_date",
			Message: "effective date cannot be zero",
		})
	}

	// Validate metric scopes
	if len(pc.MetricScopes) == 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".metric_scopes",
			Message: "must have at least one metric scope",
		})
	}

	scopeNames := make(map[string]bool)
	scopeShards := make(map[string]int)           // scope name -> shard count
	scopeDetails := make(map[string]*MetricScope) // scope name -> scope details
	for i, scope := range pc.MetricScopes {
		scopePrefix := fmt.Sprintf("%s.metric_scopes[%d]", prefix, i)

		// Validate scope name uniqueness
		if scopeNames[scope.ScopeName] {
			errors = append(errors, ValidationError{
				Field:   scopePrefix + ".scope_name",
				Message: fmt.Sprintf("duplicate scope name '%s'", scope.ScopeName),
			})
		}
		scopeNames[scope.ScopeName] = true
		scopeShards[scope.ScopeName] = scope.Shards
		scopeCopy := scope
		scopeDetails[scope.ScopeName] = &scopeCopy

		if scopeErrors := scope.validate(scopePrefix); scopeErrors != nil {
			errors = append(errors, scopeErrors...)
		}
	}

	// Validate DB groups
	if len(pc.DBGroups) == 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".db_groups",
			Message: "must have at least one DB group",
		})
	}

	dbGroupNames := make(map[string]bool)
	allTenants := make(map[string]string) // tenant -> db_group_name

	for i, dbGroup := range pc.DBGroups {
		dbGroupPrefix := fmt.Sprintf("%s.db_groups[%d]", prefix, i)

		// Validate DB group name uniqueness
		if dbGroupNames[dbGroup.DbGroupName] {
			errors = append(errors, ValidationError{
				Field:   dbGroupPrefix + ".db_group_name",
				Message: fmt.Sprintf("duplicate DB group name '%s'", dbGroup.DbGroupName),
			})
		}
		dbGroupNames[dbGroup.DbGroupName] = true

		// Validate tenant set uniqueness across DB groups
		for j, tenantSet := range dbGroup.TenantSets {
			tenantSetPrefix := fmt.Sprintf("%s.tenant_sets[%d]", dbGroupPrefix, j)

			// Validate tenant set format
			if tenantSetErrors := tenantSet.validate(tenantSetPrefix, scopeNames, scopeDetails); tenantSetErrors != nil {
				errors = append(errors, tenantSetErrors...)
			}

			// Generate all tenants for this tenant set and check for duplicates
			totalShards := scopeShards[tenantSet.MetricScopeName]
			tenantSetTenants := tenantSet.generateTenants(totalShards)
			for _, tenant := range tenantSetTenants {
				if existingGroup, exists := allTenants[tenant]; exists {
					errors = append(errors, ValidationError{
						Field: tenantSetPrefix,
						Message: fmt.Sprintf("tenant '%s' from tenant set is assigned to multiple DB groups: '%s' and '%s'",
							tenant, existingGroup, dbGroup.DbGroupName),
					})
				}
				allTenants[tenant] = dbGroup.DbGroupName
			}
		}

		if dbGroupErrors := dbGroup.validate(dbGroupPrefix); dbGroupErrors != nil {
			errors = append(errors, dbGroupErrors...)
		}
	}

	// Validate tenant assignments match metric scopes
	if tenantErrors := pc.validateTenantScopeConsistency(prefix, allTenants, scopeNames); tenantErrors != nil {
		errors = append(errors, tenantErrors...)
	}

	// Validate that all possible tenants are covered by DB groups
	if coverageErrors := pc.validateTenantCoverage(prefix, allTenants); coverageErrors != nil {
		errors = append(errors, coverageErrors...)
	}

	return errors
}

// validate validates a MetricScope.
func (ms *MetricScope) validate(prefix string) ValidationErrors {
	var errors ValidationErrors

	// Validate scope name format
	if ms.ScopeName == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".scope_name",
			Message: "scope name cannot be empty",
		})
	} else if !scopeNameRegex.MatchString(ms.ScopeName) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".scope_name",
			Message: "scope name must contain only [a-zA-Z0-9_-] characters",
		})
	}

	// Validate shard count
	if ms.Shards < 1 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".shards",
			Message: "shards must be >= 1",
		})
	}

	// Validate special metric groups
	groupNames := make(map[string]bool)
	for i, group := range ms.SpecialMetricGroups {
		groupPrefix := fmt.Sprintf("%s.special_metric_groups[%d]", prefix, i)

		// Validate group name uniqueness within scope
		if groupNames[group.GroupName] {
			errors = append(errors, ValidationError{
				Field:   groupPrefix + ".group_name",
				Message: fmt.Sprintf("duplicate special metric group name '%s'", group.GroupName),
			})
		}
		groupNames[group.GroupName] = true

		if groupErrors := group.validate(groupPrefix); groupErrors != nil {
			errors = append(errors, groupErrors...)
		}
	}

	return errors
}

// validate validates a SpecialMetricGroup.
func (smg *SpecialMetricGroup) validate(prefix string) ValidationErrors {
	var errors ValidationErrors

	// Validate group name
	if smg.GroupName == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".group_name",
			Message: "group name cannot be empty",
		})
	} else if !scopeNameRegex.MatchString(smg.GroupName) {
		errors = append(errors, ValidationError{
			Field:   prefix + ".group_name",
			Message: "group name must contain only [a-zA-Z0-9_-] characters",
		})
	}

	// Validate that at least one metric pattern is specified
	if len(smg.MetricNames) == 0 && len(smg.MetricNamePrefixes) == 0 && len(smg.MetricNameSuffixes) == 0 {
		errors = append(errors, ValidationError{
			Field:   prefix,
			Message: "special metric group must specify at least one of: metric_names, metric_name_prefixes, or metric_name_suffixes",
		})
	}

	// Validate metric names are not empty
	for i, metricName := range smg.MetricNames {
		if strings.TrimSpace(metricName) == "" {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.metric_names[%d]", prefix, i),
				Message: "metric name cannot be empty",
			})
		}
	}

	// Validate prefixes are not empty
	for i, prefix := range smg.MetricNamePrefixes {
		if strings.TrimSpace(prefix) == "" {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.metric_name_prefixes[%d]", prefix, i),
				Message: "metric name prefix cannot be empty",
			})
		}
	}

	// Validate suffixes are not empty
	for i, suffix := range smg.MetricNameSuffixes {
		if strings.TrimSpace(suffix) == "" {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.metric_name_suffixes[%d]", prefix, i),
				Message: "metric name suffix cannot be empty",
			})
		}
	}

	return errors
}

// validate validates a DBGroup.
func (dbg *DbGroup) validate(prefix string) ValidationErrors {
	var errors ValidationErrors

	// Validate DB group name
	if dbg.DbGroupName == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".db_group_name",
			Message: "DB group name cannot be empty",
		})
	}

	// Validate replica count
	if dbg.Replicas < 1 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".replicas",
			Message: "replicas must be >= 1 for production use",
		})
	} else if dbg.Replicas > 15 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".replicas",
			Message: "replicas should be <= 15 to avoid long release times",
		})
	}

	// Validate tenant count across all TenantSets
	totalTenants := 0
	for _, tenantSet := range dbg.TenantSets {
		totalTenants += len(tenantSet.Shards) + len(tenantSet.SpecialGroupNames)
	}
	if totalTenants > 30 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".tenant_sets",
			Message: fmt.Sprintf("DB group should have max 30 tenants, but has %d", totalTenants),
		})
	}

	// Validate HPA configuration
	if hpaErrors := dbg.DbHpa.validate(prefix + ".db_hpa"); hpaErrors != nil {
		errors = append(errors, hpaErrors...)
	}

	return errors
}

// validate validates a DbHpaConfig.
func (hpa *DbHpaConfig) validate(prefix string) ValidationErrors {
	var errors ValidationErrors

	// Validate replica constraints: MaxReplicas >= MinReplicas >= 0
	if hpa.MinReplicas < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".min_replicas",
			Message: "min_replicas must be >= 0",
		})
	}

	if hpa.MaxReplicas < hpa.MinReplicas {
		errors = append(errors, ValidationError{
			Field:   prefix + ".max_replicas",
			Message: fmt.Sprintf("max_replicas (%d) must be >= min_replicas (%d)", hpa.MaxReplicas, hpa.MinReplicas),
		})
	}

	if hpa.MaxReplicas > 15 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".max_replicas",
			Message: "max_replicas should be <= 15 to avoid long release times",
		})
	}

	// Check scaling limits (total pods = replicas * 3)
	totalMaxPods := hpa.MaxReplicas * 3
	totalMinPods := hpa.MinReplicas * 3

	if totalMaxPods > 45 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".max_replicas",
			Message: fmt.Sprintf("total max pods (%d) should not exceed 45 (triggers tenant reassignment)", totalMaxPods),
		})
	}

	if totalMinPods < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".min_replicas",
			Message: fmt.Sprintf("total min pods (%d) cannot be negative", totalMinPods),
		})
	}

	return errors
}

// validateTenantScopeConsistency validates that all tenants reference valid scopes.
func (pc *PantheonCluster) validateTenantScopeConsistency(prefix string, allTenants map[string]string, validScopes map[string]bool) ValidationErrors {
	var errors ValidationErrors

	for tenant, dbGroupName := range allTenants {
		// Parse tenant format: <scope>_<shard> or <scope>_<special_group>
		// Both formats use underscore as separator
		if !strings.Contains(tenant, "_") {
			errors = append(errors, ValidationError{
				Field:   prefix + ".tenants",
				Message: fmt.Sprintf("tenant '%s' in DB group '%s' has invalid format, expected '<scope>_<shard>' or '<scope>_<group>'", tenant, dbGroupName),
			})
			continue
		}

		// Extract scope name (everything before the first underscore)
		parts := strings.SplitN(tenant, "_", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			errors = append(errors, ValidationError{
				Field:   prefix + ".tenants",
				Message: fmt.Sprintf("tenant '%s' in DB group '%s' has invalid format, expected '<scope>_<shard>' or '<scope>_<group>'", tenant, dbGroupName),
			})
			continue
		}

		scopeName := parts[0]

		// Validate scope exists
		if !validScopes[scopeName] {
			errors = append(errors, ValidationError{
				Field:   prefix + ".tenants",
				Message: fmt.Sprintf("tenant '%s' in DB group '%s' references unknown scope '%s'", tenant, dbGroupName, scopeName),
			})
		}
	}

	return errors
}

// validate validates a TenantSet.
func (ts *TenantSet) validate(prefix string, validScopes map[string]bool, scopeDetails map[string]*MetricScope) ValidationErrors {
	var errors ValidationErrors

	// Validate metric scope name exists
	if ts.MetricScopeName == "" {
		errors = append(errors, ValidationError{
			Field:   prefix + ".metric_scope_name",
			Message: "metric scope name cannot be empty",
		})
		return errors
	}

	if !validScopes[ts.MetricScopeName] {
		errors = append(errors, ValidationError{
			Field:   prefix + ".metric_scope_name",
			Message: fmt.Sprintf("metric scope '%s' does not exist", ts.MetricScopeName),
		})
		return errors
	}

	scope := scopeDetails[ts.MetricScopeName]
	if scope == nil {
		errors = append(errors, ValidationError{
			Field:   prefix + ".metric_scope_name",
			Message: fmt.Sprintf("metric scope '%s' details not found", ts.MetricScopeName),
		})
		return errors
	}

	// Validate special group names exist in the referenced scope
	validGroupNames := make(map[string]bool)
	for _, group := range scope.SpecialMetricGroups {
		validGroupNames[group.GroupName] = true
	}

	// Check for duplicate special group names within this tenant set
	seenGroupNames := make(map[string]int)
	for i, groupName := range ts.SpecialGroupNames {
		if groupName == "" {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.special_group_names[%d]", prefix, i),
				Message: "special group name cannot be empty",
			})
			continue
		}

		// Check for duplicates within this tenant set
		if prevIndex, exists := seenGroupNames[groupName]; exists {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.special_group_names[%d]", prefix, i),
				Message: fmt.Sprintf("duplicate special group name '%s' (also found at index %d)", groupName, prevIndex),
			})
		}
		seenGroupNames[groupName] = i

		// Check if the special group exists in the referenced scope
		if !validGroupNames[groupName] {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.special_group_names[%d]", prefix, i),
				Message: fmt.Sprintf("special group '%s' does not exist in scope '%s'", groupName, ts.MetricScopeName),
			})
		}
	}

	// Validate shards exist in the referenced scope
	seenShards := make(map[int]int)
	for i, shard := range ts.Shards {
		if shard < 0 {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.shards[%d]", prefix, i),
				Message: "shard number must be >= 0",
			})
			continue
		}

		// Check for duplicates within this tenant set
		if prevIndex, exists := seenShards[shard]; exists {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.shards[%d]", prefix, i),
				Message: fmt.Sprintf("duplicate shard %d (also found at index %d)", shard, prevIndex),
			})
		}
		seenShards[shard] = i

		// Check if shard is within valid range for the scope
		if shard >= scope.Shards {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("%s.shards[%d]", prefix, i),
				Message: fmt.Sprintf("shard %d is out of range for scope '%s' (max: %d)", shard, ts.MetricScopeName, scope.Shards-1),
			})
		}
	}

	// Validate that tenant set has at least one shard or special group
	if len(ts.Shards) == 0 && len(ts.SpecialGroupNames) == 0 {
		errors = append(errors, ValidationError{
			Field:   prefix,
			Message: "tenant set must have at least one shard or special group",
		})
	}

	return errors
}

// generateTenants generates all tenant IDs for this TenantSet.
func (ts *TenantSet) generateTenants(totalShards int) []string {
	var tenants []string

	// Generate tenants for special groups
	for _, groupName := range ts.SpecialGroupNames {
		tenant := fmt.Sprintf("%s_%s", ts.MetricScopeName, groupName)
		tenants = append(tenants, tenant)
	}

	// Generate tenants for shards
	for _, shard := range ts.Shards {
		tenant := fmt.Sprintf("%s_%d-of-%d", ts.MetricScopeName, shard, totalShards)
		tenants = append(tenants, tenant)
	}

	return tenants
}

// validateTenantCoverage validates that all possible tenants are covered by DB groups.
func (pc *PantheonCluster) validateTenantCoverage(prefix string, assignedTenants map[string]string) ValidationErrors {
	var errors ValidationErrors

	// Generate all expected tenants from metric scopes
	expectedTenants := make(map[string]bool)

	for _, scope := range pc.MetricScopes {
		// Add regular shard tenants
		for shard := 0; shard < scope.Shards; shard++ {
			tenant := fmt.Sprintf("%s_%d-of-%d", scope.ScopeName, shard, scope.Shards)
			expectedTenants[tenant] = true
		}

		// Add special metric group tenants
		for _, group := range scope.SpecialMetricGroups {
			tenant := fmt.Sprintf("%s_%s", scope.ScopeName, group.GroupName)
			expectedTenants[tenant] = true
		}
	}

	// Check if all expected tenants are assigned
	for expectedTenant := range expectedTenants {
		if _, assigned := assignedTenants[expectedTenant]; !assigned {
			errors = append(errors, ValidationError{
				Field:   prefix,
				Message: fmt.Sprintf("tenant '%s' is not assigned to any DB group", expectedTenant),
			})
		}
	}

	// Check if there are any assigned tenants that are not expected
	for assignedTenant := range assignedTenants {
		if !expectedTenants[assignedTenant] {
			errors = append(errors, ValidationError{
				Field:   prefix,
				Message: fmt.Sprintf("tenant '%s' is assigned but not expected based on metric scopes", assignedTenant),
			})
		}
	}

	return errors
}
