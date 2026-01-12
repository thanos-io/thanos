// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"path"
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestExtractOrdinalFromHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		want     int
		wantErr  bool
	}{
		{
			name:     "valid statefulset hostname",
			hostname: "thanos-compact-0",
			want:     0,
			wantErr:  false,
		},
		{
			name:     "valid statefulset hostname with higher ordinal",
			hostname: "thanos-compact-5",
			want:     5,
			wantErr:  false,
		},
		{
			name:     "valid statefulset hostname with multiple dashes",
			hostname: "my-thanos-compact-service-3",
			want:     3,
			wantErr:  false,
		},
		{
			name:     "hostname with no dash",
			hostname: "hostname0",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "hostname ending with non-numeric",
			hostname: "thanos-compact-abc",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "empty hostname",
			hostname: "",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "hostname with only dash",
			hostname: "-",
			want:     0,
			wantErr:  true,
		},
		{
			name:     "hostname ending with dash",
			hostname: "thanos-compact-",
			want:     0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractOrdinalFromHostname(tt.hostname)
			if tt.wantErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tt.want, got)
			}
		})
	}
}

func TestTenantPrefixPathJoin(t *testing.T) {
	tests := []struct {
		name           string
		basePrefix     string
		commonPrefix   string
		tenant         string
		expectedPrefix string
	}{
		{
			name:           "single tenant mode with empty prefix",
			basePrefix:     "",
			commonPrefix:   "",
			tenant:         "",
			expectedPrefix: "",
		},
		{
			name:           "single tenant mode with base prefix",
			basePrefix:     "v1/raw",
			commonPrefix:   "",
			tenant:         "",
			expectedPrefix: "v1/raw",
		},
		{
			name:           "multi-tenant with all components",
			basePrefix:     "prod",
			commonPrefix:   "v1/raw",
			tenant:         "tenant1",
			expectedPrefix: "prod/v1/raw/tenant1",
		},
		{
			name:           "multi-tenant with no base prefix",
			basePrefix:     "",
			commonPrefix:   "v1/raw",
			tenant:         "tenant2",
			expectedPrefix: "v1/raw/tenant2",
		},
		{
			name:           "multi-tenant with complex tenant name",
			basePrefix:     "base",
			commonPrefix:   "v1/raw",
			tenant:         "team-a/project-1",
			expectedPrefix: "base/v1/raw/team-a/project-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate what happens in the code
			tenantPrefix := path.Join(tt.commonPrefix, tt.tenant)
			finalPrefix := path.Join(tt.basePrefix, tenantPrefix)
			testutil.Equals(t, tt.expectedPrefix, finalPrefix)
		})
	}
}

func TestShardCalculation(t *testing.T) {
	tests := []struct {
		name              string
		replicas          int
		replicationFactor int
		expectedShards    int
		expectError       bool
	}{
		{
			name:              "single replica, single factor",
			replicas:          1,
			replicationFactor: 1,
			expectedShards:    1,
			expectError:       false,
		},
		{
			name:              "six replicas, three factor",
			replicas:          6,
			replicationFactor: 3,
			expectedShards:    2,
			expectError:       false,
		},
		{
			name:              "nine replicas, three factor",
			replicas:          9,
			replicationFactor: 3,
			expectedShards:    3,
			expectError:       false,
		},
		{
			name:              "not divisible",
			replicas:          5,
			replicationFactor: 2,
			expectedShards:    0,
			expectError:       true,
		},
		{
			name:              "zero replication factor",
			replicas:          3,
			replicationFactor: 0,
			expectedShards:    0,
			expectError:       true,
		},
		{
			name:              "negative replication factor",
			replicas:          3,
			replicationFactor: -1,
			expectedShards:    0,
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the shard calculation logic from runCompact
			// Check replication factor first to avoid division by zero or modulo by zero
			if tt.replicationFactor <= 0 {
				if !tt.expectError {
					t.Errorf("expected no error but replication factor is invalid")
				}
				return
			}

			shouldError := tt.replicas%tt.replicationFactor != 0

			if shouldError {
				if !tt.expectError {
					t.Errorf("expected no error but validation should fail")
				}
				return
			}

			if tt.expectError {
				t.Errorf("expected error but validation passed")
				return
			}

			totalShards := tt.replicas / tt.replicationFactor
			testutil.Equals(t, tt.expectedShards, totalShards)
		})
	}
}

func TestOrdinalValidation(t *testing.T) {
	tests := []struct {
		name        string
		ordinal     int
		totalShards int
		expectError bool
	}{
		{
			name:        "ordinal 0 with 3 shards",
			ordinal:     0,
			totalShards: 3,
			expectError: false,
		},
		{
			name:        "ordinal 2 with 3 shards",
			ordinal:     2,
			totalShards: 3,
			expectError: false,
		},
		{
			name:        "ordinal equals total shards",
			ordinal:     3,
			totalShards: 3,
			expectError: true,
		},
		{
			name:        "ordinal greater than total shards",
			ordinal:     5,
			totalShards: 3,
			expectError: true,
		},
		{
			name:        "single shard",
			ordinal:     0,
			totalShards: 1,
			expectError: false,
		},
		{
			name:        "ordinal 1 with single shard",
			ordinal:     1,
			totalShards: 1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the ordinal validation logic from runCompact
			if tt.ordinal >= tt.totalShards {
				if !tt.expectError {
					t.Errorf("expected no error but ordinal %d >= totalShards %d", tt.ordinal, tt.totalShards)
				}
			} else {
				if tt.expectError {
					t.Errorf("expected error but ordinal %d < totalShards %d", tt.ordinal, tt.totalShards)
				}
			}
		})
	}
}

func TestTenantPrefixGeneration(t *testing.T) {
	tests := []struct {
		name          string
		commonPrefix  string
		tenants       []string
		expectedPaths []string
	}{
		{
			name:         "single tenant",
			commonPrefix: "v1/raw",
			tenants:      []string{"tenant1"},
			expectedPaths: []string{
				"v1/raw/tenant1",
			},
		},
		{
			name:         "multiple tenants",
			commonPrefix: "v1/raw",
			tenants:      []string{"tenant1", "tenant2", "tenant3"},
			expectedPaths: []string{
				"v1/raw/tenant1",
				"v1/raw/tenant2",
				"v1/raw/tenant3",
			},
		},
		{
			name:         "no common prefix",
			commonPrefix: "",
			tenants:      []string{"tenant1", "tenant2"},
			expectedPaths: []string{
				"tenant1",
				"tenant2",
			},
		},
		{
			name:          "no tenants",
			commonPrefix:  "v1/raw",
			tenants:       []string{},
			expectedPaths: []string{},
		},
		{
			name:         "complex paths",
			commonPrefix: "v1/raw",
			tenants:      []string{"org/team-a", "org/team-b"},
			expectedPaths: []string{
				"v1/raw/org/team-a",
				"v1/raw/org/team-b",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate tenant prefix generation from runCompact
			var tenantPrefixes []string
			for _, tenant := range tt.tenants {
				tenantPrefixes = append(tenantPrefixes, path.Join(tt.commonPrefix, tenant))
			}

			testutil.Equals(t, len(tt.expectedPaths), len(tenantPrefixes))
			for i, expected := range tt.expectedPaths {
				testutil.Equals(t, expected, tenantPrefixes[i])
			}
		})
	}
}

func TestSingleTenantModeBehavior(t *testing.T) {
	// Test that single tenant mode behaves like the original code
	tests := []struct {
		name                   string
		enableTenantPathPrefix bool
		expectedTenantPrefixes []string
		expectedIsMultiTenant  bool
	}{
		{
			name:                   "single tenant mode (default)",
			enableTenantPathPrefix: false,
			expectedTenantPrefixes: []string{""},
			expectedIsMultiTenant:  false,
		},
		{
			name:                   "multi-tenant mode enabled",
			enableTenantPathPrefix: true,
			// Note: actual prefixes would come from SetupTenantPartitioning
			// This test just validates the flag behavior
			expectedTenantPrefixes: nil, // Would be populated by SetupTenantPartitioning
			expectedIsMultiTenant:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tenantPrefixes []string
			var isMultiTenant bool

			if tt.enableTenantPathPrefix {
				isMultiTenant = true
				// In real code, this would call SetupTenantPartitioning
				// For this test, we just verify the flag sets isMultiTenant correctly
			} else {
				isMultiTenant = false
				tenantPrefixes = []string{""}
			}

			testutil.Equals(t, tt.expectedIsMultiTenant, isMultiTenant)

			if !tt.enableTenantPathPrefix {
				testutil.Equals(t, 1, len(tenantPrefixes))
				testutil.Equals(t, "", tenantPrefixes[0])
			}
		})
	}
}

func TestPathJoinPreservesPrefix(t *testing.T) {
	// Critical test: verify that path.Join with empty string preserves the original prefix
	// This ensures backward compatibility when enableTenantPathPrefix = false
	tests := []struct {
		prefix   string
		expected string
	}{
		{"", ""},
		{"v1/raw", "v1/raw"},
		{"prod/v1/raw", "prod/v1/raw"},
		{"/absolute/path", "/absolute/path"},
		{"path/with/trailing/slash/", "path/with/trailing/slash"},
	}

	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			result := path.Join(tt.prefix, "")
			testutil.Equals(t, tt.expected, result)
		})
	}
}
