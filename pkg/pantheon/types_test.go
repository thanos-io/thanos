// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pantheon

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v2"
)

func TestPantheonClusterVersions_Validate(t *testing.T) {
	tests := []struct {
		name            string
		clusterVersions *PantheonClusterVersions
		expectError     bool
		errorContains   []string
	}{
		{
			name: "valid configuration",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{
								ScopeName: "az-eastus2",
								Shards:    3,
								SpecialMetricGroups: []SpecialMetricGroup{
									{
										GroupName:   "kube-metrics",
										MetricNames: []string{"container_cpu_usage_seconds_total"},
									},
								},
							},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    3,
								DbHpa: DbHpaConfig{
									Enabled:     true,
									MaxReplicas: 10,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName:   "az-eastus2",
										Shards:            []int{0, 1},
										SpecialGroupNames: []string{"kube-metrics"},
									},
								},
							},
							{
								DbGroupName: "pantheon-db-a1",
								Replicas:    2,
								DbHpa: DbHpaConfig{
									Enabled:     true,
									MaxReplicas: 5,
									MinReplicas: 0,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{2},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "empty versions",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{},
			},
			expectError:   true,
			errorContains: []string{"must have at least one version"},
		},
		{
			name: "versions not ordered by date",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 3),
						MetricScopes: []MetricScope{
							{ScopeName: "test", Shards: 1},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "test-db",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "test",
										Shards:          []int{0},
									},
								},
							},
						},
					},
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "test", Shards: 1},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "test-db",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "test",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"versions must be ordered by effective date", "decreasingly"},
		},
		{
			name: "duplicate scope names",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
							{ScopeName: "az-eastus2", Shards: 10},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "test-db",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"duplicate scope name 'az-eastus2'"},
		},
		{
			name: "duplicate DB group names",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0},
									},
								},
							},
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    2,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 2,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{1},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"duplicate DB group name 'pantheon-db-a0'"},
		},
		{
			name: "duplicate special metric group names",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{
								ScopeName: "az-eastus2",
								Shards:    20,
								SpecialMetricGroups: []SpecialMetricGroup{
									{
										GroupName:   "kube-metrics",
										MetricNames: []string{"container_cpu_usage_seconds_total"},
									},
									{
										GroupName:   "kube-metrics",
										MetricNames: []string{"container_memory_working_set_bytes"},
									},
								},
							},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName:   "az-eastus2",
										SpecialGroupNames: []string{"kube-metrics"},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"duplicate special metric group name 'kube-metrics'"},
		},
		{
			name: "duplicate tenants across DB groups",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
									},
								},
							},
							{
								DbGroupName: "pantheon-db-a1",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"tenant 'az-eastus2_0-of-20' from tenant set is assigned to multiple DB groups"},
		},
		{
			name: "invalid scope name characters",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2@invalid", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2@invalid", // Invalid scope name
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"scope name must contain only [a-zA-Z0-9_-] characters"},
		},
		{
			name: "invalid tenant format",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "invalid-tenant-format", // Invalid scope name
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"metric scope 'invalid-tenant-format' does not exist"},
		},
		{
			name: "tenant references unknown scope",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "unknown-scope",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"references unknown scope 'unknown-scope'"},
		},
		{
			name: "invalid HPA configuration",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     true,
									MaxReplicas: 5,
									MinReplicas: 10, // Invalid: min > max
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"max_replicas (5) must be >= min_replicas (10)"},
		},
		{
			name: "zero effective date",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: Date{}, // Zero date
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"effective date cannot be zero"},
		},
		{
			name: "invalid shards count",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 0}, // Invalid: shards must be >= 1
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"shards must be >= 1"},
		},
		{
			name: "too many tenants per DB group",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 50},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}, // 31 shards - more than 30 tenants
									},
								},
							},
							{
								DbGroupName: "pantheon-db-a1",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49}, // remaining 19 shards
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"DB group should have max 30 tenants"},
		},
		{
			name: "special metric group without patterns",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{
								ScopeName: "az-eastus2",
								Shards:    20,
								SpecialMetricGroups: []SpecialMetricGroup{
									{
										GroupName: "empty-group",
										// No metrics, prefixes, or suffixes
									},
								},
							},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName:   "az-eastus2",
										SpecialGroupNames: []string{"empty-group"},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"special metric group must specify at least one of: metric_names, metric_name_prefixes, or metric_name_suffixes"},
		},
		{
			name: "min replicas can be zero",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 1},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     true,
									MaxReplicas: 5,
									MinReplicas: 0, // Should be valid now
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "negative min replicas",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 20},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     true,
									MaxReplicas: 5,
									MinReplicas: -1, // Invalid: negative
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"min_replicas must be >= 0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.clusterVersions.Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("expected validation error but got none")
					return
				}

				errMsg := err.Error()
				for _, expectedSubstring := range tt.errorContains {
					if !strings.Contains(errMsg, expectedSubstring) {
						t.Errorf("expected error message to contain '%s', but got: %s", expectedSubstring, errMsg)
					}
				}
			} else {
				if err != nil {
					t.Errorf("expected no validation error but got: %v", err)
				}
			}
		})
	}
}

func TestValidationError_Error(t *testing.T) {
	err := ValidationError{
		Field:   "test.field",
		Message: "test message",
	}

	expected := "validation error in field 'test.field': test message"
	if err.Error() != expected {
		t.Errorf("expected '%s', got '%s'", expected, err.Error())
	}
}

func TestValidationErrors_Error(t *testing.T) {
	tests := []struct {
		name     string
		errors   ValidationErrors
		expected string
	}{
		{
			name:     "no errors",
			errors:   ValidationErrors{},
			expected: "no validation errors",
		},
		{
			name: "single error",
			errors: ValidationErrors{
				ValidationError{Field: "test.field", Message: "test message"},
			},
			expected: "validation error in field 'test.field': test message",
		},
		{
			name: "multiple errors",
			errors: ValidationErrors{
				ValidationError{Field: "field1", Message: "message1"},
				ValidationError{Field: "field2", Message: "message2"},
			},
			expected: "multiple validation errors: [validation error in field 'field1': message1; validation error in field 'field2': message2]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.errors.Error(); got != tt.expected {
				t.Errorf("ValidationErrors.Error() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMetricScope_validate(t *testing.T) {
	tests := []struct {
		name          string
		scope         MetricScope
		expectError   bool
		errorContains string
	}{
		{
			name: "valid scope",
			scope: MetricScope{
				ScopeName: "az-eastus2",
				Shards:    20,
				SpecialMetricGroups: []SpecialMetricGroup{
					{
						GroupName:   "kube-metrics",
						MetricNames: []string{"container_cpu_usage_seconds_total"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "empty scope name",
			scope: MetricScope{
				ScopeName: "",
				Shards:    20,
			},
			expectError:   true,
			errorContains: "scope name cannot be empty",
		},
		{
			name: "invalid scope name characters",
			scope: MetricScope{
				ScopeName: "az-eastus2@invalid",
				Shards:    20,
			},
			expectError:   true,
			errorContains: "scope name must contain only [a-zA-Z0-9_-] characters",
		},
		{
			name: "zero shards",
			scope: MetricScope{
				ScopeName: "az-eastus2",
				Shards:    0,
			},
			expectError:   true,
			errorContains: "shards must be >= 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := tt.scope.validate("test")

			if tt.expectError {
				if len(errors) == 0 {
					t.Errorf("expected validation error but got none")
					return
				}

				found := false
				for _, err := range errors {
					if strings.Contains(err.Message, tt.errorContains) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error message to contain '%s', but got: %v", tt.errorContains, errors)
				}
			} else {
				if len(errors) > 0 {
					t.Errorf("expected no validation error but got: %v", errors)
				}
			}
		})
	}
}

func TestDBGroup_validate(t *testing.T) {
	tests := []struct {
		name          string
		dbGroup       DbGroup
		expectError   bool
		errorContains string
	}{
		{
			name: "valid DB group",
			dbGroup: DbGroup{
				DbGroupName: "pantheon-db-a0",
				Replicas:    3,
				DbHpa: DbHpaConfig{
					Enabled:     true,
					MaxReplicas: 10,
					MinReplicas: 1,
				},
				TenantSets: []TenantSet{
					{
						MetricScopeName: "az-eastus2",
						Shards:          []int{0},
					},
				},
			},
			expectError: false,
		},
		{
			name: "empty DB group name",
			dbGroup: DbGroup{
				DbGroupName: "",
				Replicas:    3,
				DbHpa: DbHpaConfig{
					Enabled:     false,
					MaxReplicas: 1,
					MinReplicas: 1,
				},
				TenantSets: []TenantSet{
					{
						MetricScopeName: "az-eastus2",
						Shards:          []int{0},
					},
				},
			},
			expectError:   true,
			errorContains: "DB group name cannot be empty",
		},
		{
			name: "zero replicas",
			dbGroup: DbGroup{
				DbGroupName: "pantheon-db-a0",
				Replicas:    0,
				DbHpa: DbHpaConfig{
					Enabled:     false,
					MaxReplicas: 1,
					MinReplicas: 1,
				},
				TenantSets: []TenantSet{
					{
						MetricScopeName: "az-eastus2",
						Shards:          []int{0},
					},
				},
			},
			expectError:   true,
			errorContains: "replicas must be >= 1 for production use",
		},
		{
			name: "too many replicas",
			dbGroup: DbGroup{
				DbGroupName: "pantheon-db-a0",
				Replicas:    20,
				DbHpa: DbHpaConfig{
					Enabled:     false,
					MaxReplicas: 20,
					MinReplicas: 20,
				},
				TenantSets: []TenantSet{
					{
						MetricScopeName: "az-eastus2",
						Shards:          []int{0},
					},
				},
			},
			expectError:   true,
			errorContains: "replicas should be <= 15 to avoid long release times",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := tt.dbGroup.validate("test")

			if tt.expectError {
				if len(errors) == 0 {
					t.Errorf("expected validation error but got none")
					return
				}

				found := false
				for _, err := range errors {
					if strings.Contains(err.Message, tt.errorContains) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error message to contain '%s', but got: %v", tt.errorContains, errors)
				}
			} else {
				if len(errors) > 0 {
					t.Errorf("expected no validation error but got: %v", errors)
				}
			}
		})
	}
}

func TestPantheonClusterVersions_UnmarshalJSON(t *testing.T) {
	jsonData := `{
		"versions": [
			{
				"effective_date": "2025-07-04",
				"metric_scopes": [
					{
						"scope_name": "az-eastus2",
						"shards": 2,
						"special_metric_groups": [
							{
								"group_name": "kube-metrics",
								"metric_names": [
									"container_cpu_usage_seconds_total",
									"container_memory_working_set_bytes"
								],
								"metric_name_prefixes": ["autoscaling__"],
								"metric_name_suffixes": [":recording_rules"]
							}
						]
					}
				],
				"db_groups": [
					{
						"db_group_name": "pantheon-db-a0",
						"replicas": 3,
						"db_hpa": {
							"enabled": true,
							"max_replicas": 10,
							"min_replicas": 1
						},
						"tenant_sets": [
							{
								"metric_scope_name": "az-eastus2",
								"shards": [0],
								"special_group_names": ["kube-metrics"]
							}
						]
					},
					{
						"db_group_name": "pantheon-db-a1",
						"replicas": 2,
						"db_hpa": {
							"enabled": true,
							"max_replicas": 10,
							"min_replicas": 1
						},
						"tenant_sets": [
							{
								"metric_scope_name": "az-eastus2",
								"shards": [1]
							}
						]
					}
				]
			}
		]
	}`

	var clusterVersions PantheonClusterVersions
	err := json.Unmarshal([]byte(jsonData), &clusterVersions)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// Validate the structure was unmarshaled correctly
	if len(clusterVersions.Versions) != 1 {
		t.Errorf("Expected 1 version, got %d", len(clusterVersions.Versions))
	}

	cluster := clusterVersions.Versions[0]

	// Check effective date
	expectedDate := NewDate(2025, time.July, 4)
	if !cluster.EffectiveDate.Equal(expectedDate) {
		t.Errorf("Expected effective date %s, got %s", expectedDate.String(), cluster.EffectiveDate.String())
	}

	// Check metric scopes
	if len(cluster.MetricScopes) != 1 {
		t.Errorf("Expected 1 metric scope, got %d", len(cluster.MetricScopes))
	}

	// Check first metric scope
	scope := cluster.MetricScopes[0]
	if scope.ScopeName != "az-eastus2" {
		t.Errorf("Expected scope name 'az-eastus2', got '%s'", scope.ScopeName)
	}
	if scope.Shards != 2 {
		t.Errorf("Expected 2 shards, got %d", scope.Shards)
	}

	// Check special metric groups
	if len(scope.SpecialMetricGroups) != 1 {
		t.Errorf("Expected 1 special metric group, got %d", len(scope.SpecialMetricGroups))
	}

	kubeGroup := scope.SpecialMetricGroups[0]
	if kubeGroup.GroupName != "kube-metrics" {
		t.Errorf("Expected group name 'kube-metrics', got '%s'", kubeGroup.GroupName)
	}
	if len(kubeGroup.MetricNames) != 2 {
		t.Errorf("Expected 2 metric names, got %d", len(kubeGroup.MetricNames))
	}
	if len(kubeGroup.MetricNamePrefixes) != 1 {
		t.Errorf("Expected 1 metric name prefix, got %d", len(kubeGroup.MetricNamePrefixes))
	}
	if len(kubeGroup.MetricNameSuffixes) != 1 {
		t.Errorf("Expected 1 metric name suffix, got %d", len(kubeGroup.MetricNameSuffixes))
	}

	// Check DB groups
	if len(cluster.DBGroups) != 2 {
		t.Errorf("Expected 2 DB groups, got %d", len(cluster.DBGroups))
	}

	// Check first DB group
	dbGroup := cluster.DBGroups[0]
	if dbGroup.DbGroupName != "pantheon-db-a0" {
		t.Errorf("Expected DB group name 'pantheon-db-a0', got '%s'", dbGroup.DbGroupName)
	}
	if dbGroup.Replicas != 3 {
		t.Errorf("Expected 3 replicas, got %d", dbGroup.Replicas)
	}

	// Check HPA config
	if !dbGroup.DbHpa.Enabled {
		t.Error("Expected HPA to be enabled")
	}
	if dbGroup.DbHpa.MaxReplicas != 10 {
		t.Errorf("Expected max replicas 10, got %d", dbGroup.DbHpa.MaxReplicas)
	}
	if dbGroup.DbHpa.MinReplicas != 1 {
		t.Errorf("Expected min replicas 1, got %d", dbGroup.DbHpa.MinReplicas)
	}

	// Check tenant sets
	if len(dbGroup.TenantSets) != 1 {
		t.Errorf("Expected 1 tenant set, got %d", len(dbGroup.TenantSets))
	}

	// Check second DB group
	secondDbGroup := cluster.DBGroups[1]
	if secondDbGroup.DbGroupName != "pantheon-db-a1" {
		t.Errorf("Expected second DB group name 'pantheon-db-a1', got '%s'", secondDbGroup.DbGroupName)
	}
	if len(secondDbGroup.TenantSets) != 1 {
		t.Errorf("Expected 1 tenant set in second DB group, got %d", len(secondDbGroup.TenantSets))
	}

	// Validate the unmarshaled structure
	if err := clusterVersions.Validate(); err != nil {
		t.Errorf("Unmarshaled structure failed validation: %v", err)
	}
}

func TestPantheonClusterVersions_UnmarshalYAML(t *testing.T) {
	yamlData := `
versions:
  - effective_date: "2025-07-04"
    metric_scopes:
      - scope_name: "az-eastus2"
        shards: 2
        special_metric_groups:
          - group_name: "kube-metrics"
            metric_names:
              - "container_cpu_usage_seconds_total"
              - "container_memory_working_set_bytes"
            metric_name_prefixes:
              - "autoscaling__"
            metric_name_suffixes:
              - ":recording_rules"
    db_groups:
      - db_group_name: "pantheon-db-a0"
        replicas: 3
        db_hpa:
          enabled: true
          max_replicas: 10
          min_replicas: 1
        tenant_sets:
          - metric_scope_name: "az-eastus2"
            shards: [0]
            special_group_names: ["kube-metrics"]
      - db_group_name: "pantheon-db-a1"
        replicas: 2
        db_hpa:
          enabled: true
          max_replicas: 10
          min_replicas: 1
        tenant_sets:
          - metric_scope_name: "az-eastus2"
            shards: [1]
`

	var clusterVersions PantheonClusterVersions
	err := yaml.Unmarshal([]byte(yamlData), &clusterVersions)
	if err != nil {
		t.Fatalf("Failed to unmarshal YAML: %v", err)
	}

	// Validate the structure was unmarshaled correctly
	if len(clusterVersions.Versions) != 1 {
		t.Errorf("Expected 1 version, got %d", len(clusterVersions.Versions))
	}

	cluster := clusterVersions.Versions[0]

	// Check effective date
	expectedDate := NewDate(2025, time.July, 4)
	if !cluster.EffectiveDate.Equal(expectedDate) {
		t.Errorf("Expected effective date %s, got %s", expectedDate.String(), cluster.EffectiveDate.String())
	}

	// Check metric scopes
	if len(cluster.MetricScopes) != 1 {
		t.Errorf("Expected 1 metric scope, got %d", len(cluster.MetricScopes))
	}

	// Check first metric scope
	scope := cluster.MetricScopes[0]
	if scope.ScopeName != "az-eastus2" {
		t.Errorf("Expected scope name 'az-eastus2', got '%s'", scope.ScopeName)
	}
	if scope.Shards != 2 {
		t.Errorf("Expected 2 shards, got %d", scope.Shards)
	}

	// Check special metric groups
	if len(scope.SpecialMetricGroups) != 1 {
		t.Errorf("Expected 1 special metric group, got %d", len(scope.SpecialMetricGroups))
	}

	kubeGroup := scope.SpecialMetricGroups[0]
	if kubeGroup.GroupName != "kube-metrics" {
		t.Errorf("Expected group name 'kube-metrics', got '%s'", kubeGroup.GroupName)
	}
	if len(kubeGroup.MetricNames) != 2 {
		t.Errorf("Expected 2 metric names, got %d", len(kubeGroup.MetricNames))
	}

	// Check DB groups
	if len(cluster.DBGroups) != 2 {
		t.Errorf("Expected 2 DB groups, got %d", len(cluster.DBGroups))
	}

	// Check first DB group
	dbGroup := cluster.DBGroups[0]
	if dbGroup.DbGroupName != "pantheon-db-a0" {
		t.Errorf("Expected DB group name 'pantheon-db-a0', got '%s'", dbGroup.DbGroupName)
	}
	if dbGroup.Replicas != 3 {
		t.Errorf("Expected 3 replicas, got %d", dbGroup.Replicas)
	}

	// Check HPA config
	if !dbGroup.DbHpa.Enabled {
		t.Error("Expected HPA to be enabled")
	}
	if dbGroup.DbHpa.MaxReplicas != 10 {
		t.Errorf("Expected max replicas 10, got %d", dbGroup.DbHpa.MaxReplicas)
	}
	if dbGroup.DbHpa.MinReplicas != 1 {
		t.Errorf("Expected min replicas 1, got %d", dbGroup.DbHpa.MinReplicas)
	}

	// Validate the unmarshaled structure
	if err := clusterVersions.Validate(); err != nil {
		t.Errorf("Unmarshaled structure failed validation: %v", err)
	}
}

func TestPantheonClusterVersions_MarshalUnmarshalRoundTrip(t *testing.T) {
	// Create a test structure
	original := PantheonClusterVersions{
		Versions: []PantheonCluster{
			{
				EffectiveDate: NewDate(2025, time.July, 4),
				MetricScopes: []MetricScope{
					{
						ScopeName: "az-eastus2",
						Shards:    2,
						SpecialMetricGroups: []SpecialMetricGroup{
							{
								GroupName:          "kube-metrics",
								MetricNames:        []string{"container_cpu_usage_seconds_total"},
								MetricNamePrefixes: []string{"autoscaling__"},
								MetricNameSuffixes: []string{":recording_rules"},
							},
						},
					},
				},
				DBGroups: []DbGroup{
					{
						DbGroupName: "pantheon-db-a0",
						Replicas:    3,
						DbHpa: DbHpaConfig{
							Enabled:     true,
							MaxReplicas: 10,
							MinReplicas: 1,
						},
						TenantSets: []TenantSet{
							{
								MetricScopeName:   "az-eastus2",
								Shards:            []int{0},
								SpecialGroupNames: []string{"kube-metrics"},
							},
						},
					},
					{
						DbGroupName: "pantheon-db-a1",
						Replicas:    1,
						DbHpa: DbHpaConfig{
							Enabled:     false,
							MaxReplicas: 1,
							MinReplicas: 1,
						},
						TenantSets: []TenantSet{
							{
								MetricScopeName: "az-eastus2",
								Shards:          []int{1},
							},
						},
					},
				},
			},
		},
	}

	// Test JSON round trip
	t.Run("JSON round trip", func(t *testing.T) {
		jsonData, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Failed to marshal to JSON: %v", err)
		}

		var unmarshaled PantheonClusterVersions
		err = json.Unmarshal(jsonData, &unmarshaled)
		if err != nil {
			t.Fatalf("Failed to unmarshal from JSON: %v", err)
		}

		// Compare key fields
		if len(unmarshaled.Versions) != 1 {
			t.Errorf("Expected 1 version after JSON round trip, got %d", len(unmarshaled.Versions))
		}

		cluster := unmarshaled.Versions[0]
		originalCluster := original.Versions[0]

		if !cluster.EffectiveDate.Equal(originalCluster.EffectiveDate) {
			t.Errorf("Effective date mismatch after JSON round trip: expected %s, got %s",
				originalCluster.EffectiveDate.String(), cluster.EffectiveDate.String())
		}

		if len(cluster.MetricScopes) != len(originalCluster.MetricScopes) {
			t.Errorf("Metric scopes count mismatch after JSON round trip: expected %d, got %d",
				len(originalCluster.MetricScopes), len(cluster.MetricScopes))
		}

		if len(cluster.DBGroups) != len(originalCluster.DBGroups) {
			t.Errorf("DB groups count mismatch after JSON round trip: expected %d, got %d",
				len(originalCluster.DBGroups), len(cluster.DBGroups))
		}

		// Validate the round-tripped structure
		if err := unmarshaled.Validate(); err != nil {
			t.Errorf("JSON round-tripped structure failed validation: %v", err)
		}
	})

	// Test YAML round trip
	t.Run("YAML round trip", func(t *testing.T) {
		yamlData, err := yaml.Marshal(original)
		if err != nil {
			t.Fatalf("Failed to marshal to YAML: %v", err)
		}

		var unmarshaled PantheonClusterVersions
		err = yaml.Unmarshal(yamlData, &unmarshaled)
		if err != nil {
			t.Fatalf("Failed to unmarshal from YAML: %v", err)
		}

		// Compare key fields
		if len(unmarshaled.Versions) != 1 {
			t.Errorf("Expected 1 version after YAML round trip, got %d", len(unmarshaled.Versions))
		}

		cluster := unmarshaled.Versions[0]
		originalCluster := original.Versions[0]

		if !cluster.EffectiveDate.Equal(originalCluster.EffectiveDate) {
			t.Errorf("Effective date mismatch after YAML round trip: expected %s, got %s",
				originalCluster.EffectiveDate.String(), cluster.EffectiveDate.String())
		}

		// Validate the round-tripped structure
		if err := unmarshaled.Validate(); err != nil {
			t.Errorf("YAML round-tripped structure failed validation: %v", err)
		}
	})
}

func TestTenantSet_validate(t *testing.T) {
	validScopes := map[string]bool{
		"az-eastus2": true,
		"az-westus":  true,
	}

	scopeDetails := map[string]*MetricScope{
		"az-eastus2": {
			ScopeName: "az-eastus2",
			Shards:    10,
			SpecialMetricGroups: []SpecialMetricGroup{
				{GroupName: "kube-metrics"},
				{GroupName: "rpc-metrics"},
			},
		},
		"az-westus": {
			ScopeName: "az-westus",
			Shards:    5,
		},
	}

	tests := []struct {
		name          string
		tenantSet     TenantSet
		expectError   bool
		errorContains string
	}{
		{
			name: "valid tenant set",
			tenantSet: TenantSet{
				MetricScopeName: "az-eastus2",
				Shards:          []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
			expectError: false,
		},
		{
			name: "empty metric scope",
			tenantSet: TenantSet{
				MetricScopeName: "",
				Shards:          []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
			expectError:   true,
			errorContains: "metric scope name cannot be empty",
		},
		{
			name: "unknown metric scope",
			tenantSet: TenantSet{
				MetricScopeName: "unknown-scope",
				Shards:          []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
			expectError:   true,
			errorContains: "metric scope 'unknown-scope' does not exist",
		},
		{
			name: "negative shard",
			tenantSet: TenantSet{
				MetricScopeName: "az-eastus2",
				Shards:          []int{-1, 1, 2},
			},
			expectError:   true,
			errorContains: "shard number must be >= 0",
		},
		{
			name: "shard out of range",
			tenantSet: TenantSet{
				MetricScopeName: "az-eastus2",
				Shards:          []int{100}, // Out of range for az-eastus2 scope
			},
			expectError:   true,
			errorContains: "shard 100 is out of range",
		},
		{
			name: "duplicate shards",
			tenantSet: TenantSet{
				MetricScopeName: "az-eastus2",
				Shards:          []int{1, 2, 1}, // Duplicate shard 1
			},
			expectError:   true,
			errorContains: "duplicate shard 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := tt.tenantSet.validate("test", validScopes, scopeDetails)

			if tt.expectError {
				if len(errors) == 0 {
					t.Errorf("expected validation error but got none")
					return
				}

				found := false
				for _, err := range errors {
					if strings.Contains(err.Message, tt.errorContains) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error message to contain '%s', but got: %v", tt.errorContains, errors)
				}
			} else {
				if len(errors) > 0 {
					t.Errorf("expected no validation error but got: %v", errors)
				}
			}
		})
	}
}

func TestTenantSet_generateTenants(t *testing.T) {
	tests := []struct {
		name      string
		tenantSet TenantSet
		expected  []string
	}{
		{
			name: "single shard",
			tenantSet: TenantSet{
				MetricScopeName: "az-eastus2",
				Shards:          []int{0},
			},
			expected: []string{"az-eastus2_0-of-10"},
		},
		{
			name: "multiple shards",
			tenantSet: TenantSet{
				MetricScopeName: "az-eastus2",
				Shards:          []int{0, 1, 2},
			},
			expected: []string{
				"az-eastus2_0-of-10",
				"az-eastus2_1-of-10",
				"az-eastus2_2-of-10",
			},
		},
		{
			name: "non-sequential shards",
			tenantSet: TenantSet{
				MetricScopeName: "az-westus",
				Shards:          []int{5, 6, 7},
			},
			expected: []string{
				"az-westus_5-of-10",
				"az-westus_6-of-10",
				"az-westus_7-of-10",
			},
		},
		{
			name: "special groups only",
			tenantSet: TenantSet{
				MetricScopeName:   "az-eastus2",
				SpecialGroupNames: []string{"kube-metrics", "rpc-metrics"},
			},
			expected: []string{
				"az-eastus2_kube-metrics",
				"az-eastus2_rpc-metrics",
			},
		},
		{
			name: "mixed shards and special groups",
			tenantSet: TenantSet{
				MetricScopeName:   "az-eastus2",
				Shards:            []int{0, 1},
				SpecialGroupNames: []string{"kube-metrics"},
			},
			expected: []string{
				"az-eastus2_kube-metrics",
				"az-eastus2_0-of-10",
				"az-eastus2_1-of-10",
			},
		},
		{
			name: "empty tenant set",
			tenantSet: TenantSet{
				MetricScopeName: "az-eastus2",
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a default of 10 total shards for the test
			result := tt.tenantSet.generateTenants(10)

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d tenants, got %d", len(tt.expected), len(result))
				return
			}

			for i, expectedTenant := range tt.expected {
				if result[i] != expectedTenant {
					t.Errorf("expected tenant[%d] = '%s', got '%s'", i, expectedTenant, result[i])
				}
			}
		})
	}
}

func TestPantheonCluster_validateTenantCoverage(t *testing.T) {
	// Create test cluster with metric scopes
	cluster := &PantheonCluster{
		MetricScopes: []MetricScope{
			{
				ScopeName: "az-eastus2",
				Shards:    3,
				SpecialMetricGroups: []SpecialMetricGroup{
					{GroupName: "kube-metrics"},
					{GroupName: "rpc-metrics"},
				},
			},
			{
				ScopeName: "az-westus",
				Shards:    2,
			},
		},
	}

	tests := []struct {
		name            string
		assignedTenants map[string]string
		expectError     bool
		errorContains   []string
	}{
		{
			name: "all tenants covered",
			assignedTenants: map[string]string{
				"az-eastus2_0-of-3":       "db-group-1",
				"az-eastus2_1-of-3":       "db-group-1",
				"az-eastus2_2-of-3":       "db-group-2",
				"az-eastus2_kube-metrics": "db-group-2",
				"az-eastus2_rpc-metrics":  "db-group-3",
				"az-westus_0-of-2":        "db-group-3",
				"az-westus_1-of-2":        "db-group-3",
			},
			expectError: false,
		},
		{
			name: "missing shard tenant",
			assignedTenants: map[string]string{
				"az-eastus2_0-of-3": "db-group-1",
				"az-eastus2_1-of-3": "db-group-1",
				// Missing "az-eastus2_2-of-3"
				"az-eastus2_kube-metrics": "db-group-2",
				"az-eastus2_rpc-metrics":  "db-group-3",
				"az-westus_0-of-2":        "db-group-3",
				"az-westus_1-of-2":        "db-group-3",
			},
			expectError:   true,
			errorContains: []string{"tenant 'az-eastus2_2-of-3' is not assigned to any DB group"},
		},
		{
			name: "missing special group tenant",
			assignedTenants: map[string]string{
				"az-eastus2_0-of-3": "db-group-1",
				"az-eastus2_1-of-3": "db-group-1",
				"az-eastus2_2-of-3": "db-group-2",
				// Missing "az-eastus2_kube-metrics"
				"az-eastus2_rpc-metrics": "db-group-3",
				"az-westus_0-of-2":       "db-group-3",
				"az-westus_1-of-2":       "db-group-3",
			},
			expectError:   true,
			errorContains: []string{"tenant 'az-eastus2_kube-metrics' is not assigned to any DB group"},
		},
		{
			name: "unexpected tenant assigned",
			assignedTenants: map[string]string{
				"az-eastus2_0-of-3":       "db-group-1",
				"az-eastus2_1-of-3":       "db-group-1",
				"az-eastus2_2-of-3":       "db-group-2",
				"az-eastus2_kube-metrics": "db-group-2",
				"az-eastus2_rpc-metrics":  "db-group-3",
				"az-westus_0-of-2":        "db-group-3",
				"az-westus_1-of-2":        "db-group-3",
				"unknown-scope_test":      "db-group-1", // Unexpected tenant
			},
			expectError:   true,
			errorContains: []string{"tenant 'unknown-scope_test' is assigned but not expected based on metric scopes"},
		},
		{
			name: "multiple missing tenants",
			assignedTenants: map[string]string{
				"az-eastus2_0-of-3": "db-group-1",
				// Missing multiple tenants
			},
			expectError: true,
			errorContains: []string{
				"tenant 'az-eastus2_1-of-3' is not assigned to any DB group",
				"tenant 'az-eastus2_2-of-3' is not assigned to any DB group",
				"tenant 'az-eastus2_kube-metrics' is not assigned to any DB group",
				"tenant 'az-eastus2_rpc-metrics' is not assigned to any DB group",
				"tenant 'az-westus_0-of-2' is not assigned to any DB group",
				"tenant 'az-westus_1-of-2' is not assigned to any DB group",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := cluster.validateTenantCoverage("test", tt.assignedTenants)

			if tt.expectError {
				if len(errors) == 0 {
					t.Errorf("expected validation error but got none")
					return
				}

				for _, expectedSubstring := range tt.errorContains {
					found := false
					for _, err := range errors {
						if strings.Contains(err.Message, expectedSubstring) {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expected error message to contain '%s', but got: %v", expectedSubstring, errors)
					}
				}
			} else {
				if len(errors) > 0 {
					t.Errorf("expected no validation error but got: %v", errors)
				}
			}
		})
	}
}

func TestPantheonClusterVersions_ValidateWithTenantSets(t *testing.T) {
	tests := []struct {
		name            string
		clusterVersions *PantheonClusterVersions
		expectError     bool
		errorContains   []string
	}{
		{
			name: "valid configuration with tenant sets",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{
								ScopeName: "az-eastus2",
								Shards:    5,
								SpecialMetricGroups: []SpecialMetricGroup{
									{GroupName: "kube-metrics", MetricNames: []string{"cpu"}},
								},
							},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    2,
								DbHpa: DbHpaConfig{
									Enabled:     true,
									MaxReplicas: 5,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0, 1, 2},
									},
								},
							},
							{
								DbGroupName: "pantheon-db-a1",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{3, 4},
									},
									{
										MetricScopeName:   "az-eastus2",
										SpecialGroupNames: []string{"kube-metrics"},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "tenant set with invalid scope",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 5},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "unknown-scope",
										Shards:          []int{0, 1, 2},
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"metric scope 'unknown-scope' does not exist"},
		},
		{
			name: "duplicate tenants across tenant sets",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{ScopeName: "az-eastus2", Shards: 5},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0, 1, 2},
									},
								},
							},
							{
								DbGroupName: "pantheon-db-a1",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{2, 3}, // Overlaps with previous range
									},
								},
							},
						},
					},
				},
			},
			expectError:   true,
			errorContains: []string{"is assigned to multiple DB groups"},
		},
		{
			name: "incomplete tenant coverage",
			clusterVersions: &PantheonClusterVersions{
				Versions: []PantheonCluster{
					{
						EffectiveDate: NewDate(2025, time.July, 4),
						MetricScopes: []MetricScope{
							{
								ScopeName: "az-eastus2",
								Shards:    5,
								SpecialMetricGroups: []SpecialMetricGroup{
									{GroupName: "kube-metrics", MetricNames: []string{"cpu"}},
								},
							},
						},
						DBGroups: []DbGroup{
							{
								DbGroupName: "pantheon-db-a0",
								Replicas:    1,
								DbHpa: DbHpaConfig{
									Enabled:     false,
									MaxReplicas: 1,
									MinReplicas: 1,
								},
								TenantSets: []TenantSet{
									{
										MetricScopeName: "az-eastus2",
										Shards:          []int{0, 1, 2}, // Missing shards 3,4 and special group
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
			errorContains: []string{
				"is not assigned to any DB group",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.clusterVersions.Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("expected validation error but got none")
					return
				}

				errMsg := err.Error()
				for _, expectedSubstring := range tt.errorContains {
					if !strings.Contains(errMsg, expectedSubstring) {
						t.Errorf("expected error message to contain '%s', but got: %s", expectedSubstring, errMsg)
					}
				}
			} else {
				if err != nil {
					t.Errorf("expected no validation error but got: %v", err)
				}
			}
		})
	}
}
