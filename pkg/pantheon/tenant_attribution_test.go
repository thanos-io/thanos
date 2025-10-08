// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pantheon

import (
	"testing"
)

func TestGetTenantFromScope(t *testing.T) {
	cluster := &PantheonCluster{
		MetricScopes: []MetricScope{
			{
				ScopeName: "hgcp",
				Shards:    10,
				SpecialMetricGroups: []SpecialMetricGroup{
					{
						GroupName:   "kube-metrics",
						MetricNames: []string{"container_cpu_usage_seconds_total", "container_memory_working_set_bytes"},
					},
					{
						GroupName:          "autoscaling",
						MetricNamePrefixes: []string{"autoscaling__"},
					},
					{
						GroupName:          "recording-rules",
						MetricNameSuffixes: []string{":recording_rules"},
					},
				},
			},
			{
				ScopeName: "meta",
				Shards:    5,
			},
		},
	}

	tests := []struct {
		name        string
		scope       string
		metricName  string
		wantTenant  string
		wantErr     bool
		errContains string
	}{
		{
			name:       "exact metric name match",
			scope:      "hgcp",
			metricName: "container_cpu_usage_seconds_total",
			wantTenant: "hgcp_kube-metrics",
			wantErr:    false,
		},
		{
			name:       "prefix match",
			scope:      "hgcp",
			metricName: "autoscaling__pod_count",
			wantTenant: "hgcp_autoscaling",
			wantErr:    false,
		},
		{
			name:       "suffix match",
			scope:      "hgcp",
			metricName: "cpu_usage:recording_rules",
			wantTenant: "hgcp_recording-rules",
			wantErr:    false,
		},
		{
			name:       "no special group match - hash shard",
			scope:      "hgcp",
			metricName: "http_requests_total",
			wantTenant: "hgcp_4-of-10", // Deterministic based on xxhash
			wantErr:    false,
		},
		{
			name:       "different scope - no special groups",
			scope:      "meta",
			metricName: "node_cpu_seconds_total",
			wantTenant: "meta_0-of-5", // Deterministic hash
			wantErr:    false,
		},
		{
			name:        "scope not found",
			scope:       "nonexistent",
			metricName:  "some_metric",
			wantErr:     true,
			errContains: "scope 'nonexistent' not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metricScope := GetMetricScope(tt.scope, cluster)
			if tt.wantErr {
				if metricScope != nil {
					t.Errorf("GetMetricScope() expected nil but got %v", metricScope)
				}
				return
			}

			if metricScope == nil {
				t.Errorf("GetMetricScope() unexpected nil")
				return
			}

			gotTenant := GetTenantFromScope(tt.metricName, metricScope)
			if gotTenant != tt.wantTenant {
				t.Errorf("GetTenantFromScope() = %v, want %v", gotTenant, tt.wantTenant)
			}
		})
	}
}

func TestMatchesSpecialGroup(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		group      *SpecialMetricGroup
		want       bool
	}{
		{
			name:       "exact match",
			metricName: "container_cpu_usage",
			group: &SpecialMetricGroup{
				MetricNames: []string{"container_cpu_usage", "container_memory_usage"},
			},
			want: true,
		},
		{
			name:       "prefix match",
			metricName: "kube_pod_status_ready",
			group: &SpecialMetricGroup{
				MetricNamePrefixes: []string{"kube_"},
			},
			want: true,
		},
		{
			name:       "suffix match",
			metricName: "cpu_usage:sum",
			group: &SpecialMetricGroup{
				MetricNameSuffixes: []string{":sum", ":avg"},
			},
			want: true,
		},
		{
			name:       "no match",
			metricName: "http_requests_total",
			group: &SpecialMetricGroup{
				MetricNames:        []string{"container_cpu"},
				MetricNamePrefixes: []string{"kube_"},
				MetricNameSuffixes: []string{":sum"},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesSpecialGroup(tt.metricName, tt.group); got != tt.want {
				t.Errorf("matchesSpecialGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeMetricShard(t *testing.T) {
	tests := []struct {
		name        string
		metricName  string
		totalShards int
		want        int
	}{
		{
			name:        "10 shards",
			metricName:  "http_requests_total",
			totalShards: 10,
			want:        4, // Deterministic based on xxhash
		},
		{
			name:        "5 shards",
			metricName:  "node_cpu_seconds_total",
			totalShards: 5,
			want:        0, // Deterministic based on xxhash
		},
		{
			name:        "1 shard",
			metricName:  "any_metric",
			totalShards: 1,
			want:        0,
		},
		{
			name:        "zero shards (edge case)",
			metricName:  "any_metric",
			totalShards: 0,
			want:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computeMetricShard(tt.metricName, tt.totalShards); got != tt.want {
				t.Errorf("computeMetricShard() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetMetricScope(t *testing.T) {
	cluster := &PantheonCluster{
		MetricScopes: []MetricScope{
			{
				ScopeName: "hgcp",
				Shards:    10,
			},
			{
				ScopeName: "meta",
				Shards:    5,
			},
		},
	}

	tests := []struct {
		name        string
		scope       string
		wantErr     bool
		errContains string
		wantShards  int
	}{
		{
			name:       "valid scope hgcp",
			scope:      "hgcp",
			wantShards: 10,
		},
		{
			name:       "valid scope meta",
			scope:      "meta",
			wantShards: 5,
		},
		{
			name:        "scope not found",
			scope:       "nonexistent",
			wantErr:     true,
			errContains: "scope 'nonexistent' not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetMetricScope(tt.scope, cluster)
			if tt.wantErr {
				if got != nil {
					t.Errorf("GetMetricScope() expected nil but got %v", got)
				}
				return
			}

			if got == nil {
				t.Errorf("GetMetricScope() returned nil MetricScope")
				return
			}

			if got.ScopeName != tt.scope {
				t.Errorf("GetMetricScope() ScopeName = %v, want %v", got.ScopeName, tt.scope)
			}

			if got.Shards != tt.wantShards {
				t.Errorf("GetMetricScope() Shards = %v, want %v", got.Shards, tt.wantShards)
			}
		})
	}
}
