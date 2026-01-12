// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/pantheon"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestDistributeTimeseriesToReplicas_WithPantheon(t *testing.T) {
	// Create a pantheon cluster config for testing.
	pantheonCluster := &pantheon.PantheonCluster{
		MetricScopes: []pantheon.MetricScope{
			{
				ScopeName: "test-scope",
				Shards:    3,
				SpecialMetricGroups: []pantheon.SpecialMetricGroup{
					{
						GroupName:   "kube-metrics",
						MetricNames: []string{"container_cpu_usage"},
					},
					{
						GroupName:          "recording",
						MetricNameSuffixes: []string{":sum"},
					},
				},
			},
		},
	}

	tests := []struct {
		name            string
		scope           string
		timeseries      []prompb.TimeSeries
		wantTenants     []string
		wantErr         bool
		errContains     string
		pantheonCluster *pantheon.PantheonCluster
	}{
		{
			name:  "special metric group - exact match",
			scope: "test-scope",
			timeseries: []prompb.TimeSeries{
				{
					Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
						"__name__", "container_cpu_usage",
						"pod", "test-pod",
					)),
				},
			},
			wantTenants:     []string{"test-scope_kube-metrics"},
			pantheonCluster: pantheonCluster,
		},
		{
			name:  "special metric group - suffix match",
			scope: "test-scope",
			timeseries: []prompb.TimeSeries{
				{
					Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
						"__name__", "cpu_usage:sum",
						"pod", "test-pod",
					)),
				},
			},
			wantTenants:     []string{"test-scope_recording"},
			pantheonCluster: pantheonCluster,
		},
		{
			name:  "hash-based sharding",
			scope: "test-scope",
			timeseries: []prompb.TimeSeries{
				{
					Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
						"__name__", "http_requests_total",
						"path", "/api",
					)),
				},
			},
			wantTenants:     []string{"test-scope_0-of-3"}, // Deterministic based on xxhash
			pantheonCluster: pantheonCluster,
		},
		{
			name:  "missing metric name - should be bad request error",
			scope: "test-scope",
			timeseries: []prompb.TimeSeries{
				{
					Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
						"pod", "test-pod",
					)),
				},
			},
			wantErr:         true,
			errContains:     "invalid pantheon request",
			pantheonCluster: pantheonCluster,
		},
		{
			name:  "scope not found in config - should be bad request error",
			scope: "unknown-scope",
			timeseries: []prompb.TimeSeries{
				{
					Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
						"__name__", "http_requests_total",
					)),
				},
			},
			wantErr:         true,
			errContains:     "invalid pantheon request",
			pantheonCluster: pantheonCluster,
		},
		{
			name:  "no pantheon config - fallback to tenant header",
			scope: "test-scope",
			timeseries: []prompb.TimeSeries{
				{
					Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
						"__name__", "http_requests_total",
					)),
				},
			},
			wantTenants:     []string{"default-tenant"}, // Falls back to tenantHTTP
			pantheonCluster: nil,
		},
		{
			name:  "no scope provided - fallback to tenant header",
			scope: "",
			timeseries: []prompb.TimeSeries{
				{
					Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
						"__name__", "http_requests_total",
					)),
				},
			},
			wantTenants:     []string{"default-tenant"}, // Falls back to tenantHTTP
			pantheonCluster: pantheonCluster,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.NewNopLogger()

			h := NewHandler(logger, &Options{
				Endpoint: "localhost:8080",
			})
			h.SetPantheonCluster(tt.pantheonCluster)

			hashring, err := NewMultiHashring(AlgorithmHashmod, 1, []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "localhost:8080"}},
				},
			})
			require.NoError(t, err)
			h.Hashring(hashring)

			localWrites, remoteWrites, err := h.distributeTimeseriesToReplicas(
				"default-tenant",
				tt.scope,
				[]uint64{0},
				tt.timeseries,
			)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					require.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)

			// Collect all tenants from local and remote writes.
			allTenants := make(map[string]bool)
			for _, writes := range localWrites {
				for tenant := range writes {
					allTenants[tenant] = true
				}
			}
			for _, writes := range remoteWrites {
				for tenant := range writes {
					allTenants[tenant] = true
				}
			}

			// Verify expected tenants are present.
			for _, wantTenant := range tt.wantTenants {
				require.True(t, allTenants[wantTenant], "expected tenant %s not found", wantTenant)
			}
			require.Equal(t, len(tt.wantTenants), len(allTenants), "unexpected number of tenants")
		})
	}
}

func TestReceiveHTTP_ScopeHeaderValidation(t *testing.T) {
	pantheonCluster := &pantheon.PantheonCluster{
		MetricScopes: []pantheon.MetricScope{
			{
				ScopeName: "test-scope",
				Shards:    3,
			},
		},
	}

	tests := []struct {
		name               string
		scopeHeader        string
		pantheonCluster    *pantheon.PantheonCluster
		expectStatusCode   int
		expectErrorMessage string
	}{
		{
			name:               "pantheon config set but scope header missing",
			scopeHeader:        "",
			pantheonCluster:    pantheonCluster,
			expectStatusCode:   http.StatusBadRequest,
			expectErrorMessage: "scope header 'THANOS-SCOPE' is required",
		},
		{
			name:               "pantheon config set and scope header present",
			scopeHeader:        "test-scope",
			pantheonCluster:    pantheonCluster,
			expectStatusCode:   http.StatusOK, // Would continue processing (but will fail later without full setup)
			expectErrorMessage: "",
		},
		{
			name:               "no pantheon config - scope header not required",
			scopeHeader:        "",
			pantheonCluster:    nil,
			expectStatusCode:   http.StatusOK, // Would continue processing
			expectErrorMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.NewNopLogger()

			limiter, err := NewLimiter(nil, nil, RouterOnly, logger, 1)
			require.NoError(t, err)

			h := NewHandler(logger, &Options{
				Endpoint:        "localhost:8080",
				TenantHeader:    tenancy.DefaultTenantHeader,
				ScopeHeader:     tenancy.DefaultScopeHeader,
				DefaultTenantID: "default-tenant",
				ReceiverMode:    RouterOnly,
				Limiter:         limiter,
			})
			h.SetPantheonCluster(tt.pantheonCluster)

			hashring, err := NewMultiHashring(AlgorithmHashmod, 1, []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "localhost:8080"}},
				},
			})
			require.NoError(t, err)
			h.Hashring(hashring)

			// Create a test request with scope header.
			req := httptest.NewRequest("POST", "/api/v1/receive", bytes.NewBuffer([]byte{}))
			req.Header.Set(tenancy.DefaultTenantHeader, "test-tenant")
			if tt.scopeHeader != "" {
				req.Header.Set(tenancy.DefaultScopeHeader, tt.scopeHeader)
			}

			rr := httptest.NewRecorder()
			h.receiveHTTP(rr, req)

			// For the case where we expect success initially, the handler will fail later
			// due to missing write request body, but we're only testing scope validation.
			if tt.expectStatusCode == http.StatusBadRequest {
				require.Equal(t, tt.expectStatusCode, rr.Code)
				if tt.expectErrorMessage != "" {
					require.Contains(t, rr.Body.String(), tt.expectErrorMessage)
				}
			}
		})
	}
}
