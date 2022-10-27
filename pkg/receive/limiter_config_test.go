// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"net/url"
	"os"
	"path"
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestParseLimiterConfig(t *testing.T) {
	tests := []struct {
		name           string
		configFileName string
		want           *RootLimitsConfig
		wantErr        bool
	}{
		{
			name:           "Parses a configuration without issues",
			configFileName: "good_limits.yaml",
			wantErr:        false,
			want: &RootLimitsConfig{
				WriteLimits: WriteLimitsConfig{
					GlobalLimits: GlobalLimitsConfig{
						MaxConcurrency:           30,
						MetaMonitoringURL:        "http://localhost:9090",
						MetaMonitoringLimitQuery: "sum(prometheus_tsdb_head_series) by (tenant)",
						metaMonitoringURL: &url.URL{
							Scheme: "http",
							Host:   "localhost:9090",
						},
					},
					DefaultLimits: DefaultLimitsConfig{
						RequestLimits: *NewEmptyRequestLimitsConfig().
							SetSizeBytesLimit(1024).
							SetSeriesLimit(1000).
							SetSamplesLimit(10),
						HeadSeriesLimit: 1000,
					},
					TenantsLimits: TenantsWriteLimitsConfig{
						"acme": NewEmptyWriteLimitConfig().
							SetRequestLimits(
								NewEmptyRequestLimitsConfig().
									SetSizeBytesLimit(0).
									SetSeriesLimit(0).
									SetSamplesLimit(0),
							).
							SetHeadSeriesLimit(2000),
						"ajax": NewEmptyWriteLimitConfig().
							SetRequestLimits(
								NewEmptyRequestLimitsConfig().
									SetSeriesLimit(50000).
									SetSamplesLimit(500),
							),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := path.Join("testdata", "limits_config", tt.configFileName)
			fileContent, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("couldn't read test limits configuration file '%s': %s", filePath, err)
			}

			got, err := ParseRootLimitConfig(fileContent)
			testutil.Ok(t, err)
			testutil.Equals(t, tt.want, got)
		})
	}
}
