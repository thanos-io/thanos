package receive

import (
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
				WriteLimits: writeLimitsConfig{
					GlobalLimits: globalLimitsConfig{MaxConcurrency: 30},
					DefaultLimits: defaultLimitsConfig{
						RequestLimits: defaultRequestLimitsConfig{
							SizeBytesLimit: 1024,
							SeriesLimit:    1000,
							SamplesLimit:   10,
						},
					},
					TenantsLimits: tenantsWriteLimitsConfig{
						"cool_tenant": &writeLimitConfig{
							RequestLimits: newEmptyRequestLimitsConfig().
								SetSizeBytesLimits(0).
								SetSeriesLimits(0).
								SetSamplesLimits(0),
						},
						"another_tenant": &writeLimitConfig{
							RequestLimits: newEmptyRequestLimitsConfig().
								SetSeriesLimits(50000).
								SetSamplesLimits(500),
						},
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
