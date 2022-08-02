// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestRequestLimiter_limitsFor(t *testing.T) {
	tenantWithLimits := "limited-tenant"
	tenantWithoutLimits := "unlimited-tenant"

	limits := writeLimitsConfig{
		DefaultLimits: defaultLimitsConfig{
			RequestLimits: *newEmptyRequestLimitsConfig().
				SetSeriesLimit(10),
		},
		TenantsLimits: tenantsWriteLimitsConfig{
			tenantWithLimits: &writeLimitConfig{
				RequestLimits: newEmptyRequestLimitsConfig().
					SetSeriesLimit(30),
			},
		},
	}
	tests := []struct {
		name       string
		tenant     string
		wantLimits *requestLimitsConfig
	}{
		{
			name:   "Gets the default limits when tenant's limits aren't present",
			tenant: tenantWithoutLimits,
			wantLimits: newEmptyRequestLimitsConfig().
				SetSeriesLimit(10).
				SetSamplesLimit(0).
				SetSizeBytesLimit(0),
		},
		{
			name:   "Gets the tenant's limits when it is present",
			tenant: tenantWithLimits,
			wantLimits: newEmptyRequestLimitsConfig().
				SetSeriesLimit(30).
				SetSamplesLimit(0).
				SetSizeBytesLimit(0),
		},
	}

	requestLimiter := newConfigRequestLimiter(nil, &limits)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := requestLimiter.limitsFor(tt.tenant)
			testutil.Equals(t, tt.wantLimits, limits)
		})
	}
}

func TestRequestLimiter_AllowRequestBodySizeBytes(t *testing.T) {
	tests := []struct {
		name          string
		defaultLimits *requestLimitsConfig
		sizeByteLimit int64
		sizeBytes     int64
		want          bool
	}{
		{
			name:          "Allowed when request size limit is < 0",
			sizeByteLimit: -1,
			sizeBytes:     30000,
			want:          true,
		},
		{
			name:          "Allowed when request size limit is 0",
			sizeByteLimit: 0,
			sizeBytes:     30000,
			want:          true,
		},
		{
			name:          "Allowed when under request size limit",
			sizeByteLimit: 50000,
			sizeBytes:     30000,
			want:          true,
		},
		{
			name:          "Allowed when equal to the request size limit",
			sizeByteLimit: 30000,
			sizeBytes:     30000,
			want:          true,
		},
		{
			name:          "Not allowed when above the request size limit",
			sizeByteLimit: 30000,
			sizeBytes:     30001,
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenant := "tenant"
			limits := writeLimitsConfig{
				DefaultLimits: defaultLimitsConfig{
					RequestLimits: *newEmptyRequestLimitsConfig().SetSeriesLimit(10),
				},
				TenantsLimits: tenantsWriteLimitsConfig{
					tenant: &writeLimitConfig{
						RequestLimits: newEmptyRequestLimitsConfig().SetSizeBytesLimit(tt.sizeByteLimit),
					},
				},
			}
			l := newConfigRequestLimiter(nil, &limits)
			testutil.Equals(t, tt.want, l.AllowSizeBytes(tenant, tt.sizeBytes))
		})
	}
}

func TestRequestLimiter_AllowSeries(t *testing.T) {
	tests := []struct {
		name        string
		seriesLimit int64
		series      int64
		want        bool
	}{
		{
			name:        "Allowed when series limit is < 0",
			seriesLimit: -1,
			series:      30000,
			want:        true,
		},
		{
			name:        "Allowed when series limit is 0",
			seriesLimit: 0,
			series:      30000,
			want:        true,
		},
		{
			name:        "Allowed when under series limit",
			seriesLimit: 50000,
			series:      30000,
			want:        true,
		},
		{
			name:        "Allowed when equal to the series limit",
			seriesLimit: 30000,
			series:      30000,
			want:        true,
		},
		{
			name:        "Not allowed when above the series limit",
			seriesLimit: 30000,
			series:      30001,
			want:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenant := "tenant"
			limits := writeLimitsConfig{
				DefaultLimits: defaultLimitsConfig{
					RequestLimits: *newEmptyRequestLimitsConfig().SetSeriesLimit(10),
				},
				TenantsLimits: tenantsWriteLimitsConfig{
					tenant: &writeLimitConfig{
						RequestLimits: newEmptyRequestLimitsConfig().SetSeriesLimit(tt.seriesLimit),
					},
				},
			}

			l := newConfigRequestLimiter(nil, &limits)
			testutil.Equals(t, tt.want, l.AllowSeries(tenant, tt.series))
		})
	}
}

func TestRequestLimiter_AllowSamples(t *testing.T) {
	tests := []struct {
		name         string
		samplesLimit int64
		samples      int64
		want         bool
	}{
		{
			name:         "Allowed when samples limit is < 0",
			samplesLimit: -1,
			samples:      30000,
			want:         true,
		},
		{
			name:         "Allowed when samples limit is 0",
			samplesLimit: 0,
			samples:      30000,
			want:         true,
		},
		{
			name:         "Allowed when under samples limit",
			samplesLimit: 50000,
			samples:      30000,
			want:         true,
		},
		{
			name:         "Allowed when equal to the samples limit",
			samplesLimit: 30000,
			samples:      30000,
			want:         true,
		},
		{
			name:         "Not allowed when above the samples limit",
			samplesLimit: 30000,
			samples:      30001,
			want:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenant := "tenant"
			limits := writeLimitsConfig{
				DefaultLimits: defaultLimitsConfig{
					RequestLimits: *newEmptyRequestLimitsConfig().SetSeriesLimit(10),
				},
				TenantsLimits: tenantsWriteLimitsConfig{
					tenant: &writeLimitConfig{
						RequestLimits: newEmptyRequestLimitsConfig().SetSamplesLimit(tt.samplesLimit),
					},
				},
			}

			l := newConfigRequestLimiter(nil, &limits)
			testutil.Equals(t, tt.want, l.AllowSamples("tenant", tt.samples))
		})
	}
}
