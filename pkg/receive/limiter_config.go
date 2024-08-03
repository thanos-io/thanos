// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"net/url"

	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/clientconfig"
	"github.com/thanos-io/thanos/pkg/errors"
)

// RootLimitsConfig is the root configuration for limits.
type RootLimitsConfig struct {
	// WriteLimits hold the limits for writing data.
	WriteLimits WriteLimitsConfig `yaml:"write"`
}

// ParseRootLimitConfig parses the root limit configuration. Even though
// the result is a pointer, it will only be nil if an error is returned.
func ParseRootLimitConfig(content []byte) (*RootLimitsConfig, error) {
	var root RootLimitsConfig
	if err := yaml.UnmarshalStrict(content, &root); err != nil {
		return nil, errors.Wrapf(err, "parsing config YAML file")
	}

	if root.WriteLimits.GlobalLimits.MetaMonitoringURL != "" {
		u, err := url.Parse(root.WriteLimits.GlobalLimits.MetaMonitoringURL)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing meta-monitoring URL")
		}

		// url.Parse might pass a URL with only path, so need to check here for scheme and host.
		// As per docs: https://pkg.go.dev/net/url#Parse.
		if u.Host == "" || u.Scheme == "" {
			return nil, errors.Newf("%s is not a valid meta-monitoring URL (scheme: %s,host: %s)", u, u.Scheme, u.Host)
		}
		root.WriteLimits.GlobalLimits.metaMonitoringURL = u
	}

	// Set default query if none specified.
	if root.WriteLimits.GlobalLimits.MetaMonitoringLimitQuery == "" {
		root.WriteLimits.GlobalLimits.MetaMonitoringLimitQuery = "sum(prometheus_tsdb_head_series) by (tenant)"
	}

	return &root, nil
}

func (r RootLimitsConfig) AreHeadSeriesLimitsConfigured() bool {
	return r.WriteLimits.GlobalLimits.MetaMonitoringURL != "" && (len(r.WriteLimits.TenantsLimits) != 0 || r.WriteLimits.DefaultLimits.HeadSeriesLimit != 0)
}

type WriteLimitsConfig struct {
	// GlobalLimits are limits that are shared across all tenants.
	GlobalLimits GlobalLimitsConfig `yaml:"global"`
	// DefaultLimits are the default limits for tenants without specified limits.
	DefaultLimits DefaultLimitsConfig `yaml:"default"`
	// TenantsLimits are the limits per tenant.
	TenantsLimits TenantsWriteLimitsConfig `yaml:"tenants"`
}

type GlobalLimitsConfig struct {
	// MaxConcurrency represents the maximum concurrency during write operations.
	MaxConcurrency int64 `yaml:"max_concurrency"`
	// MetaMonitoring options specify the query, url and client for Query API address used in head series limiting.
	MetaMonitoringURL        string                         `yaml:"meta_monitoring_url"`
	MetaMonitoringHTTPClient *clientconfig.HTTPClientConfig `yaml:"meta_monitoring_http_client"`
	MetaMonitoringLimitQuery string                         `yaml:"meta_monitoring_limit_query"`

	metaMonitoringURL *url.URL
}

type DefaultLimitsConfig struct {
	// RequestLimits holds the difficult per-request limits.
	RequestLimits requestLimitsConfig `yaml:"request"`
	// HeadSeriesLimit specifies the maximum number of head series allowed for any tenant.
	HeadSeriesLimit uint64 `yaml:"head_series_limit"`
}

// TenantsWriteLimitsConfig is a map of tenant IDs to their *WriteLimitConfig.
type TenantsWriteLimitsConfig map[string]*WriteLimitConfig

// A tenant might not always have limits configured, so things here must
// use pointers.
type WriteLimitConfig struct {
	// RequestLimits holds the difficult per-request limits.
	RequestLimits *requestLimitsConfig `yaml:"request"`
	// HeadSeriesLimit specifies the maximum number of head series allowed for a tenant.
	HeadSeriesLimit *uint64 `yaml:"head_series_limit"`
}

// Utils for initializing.
func NewEmptyWriteLimitConfig() *WriteLimitConfig {
	return &WriteLimitConfig{}
}

func (w *WriteLimitConfig) SetRequestLimits(rl *requestLimitsConfig) *WriteLimitConfig {
	w.RequestLimits = rl
	return w
}

func (w *WriteLimitConfig) SetHeadSeriesLimit(val uint64) *WriteLimitConfig {
	w.HeadSeriesLimit = &val
	return w
}

type requestLimitsConfig struct {
	SizeBytesLimit              *int64 `yaml:"size_bytes_limit"`
	SeriesLimit                 *int64 `yaml:"series_limit"`
	SamplesLimit                *int64 `yaml:"samples_limit"`
	NativeHistogramBucketsLimit *int64 `yaml:"native_histogram_buckets_limit"`
}

func NewEmptyRequestLimitsConfig() *requestLimitsConfig {
	return &requestLimitsConfig{}
}

func (rl *requestLimitsConfig) SetSizeBytesLimit(value int64) *requestLimitsConfig {
	rl.SizeBytesLimit = &value
	return rl
}

func (rl *requestLimitsConfig) SetSeriesLimit(value int64) *requestLimitsConfig {
	rl.SeriesLimit = &value
	return rl
}

func (rl *requestLimitsConfig) SetSamplesLimit(value int64) *requestLimitsConfig {
	rl.SamplesLimit = &value
	return rl
}

func (rl *requestLimitsConfig) SetNativeHistogramBucketsLimit(value int64) *requestLimitsConfig {
	rl.NativeHistogramBucketsLimit = &value
	return rl
}

// OverlayWith overlays the current configuration with another one. This means
// that limit values that are not set (have a nil value) will be overwritten in
// the caller.
func (rl *requestLimitsConfig) OverlayWith(other *requestLimitsConfig) *requestLimitsConfig {
	if rl.SamplesLimit == nil {
		rl.SamplesLimit = other.SamplesLimit
	}
	if rl.SeriesLimit == nil {
		rl.SeriesLimit = other.SeriesLimit
	}
	if rl.SizeBytesLimit == nil {
		rl.SizeBytesLimit = other.SizeBytesLimit
	}
	if rl.NativeHistogramBucketsLimit == nil {
		rl.NativeHistogramBucketsLimit = other.NativeHistogramBucketsLimit
	}
	return rl
}
