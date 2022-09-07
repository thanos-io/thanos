// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"github.com/thanos-io/thanos/pkg/errors"
	"gopkg.in/yaml.v2"
)

// RootLimitsConfig is the root configuration for limits.
type RootLimitsConfig struct {
	// WriteLimits hold the limits for writing data.
	WriteLimits writeLimitsConfig `yaml:"write"`
}

// ParseRootLimitConfig parses the root limit configuration. Even though
// the result is a pointer, it will only be nil if an error is returned.
func ParseRootLimitConfig(content []byte) (*RootLimitsConfig, error) {
	var root RootLimitsConfig
	if err := yaml.UnmarshalStrict(content, &root); err != nil {
		return nil, errors.Wrapf(err, "parsing config YAML file")
	}
	return &root, nil
}

type writeLimitsConfig struct {
	// GlobalLimits are limits that are shared across all tenants.
	GlobalLimits globalLimitsConfig `yaml:"global"`
	// DefaultLimits are the default limits for tenants without specified limits.
	DefaultLimits defaultLimitsConfig `yaml:"default"`
	// TenantsLimits are the limits per tenant.
	TenantsLimits tenantsWriteLimitsConfig `yaml:"tenants"`
}

type globalLimitsConfig struct {
	// MaxConcurrency represents the maximum concurrency during write operations.
	MaxConcurrency int64 `yaml:"max_concurrency"`
}

type defaultLimitsConfig struct {
	// RequestLimits holds the difficult per-request limits.
	RequestLimits requestLimitsConfig `yaml:"request"`
	// HeadSeriesConfig *headSeriesLimiter `yaml:"head_series"`
}

type tenantsWriteLimitsConfig map[string]*writeLimitConfig

// A tenant might not always have limits configured, so things here must
// use pointers.
type writeLimitConfig struct {
	// RequestLimits holds the difficult per-request limits.
	RequestLimits *requestLimitsConfig `yaml:"request"`
	// HeadSeriesConfig *headSeriesLimiter `yaml:"head_series"`
}

type requestLimitsConfig struct {
	SizeBytesLimit *int64 `yaml:"size_bytes_limit"`
	SeriesLimit    *int64 `yaml:"series_limit"`
	SamplesLimit   *int64 `yaml:"samples_limit"`
}

func newEmptyRequestLimitsConfig() *requestLimitsConfig {
	return &requestLimitsConfig{}
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
	return rl
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
