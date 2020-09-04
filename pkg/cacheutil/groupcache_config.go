// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

var (
	errGroupcacheConfigNoAddrs = errors.New("no groupcache addresses provided")

	defaultGroupcacheConfig = GroupcacheConfig{
		CacheBytes: 1024 << 20, // TODO(kakkoyun): Leave enough headroom, 2x.
		Timeout:    500 * time.Millisecond,
		HTTPPoolConfig: GroupcacheHTTPPoolConfig{
			BasePath:                  "/_groupcache/",
			Replicas:                  50,
			DNSProviderUpdateInterval: 10 * time.Second,
		},
	}
)

// GroupcacheConfig is the config accepted by Groupcache.
type GroupcacheConfig struct {
	// CacheBytes are maximum cache size.
	CacheBytes int64 `yaml:"cache_bytes"`

	// Timeout specifies the socket read/write timeout.
	Timeout time.Duration `yaml:"timeout"` // TODO(kakkoyun) Is this needed?

	// HTTPPoolConfig are the configurations of a HTTPPool.
	HTTPPoolConfig GroupcacheHTTPPoolConfig `yaml:"http_pool_config"`
}

// GroupcacheHTTPPoolConfig are the configurations of a HTTPPool.
type GroupcacheHTTPPoolConfig struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string `yaml:"base_path"`

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int `yaml:"replicas"`

	// Addresses specifies the list of memcached addresses. The addresses get
	// resolved with the DNS provider.
	Addresses []string `yaml:"addresses"`

	// DNSProviderUpdateInterval specifies the DNS discovery update interval.
	DNSProviderUpdateInterval time.Duration `yaml:"dns_provider_update_interval"`
}

// ParseGroupcacheConfig unmarshals a buffer into a GroupcacheConfig with default values.
func ParseGroupcacheConfig(conf []byte) (GroupcacheConfig, error) {
	config := defaultGroupcacheConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return GroupcacheConfig{}, err
	}

	return config, nil
}

func (g *GroupcacheConfig) validate() error {
	// TODO(kakkoyun): This could be allowed.
	if len(g.HTTPPoolConfig.Addresses) == 0 {
		return errGroupcacheConfigNoAddrs
	}

	return nil
}
