// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"time"

	"github.com/pkg/errors"
)

var (
	errGroupcacheConfigNoAddrs = errors.New("no groupcache addresses provided")
	defaultGroupcacheConfig    = GroupcacheConfig{
		Timeout:                   500 * time.Millisecond,
		DNSProviderUpdateInterval: 10 * time.Second,
	}
)

// GroupcacheConfig is the config accepted by Groupcache.
type GroupcacheConfig struct {
	// Addresses specifies the list of memcached addresses. The addresses get
	// resolved with the DNS provider.
	Addresses []string `yaml:"addresses"`

	// Timeout specifies the socket read/write timeout.
	Timeout time.Duration `yaml:"timeout"`

	// DNSProviderUpdateInterval specifies the DNS discovery update interval.
	DNSProviderUpdateInterval time.Duration `yaml:"dns_provider_update_interval"`

	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int
}

func (g *GroupcacheConfig) validate() error {
	// TODO(kakkoyun): This could be allowed
	if len(g.Addresses) == 0 {
		return errGroupcacheConfigNoAddrs
	}

	return nil
}
