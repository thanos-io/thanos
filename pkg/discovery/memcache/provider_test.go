// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package memcache

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestProviderUpdatesAddresses(t *testing.T) {
	ctx := context.TODO()
	clusters := []string{"memcached-cluster-1", "memcached-cluster-2"}
	provider := NewProvider(log.NewNopLogger(), nil, 5*time.Second)
	resolver := mockResolver{
		configs: map[string]*ClusterConfig{
			"memcached-cluster-1": {nodes: []Node{{dns: "dns-1", ip: "ip-1", port: 11211}}},
			"memcached-cluster-2": {nodes: []Node{{dns: "dns-2", ip: "ip-2", port: 8080}}},
		},
	}
	provider.resolver = &resolver

	err := provider.Resolve(ctx, clusters)
	addresses := provider.Addresses()
	sort.Strings(addresses)

	testutil.Ok(t, err)
	testutil.Equals(t, []string{"dns-1:11211", "dns-2:8080"}, addresses)

	resolver.configs = map[string]*ClusterConfig{
		"memcached-cluster-1": {nodes: []Node{{dns: "dns-1", ip: "ip-1", port: 11211}, {dns: "dns-3", ip: "ip-3", port: 11211}}},
		"memcached-cluster-2": {nodes: []Node{{dns: "dns-2", ip: "ip-2", port: 8080}}},
	}
	err = provider.Resolve(ctx, clusters)
	addresses = provider.Addresses()
	sort.Strings(addresses)

	testutil.Ok(t, err)
	testutil.Equals(t, []string{"dns-1:11211", "dns-2:8080", "dns-3:11211"}, addresses)
}

func TestProviderDoesNotUpdateAddressIfFailed(t *testing.T) {
	ctx := context.TODO()
	clusters := []string{"memcached-cluster-1", "memcached-cluster-2"}
	provider := NewProvider(log.NewNopLogger(), nil, 5*time.Second)
	resolver := mockResolver{
		configs: map[string]*ClusterConfig{
			"memcached-cluster-1": {nodes: []Node{{dns: "dns-1", ip: "ip-1", port: 11211}}},
			"memcached-cluster-2": {nodes: []Node{{dns: "dns-2", ip: "ip-2", port: 8080}}},
		},
	}
	provider.resolver = &resolver

	err := provider.Resolve(ctx, clusters)
	addresses := provider.Addresses()
	sort.Strings(addresses)

	testutil.Ok(t, err)
	testutil.Equals(t, []string{"dns-1:11211", "dns-2:8080"}, addresses)

	resolver.configs = nil
	resolver.err = errors.New("oops")
	err = provider.Resolve(ctx, clusters)
	addresses = provider.Addresses()
	sort.Strings(addresses)

	testutil.NotOk(t, err)
	testutil.Equals(t, []string{"dns-1:11211", "dns-2:8080"}, addresses)
}

type mockResolver struct {
	configs map[string]*ClusterConfig
	err     error
}

func (r *mockResolver) Resolve(_ context.Context, address string) (*ClusterConfig, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.configs[address], nil
}
