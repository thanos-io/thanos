// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"fmt"
	"net"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/facette/natsort"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestNatSort(t *testing.T) {

	// Validate that the order of SRV records returned by a DNS
	// lookup for a k8s StatefulSet are ordered as expected when
	// a natsort is done.
	input := []string{
		"memcached-10.memcached.thanos.svc.cluster.local.",
		"memcached-1.memcached.thanos.svc.cluster.local.",
		"memcached-6.memcached.thanos.svc.cluster.local.",
		"memcached-3.memcached.thanos.svc.cluster.local.",
		"memcached-25.memcached.thanos.svc.cluster.local.",
	}

	expected := []string{
		"memcached-1.memcached.thanos.svc.cluster.local.",
		"memcached-3.memcached.thanos.svc.cluster.local.",
		"memcached-6.memcached.thanos.svc.cluster.local.",
		"memcached-10.memcached.thanos.svc.cluster.local.",
		"memcached-25.memcached.thanos.svc.cluster.local.",
	}

	natsort.Sort(input)
	testutil.Equals(t, expected, input)
}

func TestMemcachedJumpHashSelector_PickServer(t *testing.T) {
	tests := []struct {
		addrs        []string
		key          string
		expectedAddr string
		expectedErr  error
	}{
		{
			addrs:       []string{},
			key:         "test-1",
			expectedErr: memcache.ErrNoServers,
		},
		{
			addrs:        []string{"127.0.0.1:11211"},
			key:          "test-1",
			expectedAddr: "127.0.0.1:11211",
		},
		{
			addrs:        []string{"127.0.0.1:11211", "127.0.0.2:11211"},
			key:          "test-1",
			expectedAddr: "127.0.0.1:11211",
		},
		{
			addrs:        []string{"127.0.0.1:11211", "127.0.0.2:11211"},
			key:          "test-2",
			expectedAddr: "127.0.0.2:11211",
		},
	}

	s := MemcachedJumpHashSelector{}

	for _, test := range tests {
		testutil.Ok(t, s.SetServers(test.addrs...))

		actualAddr, err := s.PickServer(test.key)

		if test.expectedErr != nil {
			testutil.Equals(t, test.expectedErr, err)
			testutil.Equals(t, nil, actualAddr)
		} else {
			testutil.Ok(t, err)
			testutil.Equals(t, test.expectedAddr, actualAddr.String())
		}
	}
}

func TestMemcachedJumpHashSelector_Each_ShouldRespectServersOrdering(t *testing.T) {
	tests := []struct {
		input    []string
		expected []string
	}{
		{
			input:    []string{"127.0.0.1:11211", "127.0.0.2:11211", "127.0.0.3:11211"},
			expected: []string{"127.0.0.1:11211", "127.0.0.2:11211", "127.0.0.3:11211"},
		},
		{
			input:    []string{"127.0.0.2:11211", "127.0.0.3:11211", "127.0.0.1:11211"},
			expected: []string{"127.0.0.1:11211", "127.0.0.2:11211", "127.0.0.3:11211"},
		},
	}

	s := MemcachedJumpHashSelector{}

	for _, test := range tests {
		testutil.Ok(t, s.SetServers(test.input...))

		actual := make([]string, 0, 3)
		err := s.Each(func(addr net.Addr) error {
			actual = append(actual, addr.String())
			return nil
		})

		testutil.Ok(t, err)
		testutil.Equals(t, test.expected, actual)
	}
}

func TestMemcachedJumpHashSelector_PickServer_ShouldEvenlyDistributeKeysToServers(t *testing.T) {
	servers := []string{"127.0.0.1:11211", "127.0.0.2:11211", "127.0.0.3:11211"}
	selector := MemcachedJumpHashSelector{}
	testutil.Ok(t, selector.SetServers(servers...))

	// Calculate the distribution of keys.
	distribution := make(map[string]int)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		addr, err := selector.PickServer(key)
		testutil.Ok(t, err)
		distribution[addr.String()]++
	}

	// Expect each server got at least 25% of keys, where the perfect split would be 33.3% each.
	minKeysPerServer := int(float64(len(servers)) * 0.25)
	testutil.Equals(t, len(servers), len(distribution))

	for addr, count := range distribution {
		if count < minKeysPerServer {
			testutil.Ok(t, errors.Errorf("expected %s to have received at least %d keys instead it received %d", addr, minKeysPerServer, count))
		}
	}
}

func TestMemcachedJumpHashSelector_PickServer_ShouldUseConsistentHashing(t *testing.T) {
	servers := []string{
		"127.0.0.1:11211",
		"127.0.0.2:11211",
		"127.0.0.3:11211",
		"127.0.0.4:11211",
		"127.0.0.5:11211",
		"127.0.0.6:11211",
		"127.0.0.7:11211",
		"127.0.0.8:11211",
		"127.0.0.9:11211",
	}

	selector := MemcachedJumpHashSelector{}
	testutil.Ok(t, selector.SetServers(servers...))

	// Pick a server for each key.
	distribution := make(map[string]string)
	numKeys := 1000

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		addr, err := selector.PickServer(key)
		testutil.Ok(t, err)
		distribution[key] = addr.String()
	}

	// Add 1 more server that - in a natural ordering - is added as last.
	servers = append(servers, "127.0.0.10:11211")
	testutil.Ok(t, selector.SetServers(servers...))

	// Calculate the number of keys who has been moved due to the resharding.
	moved := 0

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		addr, err := selector.PickServer(key)
		testutil.Ok(t, err)

		if distribution[key] != addr.String() {
			moved++
		}
	}

	// Expect we haven't moved more than (1/shards)% +2% tolerance.
	maxExpectedMovedPerc := (1.0 / float64(len(servers))) + 0.02
	maxExpectedMoved := int(float64(numKeys) * maxExpectedMovedPerc)
	if moved > maxExpectedMoved {
		testutil.Ok(t, errors.Errorf("expected resharding moved no more then %d keys while %d have been moved", maxExpectedMoved, moved))
	}
}

func TestMemcachedJumpHashSelector_PickServer_ShouldReturnErrNoServersOnNoServers(t *testing.T) {
	s := MemcachedJumpHashSelector{}
	_, err := s.PickServer("foo")
	testutil.Equals(t, memcache.ErrNoServers, err)
}

func BenchmarkMemcachedJumpHashSelector_PickServer(b *testing.B) {
	// Create a pretty long list of servers.
	servers := make([]string, 0)
	for i := 1; i <= 60; i++ {
		servers = append(servers, fmt.Sprintf("127.0.0.%d:11211", i))
	}

	selector := MemcachedJumpHashSelector{}
	err := selector.SetServers(servers...)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := selector.PickServer(fmt.Sprint(i))
		if err != nil {
			b.Error(err)
		}
	}
}
