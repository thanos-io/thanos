// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"net"
	"strings"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cespare/xxhash/v2"
	"github.com/facette/natsort"
)

// MemcachedJumpHashSelector implements the memcache.ServerSelector
// interface, utilizing a jump hash to distribute keys to servers.
//
// While adding or removing servers only requires 1/N keys to move,
// servers are treated as a stack and can only be pushed/popped.
// Therefore, MemcachedJumpHashSelector works best for servers
// with consistent DNS names where the naturally sorted order
// is predictable (ie. Kubernetes statefulsets).
type MemcachedJumpHashSelector struct {
	mu    sync.RWMutex
	addrs []net.Addr
}

// SetServers changes a MemcachedJumpHashSelector's set of servers at
// runtime and is safe for concurrent use by multiple goroutines.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any
// error occurs, no changes are made to the internal server list.
//
// To minimize the number of rehashes for keys when scaling the
// number of servers in subsequent calls to SetServers, servers
// are stored in natural sort order.
func (s *MemcachedJumpHashSelector) SetServers(servers ...string) error {
	sortedServers := make([]string, len(servers))
	copy(sortedServers, servers)
	natsort.Sort(sortedServers)

	naddr := make([]net.Addr, len(servers))
	var err error
	for i, server := range sortedServers {
		naddr[i], err = parseStaticAddr(server)
		if err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.addrs = naddr
	return nil
}

// PickServer returns the server address that a given item
// should be shared onto.
func (s *MemcachedJumpHashSelector) PickServer(key string) (net.Addr, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.addrs) == 0 {
		return nil, memcache.ErrNoServers
	} else if len(s.addrs) == 1 {
		return s.addrs[0], nil
	}
	return pickServerWithJumpHash(s.addrs, key), nil
}

// Each iterates over each server and calls the given function.
// If f returns a non-nil error, iteration will stop and that
// error will be returned.
func (s *MemcachedJumpHashSelector) Each(f func(net.Addr) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, def := range s.addrs {
		if err := f(def); err != nil {
			return err
		}
	}
	return nil
}

// PickServerForKeys is like PickServer but returns a map of server address
// and corresponding keys.
func (s *MemcachedJumpHashSelector) PickServerForKeys(keys []string) (map[string][]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// No need of a jump hash in case of 0 or 1 servers.
	if len(s.addrs) <= 0 {
		return nil, memcache.ErrNoServers
	}

	m := make(map[string][]string, len(keys))
	if len(s.addrs) == 1 {
		m[s.addrs[0].String()] = keys
		return m, nil
	}

	for _, key := range keys {
		// Pick a server using the jump hash.
		picked := pickServerWithJumpHash(s.addrs, key).String()
		m[picked] = append(m[picked], key)
	}

	return m, nil
}

// pickServerWithJumpHash returns the server address that a given item should be shared onto.
func pickServerWithJumpHash(addrs []net.Addr, key string) net.Addr {
	// Pick a server using the jump hash.
	cs := xxhash.Sum64String(key)
	idx := jumpHash(cs, len(addrs))
	picked := (addrs)[idx]
	return picked
}

// Copied from https://github.com/bradfitz/gomemcache/blob/master/memcache/selector.go#L68.
func parseStaticAddr(server string) (net.Addr, error) {
	if strings.Contains(server, "/") {
		addr, err := net.ResolveUnixAddr("unix", server)
		if err != nil {
			return nil, err
		}
		return newStaticAddr(addr), nil
	}
	tcpaddr, err := net.ResolveTCPAddr("tcp", server)
	if err != nil {
		return nil, err
	}
	return newStaticAddr(tcpaddr), nil
}

// Copied from https://github.com/bradfitz/gomemcache/blob/master/memcache/selector.go#L45
// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw, str string
}

func newStaticAddr(a net.Addr) net.Addr {
	return &staticAddr{
		ntw: a.Network(),
		str: a.String(),
	}
}

func (s *staticAddr) Network() string { return s.ntw }
func (s *staticAddr) String() string  { return s.str }
