// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecapnp

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
)

// StoreSet manages a set of Cap'n Proto store clients.
// It provides a GetStoreClients method that returns all active stores,
// which can be combined with gRPC stores from EndpointSet.
type StoreSet struct {
	logger    log.Logger
	addresses []string

	mtx     sync.RWMutex
	clients map[string]*StoreClient
}

// NewStoreSet creates a new StoreSet with the given addresses.
func NewStoreSet(logger log.Logger, addresses []string) *StoreSet {
	return &StoreSet{
		logger:    logger,
		addresses: addresses,
		clients:   make(map[string]*StoreClient),
	}
}

// Update updates the store set by connecting to all configured addresses.
func (s *StoreSet) Update(ctx context.Context) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Create clients for new addresses
	for _, addr := range s.addresses {
		if _, exists := s.clients[addr]; exists {
			continue
		}

		dialer := NewTCPDialer(addr)
		client := NewStoreClient(dialer, s.logger, addr, false)

		// Try to connect and get store info
		if err := client.connect(ctx); err != nil {
			level.Warn(s.logger).Log("msg", "failed to connect to capnp store", "addr", addr, "err", err)
			continue
		}

		// Get store info via Info API
		info, err := client.FetchInfo(ctx)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to get store info from capnp store", "addr", addr, "err", err)
			// Use defaults if info API fails - use full time range like gRPC endpoints
			client.SetTimeRange(math.MinInt64, math.MaxInt64)
		} else {
			client.SetTimeRange(info.MinTime, info.MaxTime)
			client.SetLabelSets(info.LabelSets)
			client.SetTSDBInfos(info.TSDBInfos)
			client.SetSupportsSharding(info.SupportsSharding)
			client.SetSupportsWithoutReplicaLabels(info.SupportsWithoutReplicaLabels)
		}

		s.clients[addr] = client
		level.Info(s.logger).Log("msg", "connected to capnp store", "addr", addr)
	}

	// Remove clients for addresses that are no longer configured
	addrSet := make(map[string]struct{}, len(s.addresses))
	for _, addr := range s.addresses {
		addrSet[addr] = struct{}{}
	}

	for addr, client := range s.clients {
		if _, exists := addrSet[addr]; !exists {
			client.Close()
			delete(s.clients, addr)
			level.Info(s.logger).Log("msg", "disconnected from capnp store", "addr", addr)
		}
	}
}

// GetStoreClients returns all active Cap'n Proto store clients.
// The returned clients implement the store.Client interface.
func (s *StoreSet) GetStoreClients() []store.Client {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	clients := make([]store.Client, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	return clients
}

// Close closes all store clients.
func (s *StoreSet) Close() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for addr, client := range s.clients {
		client.Close()
		delete(s.clients, addr)
	}
}

// RunUpdateLoop starts a goroutine that periodically updates the store set.
func (s *StoreSet) RunUpdateLoop(ctx context.Context, interval time.Duration) {
	runutil.Repeat(interval, ctx.Done(), func() error {
		s.Update(ctx)
		return nil
	})
}

// CombinedStoreClientFunc returns a function that combines store clients from
// multiple sources (e.g., EndpointSet and StoreSet).
func CombinedStoreClientFunc(funcs ...func() []store.Client) func() []store.Client {
	return func() []store.Client {
		var all []store.Client
		for _, f := range funcs {
			all = append(all, f()...)
		}
		return all
	}
}

// Ensure StoreClient implements store.Client
var _ store.Client = (*StoreClient)(nil)

// Additional methods to make StoreClient fully implement store.Client
// Note: These are already implemented in client.go, but we need to verify the interface

// LabelSets implements store.Client.
func (c *StoreClient) labelSetsFromInfo() []labels.Labels {
	// Return cached label sets
	return c.labelSets
}

// TSDBInfos implements store.Client.
func (c *StoreClient) tsdbInfosFromInfo() []infopb.TSDBInfo {
	// Return cached TSDB infos
	return c.tsdbInfos
}
