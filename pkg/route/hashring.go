// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package route

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

const sep = '\xff'

// insufficientNodesError is returned when a hashring does not
// have enough nodes to satisfy a request for a node.
type insufficientNodesError struct {
	have uint64
	want uint64
}

// Error implements the error interface.
func (i *insufficientNodesError) Error() string {
	return fmt.Sprintf("insufficient nodes; have %d, want %d", i.have, i.want)
}

// Hashring finds the correct node to handle a given label-set
// for a specified tenant.
// It returns the node and any error encountered.
type Hashring interface {
	// Get returns the first node that should handle the given tenant and label-set.
	Get(tenant string, labelSet []labelpb.ZLabel) (string, error)
	// GetN returns the nth node that should handle the given tenant and label-set.
	GetN(tenant string, labelSet []labelpb.ZLabel, n uint64) (string, error)
}

// hash returns a hash for the given tenant and label-set.
func hash(tenant string, labelSet []labelpb.ZLabel) uint64 {
	// Sort labelset to ensure a stable hash.
	sort.Slice(labelSet, func(i, j int) bool { return labelSet[i].Name < labelSet[j].Name })

	b := make([]byte, 0, 1024)
	b = append(b, []byte(tenant)...)
	b = append(b, sep)
	for _, v := range labelSet {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	return xxhash.Sum64(b)
}

// SingleNodeHashring always returns the same node.
type SingleNodeHashring string

// Get implements the Hashring interface.
func (s SingleNodeHashring) Get(tenant string, labelSet []labelpb.ZLabel) (string, error) {
	return s.GetN(tenant, labelSet, 0)
}

// GetN implements the Hashring interface.
func (s SingleNodeHashring) GetN(_ string, _ []labelpb.ZLabel, n uint64) (string, error) {
	if n > 0 {
		return "", &insufficientNodesError{have: 1, want: n + 1}
	}
	return string(s), nil
}

// simpleHashring represents a group of nodes handling write requests.
type simpleHashring []string

// Get returns a target to handle the given tenant and time series.
func (s simpleHashring) Get(tenant string, labelSet []labelpb.ZLabel) (string, error) {
	return s.GetN(tenant, labelSet, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (s simpleHashring) GetN(tenant string, labelSet []labelpb.ZLabel, n uint64) (string, error) {
	if n >= uint64(len(s)) {
		return "", &insufficientNodesError{have: uint64(len(s)), want: n + 1}
	}
	return s[(hash(tenant, labelSet)+n)%uint64(len(s))], nil
}

// multiHashring represents a set of hashrings.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
type multiHashring struct {
	cache      map[string]Hashring
	hashrings  []Hashring
	tenantSets []map[string]struct{}

	// We need a mutex to guard concurrent access
	// to the cache map, as this is both written to
	// and read from.
	mu sync.RWMutex
}

// Get returns a target to handle the given tenant and time series.
func (m *multiHashring) Get(tenant string, labelSet []labelpb.ZLabel) (string, error) {
	return m.GetN(tenant, labelSet, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (m *multiHashring) GetN(tenant string, labelSet []labelpb.ZLabel, n uint64) (string, error) {
	m.mu.RLock()
	h, ok := m.cache[tenant]
	m.mu.RUnlock()

	if ok {
		return h.GetN(tenant, labelSet, n)
	}
	var found bool
	// If the tenant is not in the cache, then we need to check
	// every tenant in the configuration.
	for i, t := range m.tenantSets {
		// If the hashring has no tenants, then it is
		// considered a default hashring and matches everything.
		if t == nil {
			found = true
		} else if _, ok := t[tenant]; ok {
			found = true
		}
		if found {
			m.mu.Lock()
			m.cache[tenant] = m.hashrings[i]
			m.mu.Unlock()
			return m.hashrings[i].GetN(tenant, labelSet, n)
		}
	}
	return "", errors.New("no matching hashring to handle tenant")
}

// newMultiHashring creates a multi-tenant hashring for a given slice of
// groups.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
func newMultiHashring(cfg []HashringConfig) Hashring {
	m := &multiHashring{
		cache: make(map[string]Hashring),
	}

	for _, h := range cfg {
		m.hashrings = append(m.hashrings, simpleHashring(h.Endpoints))
		var t map[string]struct{}
		if len(h.Tenants) != 0 {
			t = make(map[string]struct{})
		}
		for _, tenant := range h.Tenants {
			t[tenant] = struct{}{}
		}
		m.tenantSets = append(m.tenantSets, t)
	}
	return m
}

// HashringFromConfigWatcher creates multi-tenant hashrings from a
// hashring configuration file watcher.
// The configuration file is watched for updates.
// Hashrings are returned on the updates channel.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
// The updates chan is closed before exiting.
func HashringFromConfigWatcher(ctx context.Context, updates chan<- Hashring, cw *ConfigWatcher) error {
	defer close(updates)
	go cw.Run(ctx)

	for {
		select {
		case cfg, ok := <-cw.C():
			if !ok {
				return errors.New("hashring config watcher stopped unexpectedly")
			}
			updates <- newMultiHashring(cfg)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// HashringFromConfig loads raw configuration content and returns a Hashring if the given configuration is not valid.
func HashringFromConfig(content string) (Hashring, error) {
	config, err := parseConfig([]byte(content))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse configuration")
	}

	// If hashring is empty, return an error.
	if len(config) == 0 {
		return nil, errors.Wrapf(err, "failed to load configuration")
	}

	return newMultiHashring(config), err
}
