package receive

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/thanos-io/thanos/pkg/store/prompb"

	"github.com/cespare/xxhash"
)

const sep = '\xff'

// Hashring finds the correct node to handle a given time series
// for a specified tenant.
// It returns the node and any error encountered.
type Hashring interface {
	Get(tenant string, timeSeries *prompb.TimeSeries) (string, error)
}

// hash returns a hash for the given tenant and time series.
func hash(tenant string, ts *prompb.TimeSeries) uint64 {
	// Sort labelset to ensure a stable hash.
	sort.Slice(ts.Labels, func(i, j int) bool { return ts.Labels[i].Name < ts.Labels[j].Name })

	b := make([]byte, 0, 1024)
	b = append(b, []byte(tenant)...)
	b = append(b, sep)
	for _, v := range ts.Labels {
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
func (s SingleNodeHashring) Get(_ string, _ *prompb.TimeSeries) (string, error) {
	return string(s), nil
}

// simpleHashring represents a group of nodes handling write requests.
type simpleHashring []string

// Get returns a target to handle the given tenant and time series.
func (s simpleHashring) Get(tenant string, ts *prompb.TimeSeries) (string, error) {
	// Always return nil here to implement the Hashring interface.
	return s[hash(tenant, ts)%uint64(len(s))], nil
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
func (m *multiHashring) Get(tenant string, ts *prompb.TimeSeries) (string, error) {
	m.mu.RLock()
	h, ok := m.cache[tenant]
	m.mu.RUnlock()
	if ok {
		return h.Get(tenant, ts)
	}
	var found bool
	// If the tenant is not in the cache, then we need to check
	// every tenant in the configuration.
	for i, t := range m.tenantSets {
		// If the hashring has no tenants, then it is
		// considered a default hashring and matches everything.
		if t == nil {
			found = true
		}
		if _, ok := t[tenant]; ok {
			found = true
		}
		// If the hashring has no tenants, then it is
		// considered a default hashring and matches everything.
		if found {
			m.mu.Lock()
			m.cache[tenant] = m.hashrings[i]
			m.mu.Unlock()
			return m.hashrings[i].Get(tenant, ts)
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

// HashringFromConfig creates multi-tenant hashrings from a
// hashring configuration file watcher.
// The configuration file is watched for updates.
// Hashrings are returned on the updates channel.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
func HashringFromConfig(ctx context.Context, updates chan<- Hashring, cw *ConfigWatcher) {
	cfgUpdates := make(chan []HashringConfig)
	defer close(cfgUpdates)
	go cw.Run(ctx, cfgUpdates)

	for {
		select {
		case cfg := <-cfgUpdates:
			updates <- newMultiHashring(cfg)
		case <-ctx.Done():
			return
		}
	}
}
