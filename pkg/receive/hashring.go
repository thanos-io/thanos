// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash"

	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/store/labelpb"

	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// HashringAlgorithm is the algorithm used to distribute series in the ring.
type HashringAlgorithm string

const (
	AlgorithmHashmod HashringAlgorithm = "hashmod"
	AlgorithmKetama  HashringAlgorithm = "ketama"

	// SectionsPerNode is the number of sections in the ring assigned to each node
	// in the ketama hashring. A higher number yields a better series distribution,
	// but also comes with a higher memory cost.
	SectionsPerNode = 1000
)

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

// Hashring finds the correct node to handle a given time series
// for a specified tenant.
// It returns the node and any error encountered.
type Hashring interface {
	// Get returns the first node that should handle the given tenant and time series.
	Get(tenant string, timeSeries *prompb.TimeSeries) (string, error)
	// GetN returns the nth node that should handle the given tenant and time series.
	GetN(tenant string, timeSeries *prompb.TimeSeries, n uint64) (string, error)
}

// SingleNodeHashring always returns the same node.
type SingleNodeHashring string

// Get implements the Hashring interface.
func (s SingleNodeHashring) Get(tenant string, ts *prompb.TimeSeries) (string, error) {
	return s.GetN(tenant, ts, 0)
}

// GetN implements the Hashring interface.
func (s SingleNodeHashring) GetN(_ string, _ *prompb.TimeSeries, n uint64) (string, error) {
	if n > 0 {
		return "", &insufficientNodesError{have: 1, want: n + 1}
	}
	return string(s), nil
}

// simpleHashring represents a group of nodes handling write requests by hashmoding individual series.
type simpleHashring []string

// Get returns a target to handle the given tenant and time series.
func (s simpleHashring) Get(tenant string, ts *prompb.TimeSeries) (string, error) {
	return s.GetN(tenant, ts, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (s simpleHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (string, error) {
	if n >= uint64(len(s)) {
		return "", &insufficientNodesError{have: uint64(len(s)), want: n + 1}
	}

	return s[(labelpb.HashWithPrefix(tenant, ts.Labels)+n)%uint64(len(s))], nil
}

type section struct {
	endpointIndex uint64
	hash          uint64
	replicas      []uint64
}

type sections []*section

func (p sections) Len() int           { return len(p) }
func (p sections) Less(i, j int) bool { return p[i].hash < p[j].hash }
func (p sections) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p sections) Sort()              { sort.Sort(p) }

// ketamaHashring represents a group of nodes handling write requests with consistent hashing.
type ketamaHashring struct {
	endpoints    []string
	sections     sections
	numEndpoints uint64
}

func newKetamaHashring(endpoints []string, sectionsPerNode int, replicationFactor uint64) *ketamaHashring {
	numSections := len(endpoints) * sectionsPerNode

	hash := xxhash.New()
	ringSections := make(sections, 0, numSections)
	for endpointIndex, endpoint := range endpoints {
		for i := 1; i <= sectionsPerNode; i++ {
			_, _ = hash.Write([]byte(endpoint + ":" + strconv.Itoa(i)))
			n := &section{
				endpointIndex: uint64(endpointIndex),
				hash:          hash.Sum64(),
				replicas:      make([]uint64, 0, replicationFactor),
			}

			ringSections = append(ringSections, n)
			hash.Reset()
		}
	}
	sort.Sort(ringSections)
	calculateSectionReplicas(ringSections, replicationFactor)

	return &ketamaHashring{
		endpoints:    endpoints,
		sections:     ringSections,
		numEndpoints: uint64(len(endpoints)),
	}
}

// calculateSectionReplicas pre-calculates replicas for each section,
// ensuring that replicas for each ring section are owned by different endpoints.
func calculateSectionReplicas(ringSections sections, replicationFactor uint64) {
	for i, s := range ringSections {
		replicas := make(map[uint64]struct{})
		j := i - 1
		for uint64(len(replicas)) < replicationFactor {
			j = (j + 1) % len(ringSections)
			rep := ringSections[j]
			if _, ok := replicas[rep.endpointIndex]; ok {
				continue
			}
			replicas[rep.endpointIndex] = struct{}{}
			s.replicas = append(s.replicas, rep.endpointIndex)
		}
	}
}

func (c ketamaHashring) Get(tenant string, ts *prompb.TimeSeries) (string, error) {
	return c.GetN(tenant, ts, 0)
}

func (c ketamaHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (string, error) {
	if n >= c.numEndpoints {
		return "", &insufficientNodesError{have: c.numEndpoints, want: n + 1}
	}

	v := labelpb.HashWithPrefix(tenant, ts.Labels)

	var i uint64
	i = uint64(sort.Search(len(c.sections), func(i int) bool {
		return c.sections[i].hash >= v
	}))

	numSections := uint64(len(c.sections))
	if i == numSections {
		i = 0
	}

	endpointIndex := c.sections[i].replicas[n]
	return c.endpoints[endpointIndex], nil
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
	return m.GetN(tenant, ts, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (m *multiHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (string, error) {
	m.mu.RLock()
	h, ok := m.cache[tenant]
	m.mu.RUnlock()
	if ok {
		return h.GetN(tenant, ts, n)
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

			return m.hashrings[i].GetN(tenant, ts, n)
		}
	}
	return "", errors.New("no matching hashring to handle tenant")
}

// newMultiHashring creates a multi-tenant hashring for a given slice of
// groups.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
func newMultiHashring(algorithm HashringAlgorithm, replicationFactor uint64, cfg []HashringConfig) Hashring {
	m := &multiHashring{
		cache: make(map[string]Hashring),
	}

	newHashring := func(endpoints []string) Hashring {
		switch algorithm {
		case AlgorithmHashmod:
			return simpleHashring(endpoints)
		case AlgorithmKetama:
			return newKetamaHashring(endpoints, SectionsPerNode, replicationFactor)
		default:
			return simpleHashring(endpoints)
		}
	}

	for _, h := range cfg {
		m.hashrings = append(m.hashrings, newHashring(h.Endpoints))
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
func HashringFromConfigWatcher(ctx context.Context, algorithm HashringAlgorithm, replicationFactor uint64, updates chan<- Hashring, cw *ConfigWatcher) error {
	defer close(updates)
	go cw.Run(ctx)

	for {
		select {
		case cfg, ok := <-cw.C():
			if !ok {
				return errors.New("hashring config watcher stopped unexpectedly")
			}
			updates <- newMultiHashring(algorithm, replicationFactor, cfg)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// HashringFromConfig loads raw configuration content and returns a Hashring if the given configuration is not valid.
func HashringFromConfig(algorithm HashringAlgorithm, replicationFactor uint64, content string) (Hashring, error) {
	config, err := parseConfig([]byte(content))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse configuration")
	}

	// If hashring is empty, return an error.
	if len(config) == 0 {
		return nil, errors.Wrapf(err, "failed to load configuration")
	}

	return newMultiHashring(algorithm, replicationFactor, config), err
}
