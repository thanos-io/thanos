// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash"
	"golang.org/x/exp/slices"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/strutil"

	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// HashringAlgorithm is the algorithm used to distribute series in the ring.
type HashringAlgorithm string

const (
	AlgorithmHashmod       HashringAlgorithm = "hashmod"
	AlgorithmKetama        HashringAlgorithm = "ketama"
	AlgorithmAlignedKetama HashringAlgorithm = "aligned_ketama"

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
	Get(tenant string, timeSeries *prompb.TimeSeries) (Endpoint, error)
	// GetN returns the nth node that should handle the given tenant and time series.
	GetN(tenant string, timeSeries *prompb.TimeSeries, n uint64) (Endpoint, error)
	// Nodes returns a sorted slice of nodes that are in this hashring. Addresses could be duplicated
	// if, for example, the same address is used for multiple tenants in the multi-hashring.
	Nodes() []Endpoint

	Close()
}

// SingleNodeHashring always returns the same node.
type SingleNodeHashring string

// Get implements the Hashring interface.
func (s SingleNodeHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return s.GetN(tenant, ts, 0)
}

func (s SingleNodeHashring) Close() {}

func (s SingleNodeHashring) Nodes() []Endpoint {
	return []Endpoint{{Address: string(s), CapNProtoAddress: string(s)}}
}

// GetN implements the Hashring interface.
func (s SingleNodeHashring) GetN(_ string, _ *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if n > 0 {
		return Endpoint{}, &insufficientNodesError{have: 1, want: n + 1}
	}
	return Endpoint{
		Address:          string(s),
		CapNProtoAddress: string(s),
	}, nil
}

// simpleHashring represents a group of nodes handling write requests by hashmoding individual series.
type simpleHashring []Endpoint

func (s simpleHashring) Close() {}

func newSimpleHashring(endpoints []Endpoint) (Hashring, error) {
	for i := range endpoints {
		if endpoints[i].AZ != "" {
			return nil, errors.New("Hashmod algorithm does not support AZ aware hashring configuration. Either use Ketama or remove AZ configuration.")
		}
	}
	slices.SortFunc(endpoints, func(a, b Endpoint) int {
		return strings.Compare(a.Address, b.Address)
	})

	return simpleHashring(endpoints), nil
}

func (s simpleHashring) Nodes() []Endpoint {
	return s
}

// Get returns a target to handle the given tenant and time series.
func (s simpleHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return s.GetN(tenant, ts, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (s simpleHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if n >= uint64(len(s)) {
		return Endpoint{}, &insufficientNodesError{have: uint64(len(s)), want: n + 1}
	}

	return s[(labelpb.HashWithPrefix(tenant, ts.Labels)+n)%uint64(len(s))], nil
}

type section struct {
	az            string
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
	endpoints    []Endpoint
	sections     sections
	numEndpoints uint64
}

func (s ketamaHashring) Close() {}

func newKetamaHashring(endpoints []Endpoint, sectionsPerNode int, replicationFactor uint64) (*ketamaHashring, error) {
	numSections := len(endpoints) * sectionsPerNode

	if len(endpoints) < int(replicationFactor) {
		return nil, errors.New("ketama: amount of endpoints needs to be larger than replication factor")

	}
	hash := xxhash.New()
	availabilityZones := make(map[string]struct{})
	ringSections := make(sections, 0, numSections)

	for endpointIndex, endpoint := range endpoints {
		availabilityZones[endpoint.AZ] = struct{}{}
		for i := 1; i <= sectionsPerNode; i++ {
			_, _ = hash.Write([]byte(endpoint.Address + ":" + strconv.Itoa(i)))
			n := &section{
				az:            endpoint.AZ,
				endpointIndex: uint64(endpointIndex),
				hash:          hash.Sum64(),
				replicas:      make([]uint64, 0, replicationFactor),
			}

			ringSections = append(ringSections, n)
			hash.Reset()
		}
	}
	sort.Sort(ringSections)
	calculateSectionReplicas(ringSections, replicationFactor, availabilityZones)

	return &ketamaHashring{
		endpoints:    endpoints,
		sections:     ringSections,
		numEndpoints: uint64(len(endpoints)),
	}, nil
}

func (k *ketamaHashring) Nodes() []Endpoint {
	return k.endpoints
}

func sizeOfLeastOccupiedAZ(azSpread map[string]int64) int64 {
	minValue := int64(math.MaxInt64)
	for _, value := range azSpread {
		if value < minValue {
			minValue = value
		}
	}
	return minValue
}

// calculateSectionReplicas pre-calculates replicas for each section,
// ensuring that replicas for each ring section are owned by different endpoints.
func calculateSectionReplicas(ringSections sections, replicationFactor uint64, availabilityZones map[string]struct{}) {
	for i, s := range ringSections {
		replicas := make(map[uint64]struct{})
		azSpread := make(map[string]int64)
		for az := range availabilityZones {
			// This is to make sure each az is initially represented
			azSpread[az] = 0
		}
		j := i - 1
		for uint64(len(replicas)) < replicationFactor {
			j = (j + 1) % len(ringSections)
			rep := ringSections[j]
			if _, ok := replicas[rep.endpointIndex]; ok {
				continue
			}
			if len(azSpread) > 1 && azSpread[rep.az] > 0 && azSpread[rep.az] > sizeOfLeastOccupiedAZ(azSpread) {
				// We want to ensure even AZ spread before we add more replicas within the same AZ
				continue
			}
			replicas[rep.endpointIndex] = struct{}{}
			azSpread[rep.az]++
			s.replicas = append(s.replicas, rep.endpointIndex)
		}
	}
}

func (c ketamaHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return c.GetN(tenant, ts, 0)
}

func (c ketamaHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if n >= c.numEndpoints {
		return Endpoint{}, &insufficientNodesError{have: c.numEndpoints, want: n + 1}
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

type tenantSet map[string]tenantMatcher

func (t tenantSet) match(tenant string) (bool, error) {
	// Fast path for the common case of direct match.
	if mt, ok := t[tenant]; ok && isExactMatcher(mt) {
		return true, nil
	} else {
		for tenantPattern, matcherType := range t {
			switch matcherType {
			case TenantMatcherGlob:
				matches, err := filepath.Match(tenantPattern, tenant)
				if err != nil {
					return false, fmt.Errorf("error matching tenant pattern %s (tenant %s): %w", tenantPattern, tenant, err)
				}
				if matches {
					return true, nil
				}
			case TenantMatcherTypeExact:
				// Already checked above, skipping.
				fallthrough
			default:
				continue
			}

		}
	}
	return false, nil
}

// multiHashring represents a set of hashrings.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
type multiHashring struct {
	cache      map[string]Hashring
	hashrings  []Hashring
	tenantSets []tenantSet

	// We need a mutex to guard concurrent access
	// to the cache map, as this is both written to
	// and read from.
	mu sync.RWMutex

	nodes []Endpoint
}

func (s *multiHashring) Close() {
	for _, h := range s.hashrings {
		h.Close()
	}
}

// Get returns a target to handle the given tenant and time series.
func (m *multiHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return m.GetN(tenant, ts, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (m *multiHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
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
		} else {
			var err error
			if found, err = t.match(tenant); err != nil {
				return Endpoint{}, err
			}
		}
		if found {
			m.mu.Lock()
			m.cache[tenant] = m.hashrings[i]
			m.mu.Unlock()

			return m.hashrings[i].GetN(tenant, ts, n)
		}
	}
	return Endpoint{}, errors.New("no matching hashring to handle tenant")
}

func (m *multiHashring) Nodes() []Endpoint {
	return m.nodes
}

// shuffleShardHashring wraps a hashring implementation and applies shuffle sharding logic
// to limit which nodes are used for each tenant.
type shuffleShardHashring struct {
	baseRing Hashring

	shuffleShardingConfig ShuffleShardingConfig

	replicationFactor uint64

	nodes []Endpoint

	// cache stores tenant-specific subrings. The value is Hashring to support both
	// *ketamaHashring (regular shuffle sharding) and aligned ketama subrings.
	cache *lru.Cache[string, Hashring]

	metrics *shuffleShardCacheMetrics
}

func (s *shuffleShardHashring) Close() {
	s.metrics.close()
}

func (s *shuffleShardCacheMetrics) close() {
	s.reg.Unregister(s.requestsTotal)
	s.reg.Unregister(s.hitsTotal)
	s.reg.Unregister(s.numItems)
	s.reg.Unregister(s.maxItems)
	s.reg.Unregister(s.evicted)
}

type shuffleShardCacheMetrics struct {
	requestsTotal prometheus.Counter
	hitsTotal     prometheus.Counter
	numItems      prometheus.Gauge
	maxItems      prometheus.Gauge
	evicted       prometheus.Counter

	reg prometheus.Registerer
}

func newShuffleShardCacheMetrics(reg prometheus.Registerer, hashringName string) *shuffleShardCacheMetrics {
	reg = prometheus.WrapRegistererWith(prometheus.Labels{"hashring": hashringName}, reg)

	return &shuffleShardCacheMetrics{
		reg: reg,
		requestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_shuffle_shard_cache_requests_total",
			Help: "Total number of cache requests for shuffle shard subrings",
		}),
		hitsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_shuffle_shard_cache_hits_total",
			Help: "Total number of cache hits for shuffle shard subrings",
		}),
		numItems: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_shuffle_shard_cache_items",
			Help: "Total number of cached items",
		}),
		maxItems: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_shuffle_shard_cache_max_items",
			Help: "Maximum number of items that can be cached",
		}),
		evicted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_shuffle_shard_cache_evicted_total",
			Help: "Total number of items evicted from the cache",
		}),
	}
}

// newShuffleShardHashring creates a new shuffle sharding hashring wrapper.
func newShuffleShardHashring(baseRing Hashring, shuffleShardingConfig ShuffleShardingConfig, replicationFactor uint64, reg prometheus.Registerer, name string) (*shuffleShardHashring, error) {
	l := log.NewNopLogger()

	level.Info(l).Log(
		"msg", "Creating shuffle sharding hashring",
		"default_shard_size", shuffleShardingConfig.ShardSize,
		"total_nodes", len(baseRing.Nodes()),
	)

	if len(shuffleShardingConfig.Overrides) > 0 {
		for _, override := range shuffleShardingConfig.Overrides {
			level.Info(l).Log(
				"msg", "Tenant shard size override",
				"tenants", override.Tenants,
				"tenant_matcher_type", override.TenantMatcherType,
				"shard_size", override.ShardSize,
			)
		}
	}

	const DefaultShuffleShardingCacheSize = 100

	if shuffleShardingConfig.CacheSize <= 0 {
		shuffleShardingConfig.CacheSize = DefaultShuffleShardingCacheSize
	}

	metrics := newShuffleShardCacheMetrics(reg, name)
	metrics.maxItems.Set(float64(shuffleShardingConfig.CacheSize))

	cache, err := lru.NewWithEvict[string, Hashring](shuffleShardingConfig.CacheSize, func(key string, value Hashring) {
		metrics.evicted.Inc()
		metrics.numItems.Dec()
	})
	if err != nil {
		return nil, err
	}

	ssh := &shuffleShardHashring{
		baseRing:              baseRing,
		shuffleShardingConfig: shuffleShardingConfig,
		replicationFactor:     replicationFactor,
		cache:                 cache,
		metrics:               metrics,
	}

	// Dedupe nodes as the base ring may have duplicates. We are only interested in unique nodes.
	ssh.nodes = ssh.dedupedNodes()

	nodeCountByAZ := make(map[string]int)
	for _, node := range ssh.nodes {
		var az string = node.AZ
		if shuffleShardingConfig.ZoneAwarenessDisabled {
			az = ""
		}
		nodeCountByAZ[az]++
	}

	maxNodesInAZ := 0
	for _, count := range nodeCountByAZ {
		maxNodesInAZ = max(maxNodesInAZ, count)
	}

	if shuffleShardingConfig.ShardSize > maxNodesInAZ {
		level.Warn(l).Log(
			"msg", "Shard size is larger than the maximum number of nodes in any AZ; some tenants might get all not working nodes if that AZ goes down",
			"shard_size", shuffleShardingConfig.ShardSize,
			"max_nodes_in_az", maxNodesInAZ,
		)
	}

	for _, override := range shuffleShardingConfig.Overrides {
		if override.ShardSize < maxNodesInAZ {
			continue
		}
		level.Warn(l).Log(
			"msg", "Shard size is larger than the maximum number of nodes in any AZ; some tenants might get all not working nodes if that AZ goes down",
			"max_nodes_in_az", maxNodesInAZ,
			"shard_size", override.ShardSize,
			"tenants", override.Tenants,
			"tenant_matcher_type", override.TenantMatcherType,
		)
	}
	return ssh, nil
}

func (s *shuffleShardHashring) Nodes() []Endpoint {
	return s.nodes
}

func (s *shuffleShardHashring) dedupedNodes() []Endpoint {
	uniqueNodes := make(map[Endpoint]struct{})
	for _, node := range s.baseRing.Nodes() {
		uniqueNodes[node] = struct{}{}
	}

	// Convert the map back to a slice
	nodes := make(endpoints, 0, len(uniqueNodes))
	for node := range uniqueNodes {
		nodes = append(nodes, node)
	}

	sort.Sort(nodes)

	return nodes
}

// getShardSize returns the shard size for a specific tenant, taking into account any overrides.
func (s *shuffleShardHashring) getShardSize(tenant string) int {
	for _, override := range s.shuffleShardingConfig.Overrides {
		if override.TenantMatcherType == TenantMatcherTypeExact {
			for _, t := range override.Tenants {
				if t == tenant {
					return override.ShardSize
				}
			}
		} else if override.TenantMatcherType == TenantMatcherGlob {
			for _, t := range override.Tenants {
				matches, err := filepath.Match(t, tenant)
				if err == nil && matches {
					return override.ShardSize
				}
			}
		}
	}

	// Default shard size is used if no overrides match
	return s.shuffleShardingConfig.ShardSize
}

// ShuffleShardExpectedInstancesPerZone returns the expected number of instances per zone for a given shard size and number of zones.
// Copied from Cortex. Copyright Cortex Authors.
func ShuffleShardExpectedInstancesPerZone(shardSize, numZones int) int {
	return int(math.Ceil(float64(shardSize) / float64(numZones)))
}

var (
	seedSeparator = []byte{0}
)

// yoloBuf will return an unsafe pointer to a string, as the name yoloBuf implies. Use at your own risk.
func yoloBuf(s string) []byte {
	return *((*[]byte)(unsafe.Pointer(&s)))
}

// ShuffleShardSeed returns seed for random number generator, computed from provided identifier.
// Copied from Cortex. Copyright Cortex Authors.
func ShuffleShardSeed(identifier, zone string) int64 {
	// Use the identifier to compute a hash we'll use to seed the random.
	hasher := md5.New()
	hasher.Write(yoloBuf(identifier)) // nolint:errcheck
	if zone != "" {
		hasher.Write(seedSeparator) // nolint:errcheck
		hasher.Write(yoloBuf(zone)) // nolint:errcheck
	}
	checksum := hasher.Sum(nil)

	// Generate the seed based on the first 64 bits of the checksum.
	return int64(binary.BigEndian.Uint64(checksum))
}

func (s *shuffleShardHashring) getTenantShardCached(tenant string) (Hashring, error) {
	s.metrics.requestsTotal.Inc()

	cached, ok := s.cache.Get(tenant)
	if ok {
		s.metrics.hitsTotal.Inc()
		return cached, nil
	}

	var h Hashring
	var err error
	if s.shuffleShardingConfig.AlignedOrdinalSharding {
		h, err = s.getTenantShardAligned(tenant)
	} else {
		h, err = s.getTenantShard(tenant)
	}
	if err != nil {
		return nil, err
	}

	s.metrics.numItems.Inc()
	s.cache.Add(tenant, h)

	return h, nil
}

// getTenantShard returns or creates a consistent subset of nodes for a tenant.
// This implementation uses consistent hashing to select nodes, which provides
// the "consistency" property: adding/removing 1 node from the ring results in
// at most 1 node change in each tenant's shard.
func (s *shuffleShardHashring) getTenantShard(tenant string) (*ketamaHashring, error) {
	baseRing, ok := s.baseRing.(*ketamaHashring)
	if !ok {
		return nil, fmt.Errorf("shuffle sharding requires ketama hashring as base ring")
	}

	nodes := s.Nodes()
	nodesByAZ := make(map[string][]Endpoint)
	nodeIndexByAddr := make(map[string]int) // map address to index in nodes slice
	for i, node := range nodes {
		var az = node.AZ
		if s.shuffleShardingConfig.ZoneAwarenessDisabled {
			az = ""
		}
		nodesByAZ[az] = append(nodesByAZ[az], node)
		nodeIndexByAddr[node.Address] = i
	}

	// Build sections by AZ from the base ring
	sectionsByAZ := make(map[string]sections)
	for _, sec := range baseRing.sections {
		endpoint := baseRing.endpoints[sec.endpointIndex]
		var az = endpoint.AZ
		if s.shuffleShardingConfig.ZoneAwarenessDisabled {
			az = ""
		}
		sectionsByAZ[az] = append(sectionsByAZ[az], sec)
	}

	// Sort sections by hash within each AZ
	for az := range sectionsByAZ {
		sort.Sort(sectionsByAZ[az])
	}

	ss := s.getShardSize(tenant)
	var take int
	if s.shuffleShardingConfig.ZoneAwarenessDisabled {
		take = ss
	} else {
		take = ShuffleShardExpectedInstancesPerZone(ss, len(nodesByAZ))
	}

	var finalNodes = make([]Endpoint, 0, take*len(nodesByAZ))

	// Process each AZ independently (like Cortex does)
	for az, azNodes := range nodesByAZ {
		if take > len(azNodes) {
			return nil, fmt.Errorf("shard size %d is larger than number of nodes in AZ %s (%d)", ss, az, len(azNodes))
		}

		azSections := sectionsByAZ[az]
		if len(azSections) == 0 {
			continue
		}

		// Use deterministic seed based on tenant + az
		seed := ShuffleShardSeed(tenant, az)
		r := rand.New(rand.NewSource(seed))

		// Select nodes using consistent hashing (Cortex-style algorithm)
		// For each node we need to select:
		// 1. Generate a random position in the token ring
		// 2. Find the section at that position
		// 3. Walk the ring to find an instance we haven't selected yet
		selected := make(map[uint64]struct{}) // track selected endpoint indices

		for i := 0; i < take; i++ {
			// Generate a random position in the token ring
			randomPos := r.Uint64()

			// Find the section at or after this position
			startIdx := sort.Search(len(azSections), func(idx int) bool {
				return azSections[idx].hash >= randomPos
			})
			if startIdx == len(azSections) {
				startIdx = 0 // wrap around
			}

			// Walk the ring to find a unique instance
			for j := 0; j < len(azSections); j++ {
				idx := (startIdx + j) % len(azSections)
				sec := azSections[idx]

				if _, ok := selected[sec.endpointIndex]; ok {
					continue // already selected this instance
				}

				selected[sec.endpointIndex] = struct{}{}
				finalNodes = append(finalNodes, baseRing.endpoints[sec.endpointIndex])
				break
			}
		}
	}

	return newKetamaHashring(finalNodes, SectionsPerNode, s.replicationFactor)
}

// ordinalSection represents a section in the ordinal ring for consistent hashing.
type ordinalSection struct {
	ordinal int
	hash    uint64
}

type ordinalSections []ordinalSection

func (o ordinalSections) Len() int           { return len(o) }
func (o ordinalSections) Less(i, j int) bool { return o[i].hash < o[j].hash }
func (o ordinalSections) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// extractOrdinalStructure extracts the ordinal-to-endpoint mapping per AZ
// and returns the set of ordinals common to all AZs.
func extractOrdinalStructure(endpoints []Endpoint) (map[string]map[int]Endpoint, []int, error) {
	if len(endpoints) == 0 {
		return nil, nil, errors.New("no endpoints provided")
	}

	// Group endpoints by AZ and ordinal
	azOrdinalMap := make(map[string]map[int]Endpoint)
	for _, ep := range endpoints {
		ordinal, err := strutil.ExtractPodOrdinal(ep.Address)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to extract ordinal from address %s", ep.Address)
		}
		if _, ok := azOrdinalMap[ep.AZ]; !ok {
			azOrdinalMap[ep.AZ] = make(map[int]Endpoint)
		}
		azOrdinalMap[ep.AZ][ordinal] = ep
	}

	if len(azOrdinalMap) == 0 {
		return nil, nil, errors.New("no AZs found")
	}

	// Find common ordinals across all AZs
	var commonOrdinals []int
	var firstAZ string
	for az := range azOrdinalMap {
		firstAZ = az
		break
	}

	for ordinal := range azOrdinalMap[firstAZ] {
		presentInAll := true
		for az, ordMap := range azOrdinalMap {
			if az == firstAZ {
				continue
			}
			if _, ok := ordMap[ordinal]; !ok {
				presentInAll = false
				break
			}
		}
		if presentInAll {
			commonOrdinals = append(commonOrdinals, ordinal)
		}
	}

	if len(commonOrdinals) == 0 {
		return nil, nil, errors.New("no common ordinals found across all AZs")
	}

	sort.Ints(commonOrdinals)
	return azOrdinalMap, commonOrdinals, nil
}

// buildOrdinalRing creates a consistent hash ring of ordinals.
// Each ordinal gets multiple sections for better distribution.
func buildOrdinalRing(ordinals []int, sectionsPerOrdinal int) ordinalSections {
	ring := make(ordinalSections, 0, len(ordinals)*sectionsPerOrdinal)
	hasher := xxhash.New()

	for _, ordinal := range ordinals {
		for i := 1; i <= sectionsPerOrdinal; i++ {
			hasher.Reset()
			_, _ = hasher.Write([]byte(fmt.Sprintf("ordinal-%d:%d", ordinal, i)))
			ring = append(ring, ordinalSection{
				ordinal: ordinal,
				hash:    hasher.Sum64(),
			})
		}
	}

	sort.Sort(ring)
	return ring
}

// selectOrdinalsConsistent selects ordinals using consistent hashing.
// This provides stability: adding ordinal N only affects tenants that would hash near N.
func selectOrdinalsConsistent(ring ordinalSections, tenant string, count int) []int {
	if count >= len(ring) {
		// Return all unique ordinals if count exceeds ring size
		seen := make(map[int]struct{})
		for _, sec := range ring {
			seen[sec.ordinal] = struct{}{}
		}
		result := make([]int, 0, len(seen))
		for ord := range seen {
			result = append(result, ord)
		}
		sort.Ints(result)
		return result
	}

	seed := ShuffleShardSeed(tenant, "") // No AZ suffix for alignment
	r := rand.New(rand.NewSource(seed))

	selected := make(map[int]struct{})
	result := make([]int, 0, count)

	for len(result) < count {
		pos := r.Uint64()
		idx := sort.Search(len(ring), func(i int) bool {
			return ring[i].hash >= pos
		})
		if idx == len(ring) {
			idx = 0
		}

		// Walk ring to find unselected ordinal
		for j := 0; j < len(ring); j++ {
			checkIdx := (idx + j) % len(ring)
			ord := ring[checkIdx].ordinal
			if _, ok := selected[ord]; !ok {
				selected[ord] = struct{}{}
				result = append(result, ord)
				break
			}
		}
	}

	sort.Ints(result)
	return result
}

// getTenantShardAligned returns a tenant shard with aligned ordinals across all AZs.
// Unlike getTenantShard which selects nodes independently per AZ, this selects
// ordinals first, then takes the same ordinal from each AZ.
func (s *shuffleShardHashring) getTenantShardAligned(tenant string) (Hashring, error) {
	// Extract ordinal structure from all nodes
	azOrdinalMap, commonOrdinals, err := extractOrdinalStructure(s.nodes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract ordinal structure")
	}

	// Determine shard size (number of ordinals to select)
	shardSize := s.getShardSize(tenant)
	if shardSize > len(commonOrdinals) {
		return nil, fmt.Errorf("shard size %d exceeds available common ordinals (%d)", shardSize, len(commonOrdinals))
	}

	// Build ordinal ring for consistent hashing
	// Use fewer sections per ordinal since we have fewer ordinals than nodes
	sectionsPerOrdinal := SectionsPerNode
	ordinalRing := buildOrdinalRing(commonOrdinals, sectionsPerOrdinal)

	// Select ordinals using consistent hashing
	selectedOrdinals := selectOrdinalsConsistent(ordinalRing, tenant, shardSize)

	// Build endpoint list with same ordinals from each AZ
	// Sorted AZ order for deterministic endpoint ordering
	sortedAZs := make([]string, 0, len(azOrdinalMap))
	for az := range azOrdinalMap {
		sortedAZs = append(sortedAZs, az)
	}
	sort.Strings(sortedAZs)

	// Create aligned ketama subring manually to preserve ordinal alignment
	// without requiring sequential ordinals starting from 0
	return newAlignedSubring(azOrdinalMap, sortedAZs, selectedOrdinals, SectionsPerNode, s.replicationFactor)
}

// newAlignedSubring creates a ketama hashring with aligned replicas from a subset of ordinals.
// Unlike newAlignedKetamaHashring, this doesn't require ordinals to be sequential from 0.
// The alignment property: for any section, all replicas have the same ordinal across different AZs.
func newAlignedSubring(
	azOrdinalMap map[string]map[int]Endpoint,
	sortedAZs []string,
	selectedOrdinals []int,
	sectionsPerNode int,
	replicationFactor uint64,
) (*ketamaHashring, error) {
	numAZs := len(sortedAZs)
	numOrdinals := len(selectedOrdinals)

	if uint64(numAZs) != replicationFactor {
		return nil, fmt.Errorf("number of AZs (%d) must equal replication factor (%d)", numAZs, replicationFactor)
	}

	// Build flat endpoint list: [AZ0-ord0, AZ0-ord1, ..., AZ1-ord0, AZ1-ord1, ...]
	// where ordN refers to selectedOrdinals[N], not the actual ordinal value
	totalEndpoints := numAZs * numOrdinals
	flatEndpoints := make([]Endpoint, 0, totalEndpoints)
	for _, az := range sortedAZs {
		for _, ordinal := range selectedOrdinals {
			ep, ok := azOrdinalMap[az][ordinal]
			if !ok {
				return nil, fmt.Errorf("ordinal %d not found in AZ %s", ordinal, az)
			}
			flatEndpoints = append(flatEndpoints, ep)
		}
	}

	// Create sections with aligned replicas
	// For aligned subring, we create sections based on the first AZ's endpoints (primary)
	// Each section's replicas point to the same "position" (ordinal index) in each AZ
	hasher := xxhash.New()
	ringSections := make(sections, 0, numOrdinals*sectionsPerNode)

	for ordinalIdx := 0; ordinalIdx < numOrdinals; ordinalIdx++ {
		// Primary endpoint is from the first AZ
		primaryEndpoint := flatEndpoints[ordinalIdx] // AZ0 endpoints are at indices 0..numOrdinals-1

		for sectionIdx := 1; sectionIdx <= sectionsPerNode; sectionIdx++ {
			hasher.Reset()
			_, _ = hasher.Write([]byte(primaryEndpoint.Address + ":" + strconv.Itoa(sectionIdx)))

			sec := &section{
				hash:          hasher.Sum64(),
				az:            primaryEndpoint.AZ,
				endpointIndex: uint64(ordinalIdx), // Index within first AZ
				replicas:      make([]uint64, 0, replicationFactor),
			}

			// Add replicas: same ordinal index from each AZ
			for azIdx := 0; azIdx < numAZs; azIdx++ {
				replicaFlatIndex := azIdx*numOrdinals + ordinalIdx
				sec.replicas = append(sec.replicas, uint64(replicaFlatIndex))
			}

			ringSections = append(ringSections, sec)
		}
	}

	sort.Sort(ringSections)

	return &ketamaHashring{
		endpoints:    flatEndpoints,
		sections:     ringSections,
		numEndpoints: uint64(totalEndpoints),
	}, nil
}

// Get returns the first endpoint for a tenant and time series, respecting the shuffle sharding.
func (s *shuffleShardHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return s.GetN(tenant, ts, 0)
}

// GetN returns the nth endpoint for a tenant and time series, respecting the shuffle sharding.
func (s *shuffleShardHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	h, err := s.getTenantShardCached(tenant)
	if err != nil {
		return Endpoint{}, err
	}

	return h.GetN(tenant, ts, n)
}

// newMultiHashring creates a multi-tenant hashring for a given slice of
// groups.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
func NewMultiHashring(algorithm HashringAlgorithm, replicationFactor uint64, cfg []HashringConfig, reg prometheus.Registerer) (Hashring, error) {
	m := &multiHashring{
		cache: make(map[string]Hashring),
	}

	for _, h := range cfg {
		var hashring Hashring
		var err error
		activeAlgorithm := algorithm
		if h.Algorithm != "" {
			activeAlgorithm = h.Algorithm
		}
		hashring, err = newHashring(activeAlgorithm, h.Endpoints, replicationFactor, h.Hashring, h.Tenants, h.ShuffleShardingConfig, reg)
		if err != nil {
			return nil, err
		}
		m.nodes = append(m.nodes, hashring.Nodes()...)
		m.hashrings = append(m.hashrings, hashring)
		var t map[string]tenantMatcher
		if len(h.Tenants) != 0 {
			t = make(map[string]tenantMatcher)
		}
		for _, tenant := range h.Tenants {
			t[tenant] = h.TenantMatcherType
		}
		m.tenantSets = append(m.tenantSets, t)
	}
	slices.SortFunc(m.nodes, func(a, b Endpoint) int {
		return strings.Compare(a.Address, b.Address)
	})
	return m, nil
}

func newHashring(algorithm HashringAlgorithm, endpoints []Endpoint, replicationFactor uint64, hashring string, tenants []string, shuffleShardingConfig ShuffleShardingConfig, reg prometheus.Registerer) (Hashring, error) {

	switch algorithm {
	case AlgorithmHashmod:
		ringImpl, err := newSimpleHashring(endpoints)
		if err != nil {
			return nil, err
		}
		if shuffleShardingConfig.ShardSize > 0 {
			return nil, fmt.Errorf("hashmod algorithm does not support shuffle sharding. Either use Ketama or remove shuffle sharding configuration")
		}
		return ringImpl, nil
	case AlgorithmKetama:
		ringImpl, err := newKetamaHashring(endpoints, SectionsPerNode, replicationFactor)
		if err != nil {
			return nil, err
		}
		if shuffleShardingConfig.ShardSize > 0 {
			if shuffleShardingConfig.ShardSize > len(endpoints) {
				return nil, fmt.Errorf("shard size %d is larger than number of nodes in hashring %s (%d)", shuffleShardingConfig.ShardSize, hashring, len(endpoints))
			}
			return newShuffleShardHashring(ringImpl, shuffleShardingConfig, replicationFactor, reg, hashring)
		}
		return ringImpl, nil
	case AlgorithmAlignedKetama:
		ringImpl, err := newAlignedKetamaHashring(endpoints, SectionsPerNode, replicationFactor)
		if err != nil {
			return nil, err
		}
		if shuffleShardingConfig.ShardSize > 0 {
			if shuffleShardingConfig.ShardSize > len(endpoints) {
				return nil, fmt.Errorf("shard size %d is larger than number of nodes in hashring %s (%d)", shuffleShardingConfig.ShardSize, hashring, len(endpoints))
			}
			return newShuffleShardHashring(ringImpl, shuffleShardingConfig, replicationFactor, reg, hashring)
		}
		return ringImpl, nil
	default:
		l := log.NewNopLogger()
		level.Warn(l).Log("msg", "Unrecognizable hashring algorithm. Fall back to hashmod algorithm.",
			"hashring", hashring,
			"tenants", tenants)
		if shuffleShardingConfig.ShardSize > 0 {
			return nil, fmt.Errorf("hashmod algorithm does not support shuffle sharding. Either use Ketama or remove shuffle sharding configuration")
		}
		return newSimpleHashring(endpoints)
	}
}
