// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// makeK8sEndpoint creates an endpoint with K8s-style DNS name.
func makeK8sEndpoint(podName string, shard int, az string) Endpoint {
	return Endpoint{
		Address: fmt.Sprintf("%s-%d.svc.test.svc.cluster.local:10901", podName, shard),
		AZ:      az,
		Shard:   shard,
	}
}

// extractShardFromAddress extracts shard number from K8s-style address.
func extractShardFromAddress(t *testing.T, address string) int {
	t.Helper()
	// Address format: pod-az-N.svc.test.svc.cluster.local:10901
	parts := strings.Split(address, ".")
	require.Greater(t, len(parts), 0)
	podPart := parts[0] // pod-az-N
	lastDash := strings.LastIndex(podPart, "-")
	require.Greater(t, lastDash, 0)
	shardStr := podPart[lastDash+1:]
	var shard int
	_, err := fmt.Sscanf(shardStr, "%d", &shard)
	require.NoError(t, err)
	return shard
}

// extractShardsFromSubring extracts unique shards from a subring.
func extractShardsFromSubring(t *testing.T, subring Hashring) []int {
	t.Helper()
	nodes := subring.Nodes()
	shardSet := make(map[int]struct{})
	for _, node := range nodes {
		shardSet[extractShardFromAddress(t, node.Address)] = struct{}{}
	}
	shards := make([]int, 0, len(shardSet))
	for s := range shardSet {
		shards = append(shards, s)
	}
	sort.Ints(shards)
	return shards
}

func TestRendezvousHashringConstructor(t *testing.T) {
	t.Parallel()

	ep0a := Endpoint{Address: podDNS("pod", 0), AZ: "zone-a", Shard: 0}
	ep1a := Endpoint{Address: podDNS("pod", 1), AZ: "zone-a", Shard: 1}
	ep0b := Endpoint{Address: podDNS("pod", 0), AZ: "zone-b", Shard: 0}
	ep1b := Endpoint{Address: podDNS("pod", 1), AZ: "zone-b", Shard: 1}
	ep0c := Endpoint{Address: podDNS("pod", 0), AZ: "zone-c", Shard: 0}
	ep1c := Endpoint{Address: podDNS("pod", 1), AZ: "zone-c", Shard: 1}
	duplicateEp0a := Endpoint{Address: podDNS("anotherpod", 0), AZ: "zone-a", Shard: 0}

	testCases := map[string]struct {
		endpoints         []Endpoint
		replicationFactor uint64
		expectError       bool
		errorContains     string
	}{
		"valid 2 AZs, RF=2": {
			endpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b},
			replicationFactor: 2,
		},
		"valid 3 AZs, RF=3": {
			endpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b, ep0c, ep1c},
			replicationFactor: 3,
		},
		"error: empty input": {
			endpoints:         []Endpoint{},
			replicationFactor: 1,
			expectError:       true,
			errorContains:     "no endpoints provided",
		},
		"error: RF=0": {
			endpoints:         []Endpoint{ep0a, ep0b},
			replicationFactor: 0,
			expectError:       true,
			errorContains:     "replication factor cannot be zero",
		},
		"error: duplicate shard": {
			endpoints:         []Endpoint{ep0a, ep1a, ep0b, duplicateEp0a},
			replicationFactor: 2,
			expectError:       true,
			errorContains:     "duplicate endpoint",
		},
		"error: missing shard 0": {
			endpoints:         []Endpoint{ep1a, ep1b},
			replicationFactor: 2,
			expectError:       true,
			errorContains:     "missing endpoint with shard 0",
		},
		"error: AZ count != RF (too few AZs)": {
			endpoints:         []Endpoint{ep0a, ep1a},
			replicationFactor: 2,
			expectError:       true,
			errorContains:     "number of AZs (1) must equal replication factor (2)",
		},
		"error: AZ count != RF (too many AZs)": {
			endpoints:         []Endpoint{ep0a, ep1a, ep0b, ep1b, ep0c, ep1c},
			replicationFactor: 2,
			expectError:       true,
			errorContains:     "number of AZs (3) must equal replication factor (2)",
		},
		"unbalanced AZs uses common subset": {
			endpoints:         []Endpoint{ep0a, ep1a, ep0b},
			replicationFactor: 2,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ring, err := newRendezvousHashring(tc.endpoints, tc.replicationFactor)
			if tc.expectError {
				require.Error(t, err)
				require.Nil(t, ring)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, ring)
			}
		})
	}
}

func TestRendezvousHashringGetN(t *testing.T) {
	t.Parallel()

	ep0a := Endpoint{Address: podDNS("pod", 0), AZ: "zone-a", Shard: 0}
	ep1a := Endpoint{Address: podDNS("pod", 1), AZ: "zone-a", Shard: 1}
	ep0b := Endpoint{Address: podDNS("pod", 0), AZ: "zone-b", Shard: 0}
	ep1b := Endpoint{Address: podDNS("pod", 1), AZ: "zone-b", Shard: 1}
	ep0c := Endpoint{Address: podDNS("pod", 0), AZ: "zone-c", Shard: 0}
	ep1c := Endpoint{Address: podDNS("pod", 1), AZ: "zone-c", Shard: 1}

	ts := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{{Name: "test", Value: "replica-routing"}},
	}

	t.Run("2 AZs RF=2, replicas have same shard", func(t *testing.T) {
		ring, err := newRendezvousHashring([]Endpoint{ep1a, ep0b, ep0a, ep1b}, 2)
		require.NoError(t, err)

		r0, err := ring.GetN("tenant1", ts, 0)
		require.NoError(t, err)
		r1, err := ring.GetN("tenant1", ts, 1)
		require.NoError(t, err)

		require.Equal(t, r0.Shard, r1.Shard, "replicas should have same shard")
		require.NotEqual(t, r0.AZ, r1.AZ, "replicas should be in different AZs")
	})

	t.Run("3 AZs RF=3, replicas have same shard", func(t *testing.T) {
		ring, err := newRendezvousHashring([]Endpoint{ep1a, ep0b, ep0a, ep1b, ep0c, ep1c}, 3)
		require.NoError(t, err)

		r0, err := ring.GetN("tenant1", ts, 0)
		require.NoError(t, err)
		r1, err := ring.GetN("tenant1", ts, 1)
		require.NoError(t, err)
		r2, err := ring.GetN("tenant1", ts, 2)
		require.NoError(t, err)

		require.Equal(t, r0.Shard, r1.Shard)
		require.Equal(t, r1.Shard, r2.Shard)

		azs := map[string]struct{}{r0.AZ: {}, r1.AZ: {}, r2.AZ: {}}
		require.Len(t, azs, 3, "replicas should span all 3 AZs")
	})

	t.Run("error: n >= replicationFactor", func(t *testing.T) {
		ring, err := newRendezvousHashring([]Endpoint{ep1a, ep0b, ep0a, ep1b}, 2)
		require.NoError(t, err)

		_, err = ring.GetN("tenant1", ts, 2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "insufficient nodes")
	})
}

func TestRendezvousHashringSameShardAcrossAZs(t *testing.T) {
	t.Parallel()

	// Create 3 AZs with 20 shards each.
	var endpoints []Endpoint
	for i := 0; i < 20; i++ {
		endpoints = append(endpoints, Endpoint{Address: podDNS("pod", i), AZ: "zone-a", Shard: i})
	}
	for i := 0; i < 20; i++ {
		endpoints = append(endpoints, Endpoint{Address: podDNS("pod", i), AZ: "zone-b", Shard: i})
	}
	for i := 0; i < 20; i++ {
		endpoints = append(endpoints, Endpoint{Address: podDNS("pod", i), AZ: "zone-c", Shard: i})
	}

	ring, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	// For many series, verify all replicas go to the same shard.
	for i := 0; i < 500; i++ {
		ts := &prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: "test_metric"},
				{Name: "series", Value: fmt.Sprintf("series-%d", i)},
			},
		}

		r0, err := ring.GetN("tenant1", ts, 0)
		require.NoError(t, err)
		r1, err := ring.GetN("tenant1", ts, 1)
		require.NoError(t, err)
		r2, err := ring.GetN("tenant1", ts, 2)
		require.NoError(t, err)

		require.Equal(t, r0.Shard, r1.Shard, "series %d: shard mismatch between AZ0 and AZ1", i)
		require.Equal(t, r1.Shard, r2.Shard, "series %d: shard mismatch between AZ1 and AZ2", i)

		azs := map[string]struct{}{r0.AZ: {}, r1.AZ: {}, r2.AZ: {}}
		require.Len(t, azs, 3, "series %d: replicas should span all 3 AZs", i)
	}
}

func TestRendezvousHashringDistribution(t *testing.T) {
	t.Parallel()

	numShards := 10
	var endpoints []Endpoint
	for i := 0; i < numShards; i++ {
		endpoints = append(endpoints, Endpoint{Address: podDNS("pod", i), AZ: "zone-a", Shard: i})
		endpoints = append(endpoints, Endpoint{Address: podDNS("pod", i), AZ: "zone-b", Shard: i})
		endpoints = append(endpoints, Endpoint{Address: podDNS("pod", i), AZ: "zone-c", Shard: i})
	}

	ring, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	shardCounts := make(map[int]int)
	numSeries := 10000
	for i := 0; i < numSeries; i++ {
		ts := &prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: "test_metric"},
				{Name: "instance", Value: fmt.Sprintf("instance-%d", i)},
			},
		}

		ep, err := ring.Get("tenant1", ts)
		require.NoError(t, err)
		shardCounts[ep.Shard]++
	}

	// Expect all shards to receive some series.
	require.Len(t, shardCounts, numShards, "all shards should receive some series")

	// Check uniformity: each shard should get roughly numSeries/numShards.
	expected := float64(numSeries) / float64(numShards)
	for shard, count := range shardCounts {
		deviation := math.Abs(float64(count)-expected) / expected
		require.Less(t, deviation, 0.30,
			"shard %d has %d series (expected ~%.0f), deviation %.1f%%",
			shard, count, expected, deviation*100)
	}
}

func TestRendezvousShuffleShardingBasic(t *testing.T) {
	t.Parallel()

	// Create 3 AZs with 5 shards each.
	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize: 6,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-rendezvous")
	require.NoError(t, err)

	tenant := "test-tenant"
	shard, err := shardRing.getTenantShardRendezvous(tenant)
	require.NoError(t, err)

	// Verify we got the right number of nodes (6 total / 3 AZs = 2 shards per AZ → 6 endpoints).
	nodes := shard.Nodes()
	require.Len(t, nodes, 6, "expected 6 endpoints (2 shards * 3 AZs)")

	// Extract shards from each AZ and verify they're the same.
	shardsByAZ := make(map[string][]int)
	for _, node := range nodes {
		shardsByAZ[node.AZ] = append(shardsByAZ[node.AZ], extractShardFromAddress(t, node.Address))
	}

	require.Len(t, shardsByAZ, 3, "expected 3 AZs")
	for az, shards := range shardsByAZ {
		require.Len(t, shards, 2, "AZ %s should have 2 shards", az)
	}

	// All AZs should have the same shards.
	var referenceShards []int
	for _, shards := range shardsByAZ {
		if referenceShards == nil {
			referenceShards = shards
		} else {
			require.ElementsMatch(t, referenceShards, shards,
				"all AZs should have the same shards")
		}
	}
}

func TestRendezvousShuffleShardingConsistency(t *testing.T) {
	t.Parallel()

	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize: 6,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-consistency")
	require.NoError(t, err)

	tenant := "consistent-tenant"
	var firstShards []int

	for trial := 0; trial < 10; trial++ {
		shard, err := shardRing.getTenantShardRendezvous(tenant)
		require.NoError(t, err)
		currentShards := extractShardsFromSubring(t, shard)
		if firstShards == nil {
			firstShards = currentShards
		} else {
			require.Equal(t, firstShards, currentShards,
				"same tenant should always get same shards")
		}
	}
}

func TestRendezvousShuffleShardingDifferentTenants(t *testing.T) {
	t.Parallel()

	endpoints := make([]Endpoint, 0, 30)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 10; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize: 9,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-diff-tenants")
	require.NoError(t, err)

	tenantShards := make(map[string][]int)
	numTenants := 20

	for i := 0; i < numTenants; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		shard, err := shardRing.getTenantShardRendezvous(tenant)
		require.NoError(t, err)
		tenantShards[tenant] = extractShardsFromSubring(t, shard)
	}

	uniqueSets := make(map[string]int)
	for _, shards := range tenantShards {
		key := fmt.Sprintf("%v", shards)
		uniqueSets[key]++
	}

	require.Greater(t, len(uniqueSets), 1, "different tenants should get different shard sets")
}

func TestRendezvousShuffleShardingPreservesAlignment(t *testing.T) {
	t.Parallel()

	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize: 6,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-preserves")
	require.NoError(t, err)

	tenant := "alignment-test-tenant"

	for i := 0; i < 100; i++ {
		ts := &prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "series", Value: fmt.Sprintf("series-%d", i)},
			},
		}

		var replicas []Endpoint
		for n := uint64(0); n < 3; n++ {
			ep, err := shardRing.GetN(tenant, ts, n)
			require.NoError(t, err)
			replicas = append(replicas, ep)
		}

		// All replicas should have the same shard but different AZs.
		shardsSeen := make(map[int]struct{})
		azsSeen := make(map[string]struct{})
		for _, ep := range replicas {
			shardsSeen[extractShardFromAddress(t, ep.Address)] = struct{}{}
			azsSeen[ep.AZ] = struct{}{}
		}

		require.Len(t, shardsSeen, 1, "all replicas should have the same shard for series %d", i)
		require.Len(t, azsSeen, 3, "replicas should span all 3 AZs for series %d", i)
	}
}

func TestRendezvousShuffleShardingDataDistribution(t *testing.T) {
	t.Parallel()

	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize: 6,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-distribution")
	require.NoError(t, err)

	tenant := "distribution-test-tenant"

	shard, err := shardRing.getTenantShardRendezvous(tenant)
	require.NoError(t, err)
	selectedShards := extractShardsFromSubring(t, shard)
	require.Len(t, selectedShards, 2, "tenant should have exactly 2 shards")

	seriesByShard := make(map[int]map[int]struct{})
	for _, ord := range selectedShards {
		seriesByShard[ord] = make(map[int]struct{})
	}

	seriesByEndpoint := make(map[string]map[int]struct{})

	numSeries := 1000
	for i := 0; i < numSeries; i++ {
		ts := &prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "series", Value: fmt.Sprintf("series-%d", i)},
				{Name: "__name__", Value: "test_metric"},
			},
		}

		var replicas []Endpoint
		for n := uint64(0); n < 3; n++ {
			ep, err := shardRing.GetN(tenant, ts, n)
			require.NoError(t, err)
			replicas = append(replicas, ep)

			if seriesByEndpoint[ep.Address] == nil {
				seriesByEndpoint[ep.Address] = make(map[int]struct{})
			}
			seriesByEndpoint[ep.Address][i] = struct{}{}
		}

		// All replicas should have the same shard.
		primaryShard := extractShardFromAddress(t, replicas[0].Address)
		for _, ep := range replicas[1:] {
			epShard := extractShardFromAddress(t, ep.Address)
			require.Equal(t, primaryShard, epShard, "all replicas for series %d should have same shard", i)
		}

		seriesByShard[primaryShard][i] = struct{}{}
	}

	// Both shards should receive series.
	for ord, series := range seriesByShard {
		require.Greater(t, len(series), 0, "shard %d should receive some series", ord)
	}

	// Same shard across different AZs should receive the same series.
	endpointsByShard := make(map[int][]string)
	for addr := range seriesByEndpoint {
		ord := extractShardFromAddress(t, addr)
		endpointsByShard[ord] = append(endpointsByShard[ord], addr)
	}

	for ord, addrs := range endpointsByShard {
		if len(addrs) < 2 {
			continue
		}
		referenceSeries := seriesByEndpoint[addrs[0]]
		for _, addr := range addrs[1:] {
			otherSeries := seriesByEndpoint[addr]
			require.Equal(t, len(referenceSeries), len(otherSeries),
				"endpoints with shard %d should have same number of series", ord)
			for seriesIdx := range referenceSeries {
				_, ok := otherSeries[seriesIdx]
				require.True(t, ok, "series %d should be on all endpoints with shard %d", seriesIdx, ord)
			}
		}
	}

	// Different shards should receive different series.
	shardList := make([]int, 0, len(seriesByShard))
	for ord := range seriesByShard {
		shardList = append(shardList, ord)
	}
	if len(shardList) >= 2 {
		series1 := seriesByShard[shardList[0]]
		series2 := seriesByShard[shardList[1]]
		for seriesIdx := range series1 {
			_, overlap := series2[seriesIdx]
			require.False(t, overlap,
				"series %d should not be on both shard %d and shard %d",
				seriesIdx, shardList[0], shardList[1])
		}
	}
}

func TestRendezvousShuffleShardingValidation(t *testing.T) {
	t.Parallel()

	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newRendezvousHashring(endpoints, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize: 30, // 30 / 3 AZs = 10 per-AZ, but only 5 shards available
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-validation")
	require.NoError(t, err)

	_, err = shardRing.getTenantShardRendezvous("test-tenant")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds available common shards")
}
