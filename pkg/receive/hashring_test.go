// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestHashringGet(t *testing.T) {
	t.Parallel()

	ts := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{
			{
				Name:  "foo",
				Value: "bar",
			},
			{
				Name:  "baz",
				Value: "qux",
			},
		},
	}

	for _, tc := range []struct {
		name   string
		cfg    []HashringConfig
		nodes  map[string]struct{}
		tenant string
	}{
		{
			name:   "empty",
			cfg:    nil,
			tenant: "tenant1",
		},
		{
			name: "simple",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
				},
			},
			nodes: map[string]struct{}{"node1": {}},
		},
		{
			name: "specific",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []Endpoint{{Address: "node1"}},
				},
			},
			nodes:  map[string]struct{}{"node2": {}},
			tenant: "tenant2",
		},
		{
			name: "many tenants",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []Endpoint{{Address: "node3"}},
					Tenants:   []string{"tenant3"},
				},
			},
			nodes:  map[string]struct{}{"node1": {}},
			tenant: "tenant1",
		},
		{
			name: "many tenants error",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []Endpoint{{Address: "node3"}},
					Tenants:   []string{"tenant3"},
				},
			},
			tenant: "tenant4",
		},
		{
			name: "many nodes",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node1": {},
				"node2": {},
				"node3": {},
			},
			tenant: "tenant1",
		},
		{
			name: "many nodes default",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node4": {},
				"node5": {},
				"node6": {},
			},
		},
		{
			name: "glob hashring match",
			cfg: []HashringConfig{
				{
					Endpoints:         []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:           []string{"prefix*"},
					TenantMatcherType: TenantMatcherGlob,
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node1": {},
				"node2": {},
				"node3": {},
			},
			tenant: "prefix-1",
		},
		{
			name: "glob hashring not match",
			cfg: []HashringConfig{
				{
					Endpoints:         []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:           []string{"prefix*"},
					TenantMatcherType: TenantMatcherGlob,
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node4": {},
				"node5": {},
				"node6": {},
			},
			tenant: "suffix-1",
		},
		{
			name: "glob hashring multiple matches",
			cfg: []HashringConfig{
				{
					Endpoints:         []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:           []string{"t1-*", "t2", "t3-*"},
					TenantMatcherType: TenantMatcherGlob,
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node1": {},
				"node2": {},
				"node3": {},
			},
			tenant: "t2",
		},
	} {
		hs, err := NewMultiHashring(AlgorithmHashmod, 3, tc.cfg, prometheus.NewRegistry())
		require.NoError(t, err)

		h, err := hs.Get(tc.tenant, ts)
		if tc.nodes != nil {
			if err != nil {
				t.Errorf("case %q: got unexpected error: %v", tc.name, err)
				continue
			}
			if _, ok := tc.nodes[h.Address]; !ok {
				t.Errorf("case %q: got unexpected node %q", tc.name, h)
			}
			continue
		}
		if err == nil {
			t.Errorf("case %q: expected error", tc.name)
		}
	}
}

func TestKetamaHashringGet(t *testing.T) {
	t.Parallel()

	baseTS := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{
			{
				Name:  "pod",
				Value: "nginx",
			},
		},
	}
	tests := []struct {
		name         string
		endpoints    []Endpoint
		expectedNode string
		ts           *prompb.TimeSeries
		n            uint64
	}{
		{
			name:         "base case",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:         "base case with replication",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			n:            1,
			expectedNode: "node-1",
		},
		{
			name:         "base case with replication",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			n:            2,
			expectedNode: "node-3",
		},
		{
			name:         "base case with replication and reordered nodes",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-3"}, {Address: "node-2"}},
			ts:           baseTS,
			n:            2,
			expectedNode: "node-3",
		},
		{
			name:         "base case with new node at beginning of ring",
			endpoints:    []Endpoint{{Address: "node-0"}, {Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:         "base case with new node at end of ring",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}, {Address: "node-4"}},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:      "base case with different timeseries",
			endpoints: []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts: &prompb.TimeSeries{
				Labels: []labelpb.ZLabel{
					{
						Name:  "pod",
						Value: "thanos",
					},
				},
			},
			expectedNode: "node-3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hashRing, err := newKetamaHashring(test.endpoints, 10, test.n+1)
			require.NoError(t, err)

			result, err := hashRing.GetN("tenant", test.ts, test.n)
			require.NoError(t, err)
			require.Equal(t, test.expectedNode, result.Address)
		})
	}
}

func TestKetamaHashringBadConfigIsRejected(t *testing.T) {
	t.Parallel()

	_, err := newKetamaHashring([]Endpoint{{Address: "node-1"}}, 1, 2)
	require.Error(t, err)
}

func TestKetamaHashringConsistency(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	ringA := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	a1, err := assignSeries(series, ringA)
	require.NoError(t, err)

	ringB := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	a2, err := assignSeries(series, ringB)
	require.NoError(t, err)

	for node, ts := range a1 {
		require.Len(t, a2[node], len(ts), "node %s has an inconsistent number of series", node)
	}

	for node, ts := range a2 {
		require.Len(t, a1[node], len(ts), "node %s has an inconsistent number of series", node)
	}
}

func TestKetamaHashringIncreaseAtEnd(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	initialRing := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	initialAssignments, err := assignSeries(series, initialRing)
	require.NoError(t, err)

	resizedRing := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}, {Address: "node-4"}, {Address: "node-5"}}
	reassignments, err := assignSeries(series, resizedRing)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node.Address] {
			foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringIncreaseInMiddle(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	initialRing := []Endpoint{{Address: "node-1"}, {Address: "node-3"}}
	initialAssignments, err := assignSeries(series, initialRing)
	require.NoError(t, err)

	resizedRing := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	reassignments, err := assignSeries(series, resizedRing)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node.Address] {
			foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringReplicationConsistency(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	initialRing := []Endpoint{{Address: "node-1"}, {Address: "node-4"}, {Address: "node-5"}}
	initialAssignments, err := assignReplicatedSeries(series, initialRing, 2)
	require.NoError(t, err)

	resizedRing := []Endpoint{{Address: "node-4"}, {Address: "node-3"}, {Address: "node-1"}, {Address: "node-2"}, {Address: "node-5"}}
	reassignments, err := assignReplicatedSeries(series, resizedRing, 2)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node.Address] {
			foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringReplicationConsistencyWithAZs(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		initialRing []Endpoint
		resizedRing []Endpoint
		replicas    uint64
	}{
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "a", AZ: "1"}, {Address: "d", AZ: "2"}, {Address: "e", AZ: "4"}},
			replicas:    3,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "d", AZ: "1"}, {Address: "e", AZ: "2"}, {Address: "f", AZ: "3"}},
			replicas:    3,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "d", AZ: "4"}, {Address: "e", AZ: "5"}, {Address: "f", AZ: "6"}},
			replicas:    3,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "d", AZ: "4"}, {Address: "e", AZ: "5"}, {Address: "f", AZ: "6"}},
			replicas:    2,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "c", AZ: "2"}, {Address: "f", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "1"}, {Address: "c", AZ: "2"}, {Address: "d", AZ: "2"}, {Address: "f", AZ: "3"}},
			replicas:    2,
		},
	} {
		t.Run("", func(t *testing.T) {
			series := makeSeries()

			initialAssignments, err := assignReplicatedSeries(series, tt.initialRing, tt.replicas)
			require.NoError(t, err)

			reassignments, err := assignReplicatedSeries(series, tt.resizedRing, tt.replicas)
			require.NoError(t, err)

			// Assert that the initial nodes have no new keys after increasing the ring size
			for _, node := range tt.initialRing {
				for _, ts := range reassignments[node.Address] {
					foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
					require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
				}
			}
		})
	}
}

func TestKetamaHashringEvenAZSpread(t *testing.T) {
	t.Parallel()

	tenant := "default-tenant"
	ts := &prompb.TimeSeries{
		Labels:  labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", "bar")),
		Samples: []prompb.Sample{{Value: 1, Timestamp: 0}},
	}

	for _, tt := range []struct {
		nodes    []Endpoint
		replicas uint64
	}{
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "1"},
				{Address: "d", AZ: "2"},
			},
			replicas: 1,
		},
		{
			nodes:    []Endpoint{{Address: "a"}, {Address: "b"}, {Address: "c"}, {Address: "d"}},
			replicas: 1,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "1"},
				{Address: "d", AZ: "2"},
			},
			replicas: 2,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "1"},
				{Address: "e", AZ: "2"},
				{Address: "f", AZ: "3"},
			},
			replicas: 3,
		},
		{
			nodes:    []Endpoint{{Address: "a"}, {Address: "b"}, {Address: "c"}, {Address: "d"}, {Address: "e"}, {Address: "f"}, {Address: "g"}},
			replicas: 3,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "1"},
				{Address: "e", AZ: "2"},
				{Address: "f", AZ: "3"},
				{Address: "g", AZ: "4"},
				{Address: "h", AZ: "4"},
				{Address: "i", AZ: "4"},
				{Address: "j", AZ: "5"},
				{Address: "k", AZ: "5"},
				{Address: "l", AZ: "5"},
			},
			replicas: 10,
		},
	} {
		t.Run("", func(t *testing.T) {
			hashRing, err := newKetamaHashring(tt.nodes, SectionsPerNode, tt.replicas)
			testutil.Ok(t, err)

			availableAzs := make(map[string]int64)
			for _, endpoint := range tt.nodes {
				availableAzs[endpoint.AZ] = 0
			}

			azSpread := make(map[string]int64)
			for i := 0; i < int(tt.replicas); i++ {
				r, err := hashRing.GetN(tenant, ts, uint64(i))
				testutil.Ok(t, err)

				for _, n := range tt.nodes {
					if !strings.HasPrefix(n.Address, r.Address) {
						continue
					}
					azSpread[n.AZ]++
				}

			}

			expectedAzSpreadLength := int(tt.replicas)
			if int(tt.replicas) > len(availableAzs) {
				expectedAzSpreadLength = len(availableAzs)
			}
			testutil.Equals(t, len(azSpread), expectedAzSpreadLength)

			for _, writeToAz := range azSpread {
				minAz := sizeOfLeastOccupiedAZ(azSpread)
				testutil.Assert(t, math.Abs(float64(writeToAz-minAz)) <= 1.0)
			}
		})
	}
}

func TestKetamaHashringEvenNodeSpread(t *testing.T) {
	t.Parallel()

	tenant := "default-tenant"

	for _, tt := range []struct {
		nodes     []Endpoint
		replicas  uint64
		numSeries uint64
	}{
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "1"},
				{Address: "d", AZ: "2"},
			},
			replicas:  2,
			numSeries: 1000,
		},
		{
			nodes:     []Endpoint{{Address: "a"}, {Address: "b"}, {Address: "c"}, {Address: "d"}},
			replicas:  2,
			numSeries: 1000,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "2"},
				{Address: "e", AZ: "1"},
				{Address: "f", AZ: "3"},
			},
			replicas:  3,
			numSeries: 10000,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "2"},
				{Address: "e", AZ: "1"},
				{Address: "f", AZ: "3"},
				{Address: "g", AZ: "1"},
				{Address: "h", AZ: "2"},
				{Address: "i", AZ: "3"},
			},
			replicas:  2,
			numSeries: 10000,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "2"},
				{Address: "e", AZ: "1"},
				{Address: "f", AZ: "3"},
				{Address: "g", AZ: "1"},
				{Address: "h", AZ: "2"},
				{Address: "i", AZ: "3"},
			},
			replicas:  9,
			numSeries: 10000,
		},
	} {
		t.Run("", func(t *testing.T) {
			hashRing, err := newKetamaHashring(tt.nodes, SectionsPerNode, tt.replicas)
			testutil.Ok(t, err)
			optimalSpread := int(tt.numSeries*tt.replicas) / len(tt.nodes)
			nodeSpread := make(map[string]int)
			for i := 0; i < int(tt.numSeries); i++ {
				ts := &prompb.TimeSeries{
					Labels:  labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", fmt.Sprintf("%d", i))),
					Samples: []prompb.Sample{{Value: 1, Timestamp: 0}},
				}
				for j := 0; j < int(tt.replicas); j++ {
					r, err := hashRing.GetN(tenant, ts, uint64(j))
					testutil.Ok(t, err)

					nodeSpread[r.Address]++
				}
			}
			for _, node := range nodeSpread {
				diff := math.Abs(float64(node) - float64(optimalSpread))
				testutil.Assert(t, diff/float64(optimalSpread) < 0.1)
			}
		})
	}
}

func TestInvalidAZHashringCfg(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		cfg           []HashringConfig
		replicas      uint64
		algorithm     HashringAlgorithm
		expectedError string
	}{
		{
			cfg:           []HashringConfig{{Endpoints: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}}}},
			replicas:      2,
			algorithm:     AlgorithmHashmod,
			expectedError: "Hashmod algorithm does not support AZ aware hashring configuration. Either use Ketama or remove AZ configuration.",
		},
	} {
		t.Run("", func(t *testing.T) {
			_, err := NewMultiHashring(tt.algorithm, tt.replicas, tt.cfg, prometheus.NewRegistry())
			require.EqualError(t, err, tt.expectedError)
		})
	}
}

func TestShuffleShardHashring(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		endpoints       []Endpoint
		tenant          string
		shuffleShardCfg ShuffleShardingConfig
		err             string
		usedNodes       int
		nodeAddrs       map[string]struct{}
	}{
		{
			usedNodes: 3,
			name:      "ketama with shuffle sharding",
			endpoints: []Endpoint{
				{Address: "node-1", AZ: "az-1"},
				{Address: "node-2", AZ: "az-1"},
				{Address: "node-3", AZ: "az-2"},
				{Address: "node-4", AZ: "az-2"},
				{Address: "node-5", AZ: "az-3"},
				{Address: "node-6", AZ: "az-3"},
			},
			tenant: "tenant-1",
			shuffleShardCfg: ShuffleShardingConfig{
				ShardSize: 2,
				Overrides: []ShuffleShardingOverrideConfig{
					{
						Tenants:   []string{"special-tenant"},
						ShardSize: 2,
					},
				},
			},
		},
		{
			usedNodes: 3,
			name:      "ketama with glob tenant override",
			endpoints: []Endpoint{
				{Address: "node-1", AZ: "az-1"},
				{Address: "node-2", AZ: "az-1"},
				{Address: "node-3", AZ: "az-2"},
				{Address: "node-4", AZ: "az-2"},
				{Address: "node-5", AZ: "az-3"},
				{Address: "node-6", AZ: "az-3"},
			},
			tenant: "prefix-tenant",
			shuffleShardCfg: ShuffleShardingConfig{
				ShardSize: 2,
				Overrides: []ShuffleShardingOverrideConfig{
					{
						Tenants:           []string{"prefix*"},
						ShardSize:         3,
						TenantMatcherType: TenantMatcherGlob,
					},
				},
			},
		},
		{
			name: "big shard size",
			endpoints: []Endpoint{
				{Address: "node-1", AZ: "az-1"},
				{Address: "node-2", AZ: "az-1"},
				{Address: "node-3", AZ: "az-2"},
				{Address: "node-4", AZ: "az-2"},
				{Address: "node-5", AZ: "az-3"},
				{Address: "node-6", AZ: "az-3"},
			},
			tenant: "prefix-tenant",
			err:    `shard size 20 is larger than number of nodes in AZ`,
			shuffleShardCfg: ShuffleShardingConfig{
				ShardSize: 2,
				Overrides: []ShuffleShardingOverrideConfig{
					{
						Tenants:           []string{"prefix*"},
						ShardSize:         20,
						TenantMatcherType: TenantMatcherGlob,
					},
				},
			},
		},
		{
			name: "zone awareness disabled",
			endpoints: []Endpoint{
				{Address: "node-1", AZ: "az-1"},
				{Address: "node-2", AZ: "az-1"},
				{Address: "node-3", AZ: "az-2"},
				{Address: "node-4", AZ: "az-2"},
				{Address: "node-5", AZ: "az-2"},
				{Address: "node-6", AZ: "az-2"},
				{Address: "node-7", AZ: "az-3"},
				{Address: "node-8", AZ: "az-3"},
			},
			tenant:    "prefix-tenant",
			usedNodes: 3,
			// Note: We don't check specific nodeAddrs here because the consistent
			// hashing algorithm may select different nodes than the old Fisher-Yates
			// shuffle. What matters is: (1) exactly 3 nodes are used, and (2) the
			// selection is stable when scaling (tested in TestShuffleShardHashringStability).
			shuffleShardCfg: ShuffleShardingConfig{
				ShardSize:             1,
				ZoneAwarenessDisabled: true,
				Overrides: []ShuffleShardingOverrideConfig{
					{
						Tenants:           []string{"prefix*"},
						ShardSize:         3,
						TenantMatcherType: TenantMatcherGlob,
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var baseRing Hashring
			var err error

			baseRing, err = newKetamaHashring(tc.endpoints, SectionsPerNode, 2)
			require.NoError(t, err)

			// Create the shuffle shard hashring
			shardRing, err := newShuffleShardHashring(baseRing, tc.shuffleShardCfg, 2, prometheus.NewRegistry(), "test")
			require.NoError(t, err)

			// Test that the shuffle sharding is consistent
			usedNodes := make(map[string]struct{})

			// We'll sample multiple times to ensure consistency
			for i := 0; i < 100; i++ {
				ts := &prompb.TimeSeries{
					Labels: []labelpb.ZLabel{
						{
							Name:  "iteration",
							Value: fmt.Sprintf("%d", i),
						},
					},
				}

				h, err := shardRing.GetN(tc.tenant, ts, 0)
				if tc.err != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.err)
					return
				}
				require.NoError(t, err)
				usedNodes[h.Address] = struct{}{}
			}

			require.Len(t, usedNodes, tc.usedNodes)
			if tc.nodeAddrs != nil {
				require.Len(t, usedNodes, len(tc.nodeAddrs))
				require.Equal(t, tc.nodeAddrs, usedNodes)
			}

			// Test consistency - same tenant should always get same nodes.
			for trial := 0; trial < 50; trial++ {
				trialNodes := make(map[string]struct{})

				for i := 0; i < 10+trial; i++ {
					ts := &prompb.TimeSeries{
						Labels: []labelpb.ZLabel{
							{
								Name:  "iteration",
								Value: fmt.Sprintf("%d", i),
							},
							{
								Name:  "trial",
								Value: fmt.Sprintf("%d", trial),
							},
						},
					}

					h, err := shardRing.GetN(tc.tenant, ts, 0)
					require.NoError(t, err)
					trialNodes[h.Address] = struct{}{}
				}

				// Same tenant should get same set of nodes in every trial
				require.Equal(t, usedNodes, trialNodes, "Inconsistent node sharding between trials")
			}
		})
	}
}

func makeSeries() []prompb.TimeSeries {
	numSeries := 10000
	series := make([]prompb.TimeSeries, numSeries)
	for i := 0; i < numSeries; i++ {
		series[i] = prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{
					Name:  "pod",
					Value: fmt.Sprintf("nginx-%d", i),
				},
			},
		}
	}
	return series
}

func findSeries(initialAssignments map[string][]prompb.TimeSeries, node string, newSeries prompb.TimeSeries) bool {
	for _, oldSeries := range initialAssignments[node] {
		l1 := labelpb.ZLabelsToPromLabels(newSeries.Labels)
		l2 := labelpb.ZLabelsToPromLabels(oldSeries.Labels)
		if labels.Equal(l1, l2) {
			return true
		}
	}

	return false
}

func assignSeries(series []prompb.TimeSeries, nodes []Endpoint) (map[string][]prompb.TimeSeries, error) {
	return assignReplicatedSeries(series, nodes, 0)
}

func assignReplicatedSeries(series []prompb.TimeSeries, nodes []Endpoint, replicas uint64) (map[string][]prompb.TimeSeries, error) {
	hashRing, err := newKetamaHashring(nodes, SectionsPerNode, replicas)
	if err != nil {
		return nil, err
	}
	assignments := make(map[string][]prompb.TimeSeries)
	for i := uint64(0); i < replicas; i++ {
		for _, ts := range series {
			result, err := hashRing.GetN("tenant", &ts, i)
			if err != nil {
				return nil, err
			}
			assignments[result.Address] = append(assignments[result.Address], ts)

		}
	}

	return assignments, nil
}

// TestShuffleShardHashringStability tests that shuffle sharding is stable when
// adding/removing nodes. When scaling from N to N+1 nodes, at most 1 node should
// change in a tenant's shard (the "consistency" property).
func TestShuffleShardHashringStability(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		initialNodes   int
		scaledNodes    int
		shardSize      int
		numTenants     int
		maxAllowedDiff int // max number of nodes that can change per tenant
	}{
		{
			name:           "scale up 10 to 11, shard size 5",
			initialNodes:   10,
			scaledNodes:    11,
			shardSize:      5,
			numTenants:     100,
			maxAllowedDiff: 1, // ideally at most 1 node should change
		},
		{
			name:           "scale up 20 to 21, shard size 5",
			initialNodes:   20,
			scaledNodes:    21,
			shardSize:      5,
			numTenants:     100,
			maxAllowedDiff: 1,
		},
		{
			name:           "scale down 11 to 10, shard size 5",
			initialNodes:   11,
			scaledNodes:    10,
			shardSize:      5,
			numTenants:     100,
			maxAllowedDiff: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Create initial endpoints
			initialEndpoints := make([]Endpoint, tc.initialNodes)
			for i := 0; i < tc.initialNodes; i++ {
				initialEndpoints[i] = Endpoint{Address: fmt.Sprintf("node-%d", i)}
			}

			// Create scaled endpoints
			scaledEndpoints := make([]Endpoint, tc.scaledNodes)
			for i := 0; i < tc.scaledNodes; i++ {
				scaledEndpoints[i] = Endpoint{Address: fmt.Sprintf("node-%d", i)}
			}

			shuffleShardCfg := ShuffleShardingConfig{
				ShardSize:             tc.shardSize,
				ZoneAwarenessDisabled: true,
			}

			// Create initial hashring
			initialBaseRing, err := newKetamaHashring(initialEndpoints, SectionsPerNode, 1)
			require.NoError(t, err)
			initialShardRing, err := newShuffleShardHashring(initialBaseRing, shuffleShardCfg, 1, prometheus.NewRegistry(), "test-initial")
			require.NoError(t, err)

			// Create scaled hashring
			scaledBaseRing, err := newKetamaHashring(scaledEndpoints, SectionsPerNode, 1)
			require.NoError(t, err)
			scaledShardRing, err := newShuffleShardHashring(scaledBaseRing, shuffleShardCfg, 1, prometheus.NewRegistry(), "test-scaled")
			require.NoError(t, err)

			totalDiffs := 0
			tenantsWithMoreThanOneDiff := 0

			for tenantID := 0; tenantID < tc.numTenants; tenantID++ {
				tenant := fmt.Sprintf("tenant-%d", tenantID)

				// Get nodes used by this tenant in initial ring
				initialNodes := getTenantNodes(t, initialShardRing, tenant, tc.shardSize)

				// Get nodes used by this tenant in scaled ring
				scaledNodes := getTenantNodes(t, scaledShardRing, tenant, tc.shardSize)

				// Count differences
				added, removed := compareNodeSets(initialNodes, scaledNodes)
				diff := max(len(added), len(removed))
				totalDiffs += diff

				if diff > tc.maxAllowedDiff {
					tenantsWithMoreThanOneDiff++
				}
			}

			avgDiff := float64(totalDiffs) / float64(tc.numTenants)
			t.Logf("Average nodes changed per tenant: %.2f", avgDiff)
			t.Logf("Tenants with more than %d node change: %d/%d", tc.maxAllowedDiff, tenantsWithMoreThanOneDiff, tc.numTenants)

			// The stability requirement: when adding/removing 1 node,
			// at most 1 node should change in each tenant's shard.
			// If this fails, the shuffle sharding is unstable.
			require.Zero(t, tenantsWithMoreThanOneDiff,
				"Shuffle sharding is unstable: %d tenants had more than %d node change when scaling from %d to %d nodes",
				tenantsWithMoreThanOneDiff, tc.maxAllowedDiff, tc.initialNodes, tc.scaledNodes)
		})
	}
}

// getTenantNodes returns the set of nodes used by a tenant's shard.
func getTenantNodes(t *testing.T, ring *shuffleShardHashring, tenant string, shardSize int) map[string]struct{} {
	nodes := make(map[string]struct{})

	// Sample many time series to discover all nodes in the shard
	for i := 0; i < 1000; i++ {
		ts := &prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "series", Value: fmt.Sprintf("%d", i)},
			},
		}
		endpoint, err := ring.GetN(tenant, ts, 0)
		require.NoError(t, err)
		nodes[endpoint.Address] = struct{}{}
	}

	require.Len(t, nodes, shardSize, "tenant %s should use exactly %d nodes", tenant, shardSize)
	return nodes
}

// compareNodeSets returns the nodes added and removed between two sets.
func compareNodeSets(before, after map[string]struct{}) (added, removed []string) {
	for node := range after {
		if _, ok := before[node]; !ok {
			added = append(added, node)
		}
	}
	for node := range before {
		if _, ok := after[node]; !ok {
			removed = append(removed, node)
		}
	}
	return
}

// makeK8sEndpoint creates an endpoint with K8s-style DNS name that has an extractable ordinal.
func makeK8sEndpoint(podName string, ordinal int, az string) Endpoint {
	return Endpoint{
		Address: fmt.Sprintf("%s-%d.svc.test.svc.cluster.local:10901", podName, ordinal),
		AZ:      az,
	}
}

func TestAlignedOrdinalShardingBasic(t *testing.T) {
	t.Parallel()

	// Create 3 AZs with 5 ordinals each (15 total endpoints)
	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	// Create aligned ketama base ring with RF=3 (one per AZ)
	baseRing, err := newAlignedKetamaHashring(endpoints, SectionsPerNode, 3)
	require.NoError(t, err)

	// Create shuffle shard hashring with aligned ordinal sharding enabled
	cfg := ShuffleShardingConfig{
		ShardSize:              2, // Select 2 ordinals -> 6 endpoints (2 * 3 AZs)
		AlignedOrdinalSharding: true,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-aligned")
	require.NoError(t, err)

	// Get the tenant shard
	tenant := "test-tenant"
	shard, err := shardRing.getTenantShardAligned(tenant)
	require.NoError(t, err)

	// Verify we got the right number of nodes (2 ordinals * 3 AZs = 6)
	nodes := shard.Nodes()
	require.Len(t, nodes, 6, "expected 6 endpoints (2 ordinals * 3 AZs)")

	// Extract ordinals from each AZ and verify they're the same
	ordinalsByAZ := make(map[string][]int)
	for _, node := range nodes {
		ordinalsByAZ[node.AZ] = append(ordinalsByAZ[node.AZ], extractOrdinalFromAddress(t, node.Address))
	}

	// Verify each AZ has exactly 2 ordinals
	require.Len(t, ordinalsByAZ, 3, "expected 3 AZs")
	for az, ordinals := range ordinalsByAZ {
		require.Len(t, ordinals, 2, "AZ %s should have 2 ordinals", az)
	}

	// Verify all AZs have the SAME ordinals (the key invariant)
	var referenceOrdinals []int
	for _, ordinals := range ordinalsByAZ {
		if referenceOrdinals == nil {
			referenceOrdinals = ordinals
		} else {
			require.ElementsMatch(t, referenceOrdinals, ordinals,
				"all AZs should have the same ordinals for aligned ordinal sharding")
		}
	}

	t.Logf("Selected ordinals: %v", referenceOrdinals)
}

func TestAlignedOrdinalShardingConsistency(t *testing.T) {
	t.Parallel()

	// Create 3 AZs with 5 ordinals each
	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newAlignedKetamaHashring(endpoints, SectionsPerNode, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize:              2,
		AlignedOrdinalSharding: true,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-consistency")
	require.NoError(t, err)

	// Verify same tenant always gets same ordinals across multiple calls
	tenant := "consistent-tenant"
	var firstOrdinals []int

	for trial := 0; trial < 10; trial++ {
		shard, err := shardRing.getTenantShardAligned(tenant)
		require.NoError(t, err)

		currentOrdinals := extractOrdinalsFromShard(t, shard)
		if firstOrdinals == nil {
			firstOrdinals = currentOrdinals
		} else {
			require.Equal(t, firstOrdinals, currentOrdinals,
				"same tenant should always get same ordinals")
		}
	}
}

func TestAlignedOrdinalShardingDifferentTenants(t *testing.T) {
	t.Parallel()

	// Create 3 AZs with 10 ordinals each to have enough spread
	endpoints := make([]Endpoint, 0, 30)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 10; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newAlignedKetamaHashring(endpoints, SectionsPerNode, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize:              3, // Select 3 ordinals
		AlignedOrdinalSharding: true,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-diff-tenants")
	require.NoError(t, err)

	// Different tenants should (likely) get different ordinals
	tenantOrdinals := make(map[string][]int)
	numTenants := 20

	for i := 0; i < numTenants; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		shard, err := shardRing.getTenantShardAligned(tenant)
		require.NoError(t, err)
		tenantOrdinals[tenant] = extractOrdinalsFromShard(t, shard)
	}

	// Count unique ordinal sets
	uniqueSets := make(map[string]int)
	for _, ordinals := range tenantOrdinals {
		key := fmt.Sprintf("%v", ordinals)
		uniqueSets[key]++
	}

	// With 10 ordinals choosing 3, there are C(10,3)=120 possible combinations
	// We expect multiple unique sets across 20 tenants
	t.Logf("Unique ordinal sets: %d out of %d tenants", len(uniqueSets), numTenants)
	require.Greater(t, len(uniqueSets), 1, "different tenants should get different ordinal sets")
}

func TestAlignedOrdinalShardingPreservesAlignment(t *testing.T) {
	t.Parallel()

	// Create 3 AZs with 5 ordinals each
	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newAlignedKetamaHashring(endpoints, SectionsPerNode, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize:              2,
		AlignedOrdinalSharding: true,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-preserves")
	require.NoError(t, err)

	tenant := "alignment-test-tenant"

	// Use GetN to get replicas and verify they're aligned (same ordinal across AZs)
	for i := 0; i < 100; i++ {
		ts := &prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "series", Value: fmt.Sprintf("series-%d", i)},
			},
		}

		// Get all 3 replicas (RF=3)
		var replicas []Endpoint
		for n := uint64(0); n < 3; n++ {
			ep, err := shardRing.GetN(tenant, ts, n)
			require.NoError(t, err)
			replicas = append(replicas, ep)
		}

		// Verify all 3 replicas have the same ordinal but different AZs
		ordinals := make(map[int]struct{})
		azsSeen := make(map[string]struct{})
		for _, ep := range replicas {
			ordinals[extractOrdinalFromAddress(t, ep.Address)] = struct{}{}
			azsSeen[ep.AZ] = struct{}{}
		}

		require.Len(t, ordinals, 1, "all replicas should have the same ordinal for series %d", i)
		require.Len(t, azsSeen, 3, "replicas should span all 3 AZs for series %d", i)
	}
}

// TestAlignedOrdinalShardingDataDistribution verifies the key behavior:
// - With shard_size=2, tenant gets 2 ordinals (e.g., ordinals 1 and 4)
// - Series are distributed across both ordinals
// - a-1, b-1, c-1 always receive the same series (aligned replicas for ordinal 1)
// - a-4, b-4, c-4 always receive the same series (aligned replicas for ordinal 4)
// - Series assigned to ordinal 1 are different from series assigned to ordinal 4.
func TestAlignedOrdinalShardingDataDistribution(t *testing.T) {
	t.Parallel()

	// Create 3 AZs with 5 ordinals each
	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newAlignedKetamaHashring(endpoints, SectionsPerNode, 3)
	require.NoError(t, err)

	cfg := ShuffleShardingConfig{
		ShardSize:              2, // Select 2 ordinals
		AlignedOrdinalSharding: true,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-distribution")
	require.NoError(t, err)

	tenant := "distribution-test-tenant"

	// First, get the tenant's selected ordinals
	shard, err := shardRing.getTenantShardAligned(tenant)
	require.NoError(t, err)
	selectedOrdinals := extractOrdinalsFromShard(t, shard)
	require.Len(t, selectedOrdinals, 2, "tenant should have exactly 2 ordinals")
	t.Logf("Tenant's selected ordinals: %v", selectedOrdinals)

	// Track which series go to which ordinal
	// Key: ordinal, Value: set of series indices
	seriesByOrdinal := make(map[int]map[int]struct{})
	for _, ord := range selectedOrdinals {
		seriesByOrdinal[ord] = make(map[int]struct{})
	}

	// Track which endpoints receive which series
	// Key: endpoint address, Value: set of series indices
	seriesByEndpoint := make(map[string]map[int]struct{})

	// Generate many series and track their distribution
	numSeries := 1000
	for i := 0; i < numSeries; i++ {
		ts := &prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{Name: "series", Value: fmt.Sprintf("series-%d", i)},
				{Name: "__name__", Value: "test_metric"},
			},
		}

		// Get all 3 replicas
		var replicas []Endpoint
		for n := uint64(0); n < 3; n++ {
			ep, err := shardRing.GetN(tenant, ts, n)
			require.NoError(t, err)
			replicas = append(replicas, ep)

			// Track series per endpoint
			if seriesByEndpoint[ep.Address] == nil {
				seriesByEndpoint[ep.Address] = make(map[int]struct{})
			}
			seriesByEndpoint[ep.Address][i] = struct{}{}
		}

		// All replicas should have the same ordinal
		ordinal := extractOrdinalFromAddress(t, replicas[0].Address)
		for _, ep := range replicas[1:] {
			epOrdinal := extractOrdinalFromAddress(t, ep.Address)
			require.Equal(t, ordinal, epOrdinal, "all replicas for series %d should have same ordinal", i)
		}

		// Track which ordinal this series went to
		seriesByOrdinal[ordinal][i] = struct{}{}
	}

	// Verify 1: Series are distributed across BOTH ordinals (not just one)
	for ord, series := range seriesByOrdinal {
		t.Logf("Ordinal %d received %d series", ord, len(series))
		require.Greater(t, len(series), 0, "ordinal %d should receive some series", ord)
	}

	// Verify 2: Same ordinal across different AZs receives the SAME series
	// Group endpoints by ordinal
	endpointsByOrdinal := make(map[int][]string)
	for addr := range seriesByEndpoint {
		ord := extractOrdinalFromAddress(t, addr)
		endpointsByOrdinal[ord] = append(endpointsByOrdinal[ord], addr)
	}

	for ord, addrs := range endpointsByOrdinal {
		if len(addrs) < 2 {
			continue
		}
		// All endpoints with the same ordinal should have received the exact same series
		referenceSeries := seriesByEndpoint[addrs[0]]
		for _, addr := range addrs[1:] {
			otherSeries := seriesByEndpoint[addr]
			require.Equal(t, len(referenceSeries), len(otherSeries),
				"endpoints with ordinal %d should have same number of series", ord)
			for seriesIdx := range referenceSeries {
				_, ok := otherSeries[seriesIdx]
				require.True(t, ok,
					"series %d should be on all endpoints with ordinal %d", seriesIdx, ord)
			}
		}
		t.Logf("Verified: All %d endpoints with ordinal %d have identical %d series",
			len(addrs), ord, len(referenceSeries))
	}

	// Verify 3: Different ordinals receive DIFFERENT series (no overlap)
	ordinalList := make([]int, 0, len(seriesByOrdinal))
	for ord := range seriesByOrdinal {
		ordinalList = append(ordinalList, ord)
	}
	if len(ordinalList) >= 2 {
		series1 := seriesByOrdinal[ordinalList[0]]
		series2 := seriesByOrdinal[ordinalList[1]]
		for seriesIdx := range series1 {
			_, overlap := series2[seriesIdx]
			require.False(t, overlap,
				"series %d should not be on both ordinal %d and ordinal %d",
				seriesIdx, ordinalList[0], ordinalList[1])
		}
		t.Logf("Verified: Ordinals %d and %d have no overlapping series", ordinalList[0], ordinalList[1])
	}
}

func TestAlignedOrdinalShardingValidation(t *testing.T) {
	t.Parallel()

	endpoints := make([]Endpoint, 0, 15)
	azs := []string{"az-a", "az-b", "az-c"}
	for _, az := range azs {
		for ord := 0; ord < 5; ord++ {
			endpoints = append(endpoints, makeK8sEndpoint("pod-"+az, ord, az))
		}
	}

	baseRing, err := newAlignedKetamaHashring(endpoints, SectionsPerNode, 3)
	require.NoError(t, err)

	// Test shard size exceeding available ordinals
	cfg := ShuffleShardingConfig{
		ShardSize:              10, // Only 5 ordinals available
		AlignedOrdinalSharding: true,
	}
	shardRing, err := newShuffleShardHashring(baseRing, cfg, 3, prometheus.NewRegistry(), "test-validation")
	require.NoError(t, err)

	_, err = shardRing.getTenantShardAligned("test-tenant")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds available common ordinals")
}

// Helper function to extract ordinal from K8s-style address.
func extractOrdinalFromAddress(t *testing.T, address string) int {
	t.Helper()
	// Address format: pod-az-N.svc.test.svc.cluster.local:10901
	parts := strings.Split(address, ".")
	require.Greater(t, len(parts), 0)
	podPart := parts[0] // pod-az-N
	lastDash := strings.LastIndex(podPart, "-")
	require.Greater(t, lastDash, 0)
	ordinalStr := podPart[lastDash+1:]
	var ordinal int
	_, err := fmt.Sscanf(ordinalStr, "%d", &ordinal)
	require.NoError(t, err)
	return ordinal
}

// Helper function to extract unique ordinals from a shard.
func extractOrdinalsFromShard(t *testing.T, shard Hashring) []int {
	t.Helper()
	nodes := shard.Nodes()
	ordinalSet := make(map[int]struct{})
	for _, node := range nodes {
		ordinalSet[extractOrdinalFromAddress(t, node.Address)] = struct{}{}
	}
	ordinals := make([]int, 0, len(ordinalSet))
	for ord := range ordinalSet {
		ordinals = append(ordinals, ord)
	}
	// Sort for consistent comparison
	for i := 0; i < len(ordinals); i++ {
		for j := i + 1; j < len(ordinals); j++ {
			if ordinals[i] > ordinals[j] {
				ordinals[i], ordinals[j] = ordinals[j], ordinals[i]
			}
		}
	}
	return ordinals
}
