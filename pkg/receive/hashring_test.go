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
			nodeAddrs: map[string]struct{}{
				"node-1": {},
				"node-2": {},
				"node-6": {},
			},
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
