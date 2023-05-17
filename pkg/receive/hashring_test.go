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

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestHashringGet(t *testing.T) {
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
					Endpoints: []string{"node1"},
				},
			},
			nodes: map[string]struct{}{"node1": {}},
		},
		{
			name: "specific",
			cfg: []HashringConfig{
				{
					Endpoints: []string{"node2"},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []string{"node1"},
				},
			},
			nodes:  map[string]struct{}{"node2": {}},
			tenant: "tenant2",
		},
		{
			name: "many tenants",
			cfg: []HashringConfig{
				{
					Endpoints: []string{"node1"},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []string{"node2"},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []string{"node3"},
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
					Endpoints: []string{"node1"},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []string{"node2"},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []string{"node3"},
					Tenants:   []string{"tenant3"},
				},
			},
			tenant: "tenant4",
		},
		{
			name: "many nodes",
			cfg: []HashringConfig{
				{
					Endpoints: []string{"node1", "node2", "node3"},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []string{"node4", "node5", "node6"},
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
					Endpoints: []string{"node1", "node2", "node3"},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []string{"node4", "node5", "node6"},
				},
			},
			nodes: map[string]struct{}{
				"node4": {},
				"node5": {},
				"node6": {},
			},
		},
	} {
		hs, err := newMultiHashring(AlgorithmHashmod, 3, tc.cfg)
		require.NoError(t, err)

		h, err := hs.Get(tc.tenant, ts)
		if tc.nodes != nil {
			if err != nil {
				t.Errorf("case %q: got unexpected error: %v", tc.name, err)
				continue
			}
			if _, ok := tc.nodes[h]; !ok {
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
		nodes        []string
		expectedNode string
		ts           *prompb.TimeSeries
		n            uint64
	}{
		{
			name:         "base case",
			nodes:        []string{"node-1", "node-2", "node-3"},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:         "base case with replication",
			nodes:        []string{"node-1", "node-2", "node-3"},
			ts:           baseTS,
			n:            1,
			expectedNode: "node-1",
		},
		{
			name:         "base case with replication",
			nodes:        []string{"node-1", "node-2", "node-3"},
			ts:           baseTS,
			n:            2,
			expectedNode: "node-3",
		},
		{
			name:         "base case with replication and reordered nodes",
			nodes:        []string{"node-1", "node-3", "node-2"},
			ts:           baseTS,
			n:            2,
			expectedNode: "node-3",
		},
		{
			name:         "base case with new node at beginning of ring",
			nodes:        []string{"node-0", "node-1", "node-2", "node-3"},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:         "base case with new node at end of ring",
			nodes:        []string{"node-1", "node-2", "node-3", "node-4"},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:  "base case with different timeseries",
			nodes: []string{"node-1", "node-2", "node-3"},
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
			hashRing, err := newKetamaHashring(test.nodes, []AZAwareEndpoint{}, 10, test.n+1)
			require.NoError(t, err)

			result, err := hashRing.GetN("tenant", test.ts, test.n)
			require.NoError(t, err)
			require.Equal(t, test.expectedNode, result)
		})
	}
}

func TestKetamaHashringBadConfigIsRejected(t *testing.T) {
	_, err := newKetamaHashring([]string{"node-1"}, []AZAwareEndpoint{}, 1, 2)
	require.Error(t, err)
}

func TestKetamaHashringConsistency(t *testing.T) {
	series := makeSeries()

	ringA := []string{"node-1", "node-2", "node-3"}
	a1, err := assignSeries(series, ringA)
	require.NoError(t, err)

	ringB := []string{"node-1", "node-2", "node-3"}
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
	series := makeSeries()

	initialRing := []string{"node-1", "node-2", "node-3"}
	initialAssignments, err := assignSeries(series, initialRing)
	require.NoError(t, err)

	resizedRing := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	reassignments, err := assignSeries(series, resizedRing)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node] {
			foundInInitialAssignment := findSeries(initialAssignments, node, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringIncreaseInMiddle(t *testing.T) {
	series := makeSeries()

	initialRing := []string{"node-1", "node-3"}
	initialAssignments, err := assignSeries(series, initialRing)
	require.NoError(t, err)

	resizedRing := []string{"node-1", "node-2", "node-3"}
	reassignments, err := assignSeries(series, resizedRing)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node] {
			foundInInitialAssignment := findSeries(initialAssignments, node, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringReplicationConsistency(t *testing.T) {
	series := makeSeries()

	initialRing := []string{"node-1", "node-4", "node-5"}
	initialAssignments, err := assignReplicatedSeries(series, initialRing, 2)
	require.NoError(t, err)

	resizedRing := []string{"node-4", "node-3", "node-1", "node-2", "node-5"}
	reassignments, err := assignReplicatedSeries(series, resizedRing, 2)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node] {
			foundInInitialAssignment := findSeries(initialAssignments, node, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringEvenAZSpread(t *testing.T) {
	tenant := "default-tenant"
	ts := &prompb.TimeSeries{
		Labels:  labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", "bar")),
		Samples: []prompb.Sample{{Value: 1, Timestamp: 0}},
	}

	for _, tt := range []struct {
		nodes    []AZAwareEndpoint
		replicas uint64
	}{
		{
			nodes: []AZAwareEndpoint{
				{address: "a", az: "1"},
				{address: "b", az: "2"},
				{address: "c", az: "1"},
				{address: "d", az: "2"},
			},
			replicas: 1,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "a", az: ""},
				{address: "b", az: ""},
				{address: "c", az: ""},
				{address: "d", az: ""},
			},
			replicas: 1,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "a", az: "1"},
				{address: "b", az: "2"},
				{address: "c", az: "1"},
				{address: "d", az: "2"},
			},
			replicas: 2,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "a", az: "1"},
				{address: "b", az: "2"},
				{address: "c", az: "3"},
				{address: "d", az: "1"},
				{address: "e", az: "2"},
				{address: "f", az: "3"},
			},
			replicas: 3,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "a", az: ""},
				{address: "b", az: ""},
				{address: "c", az: ""},
				{address: "d", az: ""},
				{address: "e", az: ""},
				{address: "f", az: ""},
			},
			replicas: 3,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "a", az: "1"},
				{address: "b", az: "2"},
				{address: "c", az: "3"},
				{address: "d", az: "1"},
				{address: "e", az: "2"},
				{address: "f", az: "3"},
				{address: "g", az: "4"},
				{address: "h", az: "4"},
				{address: "i", az: "4"},
				{address: "j", az: "5"},
				{address: "k", az: "5"},
				{address: "l", az: "5"},
			},
			replicas: 10,
		},
	} {
		t.Run("", func(t *testing.T) {
			hashRing, err := newKetamaHashring([]string{}, tt.nodes, SectionsPerNode, tt.replicas)
			testutil.Ok(t, err)

			availableAzs := make(map[string]int64)
			for _, n := range tt.nodes {
				availableAzs[n.az] = 0
			}

			azSpread := make(map[string]int64)
			for i := 0; i < int(tt.replicas); i++ {
				r, err := hashRing.GetN(tenant, ts, uint64(i))
				testutil.Ok(t, err)

				for _, n := range tt.nodes {
					if !strings.HasPrefix(n.address, r) {
						continue
					}
					azSpread[n.az]++
				}
			}
			expectedAzSpreadLength := int(tt.replicas)
			if int(tt.replicas) > len(availableAzs) {
				expectedAzSpreadLength = len(availableAzs)
			}
			testutil.Equals(t, len(azSpread), expectedAzSpreadLength)

			for _, writeToAz := range azSpread {
				minAz := getMinAz(azSpread)
				testutil.Assert(t, math.Abs(float64(writeToAz-minAz)) <= 1.0)
			}
		})
	}
}

func TestKetamaHashringEvenNodeSpread(t *testing.T) {
	tenant := "default-tenant"

	for _, tt := range []struct {
		nodes     []AZAwareEndpoint
		replicas  uint64
		numSeries uint64
	}{
		{
			nodes: []AZAwareEndpoint{
				{address: "localhost:1", az: "A"},
				{address: "localhost:2", az: "B"},
				{address: "localhost:3", az: "A"},
				{address: "localhost:4", az: "B"},
			},
			replicas:  2,
			numSeries: 1000,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "localhost:1", az: ""},
				{address: "localhost:2", az: ""},
				{address: "localhost:3", az: ""},
				{address: "localhost:4", az: ""},
			},
			replicas:  2,
			numSeries: 1000,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "localhost:1", az: "A"},
				{address: "localhost:2", az: "B"},
				{address: "localhost:3", az: "C"},
				{address: "localhost:4", az: "A"},
				{address: "localhost:5", az: "B"},
				{address: "localhost:6", az: "C"},
			},
			replicas:  3,
			numSeries: 10000,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "localhost:1", az: "A"},
				{address: "localhost:2", az: "B"},
				{address: "localhost:3", az: "C"},
				{address: "localhost:4", az: "A"},
				{address: "localhost:5", az: "B"},
				{address: "localhost:6", az: "C"},
				{address: "localhost:7", az: "A"},
				{address: "localhost:8", az: "B"},
				{address: "localhost:9", az: "C"},
			},
			replicas:  2,
			numSeries: 10000,
		},
		{
			nodes: []AZAwareEndpoint{
				{address: "localhost:1", az: "A"},
				{address: "localhost:2", az: "B"},
				{address: "localhost:3", az: "C"},
				{address: "localhost:4", az: "A"},
				{address: "localhost:5", az: "B"},
				{address: "localhost:6", az: "C"},
				{address: "localhost:7", az: "A"},
				{address: "localhost:8", az: "B"},
				{address: "localhost:9", az: "C"},
			},
			replicas:  9,
			numSeries: 10000,
		},
	} {
		t.Run("", func(t *testing.T) {
			hashRing, err := newKetamaHashring([]string{}, tt.nodes, SectionsPerNode, tt.replicas)
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

					nodeSpread[r]++
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
	for _, tt := range []struct {
		cfg           []HashringConfig
		replicas      uint64
		algorithm     HashringAlgorithm
		expectedError string
	}{
		{
			cfg: []HashringConfig{
				{
					Endpoints:       []string{"a", "b", "c"},
					EndpointsWithAZ: []AZAwareEndpoint{{address: "localhost:123", az: "A"}, {address: "localhost:124", az: "B"}, {address: "localhost:125", az: "C"}},
				},
			},
			replicas:      2,
			algorithm:     AlgorithmKetama,
			expectedError: "Hashring contains both AZ and non AZ aware endpoint configurations.",
		},
		{
			cfg: []HashringConfig{
				{
					EndpointsWithAZ: []AZAwareEndpoint{{address: "localhost:123", az: "A"}, {address: "localhost:124", az: "B"}, {address: "localhost:125", az: "C"}},
				},
			},
			replicas:      2,
			algorithm:     AlgorithmHashmod,
			expectedError: "Hashmod algorithm does not support AZ aware hashring configuration. Either use Ketama or remove AZ configuration.",
		},
	} {
		t.Run("", func(t *testing.T) {
			_, err := newMultiHashring(tt.algorithm, tt.replicas, tt.cfg)
			require.EqualError(t, err, tt.expectedError)
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

func assignSeries(series []prompb.TimeSeries, nodes []string) (map[string][]prompb.TimeSeries, error) {
	return assignReplicatedSeries(series, nodes, 0)
}

func assignReplicatedSeries(series []prompb.TimeSeries, nodes []string, replicas uint64) (map[string][]prompb.TimeSeries, error) {
	hashRing, err := newKetamaHashring(nodes, []AZAwareEndpoint{}, SectionsPerNode, replicas)
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
			assignments[result] = append(assignments[result], ts)

		}
	}

	return assignments, nil
}
