// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"fmt"
	"testing"

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
		hs := newMultiHashring(AlgorithmHashmod, 3, tc.cfg)
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
			hashRing := newKetamaHashring(test.nodes, 10, test.n+1)

			result, err := hashRing.GetN("tenant", test.ts, test.n)
			require.NoError(t, err)
			require.Equal(t, test.expectedNode, result)
		})
	}
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
	hashRing := newKetamaHashring(nodes, SectionsPerNode, replicas)
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
