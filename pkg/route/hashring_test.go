// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package route

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestHash(t *testing.T) {
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

	ts2 := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{ts.Labels[1], ts.Labels[0]},
	}

	if hash("", ts.Labels) != hash("", ts2.Labels) {
		t.Errorf("expected hashes to be independent of label order")
	}
}

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
		hs := newMultiHashring(tc.cfg)
		h, err := hs.Get(tc.tenant, ts.Labels)
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
