package receive

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
)

func TestHash(t *testing.T) {
	ts := &prompb.TimeSeries{
		Labels: []prompb.Label{
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
		Labels: []prompb.Label{ts.Labels[1], ts.Labels[0]},
	}

	if hash("", ts) != hash("", ts2) {
		t.Errorf("expected hashes to be independent of label order")
	}
}

func TestHashringGet(t *testing.T) {
	ts := &prompb.TimeSeries{
		Labels: []prompb.Label{
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
		cfg    []*targetgroup.Group
		nodes  map[string]struct{}
		tenant string
	}{
		{
			name:   "empty",
			cfg:    []*targetgroup.Group{},
			tenant: "tenant1",
		},
		{
			name: "simple",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node1",
						},
					},
				},
			},
			nodes: map[string]struct{}{"node1": struct{}{}},
		},
		{
			name: "specific",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node1",
						},
					},
					Source: "",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node2",
						},
					},
					Source: "tenant1",
				},
			},
			nodes:  map[string]struct{}{"node2": struct{}{}},
			tenant: "tenant1",
		},
		{
			name: "many tenants",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node1",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node2",
						},
					},
					Source: "tenant2",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node3",
						},
					},
					Source: "tenant3",
				},
			},
			nodes:  map[string]struct{}{"node1": struct{}{}},
			tenant: "tenant1",
		},
		{
			name: "many tenants error",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node1",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node2",
						},
					},
					Source: "tenant2",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node3",
						},
					},
					Source: "tenant3",
				},
			},
			tenant: "tenant4",
		},
		{
			name: "many nodes",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node1",
						},
						model.LabelSet{
							model.AddressLabel: "node2",
						},
						model.LabelSet{
							model.AddressLabel: "node3",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node4",
						},
						model.LabelSet{
							model.AddressLabel: "node5",
						},
						model.LabelSet{
							model.AddressLabel: "node6",
						},
					},
					Source: "",
				},
			},
			nodes: map[string]struct{}{
				"node1": struct{}{},
				"node2": struct{}{},
				"node3": struct{}{},
			},
			tenant: "tenant1",
		},
		{
			name: "many nodes 2",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node1",
						},
						model.LabelSet{
							model.AddressLabel: "node2",
						},
						model.LabelSet{
							model.AddressLabel: "node3",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "node4",
						},
						model.LabelSet{
							model.AddressLabel: "node5",
						},
						model.LabelSet{
							model.AddressLabel: "node6",
						},
					},
				},
			},
			nodes: map[string]struct{}{
				"node4": struct{}{},
				"node5": struct{}{},
				"node6": struct{}{},
			},
		},
	} {
		hs := NewHashring(ExactMatcher, tc.cfg)
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
