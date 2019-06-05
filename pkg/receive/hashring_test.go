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

func TestGetHost(t *testing.T) {
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
		hosts  map[string]struct{}
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
							model.AddressLabel: "host1",
						},
					},
				},
			},
			hosts: map[string]struct{}{"host1": struct{}{}},
		},
		{
			name: "specific",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host1",
						},
					},
					Source: "",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host2",
						},
					},
					Source: "tenant1",
				},
			},
			hosts:  map[string]struct{}{"host2": struct{}{}},
			tenant: "tenant1",
		},
		{
			name: "many tenants",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host1",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host2",
						},
					},
					Source: "tenant2",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host3",
						},
					},
					Source: "tenant3",
				},
			},
			hosts:  map[string]struct{}{"host1": struct{}{}},
			tenant: "tenant1",
		},
		{
			name: "many tenants error",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host1",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host2",
						},
					},
					Source: "tenant2",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host3",
						},
					},
					Source: "tenant3",
				},
			},
			tenant: "tenant4",
		},
		{
			name: "many hosts",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host1",
						},
						model.LabelSet{
							model.AddressLabel: "host2",
						},
						model.LabelSet{
							model.AddressLabel: "host3",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host4",
						},
						model.LabelSet{
							model.AddressLabel: "host5",
						},
						model.LabelSet{
							model.AddressLabel: "host6",
						},
					},
					Source: "",
				},
			},
			hosts: map[string]struct{}{
				"host1": struct{}{},
				"host2": struct{}{},
				"host3": struct{}{},
			},
			tenant: "tenant1",
		},
		{
			name: "many hosts 2",
			cfg: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host1",
						},
						model.LabelSet{
							model.AddressLabel: "host2",
						},
						model.LabelSet{
							model.AddressLabel: "host3",
						},
					},
					Source: "tenant1",
				},
				{
					Targets: []model.LabelSet{
						model.LabelSet{
							model.AddressLabel: "host4",
						},
						model.LabelSet{
							model.AddressLabel: "host5",
						},
						model.LabelSet{
							model.AddressLabel: "host6",
						},
					},
				},
			},
			hosts: map[string]struct{}{
				"host4": struct{}{},
				"host5": struct{}{},
				"host6": struct{}{},
			},
		},
	} {
		hs := NewHashring(ExactMatcher, tc.cfg)
		h, err := hs.GetHost(tc.tenant, ts)
		if tc.hosts != nil {
			if err != nil {
				t.Errorf("case %q: got unexpected error: %v", tc.name, err)
				continue
			}
			if _, ok := tc.hosts[h]; !ok {
				t.Errorf("case %q: got unexpected host %q", tc.name, h)
			}
			continue
		}
		if err == nil {
			t.Errorf("case %q: expected error", tc.name)
		}
	}
}
