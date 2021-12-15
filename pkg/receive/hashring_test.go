// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestHashringGet(t *testing.T) {
	ts := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{
			{Name: "foo", Value: "bar"},
			{Name: "baz", Value: "qux"},
		},
	}

	for _, tc := range []struct {
		name   string
		cfg    []HashringConfig
		tenant string

		expectedNode string
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
			expectedNode: "node1",
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
			tenant:       "tenant2",
			expectedNode: "node2",
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
			tenant:       "tenant1",
			expectedNode: "node1",
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
			tenant:       "tenant1",
			expectedNode: "node2",
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
			expectedNode: "node4",
		},
	} {
		t.Run("", func(t *testing.T) {
			hs := newMultiHashring(tc.cfg)
			h, err := hs.Get(tc.tenant, ts)
			if tc.expectedNode != "" {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.expectedNode, h)
				return
			}
			testutil.NotOk(t, err)
		})
	}
}
