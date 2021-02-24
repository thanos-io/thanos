// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package exemplars

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMain(m *testing.M) {
	testutil.TolerantVerifyLeakMain(m)
}

func TestDedupExemplarsData(t *testing.T) {
	for _, tc := range []struct {
		name            string
		exemplars, want []*exemplarspb.ExemplarData
		replicaLabels   []string
	}{
		{
			name:      "nil slice",
			exemplars: nil,
			want:      nil,
		},
		{
			name:      "empty exemplars data slice",
			exemplars: []*exemplarspb.ExemplarData{},
			want:      []*exemplarspb.ExemplarData{},
		},
		{
			name: "empty exemplars data",
			exemplars: []*exemplarspb.ExemplarData{
				{
					SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "__name__", Value: "test_exemplar_metric_total"},
						{Name: "instance", Value: "localhost:8090"},
						{Name: "job", Value: "prometheus"},
						{Name: "service", Value: "bar"},
					}},
				},
			},
			want: []*exemplarspb.ExemplarData{
				{
					SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "__name__", Value: "test_exemplar_metric_total"},
						{Name: "instance", Value: "localhost:8090"},
						{Name: "job", Value: "prometheus"},
						{Name: "service", Value: "bar"},
					}},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicaLabels := make(map[string]struct{})
			for _, lbl := range tc.replicaLabels {
				replicaLabels[lbl] = struct{}{}
			}
			testutil.Equals(t, tc.want, dedupExemplarsData(tc.exemplars, replicaLabels))
		})
	}
}
