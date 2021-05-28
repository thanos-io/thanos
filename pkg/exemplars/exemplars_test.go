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

func TestDedupExemplarsResponse(t *testing.T) {
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
			want: []*exemplarspb.ExemplarData{},
		},
		{
			name:          "multiple series",
			replicaLabels: []string{"replica"},
			exemplars: []*exemplarspb.ExemplarData{
				{
					SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "__name__", Value: "test_exemplar_metric_total"},
						{Name: "instance", Value: "localhost:8090"},
						{Name: "job", Value: "prometheus"},
						{Name: "service", Value: "bar"},
						{Name: "replica", Value: "0"},
					}},
					Exemplars: []*exemplarspb.Exemplar{
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "EpTxMJ40fUus7aGY"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "EpTxMJ40fUus7aGY"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
					},
				},
				{
					SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "__name__", Value: "test_exemplar_metric_total"},
						{Name: "instance", Value: "localhost:8090"},
						{Name: "job", Value: "prometheus"},
						{Name: "service", Value: "bar"},
						{Name: "replica", Value: "1"},
					}},
					Exemplars: []*exemplarspb.Exemplar{
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "EpTxMJ40fUus7aGY"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
					},
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
					Exemplars: []*exemplarspb.Exemplar{
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "EpTxMJ40fUus7aGY"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
					},
				},
			},
		},
		{
			name:          "multiple series with multiple exemplars data",
			replicaLabels: []string{"replica"},
			exemplars: []*exemplarspb.ExemplarData{
				{
					SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "__name__", Value: "test_exemplar_metric_total"},
						{Name: "instance", Value: "localhost:8090"},
						{Name: "job", Value: "prometheus"},
						{Name: "service", Value: "bar"},
						{Name: "replica", Value: "0"},
					}},
					Exemplars: []*exemplarspb.Exemplar{
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "EpTxMJ40fUus7aGY"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "foo"},
							}},
							Value: 19,
							Ts:    1600096955470,
						},
					},
				},
				{
					SeriesLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "__name__", Value: "test_exemplar_metric_total"},
						{Name: "instance", Value: "localhost:8090"},
						{Name: "job", Value: "prometheus"},
						{Name: "service", Value: "bar"},
						{Name: "replica", Value: "1"},
					}},
					Exemplars: []*exemplarspb.Exemplar{
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "bar"},
							}},
							Value: 19,
							Ts:    1600096955579,
						},
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "EpTxMJ40fUus7aGY"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
						// Same ts but different labels, cannot dedup.
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "test"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
					},
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
					Exemplars: []*exemplarspb.Exemplar{
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "foo"},
							}},
							Value: 19,
							Ts:    1600096955470,
						},
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "EpTxMJ40fUus7aGY"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "test"},
							}},
							Value: 19,
							Ts:    1600096955479,
						},
						{
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "traceID", Value: "bar"},
							}},
							Value: 19,
							Ts:    1600096955579,
						},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicaLabels := make(map[string]struct{})
			for _, lbl := range tc.replicaLabels {
				replicaLabels[lbl] = struct{}{}
			}
			testutil.Equals(t, tc.want, dedupExemplarsResponse(tc.exemplars, replicaLabels))
		})
	}
}
