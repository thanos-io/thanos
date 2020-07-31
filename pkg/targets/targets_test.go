// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targets

import (
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestDedupTargets(t *testing.T) {
	for _, tc := range []struct {
		name          string
		targets, want *targetspb.TargetDiscovery
		replicaLabels []string
	}{
		{
			name:    "nil slice",
			targets: nil,
			want:    nil,
		},
		{
			name:          "dropped",
			replicaLabels: []string{"replica"},
			targets: &targetspb.TargetDiscovery{
				DroppedTargets: []*targetspb.DroppedTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:80"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "0"},
						}},
					},
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:80"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "1"},
						}},
					},
				},
			},
			want: &targetspb.TargetDiscovery{
				DroppedTargets: []*targetspb.DroppedTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:80"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
						}},
					},
				},
			},
		},
		{
			name:          "active simple",
			replicaLabels: []string{"replica"},
			targets: &targetspb.TargetDiscovery{
				ActiveTargets: []*targetspb.ActiveTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "0"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "0"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_UP,
					},
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "1"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "1"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_UP,
					},
				},
			},
			want: &targetspb.TargetDiscovery{
				ActiveTargets: []*targetspb.ActiveTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_UP,
					},
				},
			},
		},
		{
			name:          "active unhealth first",
			replicaLabels: []string{"replica"},
			targets: &targetspb.TargetDiscovery{
				ActiveTargets: []*targetspb.ActiveTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "0"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "0"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_UP,
					},
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "1"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "1"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_DOWN,
					},
				},
			},
			want: &targetspb.TargetDiscovery{
				ActiveTargets: []*targetspb.ActiveTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_DOWN,
					},
				},
			},
		},
		{
			name:          "active latest scrape first",
			replicaLabels: []string{"replica"},
			targets: &targetspb.TargetDiscovery{
				ActiveTargets: []*targetspb.ActiveTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "0"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "0"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_UP,
						LastScrape: time.Unix(1, 0),
					},
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "1"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
							{Name: "replica", Value: "1"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_UP,
						LastScrape: time.Unix(2, 0),
					},
				},
			},
			want: &targetspb.TargetDiscovery{
				ActiveTargets: []*targetspb.ActiveTarget{
					{
						DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "__address__", Value: "localhost:9090"},
							{Name: "__metrics_path__", Value: "/metrics"},
							{Name: "__scheme__", Value: "http"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
						}},
						Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
							{Name: "instance", Value: "localhost:9090"},
							{Name: "job", Value: "myself"},
							{Name: "prometheus", Value: "ha"},
						}},
						ScrapePool: "myself",
						ScrapeUrl:  "http://localhost:9090/metrics",
						Health:     targetspb.TargetHealth_UP,
						LastScrape: time.Unix(2, 0),
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
			testutil.Equals(t, tc.want, dedupTargets(tc.targets, replicaLabels))
		})
	}
}
