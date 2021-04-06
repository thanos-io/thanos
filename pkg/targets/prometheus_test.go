// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targets

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestPrometheus_Targets_e2e(t *testing.T) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	p.SetConfig(`
global:
  external_labels:
    region: eu-west

scrape_configs:
- job_name: 'myself'
  # Quick scrapes for test purposes.
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: ['localhost:9090','localhost:80']
  relabel_configs:
  - source_labels: ['__address__']
    regex: '^.+:80$'
    action: drop
`)
	testutil.Ok(t, p.Start())

	// For some reason it's better to wait much more than a few scrape intervals.
	time.Sleep(5 * time.Second)

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	promTargets := NewPrometheus(u, promclient.NewDefaultClient(), func() labels.Labels {
		return labels.FromStrings("replica", "test1")
	})

	expected := &targetspb.TargetDiscovery{
		ActiveTargets: []*targetspb.ActiveTarget{
			{
				DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "__address__", Value: "localhost:9090"},
					{Name: "__metrics_path__", Value: "/metrics"},
					{Name: "__scheme__", Value: "http"},
					{Name: "job", Value: "myself"},
					{Name: "replica", Value: "test1"},
				}},
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "instance", Value: "localhost:9090"},
					{Name: "job", Value: "myself"},
					{Name: "replica", Value: "test1"},
				}},
				ScrapePool:         "myself",
				ScrapeUrl:          "http://localhost:9090/metrics",
				Health:             targetspb.TargetHealth_DOWN,
				LastScrape:         time.Time{},
				LastScrapeDuration: 0,
			},
		},
		DroppedTargets: []*targetspb.DroppedTarget{
			{
				DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "__address__", Value: "localhost:80"},
					{Name: "__metrics_path__", Value: "/metrics"},
					{Name: "__scheme__", Value: "http"},
					{Name: "job", Value: "myself"},
					{Name: "replica", Value: "test1"},
				}},
			},
		},
	}

	for _, tcase := range []struct {
		requestedState targetspb.TargetsRequest_State
		expectedErr    error
	}{
		{
			requestedState: targetspb.TargetsRequest_ANY,
		},
		{
			requestedState: targetspb.TargetsRequest_ACTIVE,
		},
		{
			requestedState: targetspb.TargetsRequest_DROPPED,
		},
	} {
		t.Run(tcase.requestedState.String(), func(t *testing.T) {
			targets, w, err := NewGRPCClientWithDedup(promTargets, nil).Targets(context.Background(), &targetspb.TargetsRequest{
				State: tcase.requestedState,
			})
			testutil.Equals(t, storage.Warnings(nil), w)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)

			expectedTargets := proto.Clone(expected).(*targetspb.TargetDiscovery)

			switch tcase.requestedState {
			case targetspb.TargetsRequest_ACTIVE:
				expectedTargets.DroppedTargets = expectedTargets.DroppedTargets[:0]
			case targetspb.TargetsRequest_DROPPED:
				expectedTargets.ActiveTargets = expectedTargets.ActiveTargets[:0]
			}

			for i := range targets.ActiveTargets {
				targets.ActiveTargets[i].LastScrapeDuration = 0
				targets.ActiveTargets[i].LastScrape = time.Time{}
				targets.ActiveTargets[i].LastError = ""
			}

			testutil.Equals(t, expectedTargets.ActiveTargets, targets.ActiveTargets)
			testutil.Equals(t, expectedTargets.DroppedTargets, targets.DroppedTargets)
		})
	}
}
