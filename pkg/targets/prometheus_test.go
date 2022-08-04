// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targets

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestPrometheus_Targets_e2e(t *testing.T) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	p.SetConfig(fmt.Sprintf(`
global:
  external_labels:
    region: eu-west
scrape_configs:
- job_name: 'myself'
  # Quick scrapes for test purposes.
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: ['%s','localhost:80']
  relabel_configs:
  - source_labels: ['__address__']
    regex: '^.+:80$'
    action: drop
`, e2eutil.PromAddrPlaceHolder))
	testutil.Ok(t, p.Start())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upctx, upcancel := context.WithTimeout(ctx, 10*time.Second)
	defer upcancel()

	logger := log.NewNopLogger()
	err = p.WaitPrometheusUp(upctx, logger)
	testutil.Ok(t, err)

	u, err := url.Parse("http://" + p.Addr())
	testutil.Ok(t, err)

	c := promclient.NewClient(http.DefaultClient, logger, "")

	// Wait targets response to be ready as Prometheus scrapes targets.
	testutil.Ok(t, runutil.Retry(3*time.Second, ctx.Done(), func() error {
		targets, err := c.TargetsInGRPC(ctx, u, "")
		testutil.Ok(t, err)
		if len(targets.ActiveTargets) > 0 {
			return nil
		}
		return errors.New("empty targets response from Prometheus")
	}))

	promTargets := NewPrometheus(u, promclient.NewDefaultClient(), func() labels.Labels {
		return labels.FromStrings("replica", "test1")
	})

	expected := &targetspb.TargetDiscovery{
		ActiveTargets: []*targetspb.ActiveTarget{
			{
				DiscoveredLabels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "__address__", Value: p.Addr()},
					{Name: "__metrics_path__", Value: "/metrics"},
					{Name: "__scheme__", Value: "http"},
					{Name: "__scrape_interval__", Value: "1s"},
					{Name: "__scrape_timeout__", Value: "1s"},
					{Name: "job", Value: "myself"},
					{Name: "replica", Value: "test1"},
				}},
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "instance", Value: p.Addr()},
					{Name: "job", Value: "myself"},
					{Name: "replica", Value: "test1"},
				}},
				ScrapePool:         "myself",
				ScrapeUrl:          fmt.Sprintf("http://%s/metrics", p.Addr()),
				GlobalUrl:          "",
				Health:             targetspb.TargetHealth_UP,
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
					{Name: "__scrape_interval__", Value: "1s"},
					{Name: "__scrape_timeout__", Value: "1s"},
					{Name: "job", Value: "myself"},
					{Name: "replica", Value: "test1"},
				}},
			},
		},
	}

	grpcClient := NewGRPCClientWithDedup(promTargets, nil)
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
			targets, w, err := grpcClient.Targets(context.Background(), &targetspb.TargetsRequest{
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
				targets.ActiveTargets[i].GlobalUrl = ""
			}

			testutil.Equals(t, expectedTargets.ActiveTargets, targets.ActiveTargets)
			testutil.Equals(t, expectedTargets.DroppedTargets, targets.DroppedTargets)
		})
	}
}
