// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

func TestPrometheus_Metadata_e2e(t *testing.T) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upctx, upcancel := context.WithTimeout(ctx, 10*time.Second)
	defer upcancel()

	logger := log.NewNopLogger()

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
  - targets: ['%s']
`, e2eutil.PromAddrPlaceHolder))
	testutil.Ok(t, p.Start(upctx, logger))

	u, err := url.Parse("http://" + p.Addr())
	testutil.Ok(t, err)

	c := promclient.NewClient(http.DefaultClient, logger, "")

	// Wait metadata response to be ready as Prometheus gets metadata after scrape.
	testutil.Ok(t, runutil.Retry(3*time.Second, ctx.Done(), func() error {
		meta, err := c.MetricMetadataInGRPC(ctx, u, "", -1)
		testutil.Ok(t, err)
		if len(meta) > 0 {
			return nil
		}
		return errors.New("empty metadata response from Prometheus")
	}))

	grpcClient := NewGRPCClient(NewPrometheus(u, c))
	for _, tcase := range []struct {
		name         string
		metric       string
		limit        int32
		expectedFunc func(map[string][]metadatapb.Meta) bool
	}{
		{
			name:  "all metadata return",
			limit: -1,
			// We just check two metrics here.
			expectedFunc: func(m map[string][]metadatapb.Meta) bool {
				return len(m["prometheus_build_info"]) > 0 && len(m["prometheus_engine_query_duration_seconds"]) > 0
			},
		},
		{
			name:  "no metadata return",
			limit: 0,
			expectedFunc: func(m map[string][]metadatapb.Meta) bool {
				return len(m) == 0
			},
		},
		{
			name:  "only 1 metadata return",
			limit: 1,
			expectedFunc: func(m map[string][]metadatapb.Meta) bool {
				return len(m) == 1
			},
		},
		{
			name:   "only prometheus_build_info metadata return",
			metric: "prometheus_build_info",
			limit:  1,
			expectedFunc: func(m map[string][]metadatapb.Meta) bool {
				return len(m) == 1 && len(m["prometheus_build_info"]) > 0
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			meta, w, err := grpcClient.MetricMetadata(ctx, &metadatapb.MetricMetadataRequest{
				Metric: tcase.metric,
				Limit:  tcase.limit,
			})

			testutil.Equals(t, annotations.Annotations(nil), w)
			testutil.Ok(t, err)
			testutil.Assert(t, tcase.expectedFunc(meta))
		})
	}
}
