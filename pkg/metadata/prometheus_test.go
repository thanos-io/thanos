// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package metadata

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPrometheus_Metadata_e2e(t *testing.T) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	testutil.Ok(t, p.SetConfig(`
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
`))
	testutil.Ok(t, p.Start())

	time.Sleep(10 * time.Second)

	u, err := url.Parse("http://" + p.Addr())
	testutil.Ok(t, err)

	prom := NewPrometheus(u, promclient.NewDefaultClient())

	for _, tcase := range []struct {
		name         string
		metric       string
		limit        int32
		expectedFunc func(map[string][]metadatapb.Meta) bool
	}{
		{
			name:  "all metadata return",
			limit: -1,
			expectedFunc: func(m map[string][]metadatapb.Meta) bool {
				return len(m["thanos_build_info"]) > 0 && len(m["prometheus_build_info"]) > 0
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
			name:   "only thanos_build_info metadata return",
			metric: "thanos_build_info",
			limit:  1,
			expectedFunc: func(m map[string][]metadatapb.Meta) bool {
				return len(m) == 1 && len(m["thanos_build_info"]) > 0
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			meta, w, err := NewGRPCClient(prom).MetricMetadata(context.Background(), &metadatapb.MetricMetadataRequest{
				Metric: tcase.metric,
				Limit:  tcase.limit,
			})
			testutil.Equals(t, storage.Warnings(nil), w)
			testutil.Ok(t, err)
			testutil.Assert(t, true, tcase.expectedFunc(meta))
		})
	}
}
