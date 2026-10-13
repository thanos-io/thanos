// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/clientconfig"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	baseAPI "github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

func TestFlushEndpoint(t *testing.T) {
	ctx := context.Background()

	logger := log.NewNopLogger()

	prom, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)

	prom.SetConfig(fmt.Sprintf(`
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
`, prom.Addr()))
	testutil.Ok(t, prom.Start(ctx, logger))
	defer func() { testutil.Ok(t, prom.Stop()) }()

	promURL, err := url.Parse("http://" + prom.Addr())
	testutil.Ok(t, err)
	httpClient, err := clientconfig.NewHTTPClient(clientconfig.NewDefaultHTTPClientConfig(), "thanos-sidecar")
	testutil.Ok(t, err)
	promClient := promclient.NewClient(httpClient, logger, "thanos-sidecar")

	// Waits for targets to be present in prometheus, so we know there will be active chunks in the head block
	// to snapshot
	testutil.Ok(t, runutil.Retry(1*time.Second, ctx.Done(), func() error {
		targets, err := promClient.TargetsInGRPC(ctx, promURL, "")
		testutil.Ok(t, err)
		if len(targets.ActiveTargets) > 0 {
			return nil
		}
		return errors.New("empty targets response from Prometheus")
	}))

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	externalLabels := labels.FromStrings("external", "label")
	s := shipper.New(logger, nil, prom.Dir(), bkt, func() labels.Labels { return externalLabels }, metadata.TestSource, nil, false, metadata.NoneFunc, shipper.DefaultMetaFilename)
	now := time.Now()

	api := &SidecarAPI{
		baseAPI: &baseAPI.BaseAPI{
			Now: func() time.Time { return now },
		},
		logger:  logger,
		promURL: promURL,
		dataDir: prom.Dir(),
		client:  promClient,
		shipper: s,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "", nil)
	testutil.Ok(t, err)

	res, _, err, _ := api.flush(req)
	r := res.(*flushResponse)
	testutil.Assert(t, r.BlocksUploaded > 0)
	testutil.Equals(t, (*baseAPI.ApiError)(nil), err)
}
