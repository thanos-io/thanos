// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"

	"github.com/efficientgo/core/testutil"
	v1 "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestToolsBucketWebExternalPrefixWithoutReverseProxy(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("rt-prfx-xtprf")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "testThanos"

	const bucket = "compact-test"
	m := e2edb.NewMinio(e, "thanos", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	svcConfig := client.BucketConfig{
		Type:   client.S3,
		Config: e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.InternalDir()),
	}

	b := e2ethanos.NewToolsBucketWeb(
		e,
		"1",
		svcConfig,
		"",
		externalPrefix,
		"",
		"",
		"",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(b))

	checkNetworkRequests(t, "http://"+b.Endpoint("http")+"/"+externalPrefix+"/blocks")
}

func TestToolsBucketWebExternalPrefix(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("external-prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "testThanos"
	const bucket = "toolsBucketWeb-test"
	m := e2edb.NewMinio(e, "thanos", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	svcConfig := client.BucketConfig{
		Type:   client.S3,
		Config: e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.InternalDir()),
	}

	b := e2ethanos.NewToolsBucketWeb(
		e,
		"1",
		svcConfig,
		"",
		externalPrefix,
		"",
		"",
		"",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(b))

	toolsBucketWebURL := urlParse(t, "http://"+b.Endpoint("http")+"/"+externalPrefix)

	toolsBucketWebProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(toolsBucketWebURL, externalPrefix))
	t.Cleanup(toolsBucketWebProxy.Close)

	checkNetworkRequests(t, toolsBucketWebProxy.URL+"/"+externalPrefix+"/blocks")
}

func TestToolsBucketWebExternalPrefixAndRoutePrefix(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("rt-prfx-xtrtprf")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	externalPrefix := "testThanos"
	routePrefix := "test"
	const bucket = "toolsBucketWeb-test"
	m := e2edb.NewMinio(e, "thanos", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	svcConfig := client.BucketConfig{
		Type:   client.S3,
		Config: e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.InternalDir()),
	}

	b := e2ethanos.NewToolsBucketWeb(
		e,
		"1",
		svcConfig,
		routePrefix,
		externalPrefix,
		"",
		"",
		"",
	)
	testutil.Ok(t, e2e.StartAndWaitReady(b))

	toolsBucketWebURL := urlParse(t, "http://"+b.Endpoint("http")+"/"+routePrefix)

	toolsBucketWebProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(toolsBucketWebURL, externalPrefix))
	t.Cleanup(toolsBucketWebProxy.Close)

	checkNetworkRequests(t, toolsBucketWebProxy.URL+"/"+externalPrefix+"/blocks")
}

func TestToolsBucketWebWithTimeAndRelabelFilter(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("time-relabel")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	// Create Minio.
	const bucket = "toolsBucketWeb-test"
	m := e2edb.NewMinio(e, "thanos", bucket, e2edb.WithMinioTLS())
	testutil.Ok(t, e2e.StartAndWaitReady(m))

	// Create bucket.
	logger := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(logger,
		e2ethanos.NewS3Config(bucket, m.Endpoint("http"), m.Dir()), "tools")
	testutil.Ok(t, err)

	// Create share dir for upload.
	dir := filepath.Join(e.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))

	// Upload blocks.
	now, err := time.Parse(time.RFC3339, "2021-07-24T08:00:00Z")
	testutil.Ok(t, err)
	blocks := []blockDesc{
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("tenant_id", "b", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("tenant_id", "a", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("tenant_id", "b", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
	}
	for _, b := range blocks {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)

		id, err := b.Create(ctx, dir, 0, b.hashFunc, 120)
		testutil.Ok(t, err)
		testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
			return objstore.UploadDir(ctx, logger, bkt, path.Join(dir, id.String()), id.String())
		}))
	}
	// Start thanos tool bucket web.
	svcConfig := client.BucketConfig{
		Type:   client.S3,
		Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("http"), m.InternalDir()),
	}
	b := e2ethanos.NewToolsBucketWeb(
		e,
		"1",
		svcConfig,
		"",
		"",
		now.Format(time.RFC3339),
		now.Add(1*time.Hour).Format(time.RFC3339),
		`
- action: keep
  regex: "b"
  source_labels: ["tenant_id"]`,
	)
	testutil.Ok(t, e2e.StartAndWaitReady(b))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	var respData struct {
		Status string
		Data   *v1.BlocksInfo
	}

	testutil.Ok(t, runutil.Retry(5*time.Second, ctx.Done(), func() error {
		// Request blocks api.
		resp, err := http.DefaultClient.Get("http://" + b.Endpoint("http") + "/api/v1/blocks")
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			return errors.Newf("statuscode is not 200, got %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrapf(err, "error reading body")
		}

		if err := resp.Body.Close(); err != nil {
			return errors.Wrapf(err, "error closing body")
		}

		if err := json.Unmarshal(body, &respData); err != nil {
			return errors.Wrapf(err, "error unmarshaling body")
		}

		if respData.Status != "success" {
			return errors.Newf("status is not success, got %s", respData.Status)
		}

		// Filtered by time and relabel, result only one blocks.
		if len(respData.Data.Blocks) == 1 {
			return nil
		}

		return errors.Newf("expected 1 block, got %d", len(respData.Data.Blocks))
	}))

	testutil.Equals(t, 1, len(respData.Data.Blocks))
	testutil.Equals(t, respData.Data.Blocks[0].MaxTime, blocks[0].maxt)
	testutil.Equals(t, respData.Data.Blocks[0].MinTime, blocks[0].mint)
	testutil.Equals(t, respData.Data.Blocks[0].Thanos.Labels, blocks[0].extLset.Map())
}
