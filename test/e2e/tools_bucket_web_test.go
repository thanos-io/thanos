// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"

	v1 "github.com/thanos-io/thanos/pkg/api/blocks"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestToolsBucketWebExternalPrefixWithoutReverseProxy(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_tools_bucket_web_route_prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	externalPrefix := "testThanos"
	m := e2edb.NewMinio(8080, "thanos")
	testutil.Ok(t, s.StartAndWaitReady(m))

	svcConfig := client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    "thanos",
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	}

	b, err := e2ethanos.NewToolsBucketWeb(
		"1",
		svcConfig,
		"",
		externalPrefix,
		"",
		"",
		"",
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(b))

	checkNetworkRequests(t, "http://"+b.HTTPEndpoint()+"/"+externalPrefix+"/blocks")
}

func TestToolsBucketWebExternalPrefix(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_tools_bucket_web_external_prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	externalPrefix := "testThanos"
	const bucket = "toolsBucketWeb_test"
	m := e2edb.NewMinio(8080, bucket)
	testutil.Ok(t, s.StartAndWaitReady(m))

	svcConfig := client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    bucket,
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	}

	b, err := e2ethanos.NewToolsBucketWeb(
		"1",
		svcConfig,
		"",
		externalPrefix,
		"",
		"",
		"",
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(b))

	toolsBucketWebURL := mustURLParse(t, "http://"+b.HTTPEndpoint()+"/"+externalPrefix)

	toolsBucketWebProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(toolsBucketWebURL, externalPrefix))
	t.Cleanup(toolsBucketWebProxy.Close)

	checkNetworkRequests(t, toolsBucketWebProxy.URL+"/"+externalPrefix+"/blocks")
}

func TestToolsBucketWebExternalPrefixAndRoutePrefix(t *testing.T) {
	t.Parallel()

	s, err := e2e.NewScenario("e2e_test_tools_bucket_web_and_route_prefix")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))

	externalPrefix := "testThanos"
	routePrefix := "test"
	const bucket = "toolsBucketWeb_test"
	m := e2edb.NewMinio(8080, bucket)
	testutil.Ok(t, s.StartAndWaitReady(m))

	svcConfig := client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    bucket,
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	}

	b, err := e2ethanos.NewToolsBucketWeb(
		"1",
		svcConfig,
		routePrefix,
		externalPrefix,
		"",
		"",
		"",
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(b))

	toolsBucketWebURL := mustURLParse(t, "http://"+b.HTTPEndpoint()+"/"+routePrefix)

	toolsBucketWebProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(toolsBucketWebURL, externalPrefix))
	t.Cleanup(toolsBucketWebProxy.Close)

	checkNetworkRequests(t, toolsBucketWebProxy.URL+"/"+externalPrefix+"/blocks")
}

func TestToolsBucketWebWithTimeAndRelabelFilter(t *testing.T) {
	t.Parallel()
	// Create network.
	s, err := e2e.NewScenario("e2e_test_tools_bucket_web_time_and_relabel_filter")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, s))
	// Create Minio.
	const bucket = "toolsBucketWeb_test"
	m := e2edb.NewMinio(8080, bucket)
	testutil.Ok(t, s.StartAndWaitReady(m))
	// Create bucket.
	logger := log.NewLogfmtLogger(os.Stdout)
	bkt, err := s3.NewBucketWithConfig(logger, s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.HTTPEndpoint(),
		Insecure:  true,
	}, "tools")
	testutil.Ok(t, err)
	// Create share dir for upload.
	dir := filepath.Join(s.SharedDir(), "tmp")
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
		id, err := b.Create(context.Background(), dir, 0, b.hashFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, objstore.UploadDir(context.Background(), logger, bkt, path.Join(dir, id.String()), id.String()))
	}
	// Start thanos tool bucket web.
	svcConfig := client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    bucket,
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	}
	b, err := e2ethanos.NewToolsBucketWeb(
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
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(b))
	// Request blocks api.
	resp, err := http.DefaultClient.Get("http://" + b.HTTPEndpoint() + "/api/v1/blocks")
	testutil.Ok(t, err)
	testutil.Equals(t, http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	testutil.Ok(t, err)
	var data struct {
		Status string
		Data   *v1.BlocksInfo
	}
	testutil.Ok(t, json.Unmarshal(body, &data))
	testutil.Equals(t, "success", data.Status)
	// Filtered by time and relabel, result only one blocks.
	testutil.Equals(t, 1, len(data.Data.Blocks))
	testutil.Equals(t, data.Data.Blocks[0].MaxTime, blocks[0].maxt)
	testutil.Equals(t, data.Data.Blocks[0].MinTime, blocks[0].mint)
	testutil.Equals(t, data.Data.Blocks[0].Thanos.Labels, blocks[0].extLset.Map())
}
