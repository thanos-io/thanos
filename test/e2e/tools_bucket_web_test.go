// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"

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

	s, err := e2e.NewScenario("e2e_test_tools_bucket_web_external_prefix_and_route_prefix")
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
	)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(b))

	toolsBucketWebURL := mustURLParse(t, "http://"+b.HTTPEndpoint()+"/"+routePrefix)

	toolsBucketWebProxy := httptest.NewServer(e2ethanos.NewSingleHostReverseProxy(toolsBucketWebURL, externalPrefix))
	t.Cleanup(toolsBucketWebProxy.Close)

	checkNetworkRequests(t, toolsBucketWebProxy.URL+"/"+externalPrefix+"/blocks")
}
