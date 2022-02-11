// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestInfo(t *testing.T) {
	t.Parallel()

	e, err := e2e.NewDockerEnvironment("e2e_test_info")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	prom1, sidecar1, err := e2ethanos.NewPrometheusWithSidecar(e, "alone1", defaultPromConfig("prom-alone1", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	prom2, sidecar2, err := e2ethanos.NewPrometheusWithSidecar(e, "alone2", defaultPromConfig("prom-alone2", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	prom3, sidecar3, err := e2ethanos.NewPrometheusWithSidecar(e, "alone3", defaultPromConfig("prom-alone3", 0, "", ""), "", e2ethanos.DefaultPrometheusImage(), "")
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2, prom3, sidecar3))

	const bucket = "info-api-test"
	m, err := e2ethanos.NewMinio(e, "thanos-minio", bucket)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(m))
	store, err := e2ethanos.NewStoreGW(
		e,
		"1",
		client.BucketConfig{
			Type:   client.S3,
			Config: e2ethanos.NewS3Config(bucket, m.InternalEndpoint("https"), e2ethanos.ContainerSharedDir),
		},
		"",
		nil,
	)
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(store))

	// Register `sidecar1` in all flags (i.e. '--store', '--rule', '--target', '--metadata', '--exemplar', '--endpoint') to verify
	// '--endpoint' flag works properly works together with other flags ('--target', '--metadata' etc.).
	// Register 2 sidecars and 1 storeGW using '--endpoint'.
	// Register `sidecar3` twice to verify it is deduplicated.
	q, err := e2ethanos.NewQuerierBuilder(e, "1", sidecar1.InternalEndpoint("grpc")).
		WithTargetAddresses(sidecar1.InternalEndpoint("grpc")).
		WithMetadataAddresses(sidecar1.InternalEndpoint("grpc")).
		WithExemplarAddresses(sidecar1.InternalEndpoint("grpc")).
		WithRuleAddresses(sidecar1.InternalEndpoint("grpc")).
		WithEndpoints(
			sidecar1.InternalEndpoint("grpc"),
			sidecar2.InternalEndpoint("grpc"),
			sidecar3.InternalEndpoint("grpc"),
			store.InternalEndpoint("grpc"),
		).
		Build()
	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	expected := map[string][]query.EndpointStatus{
		"sidecar": {
			{
				Name: "e2e_test_info-sidecar-alone1:9091",
				LabelSets: []labels.Labels{{
					{
						Name:  "prometheus",
						Value: "prom-alone1",
					},
					{
						Name:  "replica",
						Value: "0",
					},
				}},
			},
			{
				Name: "e2e_test_info-sidecar-alone2:9091",
				LabelSets: []labels.Labels{{
					{
						Name:  "prometheus",
						Value: "prom-alone2",
					},
					{
						Name:  "replica",
						Value: "0",
					},
				}},
			},
			{
				Name: "e2e_test_info-sidecar-alone3:9091",
				LabelSets: []labels.Labels{{
					{
						Name:  "prometheus",
						Value: "prom-alone3",
					},
					{
						Name:  "replica",
						Value: "0",
					},
				}},
			},
		},
		"store": {
			{
				Name:      "e2e_test_info-store-gw-1:9091",
				LabelSets: []labels.Labels{},
			},
		},
	}

	url := "http://" + path.Join(q.Endpoint("http"), "/api/v1/stores")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	err = runutil.Retry(time.Second, ctx.Done(), func() error {

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		body, err := ioutil.ReadAll(resp.Body)
		defer runutil.CloseWithErrCapture(&err, resp.Body, "response body close")

		var res struct {
			Data map[string][]query.EndpointStatus `json:"data"`
		}

		err = json.Unmarshal(body, &res)
		if err != nil {
			return err
		}

		if err = assertStoreStatus(t, "sidecar", res.Data, expected); err != nil {
			return err
		}

		if err = assertStoreStatus(t, "store", res.Data, expected); err != nil {
			return err
		}

		return nil
	})
	testutil.Ok(t, err)
}

func assertStoreStatus(t *testing.T, component string, res map[string][]query.EndpointStatus, expected map[string][]query.EndpointStatus) error {
	t.Helper()

	if len(res[component]) != len(expected[component]) {
		return fmt.Errorf("expected %d %s, got: %d", len(expected[component]), component, len(res[component]))
	}

	for i, v := range res[component] {
		// Set value of the fields which keep changing in every test run to their default value.
		v.MaxTime = 0
		v.MinTime = 0
		v.LastCheck = time.Time{}

		testutil.Equals(t, expected[component][i], v)
	}

	return nil
}
