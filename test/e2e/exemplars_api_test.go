// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	e2eobs "github.com/efficientgo/e2e/observable"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

const (
	traceIDLabel = "traceID"
)

func TestExemplarsAPI_Fanout(t *testing.T) {
	t.Parallel()
	var (
		prom1, prom2       *e2eobs.Observable
		sidecar1, sidecar2 *e2eobs.Observable
		err                error
		e                  *e2e.DockerEnvironment
	)

	e, err = e2e.NewDockerEnvironment("exemplars-fanout")
	testutil.Ok(t, err)
	t.Cleanup(e2ethanos.CleanScenario(t, e))

	qBuilder := e2ethanos.NewQuerierBuilder(e, "query")

	prom1, sidecar1 = e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom1",
		e2ethanos.DefaultPromConfig("ha", 0, "", "", "localhost:9090", qBuilder.InternalEndpoint("http"), e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(),
		"",
		e2ethanos.FeatureExemplarStorage,
	)
	prom2, sidecar2 = e2ethanos.NewPrometheusWithSidecar(
		e,
		"prom2",
		e2ethanos.DefaultPromConfig("ha", 1, "", "", "localhost:9090", qBuilder.InternalEndpoint("http"), e2ethanos.LocalPrometheusTarget),
		"",
		e2ethanos.DefaultPrometheusImage(),
		"",
		e2ethanos.FeatureExemplarStorage,
	)

	tracingCfg := fmt.Sprintf(`type: JAEGER
config:
  sampler_type: const
  sampler_param: 1
  service_name: %s`, qBuilder.Name())

	stores := []string{sidecar1.InternalEndpoint("grpc"), sidecar2.InternalEndpoint("grpc")}

	qBuilder = qBuilder.
		WithStoreAddresses(stores...).
		WithExemplarAddresses(stores...).
		WithTracingConfig(tracingCfg)

	q := qBuilder.Init()
	testutil.Ok(t, e2e.StartAndWaitReady(q))

	testutil.Ok(t, err)
	testutil.Ok(t, e2e.StartAndWaitReady(prom1, sidecar1, prom2, sidecar2))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Cleanup(cancel)

	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_store_nodes_grpc_connections"}, e2emon.WaitMissingMetrics()))
	testutil.Ok(t, q.WaitSumMetricsWithOptions(e2emon.Equals(2), []string{"thanos_query_exemplar_apis_dns_provider_results"}, e2emon.WaitMissingMetrics()))

	now := time.Now()
	start := timestamp.FromTime(now.Add(-time.Hour))
	end := timestamp.FromTime(now.Add(time.Hour))

	// Send HTTP requests to thanos query to trigger exemplars.
	labelNames(t, ctx, q.Endpoint("http"), nil, start, end, 0, func(res []string) bool { return true })

	t.Run("Basic exemplars query", func(t *testing.T) {
		queryExemplars(t, ctx, q.Endpoint("http"), `http_request_duration_seconds_bucket{handler="label_names"}`, start, end, exemplarsOnExpectedSeries(map[string]string{
			"__name__":   "http_request_duration_seconds_bucket",
			"handler":    "label_names",
			"job":        "myself",
			"method":     "get",
			"prometheus": "ha",
		}))
	})

	t.Run("Exemplars query with matched external label", func(t *testing.T) {
		// Here replica is an external label.
		queryExemplars(t, ctx, q.Endpoint("http"), `http_request_duration_seconds_bucket{handler="label_names", replica="0"}`, start, end, exemplarsOnExpectedSeries(map[string]string{
			"__name__":   "http_request_duration_seconds_bucket",
			"handler":    "label_names",
			"job":        "myself",
			"method":     "get",
			"prometheus": "ha",
		}))
	})

	t.Run("Exemplars query doesn't match external label", func(t *testing.T) {
		// Here replica is an external label, but it doesn't match.
		queryExemplars(t, ctx, q.Endpoint("http"), `http_request_duration_seconds_bucket{handler="label_names", replica="foo"}`,
			start, end, func(data []*exemplarspb.ExemplarData) error {
				if len(data) > 0 {
					return errors.Errorf("expected no examplers, got %v", data)
				}
				return nil
			})
	})
}

func exemplarsOnExpectedSeries(requiredSeriesLabels map[string]string) func(data []*exemplarspb.ExemplarData) error {
	return func(data []*exemplarspb.ExemplarData) error {
		if len(data) != 1 {
			return errors.Errorf("unexpected result size, expected 1, got: %v", len(data))
		}

		// Compare series labels.
		seriesLabels := labelpb.ZLabelSetsToPromLabelSets(data[0].SeriesLabels)
		for _, lbls := range seriesLabels {
			for k, v := range requiredSeriesLabels {
				if lbls.Get(k) != v {
					return errors.Errorf("unexpected labels in result, expected %v, got: %v", requiredSeriesLabels, seriesLabels)
				}
			}
		}

		// Make sure the exemplar contains the correct traceID label.
		for _, exemplar := range data[0].Exemplars {
			for _, lbls := range labelpb.ZLabelSetsToPromLabelSets(exemplar.Labels) {
				if !lbls.Has(traceIDLabel) {
					return errors.Errorf("unexpected labels in exemplar, expected %v, got: %v", traceIDLabel, exemplar.Labels)
				}
			}
		}
		return nil
	}
}
