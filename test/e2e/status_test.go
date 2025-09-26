// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/efficientgo/e2e"
	e2emon "github.com/efficientgo/e2e/monitoring"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/status/statuspb"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestStatus(t *testing.T) {
	t.Run("ha_ingestor_without_tenancy", func(t *testing.T) {
		/*
			The ha_ingestor_without_tenancy suite represents a configuration of a
			naive HA Thanos Receive with HA Prometheus.

			 ┌──────┐  ┌──────┬──────┐
			 │ Prom │  │      │ Prom │
			 └─┬────┴──┼────┐ └─────┬┘
			   │       │    │       │
			   │       │    │       │
			 ┌─▼───────▼┐ ┌─▼───────▼┐
			 │ Ingestor	│ │ Ingestor │
			 └───────┬──┘ └──┬───────┘
			         │       │
			        ┌▼───────▼┐
			        │  Query  │
			        └─────────┘
			NB: Made with asciiflow.com - you can copy & paste the above there to modify.
		*/
		t.Parallel()
		e, err := e2e.New(e2e.WithName("haingest-haprom"))
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		// 2 receivers * (10 series for test_metric1 + 2*5 scrape metrics).
		const seriesCount = 40

		// Setup Receives.
		r1 := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled().WithExpandedPostingsCache().Init()
		r2 := e2ethanos.NewReceiveBuilder(e, "2").WithIngestionEnabled().WithExpandedPostingsCache().Init()

		testutil.Ok(t, e2e.StartAndWaitReady(r1, r2))

		// Setup endpoints serving static metrics.
		// Each endpoint exposes the same metric but with label sets (e.g.
		// different series) and is scraped by a different Prometheus server.
		metrics1 := []byte(`
# HELP test_metric A test metric
# TYPE test_metric gauge
test_metric1{a="1", b="1"} 1
test_metric1{a="1", b="2"} 1
test_metric1{a="2", b="1"} 1
test_metric1{a="2", b="2"} 1
test_metric1{a="3", b="1"} 1
test_metric1{a="4", b="1"} 1`)
		static1 := e2emon.NewStaticMetricsServer(e, "static1", metrics1)
		metrics2 := []byte(`
# HELP test_metric A test metric
# TYPE test_metric gauge
test_metric1{a="3", b="1"} 1
test_metric1{a="4", b="1"} 1
test_metric1{a="4", b="2"} 1
test_metric1{a="4", b="3"} 1`)
		static2 := e2emon.NewStaticMetricsServer(e, "static2", metrics2)

		testutil.Ok(t, e2e.StartAndWaitReady(static1, static2))

		// Setup a pair of HA Prometheus which send metrics to both receivers.
		prom1a := e2ethanos.NewPrometheus(
			e, "1",
			e2ethanos.DefaultPromConfig(
				"prom1",
				0,
				e2ethanos.RemoteWriteEndpoints(r1.InternalEndpoint("remote-write"), r2.InternalEndpoint("remote-write")),
				"",
				static1.InternalEndpoint("http"),
			),
			"", e2ethanos.DefaultPrometheusImage())
		prom1b := e2ethanos.NewPrometheus(
			e, "2",
			e2ethanos.DefaultPromConfig(
				"prom1",
				1,
				e2ethanos.RemoteWriteEndpoints(r1.InternalEndpoint("remote-write"), r2.InternalEndpoint("remote-write")),
				"",
				static2.InternalEndpoint("http"),
			), "", e2ethanos.DefaultPrometheusImage())
		testutil.Ok(t, e2e.StartAndWaitReady(prom1a, prom1b))

		// Setup Thanos Query.
		q := e2ethanos.NewQuerierBuilder(e, "query", r1.InternalEndpoint("grpc"), r2.InternalEndpoint("grpc")).
			WithReplicaLabels("replica", "receive").
			Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		logger := log.NewLogfmtLogger(os.Stdout)
		testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {

			stats, err := promclient.NewDefaultClient().TSDBStatusInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), 100)
			if err != nil {
				return err
			}

			if err = assertHeadStatistics(stats, seriesCount); err != nil {
				return err
			}

			// Expect 6 metrics: test_metric1 + 5 Prometheus scrape metrics
			// (e.g. `up`, `scrape_duration_seconds`, etc.).
			if len(stats.SeriesCountByMetricName) != 6 {
				return errors.Errorf("expecting 6 metric names in SeriesCountByMetricName, got %d", len(stats.SeriesCountByMetricName))
			}

			// test_metric1 should be the metric with the highest number of series and
			// we expect 2*10 because each receiver should report 10 series.
			if err = statisticEqual(stats.SeriesCountByMetricName[0], "test_metric1", 20); err != nil {
				return errors.Wrap(err, "SeriesCountByMetricName[0]")
			}

			if err = statisticsContains(stats.LabelValueCountByLabelName, "a", 4); err != nil {
				return errors.Wrap(err, "LabelValueCountByLabelName")
			}

			if err = statisticsContains(stats.LabelValueCountByLabelName, "b", 3); err != nil {
				return errors.Wrap(err, "LabelValueCountByLabelName")
			}

			// This label/value pair is present for all series.
			if err = statisticsContains(stats.SeriesCountByLabelValuePair, "prometheus=prom1", seriesCount); err != nil {
				return errors.Wrap(err, "SeriesCountByLabelValuePair")
			}

			// This label/value pair is present for all series.
			if err = statisticsContains(stats.SeriesCountByLabelValuePair, "job=myself", seriesCount); err != nil {
				return errors.Wrap(err, "SeriesCountByLabelValuePair")
			}

			if err = statisticsContains(stats.SeriesCountByLabelValuePair, "a=1", 2*2); err != nil {
				return errors.Wrap(err, "SeriesCountByLabelValuePair")
			}

			if err = statisticsContains(stats.SeriesCountByLabelValuePair, "b=1", 2*6); err != nil {
				return errors.Wrap(err, "SeriesCountByLabelValuePair")
			}

			return nil
		}))

		// Check with limit=1.
		testutil.Ok(t, runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
			stats, err := promclient.NewDefaultClient().TSDBStatusInGRPC(ctx, urlParse(t, "http://"+q.Endpoint("http")), 1)
			if err != nil {
				return err
			}

			if err = assertHeadStatistics(stats, seriesCount); err != nil {
				return err
			}

			if len(stats.SeriesCountByMetricName) != 1 {
				return errors.Errorf("expecting 1 metric name in SeriesCountByMetricName, got %d", len(stats.SeriesCountByMetricName))
			}

			return nil
		}))
	})

	t.Run("multitenancy", func(t *testing.T) {
		t.Parallel()

		e, err := e2e.NewDockerEnvironment("multitenancy")
		testutil.Ok(t, err)
		t.Cleanup(e2ethanos.CleanScenario(t, e))

		r1 := e2ethanos.NewReceiveBuilder(e, "1").WithIngestionEnabled()

		h := receive.HashringConfig{
			Endpoints: []receive.Endpoint{
				{Address: r1.InternalEndpoint("grpc")},
			},
		}

		// Create with hashring config.
		r1Runnable := r1.WithRouting(1, h).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(r1Runnable))

		// Setup endpoints serving static metrics.
		// Each endpoint exposes the same metric but with label sets (e.g.
		// different series) and is scraped by a different Prometheus server.
		metrics1 := []byte(`
# HELP test_metric A test metric
# TYPE test_metric gauge
test_metric1{a="1", b="1"} 1
test_metric1{a="1", b="2"} 1
test_metric1{a="2", b="1"} 1
test_metric1{a="2", b="2"} 1
test_metric1{a="3", b="1"} 1
test_metric1{a="4", b="1"} 1`)
		static1 := e2emon.NewStaticMetricsServer(e, "static1", metrics1)
		metrics2 := []byte(`
# HELP test_metric A test metric
# TYPE test_metric gauge
test_metric1{a="3", b="1"} 1
test_metric1{a="4", b="1"} 1
test_metric1{a="4", b="2"} 1
test_metric1{a="4", b="3"} 1`)
		static2 := e2emon.NewStaticMetricsServer(e, "static2", metrics2)
		testutil.Ok(t, e2e.StartAndWaitReady(static1, static2))

		const (
			tenant1 = "tenant-1"
			tenant2 = "tenant-2"
			// tenant-1 sends 5 test_metric1 series + 5 Prometheus scrape metrics.
			seriesCountTenant1 = 11
			// tenant-2 sends 4 test_metric1 series + 5 Prometheus scrape metrics.
			seriesCountTenant2 = 9
		)
		rp1 := e2ethanos.NewReverseProxy(e, "rp1", tenant1, "http://"+r1.InternalEndpoint("remote-write"))
		rp2 := e2ethanos.NewReverseProxy(e, "rp2", tenant2, "http://"+r1.InternalEndpoint("remote-write"))
		testutil.Ok(t, e2e.StartAndWaitReady(rp1, rp2))

		prom1 := e2ethanos.NewPrometheus(
			e,
			"1",
			e2ethanos.DefaultPromConfig(
				"prom1",
				0,
				"http://"+rp1.InternalEndpoint("http")+"/api/v1/receive",
				"",
				static1.InternalEndpoint("http"),
			),
			"",
			e2ethanos.DefaultPrometheusImage(),
		)
		prom2 := e2ethanos.NewPrometheus(
			e,
			"2",
			e2ethanos.DefaultPromConfig(
				"prom2",
				0,
				"http://"+rp2.InternalEndpoint("http")+"/api/v1/receive",
				"",
				static2.InternalEndpoint("http"),
			),
			"",
			e2ethanos.DefaultPrometheusImage(),
		)
		testutil.Ok(t, e2e.StartAndWaitReady(prom1, prom2))

		// Start Thanos query with enforced tenancy.
		q := e2ethanos.NewQuerierBuilder(e, "q1", r1.InternalEndpoint("grpc")).WithTenancy(true).Init()
		testutil.Ok(t, e2e.StartAndWaitReady(q))

		// Start 2 query proxies, one for each tenant.
		qp1 := e2ethanos.NewReverseProxy(e, "qp1", tenant1, "http://"+q.InternalEndpoint("http"))
		qp2 := e2ethanos.NewReverseProxy(e, "qp2", tenant2, "http://"+q.InternalEndpoint("http"))
		testutil.Ok(t, e2e.StartAndWaitReady(qp1, qp2))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)

		logger := log.NewLogfmtLogger(os.Stdout)
		t.Run(tenant1, func(t *testing.T) {
			t.Parallel()

			testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {
				stats, err := promclient.NewDefaultClient().TSDBStatusInGRPC(ctx, urlParse(t, "http://"+qp1.Endpoint("http")), 100)
				if err != nil {
					return err
				}

				if err = assertHeadStatistics(stats, seriesCountTenant1); err != nil {
					return err
				}

				// Expect 6 metrics: test_metric1 + 5 Prometheus scrape metrics
				// (e.g. `up`, `scrape_duration_seconds`, etc.).
				if len(stats.SeriesCountByMetricName) != 6 {
					return errors.Errorf("expecting 6 metric names in SeriesCountByMetricName, got %d", len(stats.SeriesCountByMetricName))
				}

				// test_metric1 should be the metric with the highest number of series and
				// we expect the 6 series exposed by static1.
				if err = statisticEqual(stats.SeriesCountByMetricName[0], "test_metric1", 6); err != nil {
					return errors.Wrap(err, "SeriesCountByMetricName[0]")
				}

				if err = statisticsContains(stats.LabelValueCountByLabelName, "a", 4); err != nil {
					return errors.Wrap(err, "LabelValueCountByLabelName")
				}

				if err = statisticsContains(stats.LabelValueCountByLabelName, "b", 2); err != nil {
					return errors.Wrap(err, "LabelValueCountByLabelName")
				}

				// This label/value pair is present for all series.
				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "prometheus=prom1", seriesCountTenant1); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				// This label/value pair is present for all series.
				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "job=myself", seriesCountTenant1); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "a=1", 2); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "a=4", 1); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "b=1", 4); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				return nil
			}))
		})

		t.Run(tenant2, func(t *testing.T) {
			t.Parallel()

			testutil.Ok(t, runutil.RetryWithLog(logger, 5*time.Second, ctx.Done(), func() error {
				stats, err := promclient.NewDefaultClient().TSDBStatusInGRPC(ctx, urlParse(t, "http://"+qp2.Endpoint("http")), 100)
				if err != nil {
					return err
				}

				if err = assertHeadStatistics(stats, seriesCountTenant2); err != nil {
					return err
				}

				// Expect 6 metrics: test_metric1 + 5 Prometheus scrape metrics
				// (e.g. `up`, `scrape_duration_seconds`, etc.).
				if len(stats.SeriesCountByMetricName) != 6 {
					return errors.Errorf("expecting 6 metric names in SeriesCountByMetricName, got %d", len(stats.SeriesCountByMetricName))
				}

				// test_metric1 should be the metric with the highest number of series and
				// we expect the 4 series exposed by static2.
				if err = statisticEqual(stats.SeriesCountByMetricName[0], "test_metric1", 4); err != nil {
					return errors.Wrap(err, "SeriesCountByMetricName[0]")
				}

				if err = statisticsContains(stats.LabelValueCountByLabelName, "a", 2); err != nil {
					return errors.Wrap(err, "LabelValueCountByLabelName")
				}

				if err = statisticsContains(stats.LabelValueCountByLabelName, "b", 3); err != nil {
					return errors.Wrap(err, "LabelValueCountByLabelName")
				}

				// This label/value pair is present for all series.
				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "prometheus=prom2", seriesCountTenant2); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				// This label/value pair is present for all series.
				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "job=myself", seriesCountTenant2); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "a=4", 3); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				if err = statisticsContains(stats.SeriesCountByLabelValuePair, "b=1", 2); err != nil {
					return errors.Wrap(err, "SeriesCountByLabelValuePair")
				}

				return nil
			}))
		})
	})
}

// statisticsContains checks that the (name,value) tuple exists in the stats slice.
func statisticsContains(stats []statuspb.Statistic, name string, value uint64) error {
	for _, stat := range stats {
		if stat.Name == name {
			if stat.Value != value {
				return errors.Errorf("%s: expecting %d got %d", name, value, stat.Value)
			}
			return nil
		}
	}

	return errors.Errorf("%s: not found", name)
}

// statisticEqual checks that the given stat matches the (name,value) tuple.
func statisticEqual(stat statuspb.Statistic, name string, value uint64) error {
	if stat.Name != name {
		return errors.Errorf("expecting name %q, got %q", name, stat.Name)
	}

	if stat.Value != value {
		return errors.Errorf("expecting value %d for name %q, got %d", value, name, stat.Value)
	}

	return nil
}

func assertHeadStatistics(stats *statuspb.TSDBStatisticsEntry, numSeries int) error {
	if stats.HeadStatistics.NumSeries < uint64(numSeries) {
		return errors.Errorf("expected at least %d series, got %d", numSeries, stats.HeadStatistics.NumSeries)
	}

	if stats.HeadStatistics.MinTime <= 0 {
		return errors.Errorf("expected a positive minTime, got %d", stats.HeadStatistics.MinTime)
	}

	if stats.HeadStatistics.MaxTime <= 0 {
		return errors.Errorf("expected a positive maxTime, got %d", stats.HeadStatistics.MaxTime)
	}

	return nil
}
