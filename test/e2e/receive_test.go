// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/receive"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type receiveTestConfig struct {
	name    string
	cmds    []scheduler
	metrics []model.Metric

	queryAddress address
}

func TestReceive(t *testing.T) {
	var testCases []receiveTestConfig

	a := newLocalAddresser()
	{
		// The hashring suite creates three receivers, each with a Prometheus
		// remote-writing data to it. However, due to the hashing of the labels,
		// the time series from the Prometheus is forwarded to a different
		// receiver in the hashring than the one handling the request.
		// The querier queries all the receivers and the test verifies
		// the time series are forwarded to the correct receive node.
		receiveGRPC1, receiveGRPC2, receiveGRPC3 := a.New(), a.New(), a.New()

		h := receive.HashringConfig{
			Endpoints: []string{receiveGRPC1.HostPort(), receiveGRPC2.HostPort(), receiveGRPC3.HostPort()},
		}

		r1 := receiver(a.New(), receiveGRPC1, a.New(), 1, h)
		r2 := receiver(a.New(), receiveGRPC2, a.New(), 1, h)
		r3 := receiver(a.New(), receiveGRPC3, a.New(), 1, h)

		prom1 := prometheus(a.New(), defaultPromConfig("prom1", 1, remoteWriteEndpoint(r1.HTTP)))
		prom2 := prometheus(a.New(), defaultPromConfig("prom2", 1, remoteWriteEndpoint(r2.HTTP)))
		prom3 := prometheus(a.New(), defaultPromConfig("prom3", 1, remoteWriteEndpoint(r3.HTTP)))

		q1 := querier(a.New(), a.New(), []address{r1.GRPC, r2.GRPC, r3.GRPC}, nil)

		testCases = append(testCases, receiveTestConfig{
			name:         "hashring",
			cmds:         []scheduler{q1, prom1, prom2, prom3, r1, r2, r3},
			queryAddress: q1.HTTP,
			metrics: []model.Metric{
				{
					"job":        "test",
					"prometheus": "prom1",
					"receive":    model.LabelValue(r1.HTTP.Port),
					"replica":    "1",
				},
				{
					"job":        "test",
					"prometheus": "prom2",
					"receive":    model.LabelValue(r2.HTTP.Port),
					"replica":    "1",
				},
				{
					"job":        "test",
					"prometheus": "prom3",
					"receive":    model.LabelValue(r2.HTTP.Port),
					"replica":    "1",
				},
			}})
	}
	{
		// The replication suite creates three receivers but only one
		// receives Prometheus remote-written data. The querier queries all
		// receivers and the test verifies that the time series are
		// replicated to all of the nodes.
		receiveGRPC1, receiveGRPC2, receiveGRPC3 := a.New(), a.New(), a.New()

		h := receive.HashringConfig{
			Endpoints: []string{receiveGRPC1.HostPort(), receiveGRPC2.HostPort(), receiveGRPC3.HostPort()},
		}

		r1 := receiver(a.New(), receiveGRPC1, a.New(), 3, h)
		r2 := receiver(a.New(), receiveGRPC2, a.New(), 3, h)
		r3 := receiver(a.New(), receiveGRPC3, a.New(), 3, h)

		prom1 := prometheus(a.New(), defaultPromConfig("prom1", 1, remoteWriteEndpoint(r1.HTTP)))

		q1 := querier(a.New(), a.New(), []address{r1.GRPC, r2.GRPC, r3.GRPC}, nil)

		testCases = append(testCases, receiveTestConfig{
			name:         "replication",
			cmds:         []scheduler{q1, prom1, r1, r2, r3},
			queryAddress: q1.HTTP,
			metrics: []model.Metric{
				{
					"job":        "test",
					"prometheus": "prom1",
					"receive":    model.LabelValue(r1.HTTP.Port),
					"replica":    "1",
				},
				{
					"job":        "test",
					"prometheus": "prom1",
					"receive":    model.LabelValue(r2.HTTP.Port),
					"replica":    "1",
				},
				{
					"job":        "test",
					"prometheus": "prom1",
					"receive":    model.LabelValue(r3.HTTP.Port),
					"replica":    "1",
				},
			}})
	}
	{
		// The replication suite creates a three-node hashring but one of the
		// receivers is dead. In this case, replication should still
		// succeed and the time series should be replicated to the other nodes.
		receiveGRPC1, receiveGRPC2, receiveGRPC3 := a.New(), a.New(), a.New()

		h := receive.HashringConfig{
			Endpoints: []string{receiveGRPC1.HostPort(), receiveGRPC2.HostPort(), receiveGRPC3.HostPort()},
		}

		r1 := receiver(a.New(), receiveGRPC1, a.New(), 3, h)
		r2 := receiver(a.New(), receiveGRPC2, a.New(), 3, h)

		prom1 := prometheus(a.New(), defaultPromConfig("prom1", 1, remoteWriteEndpoint(r1.HTTP)))

		q1 := querier(a.New(), a.New(), []address{r1.GRPC, r2.GRPC}, nil)

		testCases = append(testCases, receiveTestConfig{
			name:         "replication with outage",
			cmds:         []scheduler{q1, prom1, r1, r2},
			queryAddress: q1.HTTP,
			metrics: []model.Metric{
				{
					"job":        "test",
					"prometheus": "prom1",
					"receive":    model.LabelValue(r1.HTTP.Port),
					"replica":    "1",
				},
				{
					"job":        "test",
					"prometheus": "prom1",
					"receive":    model.LabelValue(r2.HTTP.Port),
					"replica":    "1",
				},
			}})
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			testReceive(t, tt)
		})
	}
}

// testReceive runs a setup of Prometheus servers, receive nodes, and query nodes and verifies that
// queries return data from the Prometheus servers. Additionally it verifies that remote-writes were routed through the correct receive node.
func testReceive(t *testing.T, conf receiveTestConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

	exit, err := e2eSpinup(t, ctx, conf.cmds...)
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		cancel()
		return
	}

	defer func() {
		cancel()
		<-exit
	}()

	var res model.Vector

	// Query without deduplication so we can check what replica the
	// time series ended up on.
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		var (
			err      error
			warnings []string
		)
		res, warnings, err = promclient.QueryInstant(ctx, nil, urlParse(t, conf.queryAddress.URL()), queryUpWithoutInstance, time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		if len(res) != len(conf.metrics) {
			return errors.Errorf("unexpected result size, expected %d; result: %v", len(conf.metrics), res)
		}

		return nil
	}))

	select {
	case <-exit:
		return
	default:
	}

	sortResults(res)
	for i, metric := range conf.metrics {
		testutil.Equals(t, metric, res[i].Metric)
	}
}
