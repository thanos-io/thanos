package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

var (
	// The hashring suite creates three receivers, each with a Prometheus
	// remote-writing data to it. However, due to the hashing of the labels,
	// the time series from the Prometheus is forwarded to a different
	// receiver in the hashring than the one handling the request.
	// The querier queries all the receivers and the test verifies
	// the time series are forwarded to the correct receive node.
	receiveHashringSuite = newSpinupSuite().
				Add(querierWithStoreFlags(1, "replica", remoteWriteReceiveGRPC(1), remoteWriteReceiveGRPC(2), remoteWriteReceiveGRPC(3))).
				Add(receiver(1, defaultPromRemoteWriteConfig(nodeExporterHTTP(1), remoteWriteEndpoint(1)), 1, remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3))).
				Add(receiver(2, defaultPromRemoteWriteConfig(nodeExporterHTTP(2), remoteWriteEndpoint(2)), 1, remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3))).
				Add(receiver(3, defaultPromRemoteWriteConfig(nodeExporterHTTP(3), remoteWriteEndpoint(3)), 1, remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3)))
	receiveHashringMetrics = []model.Metric{
		{
			"__name__": "up",
			"instance": model.LabelValue(nodeExporterHTTP(1)),
			"job":      "node",
			"receive":  "true",
			"replica":  model.LabelValue("2"),
		},
		{
			"__name__": "up",
			"instance": model.LabelValue(nodeExporterHTTP(2)),
			"job":      "node",
			"receive":  "true",
			"replica":  model.LabelValue("3"),
		},
		{
			"__name__": "up",
			"instance": model.LabelValue(nodeExporterHTTP(3)),
			"job":      "node",
			"receive":  "true",
			"replica":  model.LabelValue("1"),
		},
	}
	// The replication suite creates three receivers but only one
	// Prometheus that remote-writes data. The querier queries all
	// receivers and the test verifies that the time series are
	// replicated to all of the nodes.
	receiveReplicationSuite = newSpinupSuite().
				Add(querierWithStoreFlags(1, "replica", remoteWriteReceiveGRPC(1), remoteWriteReceiveGRPC(2), remoteWriteReceiveGRPC(3))).
				Add(receiver(1, defaultPromRemoteWriteConfig(nodeExporterHTTP(1), remoteWriteEndpoint(1)), 3, remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3))).
				Add(receiver(2, defaultPromConfig("no-remote-write", 2), 3, remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3))).
				Add(receiver(3, defaultPromConfig("no-remote-write", 3), 3, remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3)))
	receiveReplicationMetrics = []model.Metric{
		{
			"__name__": "up",
			"instance": model.LabelValue(nodeExporterHTTP(1)),
			"job":      "node",
			"receive":  "true",
			"replica":  model.LabelValue("1"),
		},
		{
			"__name__": "up",
			"instance": model.LabelValue(nodeExporterHTTP(1)),
			"job":      "node",
			"receive":  "true",
			"replica":  model.LabelValue("2"),
		},
		{
			"__name__": "up",
			"instance": model.LabelValue(nodeExporterHTTP(1)),
			"job":      "node",
			"receive":  "true",
			"replica":  model.LabelValue("3"),
		},
	}
)

type receiveTestConfig struct {
	testConfig
	metrics []model.Metric
}

func TestReceive(t *testing.T) {
	for _, tt := range []receiveTestConfig{
		{
			testConfig{
				"hashring",
				receiveHashringSuite,
			},
			receiveHashringMetrics,
		},
		{
			testConfig{
				"replication",
				receiveReplicationSuite,
			},
			receiveReplicationMetrics,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testReceive(t, tt)
		})
	}
}

// testReceive runs a setup of Prometheus servers, receive nodes, and query nodes and verifies that
// queries return data from the Prometheus servers. Additionally it verifies that remote-writes were routed through the correct receive node.
func testReceive(t *testing.T, conf receiveTestConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

	exit, err := conf.suite.Exec(t, ctx, conf.name)
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
		res, warnings, err = promclient.QueryInstant(ctx, nil, urlParse(t, "http://"+queryHTTP(1)), "up", time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		expectedRes := len(conf.metrics)
		if len(res) != expectedRes {
			return errors.Errorf("unexpected result size %d, expected %d", len(res), expectedRes)
		}

		return nil
	}))

	for i, metric := range conf.metrics {
		testutil.Equals(t, metric, res[i].Metric)
	}
}
