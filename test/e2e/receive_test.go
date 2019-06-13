package e2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/promclient"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

var (
	receiveHashringSuite = newSpinupSuite().
		Add(querierWithStoreFlags(1, "replica", remoteWriteReceiveGRPC(1), remoteWriteReceiveGRPC(2), remoteWriteReceiveGRPC(3))).
		Add(receiver(1, defaultPromRemoteWriteConfig(nodeExporterHTTP(1), remoteWriteEndpoint(1)), remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3))).
		Add(receiver(2, defaultPromRemoteWriteConfig(nodeExporterHTTP(2), remoteWriteEndpoint(2)), remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3))).
		Add(receiver(3, defaultPromRemoteWriteConfig(nodeExporterHTTP(3), remoteWriteEndpoint(3)), remoteWriteEndpoint(1), remoteWriteEndpoint(2), remoteWriteEndpoint(3)))
)

func TestReceive(t *testing.T) {
	for _, tt := range []testConfig{
		{
			"hashring",
			receiveHashringSuite,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testReceive(t, tt)
		})
	}
}

// testReceive runs a setup of Prometheus servers, receive nodes, and query nodes and verifies that
// queries return data from the Prometheus servers. Additionally it verifies that remote-writes were routed through the correct receive node.
func testReceive(t *testing.T, conf testConfig) {
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

	w := log.NewSyncWriter(os.Stderr)
	l := log.NewLogfmtLogger(w)
	l = log.With(l, "conf-name", conf.name)

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

		expectedRes := 3
		if len(res) != expectedRes {
			return errors.Errorf("unexpected result size %d, expected %d", len(res), expectedRes)
		}

		return nil
	}))

	testutil.Equals(t, model.Metric{
		"__name__": "up",
		"instance": model.LabelValue(nodeExporterHTTP(1)),
		"job":      "node",
		"receive":  "true",
		"replica":  model.LabelValue("2"),
	}, res[0].Metric)
	testutil.Equals(t, model.Metric{
		"__name__": "up",
		"instance": model.LabelValue(nodeExporterHTTP(2)),
		"job":      "node",
		"receive":  "true",
		"replica":  model.LabelValue("3"),
	}, res[1].Metric)
	testutil.Equals(t, model.Metric{
		"__name__": "up",
		"instance": model.LabelValue(nodeExporterHTTP(3)),
		"job":      "node",
		"receive":  "true",
		"replica":  model.LabelValue("1"),
	}, res[2].Metric)
}
