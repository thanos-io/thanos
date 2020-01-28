// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

// NOTE: by using aggregation all results are now unsorted.
const queryUpWithoutInstance = "sum(up) without (instance)"

// defaultPromConfig returns Prometheus config that sets Prometheus to:
// * expose 2 external labels, source and replica.
// * scrape fake target. This will produce up == 0 metric which we can assert on.
// * optionally remote write endpoint to write into.
func defaultPromConfig(name string, replica int, remoteWriteEndpoint string) string {
	config := fmt.Sprintf(`
global:
  external_labels:
    prometheus: %v
    replica: %v
scrape_configs:
- job_name: 'test'
  static_configs:
  - targets: ['fake']
`, name, replica)

	if remoteWriteEndpoint != "" {
		config = fmt.Sprintf(`
%s
remote_write:
- url: "%s"
`, config, remoteWriteEndpoint)
	}
	return config
}

func sortResults(res model.Vector) {
	sort.Slice(res, func(i, j int) bool {
		return res[i].String() < res[j].String()
	})
}

func TestQuery(t *testing.T) {
	a := newLocalAddresser()

	// Thanos Receive.
	r := receiver(a.New(), a.New(), a.New(), 1)

	// Prometheus-es.
	prom1 := prometheus(a.New(), defaultPromConfig("prom-alone", 0, ""))
	prom2 := prometheus(a.New(), defaultPromConfig("prom-both-remote-write-and-sidecar", 1234, remoteWriteEndpoint(r.HTTP)))
	prom3 := prometheus(a.New(), defaultPromConfig("prom-ha", 0, ""))
	prom4 := prometheus(a.New(), defaultPromConfig("prom-ha", 1, ""))

	// Sidecars per each Prometheus.
	s1 := sidecar(a.New(), a.New(), prom1)
	s2 := sidecar(a.New(), a.New(), prom2)
	s3 := sidecar(a.New(), a.New(), prom3)
	s4 := sidecar(a.New(), a.New(), prom4)

	// Querier. Both fileSD and directly by flags.
	q := querier(a.New(), a.New(), []address{s1.GRPC, s2.GRPC, r.GRPC}, []address{s3.GRPC, s4.GRPC})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

	exit, err := e2eSpinup(t, ctx, r, prom1, prom2, prom3, prom4, s1, s2, s3, s4, q)
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

	// Try query without deduplication.
	testutil.Ok(t, runutil.RetryWithLog(l, time.Second, ctx.Done(), func() error {
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
		res, warnings, err = promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), queryUpWithoutInstance, time.Now(), promclient.QueryOptions{
			Deduplicate: false,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		expectedRes := 5
		if len(res) != expectedRes {
			return errors.Errorf("unexpected result size, expected %d; result: %v", expectedRes, res)
		}
		return nil
	}))

	select {
	case <-exit:
		return
	default:
	}

	sortResults(res)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-alone",
		"replica":    "0",
	}, res[0].Metric)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-both-remote-write-and-sidecar",
		"receive":    model.LabelValue(r.HTTP.Port),
		"replica":    model.LabelValue("1234"),
	}, res[1].Metric)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-both-remote-write-and-sidecar",
		"replica":    model.LabelValue("1234"),
	}, res[2].Metric)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-ha",
		"replica":    model.LabelValue("0"),
	}, res[3].Metric)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-ha",
		"replica":    model.LabelValue("1"),
	}, res[4].Metric)

	// With deduplication.
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
		res, warnings, err = promclient.QueryInstant(ctx, nil, urlParse(t, q.HTTP.URL()), queryUpWithoutInstance, time.Now(), promclient.QueryOptions{
			Deduplicate: true,
		})
		if err != nil {
			return err
		}

		if len(warnings) > 0 {
			// we don't expect warnings.
			return errors.Errorf("unexpected warnings %s", warnings)
		}

		expectedRes := 4
		if len(res) != expectedRes {
			return errors.Errorf("unexpected result size, expected %d; result: %v", expectedRes, res)
		}

		return nil
	}))

	select {
	case <-exit:
		return
	default:
	}

	sortResults(res)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-alone",
	}, res[0].Metric)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-both-remote-write-and-sidecar",
		"receive":    model.LabelValue(r.HTTP.Port),
	}, res[1].Metric)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-both-remote-write-and-sidecar",
	}, res[2].Metric)
	testutil.Equals(t, model.Metric{
		"job":        "test",
		"prometheus": "prom-ha",
	}, res[3].Metric)
}

func urlParse(t *testing.T, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)

	return u
}
