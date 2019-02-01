package e2e_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/promclient"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

type testConfig struct {
	name  string
	suite *spinupSuite
}

var (
	firstPromPort = promHTTPPort(1)

	queryGossipSuite = newSpinupSuite().
				Add(scraper(1, defaultPromConfig("prom-"+firstPromPort, 0), true)).
				Add(scraper(2, defaultPromConfig("prom-ha", 0), true)).
				Add(scraper(3, defaultPromConfig("prom-ha", 1), true)).
				Add(querier(1, "replica"), queryCluster(1)).
				Add(querier(2, "replica"), queryCluster(2))

	queryStaticFlagsSuite = newSpinupSuite().
				Add(scraper(1, defaultPromConfig("prom-"+firstPromPort, 0), false)).
				Add(scraper(2, defaultPromConfig("prom-ha", 0), false)).
				Add(scraper(3, defaultPromConfig("prom-ha", 1), false)).
				Add(querierWithStoreFlags(1, "replica", sidecarGRPC(1), sidecarGRPC(2), sidecarGRPC(3)), "").
				Add(querierWithStoreFlags(2, "replica", sidecarGRPC(1), sidecarGRPC(2), sidecarGRPC(3)), "")

	queryFileSDSuite = newSpinupSuite().
				Add(scraper(1, defaultPromConfig("prom-"+firstPromPort, 0), false)).
				Add(scraper(2, defaultPromConfig("prom-ha", 0), false)).
				Add(scraper(3, defaultPromConfig("prom-ha", 1), false)).
				Add(querierWithFileSD(1, "replica", sidecarGRPC(1), sidecarGRPC(2), sidecarGRPC(3)), "").
				Add(querierWithFileSD(2, "replica", sidecarGRPC(1), sidecarGRPC(2), sidecarGRPC(3)), "")
)

func TestQuery(t *testing.T) {
	for _, tt := range []testConfig{
		{
			"gossip",
			queryGossipSuite,
		},
		{
			"staticFlag",
			queryStaticFlagsSuite,
		},
		{
			"fileSD",
			queryFileSDSuite,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testQuerySimple(t, tt)
		})
	}
}

// testQuerySimple runs a setup of Prometheus servers, sidecars, and query nodes and verifies that
// queries return data merged from all Prometheus servers. Additionally it verifies if deduplication works for query.
func testQuerySimple(t *testing.T, conf testConfig) {
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

	// Try query without deduplication.
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		var err error
		res, err = promclient.QueryInstant(ctx, nil, urlParse(t, "http://"+queryHTTP(1)), "up", time.Now(), false)
		if err != nil {
			return err
		}
		if len(res) != 3 {
			return errors.Errorf("unexpected result size %d", len(res))
		}
		return nil
	}))

	// In our model result are always sorted.
	testutil.Equals(t, model.Metric{
		"__name__":   "up",
		"instance":   model.LabelValue(promHTTP(1)),
		"job":        "prometheus",
		"prometheus": model.LabelValue("prom-" + promHTTPPort(1)),
		"replica":    model.LabelValue("0"),
	}, res[0].Metric)
	testutil.Equals(t, model.Metric{
		"__name__":   "up",
		"instance":   model.LabelValue(promHTTP(1)),
		"job":        "prometheus",
		"prometheus": "prom-ha",
		"replica":    model.LabelValue("0"),
	}, res[1].Metric)
	testutil.Equals(t, model.Metric{
		"__name__":   "up",
		"instance":   model.LabelValue(promHTTP(1)),
		"job":        "prometheus",
		"prometheus": "prom-ha",
		"replica":    model.LabelValue("1"),
	}, res[2].Metric)

	// Try query with deduplication.
	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case <-exit:
			cancel()
			return nil
		default:
		}

		var err error
		res, err = promclient.QueryInstant(ctx, nil, urlParse(t, "http://"+queryHTTP(1)), "up", time.Now(), true)
		if err != nil {
			return err
		}
		if len(res) != 2 {
			return errors.Errorf("unexpected result size for query with deduplication %d", len(res))
		}

		return nil
	}))

	testutil.Equals(t, model.Metric{
		"__name__":   "up",
		"instance":   model.LabelValue(promHTTP(1)),
		"job":        "prometheus",
		"prometheus": model.LabelValue("prom-" + promHTTPPort(1)),
	}, res[0].Metric)
	testutil.Equals(t, model.Metric{
		"__name__":   "up",
		"instance":   model.LabelValue(promHTTP(1)),
		"job":        "prometheus",
		"prometheus": "prom-ha",
	}, res[1].Metric)
}

func urlParse(t *testing.T, addr string) *url.URL {
	u, err := url.Parse(addr)
	testutil.Ok(t, err)

	return u
}

func defaultPromConfig(name string, replicas int) string {
	return fmt.Sprintf(`
global:
  external_labels:
    prometheus: %s
    replica: %v
scrape_configs:
- job_name: prometheus
  scrape_interval: 1s
  static_configs:
  - targets:
    - "localhost:%s"
`, name, replicas, firstPromPort)
}
