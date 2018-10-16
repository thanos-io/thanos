package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

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
		res, err = queryPrometheus(ctx, "http://"+queryHTTP(1), time.Now(), "up", false)
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
		res, err = queryPrometheus(ctx, "http://"+queryHTTP(1), time.Now(), "up", true)
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

// queryPrometheus runs an instant query against the Prometheus HTTP v1 API.
func queryPrometheus(ctx context.Context, ustr string, ts time.Time, q string, dedup bool) (model.Vector, error) {
	u, err := url.Parse(ustr)
	if err != nil {
		return nil, err
	}
	args := url.Values{}
	args.Add("query", q)
	args.Add("time", ts.Format(time.RFC3339Nano))
	args.Add("dedup", fmt.Sprintf("%v", dedup))

	u.Path += "/api/v1/query"
	u.RawQuery = args.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(nil, resp.Body, "close body query")

	var m struct {
		Data struct {
			Result model.Vector `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m.Data.Result, nil
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
