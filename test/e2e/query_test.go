package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

// TestQuerySimple runs a setup of Prometheus servers, sidecars, and query nodes and verifies that
// queries return data merged from all Prometheus servers. Additionally it verifies if deduplication works for query.
func TestQuerySimple(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_query_simple")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	unexpectedExit, err := spinup(t, ctx, config{
		promConfigFns: []configConstructor{
			func(port int) string {
				// Self scraping config with unique external label.
				return fmt.Sprintf(`
global:
  external_labels:
    prometheus: prom-%d
scrape_configs:
- job_name: prometheus
  scrape_interval: 1s
  static_configs:
  - targets:
    - "localhost:%d"
`, port, port)
			},
			func(port int) string {
				// Self scraping config for first of two HA replica Prometheus.
				return fmt.Sprintf(`
global:
  external_labels:
    prometheus: prom-ha
scrape_configs:
- job_name: prometheus
  scrape_interval: 1s
  static_configs:
  - targets:
    - "localhost:%d"
`, port)
			},
			func(port int) string {
				// Self scraping config for second of two HA replica Prometheus.
				return fmt.Sprintf(`
global:
  external_labels:
    prometheus: prom-ha
scrape_configs:
- job_name: prometheus
  scrape_interval: 1s
  static_configs:
  - targets:
    - "localhost:%d"
`, port)
			},
		},
		workDir:    dir,
		numQueries: 2,
		// instance is added by default as address of the scrape target.
		queriesReplicaLabel: "instance",
	})
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		return
	}

	// Try query without deduplication.
	err = runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case err := <-unexpectedExit:
			t.Errorf("Some process exited unexpectedly: %v", err)
			return nil
		default:
		}

		res, err := queryPrometheus(ctx, "http://"+queryHTTP(1), time.Now(), "up", false)
		if err != nil {
			return err
		}
		if len(res) != 3 {
			return errors.Errorf("unexpected result size %d", len(res))
		}

		// In our model result are always sorted.
		match := reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   model.LabelValue(promHTTP(1)),
			"job":        "prometheus",
			"prometheus": model.LabelValue("prom-" + promHTTPPort(1)),
		}, res[0].Metric)
		match = match && reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   model.LabelValue(promHTTP(2)),
			"job":        "prometheus",
			"prometheus": "prom-ha",
		}, res[1].Metric)
		match = match && reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   model.LabelValue(promHTTP(3)),
			"job":        "prometheus",
			"prometheus": "prom-ha",
		}, res[2].Metric)

		if !match {
			return errors.New("metrics mismatch, retrying...")
		}
		return nil
	})
	testutil.Ok(t, err)

	// Try query with deduplication.
	err = runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case err := <-unexpectedExit:
			t.Errorf("Some process exited unexpectedly: %v", err)
			return nil
		default:
		}

		res, err := queryPrometheus(ctx, "http://"+queryHTTP(1), time.Now(), "up", true)
		if err != nil {
			return err
		}
		if len(res) != 2 {
			return errors.Errorf("unexpected result size for query with deduplication %d", len(res))
		}

		match := reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"job":        "prometheus",
			"prometheus": model.LabelValue("prom-" + promHTTPPort(1)),
		}, res[0].Metric)
		match = match && reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"job":        "prometheus",
			"prometheus": "prom-ha",
		}, res[1].Metric)

		if !match {
			return errors.New("metrics mismatch for query with deduplication, retrying...")
		}
		return nil
	})
	testutil.Ok(t, err)
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
	defer resp.Body.Close()

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

// safeWriter wraps an io.Writer and makes it thread safe.
type safeWriter struct {
	io.Writer
	mtx sync.Mutex
}

func (w *safeWriter) Write(b []byte) (int, error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	return w.Writer.Write(b)
}
