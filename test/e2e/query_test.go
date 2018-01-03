package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"reflect"

	"fmt"

	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

// TestQuerySimple runs a setup of Prometheus servers, sidecars, and query nodes and verifies that
// queries return data merged from all Prometheus servers.
func TestQuerySimple(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_query_simple")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
	defer cancel()

	unexpectedExit, err := spinup(t, ctx, config{
		promConfigFn: func(port int) string {
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
		workDir:       dir,
		numPrometheus: 3,
		numQueries:    2,
	})
	if err != nil {
		t.Errorf("spinup failed: %v", err)
		return
	}

	err = runutil.Retry(time.Second, ctx.Done(), func() error {
		select {
		case err := <-unexpectedExit:
			t.Errorf("Some process exited unexpectedly: %v", err)
			return nil
		default:
		}

		res, err := queryPrometheus(ctx, "http://"+queryHTTP(1), time.Now(), "up")
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
			"prometheus": "prom-9091",
		}, res[0].Metric)
		match = match && reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   model.LabelValue(promHTTP(2)),
			"job":        "prometheus",
			"prometheus": "prom-9092",
		}, res[1].Metric)
		match = match && reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   model.LabelValue(promHTTP(3)),
			"job":        "prometheus",
			"prometheus": "prom-9093",
		}, res[2].Metric)

		if !match {
			return errors.New("metrics mismatch, retrying...")
		}
		return nil
	})
	testutil.Ok(t, err)
}

// queryPrometheus runs an instant query against the Prometheus HTTP v1 API.
func queryPrometheus(ctx context.Context, ustr string, ts time.Time, q string) (model.Vector, error) {
	u, err := url.Parse(ustr)
	if err != nil {
		return nil, err
	}
	args := url.Values{}
	args.Add("query", q)
	args.Add("time", ts.Format(time.RFC3339Nano))

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
