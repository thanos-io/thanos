package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"reflect"

	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func selfScrapePromConfig(port int) string {
	return fmt.Sprintf(`
global:
  external_labels:
    prometheus: prom-%d
scrape_configs:
- job_name: prometheus
  scrape_interval: 5s
  static_configs:
  - targets:
    - "localhost:%d"
`, port, port)
}

type spinupConfig struct {
	promConfigFn func(port int) string
	workDir      string

	numPrometheus int
	numQueries    int
}

// NOTE: It is important to install Thanos before using this function to compile latest changes.
func spinup(t testing.TB, cfg spinupConfig) (close func()) {
	var commands []*exec.Cmd
	var closers []*exec.Cmd

	for i := 1; i <= cfg.numPrometheus; i++ {
		promDir := fmt.Sprintf("%s/data/prom%d", cfg.workDir, i)

		if err := os.MkdirAll(promDir, 0777); err != nil {
			return func() {}
		}
		f, err := os.Create(promDir + "/prometheus.yml")
		if err != nil {
			return func() {}
		}
		_, err = f.Write([]byte(cfg.promConfigFn(9090 + i)))
		f.Close()
		if err != nil {
			return func() {}
		}

		commands = append(commands, exec.Command("prometheus",
			"--config.file", promDir+"/prometheus.yml",
			"--storage.tsdb.path", promDir,
			"--log.level", "info",
			"--web.listen-address", fmt.Sprintf("0.0.0.0:%d", 9090+i),
		))
		commands = append(commands, exec.Command("thanos", "sidecar",
			"--debug.name", fmt.Sprintf("sidecar-%d", i),
			"--api-address", fmt.Sprintf("0.0.0.0:%d", 19090+i),
			"--metrics-address", fmt.Sprintf("0.0.0.0:%d", 19190+i),
			"--prometheus.url", fmt.Sprintf("http://localhost:%d", 9090+i),
			"--tsdb.path", promDir,
			"--cluster.address", fmt.Sprintf("0.0.0.0:%d", 19390+i),
			"--cluster.advertise-address", fmt.Sprintf("127.0.0.1:%d", 19390+i),
			"--cluster.peers", "127.0.0.1:19391",
		))
	}

	for i := 1; i <= cfg.numQueries; i++ {
		commands = append(commands, exec.Command("thanos", "query",
			"--debug.name", fmt.Sprintf("query-%d", i),
			"--api-address", fmt.Sprintf("0.0.0.0:%d", 19490+i),
			"--cluster.address", fmt.Sprintf("0.0.0.0:%d", 19590+i),
			"--cluster.advertise-address", fmt.Sprintf("127.0.0.1:%d", 19590+i),
			"--cluster.peers", "127.0.0.1:19391",
		))
	}

	var stderr bytes.Buffer
	stderrw := &safeWriter{Writer: &stderr}

	close = func() {
		for _, c := range closers {
			c.Process.Signal(syscall.SIGTERM)
			c.Wait()
		}
		t.Logf("STDERR\n %s", stderr.String())
	}
	for _, cmd := range commands {
		cmd.Stderr = stderrw

		if err := cmd.Start(); err != nil {
			close()
			return func() {}
		}
		closers = append(closers, cmd)
	}
	return close
}

// TestQuerySimple runs a setup of Prometheus servers, sidecars, and query nodes and verifies that
// queries return data merged from all Prometheus servers.
func TestQuerySimple(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_query_simple")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	closeFn := spinup(t, spinupConfig{
		promConfigFn:  selfScrapePromConfig,
		workDir:       dir,
		numPrometheus: 3,
		numQueries:    2,
	})
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err = runutil.Retry(time.Second, ctx.Done(), func() error {
		res, err := queryPrometheus(ctx, "http://localhost:19491", time.Now(), "up")
		if err != nil {
			return err
		}
		if len(res) != 3 {
			return errors.Errorf("unexpected result size %d", len(res))
		}

		// In our model result are always sorted.
		match := reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   "localhost:9091",
			"job":        "prometheus",
			"prometheus": "prom-9091",
		}, res[0].Metric)
		match = match && reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   "localhost:9092",
			"job":        "prometheus",
			"prometheus": "prom-9092",
		}, res[1].Metric)
		match = match && reflect.DeepEqual(model.Metric{
			"__name__":   "up",
			"instance":   "localhost:9093",
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
