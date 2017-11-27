package e2e_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

type config struct {
	promConfigFn func(port int) string
	rules        string
	workDir      string

	numPrometheus int
	numQueries    int
	numRules      int
}

// NOTE: It is important to install Thanos before using this function to compile latest changes.
func spinup(t testing.TB, cfg config) (close func()) {
	var commands []*exec.Cmd
	var closers []*exec.Cmd

	for i := 1; i <= cfg.numPrometheus; i++ {
		promDir := fmt.Sprintf("%s/data/prom%d", cfg.workDir, i)

		if err := os.MkdirAll(promDir, 0777); err != nil {
			t.Errorf("create dir failed: %s", err)
			return func() {}
		}
		err := ioutil.WriteFile(promDir+"/prometheus.yml", []byte(cfg.promConfigFn(9090+i)), 0666)
		if err != nil {
			t.Errorf("creating config failed: %s", err)
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
			"--grpc-address", fmt.Sprintf("0.0.0.0:%d", 19090+i),
			"--http-address", fmt.Sprintf("0.0.0.0:%d", 19190+i),
			"--prometheus.url", fmt.Sprintf("http://localhost:%d", 9090+i),
			"--tsdb.path", promDir,
			"--cluster.address", fmt.Sprintf("0.0.0.0:%d", 19390+i),
			"--cluster.advertise-address", fmt.Sprintf("127.0.0.1:%d", 19390+i),
			"--cluster.peers", "127.0.0.1:19391",
			"--cluster.peers", "127.0.0.1:19591",
			"--log.level", "debug",
		))
	}

	for i := 1; i <= cfg.numQueries; i++ {
		commands = append(commands, exec.Command("thanos", "query",
			"--debug.name", fmt.Sprintf("query-%d", i),
			"--http-address", fmt.Sprintf("0.0.0.0:%d", 19490+i),
			"--cluster.address", fmt.Sprintf("0.0.0.0:%d", 19590+i),
			"--cluster.advertise-address", fmt.Sprintf("127.0.0.1:%d", 19590+i),
			"--cluster.peers", "127.0.0.1:19391",
			"--cluster.peers", "127.0.0.1:19591",
			"--log.level", "debug",
		))
	}

	for i := 1; i <= cfg.numRules; i++ {
		dbDir := fmt.Sprintf("%s/data/rule%d", cfg.workDir, i)

		if err := os.MkdirAll(dbDir, 0777); err != nil {
			t.Errorf("creating dir failed: %s", err)
			return func() {}
		}
		err := ioutil.WriteFile(dbDir+"/rules.yaml", []byte(cfg.rules), 0666)
		if err != nil {
			t.Errorf("creating rule file failed: %s", err)
			return func() {}
		}

		commands = append(commands, exec.Command("thanos", "rule",
			"--debug.name", fmt.Sprintf("rule-%d", i),
			"--data-dir", dbDir,
			"--rule-dir", dbDir,
			"--eval-interval", "1s",
			"--grpc-address", fmt.Sprintf("0.0.0.0:%d", 19690+i),
			"--http-address", fmt.Sprintf("0.0.0.0:%d", 19790+i),
			"--cluster.address", fmt.Sprintf("0.0.0.0:%d", 19780+i),
			"--cluster.advertise-address", fmt.Sprintf("127.0.0.1:%d", 19890+i),
			"--cluster.peers", "127.0.0.1:19391",
			"--cluster.peers", "127.0.0.1:19591",
			"--log.level", "debug",
		))
	}

	var stderr bytes.Buffer
	stderrw := &safeWriter{Writer: &stderr}

	close = func() {
		for _, c := range closers {
			c.Process.Signal(syscall.SIGTERM)
			if err := c.Wait(); err != nil {
				t.Errorf("wait failed: %s", err)
			}
		}
		t.Logf("STDERR\n %s", stderr.String())
	}
	for _, cmd := range commands {
		cmd.Stderr = stderrw

		if err := cmd.Start(); err != nil {
			t.Errorf("start failed: %s", err)
			close()
			return func() {}
		}
		closers = append(closers, cmd)
	}
	return close
}
