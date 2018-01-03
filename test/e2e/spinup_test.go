package e2e_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"
	"time"
)

var (
	promHTTP = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 9090+i) }

	sidecarGRPC    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19090+1) }
	sidecarHTTP    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19190+1) }
	sidecarCluster = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19390+1) }

	queryGRPC    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19490+1) }
	queryHTTP    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19590+1) }
	queryCluster = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19690+1) }

	rulerGRPC    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19790+1) }
	rulerHTTP    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19890+1) }
	rulerCluster = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19990+1) }
)

type config struct {
	promConfigFn func(port int) string
	rules        string
	workDir      string

	numPrometheus    int
	numQueries       int
	numRules         int
	numAlertmanagers int
}

func evalClusterPeersFlags(cfg config) []string {
	var flags []string
	for i := 1; i <= cfg.numPrometheus; i++ {
		flags = append(flags, "--cluster.peers", sidecarCluster(i))
	}
	for i := 1; i <= cfg.numQueries; i++ {
		flags = append(flags, "--cluster.peers", queryCluster(i))
	}
	for i := 1; i <= cfg.numRules; i++ {
		flags = append(flags, "--cluster.peers", rulerCluster(i))
	}
	return flags
}

// NOTE: It is important to install Thanos before using this function to compile latest changes.
func spinup(t testing.TB, cfg config) (close func()) {
	var commands []*exec.Cmd
	var closers []*exec.Cmd

	clusterPeers := evalClusterPeersFlags(cfg)

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
			"--web.listen-address", promHTTP(i),
		))
		commands = append(commands, exec.Command("thanos",
			append([]string{
				"sidecar",
				"--debug.name", fmt.Sprintf("sidecar-%d", i),
				"--grpc-address", sidecarGRPC(i),
				"--http-address", sidecarHTTP(i),
				"--prometheus.url", fmt.Sprintf("http://%s", promHTTP(i)),
				"--tsdb.path", promDir,
				"--cluster.address", sidecarCluster(i),
				"--cluster.advertise-address", sidecarCluster(i),
				"--cluster.gossip-interval", "200ms",
				"--cluster.pushpull-interval", "200ms",
				"--log.level", "debug",
			},
				clusterPeers...)...,
		))
		time.Sleep(200 * time.Millisecond)
	}

	for i := 1; i <= cfg.numQueries; i++ {
		commands = append(commands, exec.Command("thanos",
			append([]string{"query",
				"--debug.name", fmt.Sprintf("query-%d", i),
				"--grpc-address", queryGRPC(i),
				"--http-address", queryHTTP(i),
				"--cluster.address", queryCluster(i),
				"--cluster.advertise-address", queryCluster(i),
				"--cluster.gossip-interval", "200ms",
				"--cluster.pushpull-interval", "200ms",
				"--log.level", "debug",
			},
				clusterPeers...)...,
		))
		time.Sleep(200 * time.Millisecond)
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

		commands = append(commands, exec.Command("thanos",
			append([]string{"rule",
				"--debug.name", fmt.Sprintf("rule-%d", i),
				"--label", fmt.Sprintf(`replica="%d"`, i),
				"--data-dir", dbDir,
				"--rule-file", path.Join(dbDir, "*.yaml"),
				"--eval-interval", "1s",
				"--alertmanagers.url", "http://127.0.0.1:29093",
				"--grpc-address", rulerGRPC(i),
				"--http-address", rulerHTTP(i),
				"--cluster.address", rulerCluster(i),
				"--cluster.advertise-address", rulerCluster(i),
				"--cluster.gossip-interval", "200ms",
				"--cluster.pushpull-interval", "200ms",
				"--log.level", "debug",
			},
				clusterPeers...)...,
		))
		time.Sleep(200 * time.Millisecond)
	}

	for i := 1; i <= cfg.numAlertmanagers; i++ {
		dir := fmt.Sprintf("%s/data/alertmanager%d", cfg.workDir, i)

		if err := os.MkdirAll(dir, 0777); err != nil {
			t.Errorf("creating dir failed: %s", err)
			return func() {}
		}
		config := `
route:
  group_by: ['alertname']
  group_wait: 1s
  group_interval: 1s
  receiver: 'null'
receivers:
- name: 'null'
`
		err := ioutil.WriteFile(dir+"/config.yaml", []byte(config), 0666)
		if err != nil {
			t.Errorf("creating config file failed: %s", err)
			return func() {}
		}
		commands = append(commands, exec.Command("alertmanager",
			"-config.file", dir+"/config.yaml",
			"-web.listen-address", "127.0.0.1:29093",
			"-log.level", "debug",
		))
	}

	var stderr, stdout bytes.Buffer

	stderrw := &safeWriter{Writer: &stderr}
	stdoutw := &safeWriter{Writer: &stdout}

	close = func() {
		for _, c := range closers {
			c.Process.Signal(syscall.SIGTERM)
			if err := c.Wait(); err != nil {
				t.Errorf("wait failed: %s", err)
			}
		}
		t.Logf("STDERR\n %s", stderr.String())
		t.Logf("STDOUT\n %s", stdout.String())
	}
	for _, cmd := range commands {
		cmd.Stderr = stderrw
		cmd.Stdout = stdoutw

		if err := cmd.Start(); err != nil {
			t.Errorf("start failed: %s", err)
			close()
			return func() {}
		}
		closers = append(closers, cmd)
	}
	return close
}
