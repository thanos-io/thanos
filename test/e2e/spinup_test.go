package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"

	"github.com/oklog/run"
	"github.com/pkg/errors"
)

var (
	promHTTPPort = func(i int) string { return fmt.Sprintf("%d", 9090+i) }

	// We keep this one with localhost, to have perfect match with what Prometheus will expose in up metric.
	promHTTP = func(i int) string { return fmt.Sprintf("localhost:%s", promHTTPPort(i)) }

	sidecarGRPC    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19090+i) }
	sidecarHTTP    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19190+i) }
	sidecarCluster = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19390+i) }

	queryGRPC    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19490+i) }
	queryHTTP    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19590+i) }
	queryCluster = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19690+i) }

	rulerGRPC    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19790+i) }
	rulerHTTP    = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19890+i) }
	rulerCluster = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19990+i) }
)

type cmdScheduleFunc func(workDir string, clusterPeerFlags []string) ([]*exec.Cmd, error)

type spinupSuite struct {
	cmdScheduleFuncs []cmdScheduleFunc
	clusterPeerFlags []string
}

func newSpinupSuite() *spinupSuite { return &spinupSuite{} }

func (s *spinupSuite) Add(cmdSchedule cmdScheduleFunc, gossipAddress string) *spinupSuite {
	s.cmdScheduleFuncs = append(s.cmdScheduleFuncs, cmdSchedule)
	if gossipAddress != "" {
		s.clusterPeerFlags = append(s.clusterPeerFlags, fmt.Sprintf("--cluster.peers"), gossipAddress)
	}
	return s
}

func scraper(i int, config string, gossip bool) (cmdScheduleFunc, string) {
	gossipAddress := ""
	if gossip {
		gossipAddress = sidecarCluster(i)
	}

	return func(workDir string, clusterPeerFlags []string) ([]*exec.Cmd, error) {
		promDir := fmt.Sprintf("%s/data/prom%d", workDir, i)
		if err := os.MkdirAll(promDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create prom dir failed")
		}

		if err := ioutil.WriteFile(promDir+"/prometheus.yml", []byte(config), 0666); err != nil {
			return nil, errors.Wrap(err, "creating prom config failed")
		}

		var cmds []*exec.Cmd
		cmds = append(cmds, exec.Command(testutil.PrometheusBinary(),
			"--config.file", promDir+"/prometheus.yml",
			"--storage.tsdb.path", promDir,
			"--log.level", "info",
			"--web.listen-address", promHTTP(i),
		))
		args := []string{
			"sidecar",
			"--debug.name", fmt.Sprintf("sidecar-%d", i),
			"--grpc-address", sidecarGRPC(i),
			"--http-address", sidecarHTTP(i),
			"--prometheus.url", fmt.Sprintf("http://%s", promHTTP(i)),
			"--tsdb.path", promDir,
			"--cluster.address", sidecarCluster(i),

			"--log.level", "debug",
		}

		if gossip {
			args = append(args, []string{
				"--cluster.advertise-address", sidecarCluster(i),
				"--cluster.gossip-interval", "200ms",
				"--cluster.pushpull-interval", "200ms",
			}...)
			args = append(args, clusterPeerFlags...)
		}
		cmds = append(cmds, exec.Command("thanos", args...))

		return cmds, nil
	}, gossipAddress
}

func querier(i int, replicaLabel string) (cmdScheduleFunc, string) {
	return func(_ string, clusterPeerFlags []string) ([]*exec.Cmd, error) {
		args := append(defaultQuerierFlags(i, replicaLabel),
			"--cluster.advertise-address", queryCluster(i),
			"--cluster.gossip-interval", "200ms",
			"--cluster.pushpull-interval", "200ms")
		args = append(args, clusterPeerFlags...)
		return []*exec.Cmd{exec.Command("thanos", args...)}, nil
	}, queryCluster(i)
}

func querierWithStoreFlags(i int, replicaLabel string, storesAddresses []string) (cmdScheduleFunc, string) {
	return func(workDir string, clusterPeerFlags []string) ([]*exec.Cmd, error) {
		args := defaultQuerierFlags(i, replicaLabel)

		for _, addr := range storesAddresses {
			args = append(args, "--store", addr)
		}

		return []*exec.Cmd{exec.Command("thanos", args...)}, nil
	}, ""
}

func querierWithFileSD(i int, replicaLabel string, storesAddresses []string) (cmdScheduleFunc, string) {
	return func(workDir string, clusterPeerFlags []string) ([]*exec.Cmd, error) {
		queryFileSDDir := fmt.Sprintf("%s/data/queryFileSd%d", workDir, i)
		if err := os.MkdirAll(queryFileSDDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create prom dir failed")
		}

		if err := ioutil.WriteFile(queryFileSDDir+"/filesd.json", []byte(generateFileSD(storesAddresses)), 0666); err != nil {
			return nil, errors.Wrap(err, "creating prom config failed")
		}

		args := append(defaultQuerierFlags(i, replicaLabel),
			"--store-sd-file", path.Join(queryFileSDDir, "filesd.json"))

		return []*exec.Cmd{exec.Command("thanos", args...)}, nil
	}, ""
}

func ruler(i int, rules string) (cmdScheduleFunc, string) {
	return func(workDir string, clusterPeerFlags []string) ([]*exec.Cmd, error) {
		dbDir := fmt.Sprintf("%s/data/rule%d", workDir, i)

		if err := os.MkdirAll(dbDir, 0777); err != nil {
			return nil, errors.Wrap(err, "creating ruler dir failed")
		}
		err := ioutil.WriteFile(dbDir+"/rules.yaml", []byte(rules), 0666)
		if err != nil {
			return nil, errors.Wrap(err, "creating ruler file failed")
		}

		return []*exec.Cmd{exec.Command("thanos",
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
				clusterPeerFlags...)...,
		)}, nil
	}, rulerCluster(i)
}

func alertManager(i int) (cmdScheduleFunc, string) {
	return func(workDir string, clusterPeerFlags []string) ([]*exec.Cmd, error) {
		dir := fmt.Sprintf("%s/data/alertmanager%d", workDir, i)

		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, errors.Wrap(err, "creating alertmanager dir failed")
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
		if err := ioutil.WriteFile(dir+"/config.yaml", []byte(config), 0666); err != nil {
			return nil, errors.Wrap(err, "creating alertmanager config file failed")
		}
		return []*exec.Cmd{exec.Command(testutil.AlertmanagerBinary(),
			"--config.file", dir+"/config.yaml",
			"--web.listen-address", "127.0.0.1:29093",
			"--log.level", "debug",
		)}, nil
	}, ""
}

// NOTE: It is important to install Thanos before using this function to compile latest changes.
// This means that export GOCACHE=/unique/path is must have to avoid having this test cached.
func (s *spinupSuite) Exec(t testing.TB, ctx context.Context, testName string) (chan error, error) {
	dir, err := ioutil.TempDir("", testName)
	if err != nil {
		return nil, err
	}

	var g run.Group

	// Interrupt go routine.
	{
		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error {
			<-ctx.Done()

			// This go routine will return only when:
			// 1) Any other process from group exited unexpectedly
			// 2) Global context will be cancelled.
			return nil
		}, func(error) {
			cancel()
			if err := os.RemoveAll(dir); err != nil {
				t.Log(err)
			}
		})
	}

	var commands []*exec.Cmd

	for _, cmdFunc := range s.cmdScheduleFuncs {
		cmds, err := cmdFunc(dir, s.clusterPeerFlags)
		if err != nil {
			if err := os.RemoveAll(dir); err != nil {
				t.Log(err)
			}
			return nil, err
		}

		commands = append(commands, cmds...)
	}

	// Run go routine for each command.
	for _, c := range commands {
		t.Logf("Executing: %v\n", c.Args)
		var stderr, stdout bytes.Buffer
		c.Stderr = &stderr
		c.Stdout = &stdout

		err := c.Start()
		if err != nil {
			// Let already started commands finish.
			go func() { _ = g.Run() }()

			if err := os.RemoveAll(dir); err != nil {
				t.Log(err)
			}
			return nil, errors.Wrap(err, "failed to start")
		}

		cmd := c
		g.Add(func() error {
			err := cmd.Wait()

			if stderr.Len() > 0 {
				t.Logf("%s STDERR\n %s", cmd.Path, stderr.String())
			}
			if stdout.Len() > 0 {
				t.Logf("%s STDOUT\n %s", cmd.Path, stdout.String())
			}

			return err
		}, func(error) {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		})
	}

	var exit = make(chan error, 1)
	go func(g run.Group) {
		exit <- g.Run()
		close(exit)
	}(g)

	return exit, nil
}

func generateFileSD(addresses []string) string {
	conf := "[ { \"targets\": ["
	for index, addr := range addresses {
		conf += fmt.Sprintf("\"%s\"", addr)
		if index+1 < len(addresses) {
			conf += ","
		}
	}
	conf += "] } ]"
	return conf
}

func defaultQuerierFlags(i int, replicaLabel string) []string {
	return []string{
		"query",
		"--debug.name", fmt.Sprintf("querier-%d", i),
		"--grpc-address", queryGRPC(i),
		"--http-address", queryHTTP(i),
		"--log.level", "debug",
		"--query.replica-label", replicaLabel,
		"--cluster.address", queryCluster(i),
	}
}