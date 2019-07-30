package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"
)

var (
	promHTTPPort = func(i int) string { return fmt.Sprintf("%d", 9090+i) }

	// We keep this one with localhost, to have perfect match with what Prometheus will expose in up metric.
	promHTTP            = func(i int) string { return fmt.Sprintf("localhost:%s", promHTTPPort(i)) }
	promRemoteWriteHTTP = func(i int) string { return fmt.Sprintf("localhost:%s", promHTTPPort(100+i)) }

	nodeExporterHTTP = func(i int) string { return fmt.Sprintf("localhost:%d", 9100+i) }

	sidecarGRPC = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19090+i) }
	sidecarHTTP = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19190+i) }

	queryGRPC = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19490+i) }
	queryHTTP = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19590+i) }

	rulerGRPC = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19790+i) }
	rulerHTTP = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19890+i) }

	remoteWriteEndpoint          = func(i int) string { return fmt.Sprintf("http://%s/api/v1/receive", remoteWriteReceiveHTTP(i)) }
	remoteWriteReceiveHTTP       = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 18690+i) }
	remoteWriteReceiveGRPC       = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 18790+i) }
	remoteWriteReceiveMetricHTTP = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 18890+i) }

	storeGatewayGRPC = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 20090+i) }
	storeGatewayHTTP = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 20190+i) }

	minioHTTP = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 20290+i) }

	fakeStoreAPIGRPC = func(i int) string { return fmt.Sprintf("127.0.0.1:%d", 21090+i) }
)

type Exec interface {
	Start(stdout io.Writer, stderr io.Writer) error
	Wait() error
	Kill() error

	String() string
}

type cmdExec struct {
	*exec.Cmd
}

func newCmdExec(cmd *exec.Cmd) *cmdExec {
	return &cmdExec{Cmd: cmd}
}

func (c *cmdExec) Start(stdout io.Writer, stderr io.Writer) error {
	c.Stderr = stderr
	c.Stdout = stdout
	return c.Cmd.Start()
}

func (c *cmdExec) Kill() error { return c.Process.Signal(syscall.SIGKILL) }

func (c *cmdExec) String() string { return fmt.Sprintf("%s %v", c.Path, c.Args[1:]) }

type cmdScheduleFunc func(workDir string) ([]Exec, error)

type spinupSuite struct {
	cmdScheduleFuncs []cmdScheduleFunc

	minioConfig         s3.Config
	withPreStartedMinio bool
}

func newSpinupSuite() *spinupSuite { return &spinupSuite{} }

func (s *spinupSuite) Add(cmdSchedule cmdScheduleFunc) *spinupSuite {
	s.cmdScheduleFuncs = append(s.cmdScheduleFuncs, cmdSchedule)
	return s
}

func scraper(i int, config string) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
		promDir := fmt.Sprintf("%s/data/prom%d", workDir, i)
		if err := os.MkdirAll(promDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create prom dir failed")
		}

		if err := ioutil.WriteFile(promDir+"/prometheus.yml", []byte(config), 0666); err != nil {
			return nil, errors.Wrap(err, "creating prom config failed")
		}

		var cmds []Exec
		cmds = append(cmds, newCmdExec(exec.Command(testutil.PrometheusBinary(),
			"--config.file", promDir+"/prometheus.yml",
			"--storage.tsdb.path", promDir,
			"--storage.tsdb.max-block-duration", "2h",
			"--log.level", "info",
			"--web.listen-address", promHTTP(i),
		)))
		return append(cmds, newCmdExec(exec.Command("thanos", "sidecar",
			"--debug.name", fmt.Sprintf("sidecar-%d", i),
			"--grpc-address", sidecarGRPC(i),
			"--http-address", sidecarHTTP(i),
			"--prometheus.url", fmt.Sprintf("http://%s", promHTTP(i)),
			"--tsdb.path", promDir,
			"--log.level", "debug"))), nil
	}
}

func receiver(i int, config string, replicationFactor int, receiveAddresses ...string) cmdScheduleFunc {
	if len(receiveAddresses) == 0 {
		receiveAddresses = []string{remoteWriteEndpoint(1)}
	}
	return func(workDir string) ([]Exec, error) {
		promDir := fmt.Sprintf("%s/data/remote-write-prom%d", workDir, i)
		if err := os.MkdirAll(promDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create prom dir failed")
		}

		if err := ioutil.WriteFile(promDir+"/prometheus.yml", []byte(config), 0666); err != nil {
			return nil, errors.Wrap(err, "creating prom config failed")
		}

		var cmds []Exec
		cmds = append(cmds, newCmdExec(exec.Command(testutil.PrometheusBinary(),
			"--config.file", promDir+"/prometheus.yml",
			"--storage.tsdb.path", promDir,
			"--log.level", "info",
			"--web.listen-address", promRemoteWriteHTTP(i),
		)))

		hashringsFileDir := fmt.Sprintf("%s/data/receiveFile%d", workDir, i)
		if err := os.MkdirAll(hashringsFileDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create receive dir failed")
		}

		if err := ioutil.WriteFile(path.Join(hashringsFileDir, "hashrings.json"), []byte(generateHashringsFile(receiveAddresses)), 0666); err != nil {
			return nil, errors.Wrap(err, "creating receive config failed")
		}

		return append(cmds, newCmdExec(exec.Command("thanos", "receive",
			"--debug.name", fmt.Sprintf("remote-write-receive-%d", i),
			"--grpc-address", remoteWriteReceiveGRPC(i),
			"--http-address", remoteWriteReceiveMetricHTTP(i),
			"--remote-write.address", remoteWriteReceiveHTTP(i),
			"--labels", "receive=\"true\"",
			"--labels", fmt.Sprintf(`replica="%d"`, i),
			"--tsdb.path", promDir,
			"--log.level", "debug",
			"--receive.replication-factor", strconv.Itoa(replicationFactor),
			"--receive.local-endpoint", remoteWriteEndpoint(i),
			"--receive.hashrings-file", path.Join(hashringsFileDir, "hashrings.json"),
			"--receive.hashrings-file-refresh-interval", "5s"))), nil
	}
}

func querierWithStoreFlags(i int, replicaLabel string, storeAddresses ...string) cmdScheduleFunc {
	return func(_ string) ([]Exec, error) {
		args := defaultQuerierFlags(i, replicaLabel)

		for _, addr := range storeAddresses {
			args = append(args, "--store", addr)
		}
		return []Exec{newCmdExec(exec.Command("thanos", args...))}, nil
	}
}

func querierWithFileSD(i int, replicaLabel string, storeAddresses ...string) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
		queryFileSDDir := fmt.Sprintf("%s/data/queryFileSd%d", workDir, i)
		if err := os.MkdirAll(queryFileSDDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create query dir failed")
		}

		if err := ioutil.WriteFile(queryFileSDDir+"/filesd.json", []byte(generateFileSD(storeAddresses)), 0666); err != nil {
			return nil, errors.Wrap(err, "creating query SD config failed")
		}

		args := append(
			defaultQuerierFlags(i, replicaLabel),
			"--store.sd-files", path.Join(queryFileSDDir, "filesd.json"),
			"--store.sd-interval", "5s",
		)

		return []Exec{newCmdExec(exec.Command("thanos", args...))}, nil
	}
}

func storeGateway(i int, bucketConfig []byte) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
		dbDir := fmt.Sprintf("%s/data/store-gateway%d", workDir, i)

		if err := os.MkdirAll(dbDir, 0777); err != nil {
			return nil, errors.Wrap(err, "creating store gateway dir failed")
		}

		return []Exec{newCmdExec(exec.Command("thanos",
			"store",
			"--debug.name", fmt.Sprintf("store-%d", i),
			"--data-dir", dbDir,
			"--grpc-address", storeGatewayGRPC(i),
			"--http-address", storeGatewayHTTP(i),
			"--log.level", "debug",
			"--objstore.config", string(bucketConfig),
			// Accelerated sync time for quicker test (3m by default)
			"--sync-block-duration", "5s",
		))}, nil
	}
}

func alertManager(i int) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
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
		return []Exec{newCmdExec(exec.Command(testutil.AlertmanagerBinary(),
			"--config.file", dir+"/config.yaml",
			"--web.listen-address", "127.0.0.1:29093",
			"--log.level", "debug",
		))}, nil
	}
}

func rulerWithQueryFlags(i int, rules []string, queryAddresses ...string) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
		dbDir := fmt.Sprintf("%s/data/rule%d", workDir, i)

		if err := os.MkdirAll(dbDir, 0777); err != nil {
			return nil, errors.Wrap(err, "creating ruler dir")
		}
		for i, rule := range rules {
			if err := ioutil.WriteFile(path.Join(dbDir, fmt.Sprintf("/rules-%d.yaml", i)), []byte(rule), 0666); err != nil {
				return nil, errors.Wrapf(err, "writing rule %s", path.Join(dbDir, fmt.Sprintf("/rules-%d.yaml", i)))
			}
		}

		args := defaultRulerFlags(i, dbDir, dbDir)

		for _, addr := range queryAddresses {
			args = append(args, "--query", addr)
		}
		return []Exec{newCmdExec(exec.Command("thanos", args...))}, nil
	}
}

func rulerWithDir(i int, ruleDir string, queryAddresses ...string) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
		dbDir := fmt.Sprintf("%s/data/rule%d", workDir, i)

		if err := os.MkdirAll(dbDir, 0777); err != nil {
			return nil, errors.Wrap(err, "creating ruler dir")
		}

		args := defaultRulerFlags(i, dbDir, ruleDir)

		for _, addr := range queryAddresses {
			args = append(args, "--query", addr)
		}
		return []Exec{newCmdExec(exec.Command("thanos", args...))}, nil
	}
}

func rulerWithFileSD(i int, rules []string, queryAddresses ...string) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
		dbDir := fmt.Sprintf("%s/data/rule%d", workDir, i)

		if err := os.MkdirAll(dbDir, 0777); err != nil {
			return nil, errors.Wrap(err, "creating ruler dir")
		}
		for i, rule := range rules {
			if err := ioutil.WriteFile(path.Join(dbDir, fmt.Sprintf("/rules-%d.yaml", i)), []byte(rule), 0666); err != nil {
				return nil, errors.Wrapf(err, "writing rule %s", path.Join(dbDir, fmt.Sprintf("/rules-%d.yaml", i)))
			}
		}

		ruleFileSDDir := fmt.Sprintf("%s/data/ruleFileSd%d", workDir, i)
		if err := os.MkdirAll(ruleFileSDDir, 0777); err != nil {
			return nil, errors.Wrap(err, "create ruler filesd dir")
		}

		if err := ioutil.WriteFile(ruleFileSDDir+"/filesd.json", []byte(generateFileSD(queryAddresses)), 0666); err != nil {
			return nil, errors.Wrap(err, "creating ruler filesd config")
		}

		args := append(defaultRulerFlags(i, dbDir, dbDir),
			"--query.sd-files", path.Join(ruleFileSDDir, "filesd.json"),
			"--query.sd-interval", "5s")

		return []Exec{newCmdExec(exec.Command("thanos", args...))}, nil
	}
}

type sameProcessGRPCServiceExec struct {
	i      int
	stdout io.Writer
	stderr io.Writer

	ctx     context.Context
	cancel  context.CancelFunc
	srvChan <-chan error
	srv     *grpc.Server
}

func (c *sameProcessGRPCServiceExec) Start(stdout io.Writer, stderr io.Writer) error {
	c.stderr = stderr
	c.stdout = stdout

	if c.ctx != nil {
		return errors.New("process already started")
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	l, err := net.Listen("tcp", fakeStoreAPIGRPC(c.i))
	if err != nil {
		return errors.Wrap(err, "listen API address")
	}

	srvChan := make(chan error)
	go func() {
		defer close(srvChan)
		if err := c.srv.Serve(l); err != nil {
			srvChan <- err
			_, _ = c.stderr.Write([]byte(fmt.Sprintf("server failed: %s", err)))
		}
	}()
	c.srvChan = srvChan
	return nil
}

func (c *sameProcessGRPCServiceExec) Wait() error {
	err := <-c.srvChan
	if c.ctx.Err() == nil && err != nil {
		return err
	}
	return err
}

func (c *sameProcessGRPCServiceExec) Kill() error {
	c.cancel()
	c.srv.Stop()

	return nil
}

func (c *sameProcessGRPCServiceExec) String() string {
	return fmt.Sprintf("gRPC service %v on %v", c.i, fakeStoreAPIGRPC(c.i))
}

func fakeStoreAPI(i int, svc storepb.StoreServer) cmdScheduleFunc {
	return func(_ string) ([]Exec, error) {
		srv := grpc.NewServer()
		storepb.RegisterStoreServer(srv, svc)

		return []Exec{&sameProcessGRPCServiceExec{i: i, srv: srv}}, nil
	}
}

func minio(accessKey string, secretKey string) cmdScheduleFunc {
	return func(workDir string) ([]Exec, error) {
		dbDir := fmt.Sprintf("%s/data/minio", workDir)

		if err := os.MkdirAll(dbDir, 0777); err != nil {
			return nil, errors.Wrap(err, "creating minio dir failed")
		}

		cmd := exec.Command(testutil.MinioBinary(),
			"server",
			"--address", minioHTTP(1),
			dbDir,
		)
		cmd.Env = append(os.Environ(),
			fmt.Sprintf("MINIO_ACCESS_KEY=%s", accessKey),
			fmt.Sprintf("MINIO_SECRET_KEY=%s", secretKey))

		return []Exec{newCmdExec(cmd)}, nil
	}
}

func (s *spinupSuite) WithPreStartedMinio(config s3.Config) *spinupSuite {
	s.minioConfig = config
	s.withPreStartedMinio = true
	return s
}

// NOTE: It is important to install Thanos before using this function to compile latest changes.
// This means that export GOCACHE=/unique/path is must have to avoid having this test cached.
func (s *spinupSuite) Exec(t testing.TB, ctx context.Context, testName string) (exit chan struct{}, err error) {
	dir, err := ioutil.TempDir("", testName)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			if rerr := os.RemoveAll(dir); rerr != nil {
				t.Log(rerr)
			}
		}
	}()

	var minioExit chan struct{}
	if s.withPreStartedMinio {
		// Start minio before anything else.
		// NewTestBucketFromConfig is responsible for healthchecking by creating a requested bucket in retry loop.
		minioExit, err = newSpinupSuite().
			Add(minio(s.minioConfig.AccessKey, s.minioConfig.SecretKey)).
			Exec(t, ctx, testName+"_minio")
		if err != nil {
			return nil, errors.Wrap(err, "start minio")
		}

		ctx, cancel := context.WithCancel(ctx)
		if err := runutil.Retry(time.Second, ctx.Done(), func() error {
			select {
			case <-minioExit:
				cancel()
				return nil
			default:
			}

			bkt, _, err := s3.NewTestBucketFromConfig(t, "eu-west1", s.minioConfig, false)
			if err != nil {
				return errors.Wrap(err, "create bkt client for minio healthcheck")
			}

			return bkt.Close()
		}); err != nil {
			return nil, errors.Wrap(err, "minio not ready in time")
		}
	}

	var g run.Group

	// Interrupt go routine.
	{
		ctx, cancel := context.WithCancel(ctx)
		g.Add(func() error {
			if s.withPreStartedMinio {
				select {
				case <-ctx.Done():
				case <-minioExit:
				}
			} else {
				<-ctx.Done()
			}

			// This go routine will return only when:
			// 1) Any other process from group exited unexpectedly
			// 2) Global context will be cancelled.
			// 3) Minio (if started) exited unexpectedly.
			return nil
		}, func(error) {
			cancel()
			if err := os.RemoveAll(dir); err != nil {
				t.Log(err)
			}
		})
	}

	var commands []Exec

	for _, cmdFunc := range s.cmdScheduleFuncs {
		cmds, err := cmdFunc(dir)
		if err != nil {
			return nil, err
		}

		commands = append(commands, cmds...)
	}

	// Run go routine for each command.
	for _, c := range commands {
		var stderr, stdout bytes.Buffer
		if err := c.Start(&stdout, &stderr); err != nil {
			// Let already started commands finish.
			go func() { _ = g.Run() }()
			return nil, errors.Wrap(err, "failed to start")
		}

		cmd := c
		g.Add(func() error {
			id := c.String()

			err := cmd.Wait()

			if stderr.Len() > 0 {
				t.Logf("%s STDERR\n %s", id, stderr.String())
			}
			if stdout.Len() > 0 {
				t.Logf("%s STDOUT\n %s", id, stdout.String())
			}

			return errors.Wrap(err, id)
		}, func(error) {
			// This's accepted scenario to kill a process immediately for sure and run tests as fast as possible.
			_ = cmd.Kill()
		})
	}

	exit = make(chan struct{})
	go func(g run.Group) {
		if err := g.Run(); err != nil && ctx.Err() == nil {
			t.Errorf("Some process exited unexpectedly: %v", err)
		}
		if minioExit != nil {
			<-minioExit
		}
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

func generateHashringsFile(addresses []string) string {
	conf := "[ { \"endpoints\": ["
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
		"--store.sd-dns-interval", "5s",
	}
}

func defaultRulerFlags(i int, dbDir string, ruleDir string) []string {
	return []string{
		"rule",
		"--debug.name", fmt.Sprintf("rule-%d", i),
		"--label", fmt.Sprintf(`replica="%d"`, i),
		"--data-dir", dbDir,
		"--rule-file", path.Join(ruleDir, "*.yaml"),
		"--eval-interval", "1s",
		"--alertmanagers.url", "http://127.0.0.1:29093",
		"--grpc-address", rulerGRPC(i),
		"--http-address", rulerHTTP(i),
		"--log.level", "debug",
		"--query.sd-dns-interval", "5s",
	}
}
