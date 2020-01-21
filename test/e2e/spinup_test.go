package e2e_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"google.golang.org/grpc"
)

const portMin = 10000

func remoteWriteEndpoint(addr address) string { return fmt.Sprintf("%s/api/v1/receive", addr.URL()) }

type address struct {
	host string
	Port string
}

func (a address) HostPort() string {
	return net.JoinHostPort(a.host, a.Port)
}

func (a address) URL() string {
	return fmt.Sprintf("http://%s", net.JoinHostPort(a.host, a.Port))
}

// portPool allows to reserve ports within unit test. This naive implementation assumes that all ports from portMin-X are free outside.
// No top boundary, no thread safety.
// TODO(bwplotka): Make it more resilient.
type portPool struct {
	lastPort int
}

func (pp *portPool) New() int {
	if pp.lastPort < portMin {
		pp.lastPort = portMin - 1
	}
	pp.lastPort++
	return pp.lastPort
}

type addresser struct {
	host string
	pp   *portPool
}

func (a *addresser) New() address {
	return address{host: a.host, Port: fmt.Sprintf("%d", a.pp.New())}
}

func newLocalAddresser() *addresser {
	// We keep this one with localhost, not 127.0.0.1 to have perfect match with what Prometheus will expose in up metric.
	return &addresser{host: "localhost", pp: &portPool{}}
}

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
	c.SysProcAttr = testutil.SysProcAttr()
	return c.Cmd.Start()
}

func (c *cmdExec) Kill() error { return c.Process.Signal(syscall.SIGKILL) }

func (c *cmdExec) String() string { return fmt.Sprintf("%s %v", c.Path, c.Args[1:]) }

type scheduler interface {
	Schedule(workDir string) (Exec, error)
}

type serverScheduler struct {
	schedule func(workDir string) (Exec, error)

	HTTP address
	GRPC address
}

func (s *serverScheduler) Schedule(workDir string) (Exec, error) { return s.schedule(workDir) }

type prometheusScheduler struct {
	serverScheduler

	RelDir string
}

func prometheus(http address, config string) *prometheusScheduler {
	s := &prometheusScheduler{
		RelDir: filepath.Join("data", "prom", http.Port),
	}

	s.serverScheduler = serverScheduler{
		HTTP: http,
		schedule: func(workDir string) (execs Exec, e error) {
			promDir := filepath.Join(workDir, s.RelDir)
			if err := os.MkdirAll(promDir, 0777); err != nil {
				return nil, errors.Wrap(err, "create prom dir failed")
			}

			if err := ioutil.WriteFile(promDir+"/prometheus.yml", []byte(config), 0666); err != nil {
				return nil, errors.Wrap(err, "creating prom config failed")
			}

			return newCmdExec(exec.Command(testutil.PrometheusBinary(),
				"--config.file", promDir+"/prometheus.yml",
				"--storage.tsdb.path", promDir,
				"--storage.tsdb.max-block-duration", "2h",
				"--log.level", "info",
				"--web.listen-address", http.HostPort(),
			)), nil
		},
	}
	return s
}

func sidecar(http, grpc address, prom *prometheusScheduler) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		GRPC: grpc,
		schedule: func(workDir string) (Exec, error) {
			promDir := filepath.Join(workDir, prom.RelDir)
			return newCmdExec(exec.Command("thanos", "sidecar",
				"--debug.name", fmt.Sprintf("sidecar-%s", http.Port),
				"--grpc-address", grpc.HostPort(),
				"--grpc-grace-period", "0s",
				"--http-address", http.HostPort(),
				"--prometheus.url", prom.HTTP.URL(),
				"--tsdb.path", promDir,
				"--log.level", "debug")), nil
		},
	}
}

func receiver(http, grpc, metric address, replicationFactor int, hashring ...receive.HashringConfig) *serverScheduler {
	if len(hashring) == 0 {
		hashring = []receive.HashringConfig{{Endpoints: []string{grpc.HostPort()}}}
	}

	return &serverScheduler{
		HTTP: http,
		GRPC: grpc,
		schedule: func(workDir string) (Exec, error) {
			receiveDir := filepath.Join(workDir, "data", "receive", http.Port)
			if err := os.MkdirAll(receiveDir, 0777); err != nil {
				return nil, errors.Wrap(err, "create receive dir")
			}

			b, err := json.Marshal(hashring)
			if err != nil {
				return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
			}

			if err := ioutil.WriteFile(filepath.Join(receiveDir, "hashrings.json"), b, 0666); err != nil {
				return nil, errors.Wrap(err, "creating receive config")
			}

			return newCmdExec(exec.Command("thanos", "receive",
				"--debug.name", fmt.Sprintf("receive-%s", http.Port),
				"--grpc-address", grpc.HostPort(),
				"--grpc-grace-period", "0s",
				"--http-address", metric.HostPort(),
				"--remote-write.address", http.HostPort(),
				"--label", fmt.Sprintf(`receive="%s"`, http.Port),
				"--tsdb.path", filepath.Join(receiveDir, "tsdb"),
				"--log.level", "debug",
				"--receive.replication-factor", strconv.Itoa(replicationFactor),
				"--receive.local-endpoint", grpc.HostPort(),
				"--receive.hashrings-file", filepath.Join(receiveDir, "hashrings.json"),
				"--receive.hashrings-file-refresh-interval", "5s")), nil
		},
	}
}

func querier(http, grpc address, storeAddresses []address, fileSDStoreAddresses []address) *serverScheduler {
	const replicaLabel = "replica"
	return &serverScheduler{
		HTTP: http,
		GRPC: grpc,
		schedule: func(workDir string) (Exec, error) {
			args := []string{
				"query",
				"--debug.name", fmt.Sprintf("querier-%s", http.Port),
				"--grpc-address", grpc.HostPort(),
				"--grpc-grace-period", "0s",
				"--http-address", http.HostPort(),
				"--log.level", "debug",
				"--query.replica-label", replicaLabel,
				"--store.sd-dns-interval", "5s",
			}
			for _, addr := range storeAddresses {
				args = append(args, "--store", addr.HostPort())
			}

			if len(fileSDStoreAddresses) > 0 {
				queryFileSDDir := filepath.Join(workDir, "data", "querier", http.Port)
				if err := os.MkdirAll(queryFileSDDir, 0777); err != nil {
					return nil, errors.Wrap(err, "create query dir failed")
				}

				if err := ioutil.WriteFile(queryFileSDDir+"/filesd.json", []byte(generateFileSD(fileSDStoreAddresses)), 0666); err != nil {
					return nil, errors.Wrap(err, "creating query SD config failed")
				}

				args = append(args,
					"--store.sd-files", filepath.Join(queryFileSDDir, "filesd.json"),
					"--store.sd-interval", "5s",
				)
			}

			return newCmdExec(exec.Command("thanos", args...)), nil
		},
	}
}

func storeGateway(http, grpc address, bucketConfig []byte, relabelConfig []byte) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		GRPC: grpc,
		schedule: func(workDir string) (Exec, error) {
			dbDir := filepath.Join(workDir, "data", "store-gateway", http.Port)

			if err := os.MkdirAll(dbDir, 0777); err != nil {
				return nil, errors.Wrap(err, "creating store gateway dir failed")
			}

			return newCmdExec(exec.Command("thanos",
				"store",
				"--debug.name", fmt.Sprintf("store-gw-%s", http.Port),
				"--data-dir", dbDir,
				"--grpc-address", grpc.HostPort(),
				"--grpc-grace-period", "0s",
				"--http-address", http.HostPort(),
				"--log.level", "debug",
				"--objstore.config", string(bucketConfig),
				// Accelerated sync time for quicker test (3m by default).
				"--sync-block-duration", "5s",
				"--selector.relabel-config", string(relabelConfig),
				"--experimental.enable-index-header",
			)), nil
		},
	}
}

func alertManager(http address) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		schedule: func(workDir string) (Exec, error) {
			dir := filepath.Join(workDir, "data", "alertmanager", http.Port)

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
			return newCmdExec(exec.Command(testutil.AlertmanagerBinary(),
				"--config.file", dir+"/config.yaml",
				"--web.listen-address", http.HostPort(),
				"--cluster.listen-address", "",
				"--log.level", "debug",
			)), nil
		},
	}
}

func rule(http, grpc address, ruleDir string, amCfg []byte, queryCfg []byte) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		GRPC: grpc,
		schedule: func(workDir string) (Exec, error) {
			err := ioutil.WriteFile(filepath.Join(workDir, "query.cfg"), queryCfg, 0666)
			if err != nil {
				return nil, errors.Wrap(err, "creating query config for ruler")
			}
			args := []string{
				"rule",
				"--debug.name", fmt.Sprintf("rule-%s", http.Port),
				"--label", fmt.Sprintf(`replica="%s"`, http.Port),
				"--data-dir", filepath.Join(workDir, "data"),
				"--rule-file", filepath.Join(ruleDir, "*.yaml"),
				"--eval-interval", "1s",
				"--alertmanagers.config", string(amCfg),
				"--alertmanagers.sd-dns-interval", "5s",
				"--grpc-address", grpc.HostPort(),
				"--grpc-grace-period", "0s",
				"--http-address", http.HostPort(),
				"--log.level", "debug",
				"--query.config-file", filepath.Join(workDir, "query.cfg"),
				"--query.sd-dns-interval", "5s",
				"--resend-delay", "5s",
			}
			return newCmdExec(exec.Command("thanos", args...)), nil
		},
	}
}

type sameProcessGRPCServiceExec struct {
	addr   string
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

	l, err := net.Listen("tcp", c.addr)
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
	return fmt.Sprintf("gRPC service %v", c.addr)
}

func fakeStoreAPI(grpcAddr address, svc storepb.StoreServer) *serverScheduler {
	return &serverScheduler{
		GRPC: grpcAddr,
		schedule: func(_ string) (Exec, error) {

			srv := grpc.NewServer()
			storepb.RegisterStoreServer(srv, svc)

			return &sameProcessGRPCServiceExec{addr: grpcAddr.HostPort(), srv: srv}, nil
		},
	}
}

func minio(http address, config s3.Config) *serverScheduler {
	return &serverScheduler{
		HTTP: http,
		schedule: func(workDir string) (Exec, error) {
			dbDir := filepath.Join(workDir, "data", "minio", http.Port)
			if err := os.MkdirAll(dbDir, 0777); err != nil {
				return nil, errors.Wrap(err, "creating minio dir failed")
			}

			cmd := exec.Command(testutil.MinioBinary(),
				"server",
				"--address", http.HostPort(),
				dbDir,
			)
			cmd.Env = append(os.Environ(),
				fmt.Sprintf("MINIO_ACCESS_KEY=%s", config.AccessKey),
				fmt.Sprintf("MINIO_SECRET_KEY=%s", config.SecretKey))

			return newCmdExec(cmd), nil
		},
	}
}

// NOTE: It is important to install Thanos before using this function to compile latest changes.
// This means that export GOCACHE=/unique/path is must have to avoid having this test cached locally.
func e2eSpinup(t testing.TB, ctx context.Context, cmds ...scheduler) (exit chan struct{}, err error) {
	return e2eSpinupWithS3ObjStorage(t, ctx, address{}, nil, cmds...)
}

func e2eSpinupWithS3ObjStorage(t testing.TB, ctx context.Context, minioAddr address, s3Config *s3.Config, cmds ...scheduler) (exit chan struct{}, err error) {
	dir, err := ioutil.TempDir("", "spinup_test")
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

	var s3Exit chan struct{}
	if s3Config != nil {
		s3Exit, err = e2eSpinupWithS3ObjStorage(t, ctx, address{}, nil, minio(minioAddr, *s3Config))
		if err != nil {
			return nil, errors.Wrap(err, "start minio")
		}

		ctx, cancel := context.WithCancel(ctx)
		if err := runutil.Retry(time.Second, ctx.Done(), func() error {
			select {
			case <-s3Exit:
				cancel()
				return nil
			default:
			}

			bkt, _, err := s3.NewTestBucketFromConfig(t, "eu-west1", *s3Config, false)
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
			// This go routine will return only when:
			// 1) Any other process from group exited unexpectedly
			// 2) Global context will be cancelled.
			// 3) Minio (if started) exited unexpectedly.

			if s3Exit != nil {
				select {
				case <-ctx.Done():
				case <-s3Exit:
				}
				return nil
			}

			<-ctx.Done()
			return nil

		}, func(error) {
			cancel()
			if err := os.RemoveAll(dir); err != nil {
				t.Log(err)
			}
		})
	}

	var stdFiles []*os.File
	// Run go routine for each command.
	for _, command := range cmds {
		c, err := command.Schedule(dir)
		if err != nil {
			return nil, err
		}
		// Store buffers in temp files to avoid excessive memory consumption.
		stdout, err := ioutil.TempFile(dir, "stdout")
		if err != nil {
			return nil, errors.Wrap(err, "create file for stdout")
		}

		stderr, err := ioutil.TempFile(dir, "stderr")
		if err != nil {
			return nil, errors.Wrap(err, "create file for stderr")
		}

		stdFiles = append(stdFiles, stdout, stderr)
		if err := c.Start(stdout, stderr); err != nil {
			// Let already started commands finish.
			go func() { _ = g.Run() }()
			return nil, errors.Wrap(err, "start")
		}

		cmd := c
		g.Add(func() error {
			return errors.Wrap(cmd.Wait(), cmd.String())
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

		if s3Exit != nil {
			<-s3Exit
		}

		printAndCloseFiles(t, stdFiles)
		close(exit)
	}(g)

	return exit, nil
}

func generateFileSD(addresses []address) string {
	conf := "[ { \"targets\": ["
	for index, addr := range addresses {
		conf += fmt.Sprintf("\"%s\"", addr.HostPort())
		if index+1 < len(addresses) {
			conf += ","
		}
	}
	conf += "] } ]"
	return conf
}

func printAndCloseFiles(t testing.TB, files []*os.File) {
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()

	for _, f := range files {
		info, err := f.Stat()
		if err != nil {
			t.Error(err)
		}

		if info.Size() == 0 {
			continue
		}

		if _, err := f.Seek(0, 0); err != nil {
			t.Error(err)
		}
		t.Logf("-------------------------------------------")
		t.Logf("-------------------  %s  ------------------", f.Name())
		t.Logf("-------------------------------------------")

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			t.Log(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			t.Error(err)
		}
	}
}
