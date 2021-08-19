// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2eutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const (
	defaultPrometheusVersion   = "v1.8.2-0.20200724121523-657ba532e42f"
	defaultAlertmanagerVersion = "v0.20.0"
	defaultMinioVersion        = "RELEASE.2018-10-06T00-15-16Z"

	// Space delimited list of versions.
	promPathsEnvVar       = "THANOS_TEST_PROMETHEUS_PATHS"
	alertmanagerBinEnvVar = "THANOS_TEST_ALERTMANAGER_PATH"
	minioBinEnvVar        = "THANOS_TEST_MINIO_PATH"

	// A placeholder for actual Prometheus instance address in the scrape config.
	PromAddrPlaceHolder = "PROMETHEUS_ADDRESS"
)

func PrometheusBinary() string {
	return "prometheus-" + defaultPrometheusVersion
}

func AlertmanagerBinary() string {
	b := os.Getenv(alertmanagerBinEnvVar)
	if b == "" {
		return fmt.Sprintf("alertmanager-%s", defaultAlertmanagerVersion)
	}
	return b
}

func MinioBinary() string {
	b := os.Getenv(minioBinEnvVar)
	if b == "" {
		return fmt.Sprintf("minio-%s", defaultMinioVersion)
	}
	return b
}

// Prometheus represents a test instance for integration testing.
// It can be populated with data before being started.
type Prometheus struct {
	dir     string
	db      *tsdb.DB
	prefix  string
	binPath string

	running            bool
	cmd                *exec.Cmd
	disabledCompaction bool
	addr               string

	config string
}

func NewTSDB() (*tsdb.DB, error) {
	dir, err := ioutil.TempDir("", "prometheus-test")
	if err != nil {
		return nil, err
	}
	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = math.MaxInt64
	return tsdb.Open(dir, nil, nil, opts)
}

func ForeachPrometheus(t *testing.T, testFn func(t testing.TB, p *Prometheus)) {
	paths := os.Getenv(promPathsEnvVar)
	if paths == "" {
		paths = PrometheusBinary()
	}

	for _, path := range strings.Split(paths, " ") {
		if ok := t.Run(path, func(t *testing.T) {
			p, err := newPrometheus(path, "")
			testutil.Ok(t, err)

			testFn(t, p)
			testutil.Ok(t, p.Stop())
		}); !ok {
			return
		}
	}
}

// NewPrometheus creates a new test Prometheus instance that will listen on local address.
// Use ForeachPrometheus if you want to test against set of Prometheus versions.
// TODO(bwplotka): Improve it with https://github.com/thanos-io/thanos/issues/758.
func NewPrometheus() (*Prometheus, error) {
	return newPrometheus("", "")
}

// NewPrometheusOnPath creates a new test Prometheus instance that will listen on local address and given prefix path.
func NewPrometheusOnPath(prefix string) (*Prometheus, error) {
	return newPrometheus("", prefix)
}

func newPrometheus(binPath, prefix string) (*Prometheus, error) {
	if binPath == "" {
		binPath = PrometheusBinary()
	}

	db, err := NewTSDB()
	if err != nil {
		return nil, err
	}

	// Just touch an empty config file. We don't need to actually scrape anything.
	_, err = os.Create(filepath.Join(db.Dir(), "prometheus.yml"))
	if err != nil {
		return nil, err
	}

	return &Prometheus{
		dir:     db.Dir(),
		db:      db,
		prefix:  prefix,
		binPath: binPath,
		addr:    "<prometheus-not-started>",
	}, nil
}

// Start running the Prometheus instance and return.
func (p *Prometheus) Start() error {
	if p.running {
		return errors.New("Already started")
	}

	if err := p.db.Close(); err != nil {
		return err
	}
	return p.start()
}

func (p *Prometheus) start() error {
	p.running = true

	port, err := FreePort()
	if err != nil {
		return err
	}

	var extra []string
	if p.disabledCompaction {
		extra = append(extra,
			"--storage.tsdb.min-block-duration=2h",
			"--storage.tsdb.max-block-duration=2h",
		)
	}
	p.addr = fmt.Sprintf("localhost:%d", port)
	// Write the final config to the config file.
	// The address placeholder will be replaced with the actual address.
	if err := p.writeConfig(strings.ReplaceAll(p.config, PromAddrPlaceHolder, p.addr)); err != nil {
		return err
	}
	args := append([]string{
		"--storage.tsdb.retention=2d", // Pass retention cause prometheus since 2.8.0 don't show default value for that flags in web/api: https://github.com/prometheus/prometheus/pull/5433.
		"--storage.tsdb.path=" + p.db.Dir(),
		"--web.listen-address=" + p.addr,
		"--web.route-prefix=" + p.prefix,
		"--web.enable-admin-api",
		"--config.file=" + filepath.Join(p.db.Dir(), "prometheus.yml"),
	}, extra...)

	p.cmd = exec.Command(p.binPath, args...)
	p.cmd.SysProcAttr = SysProcAttr()

	go func() {
		if b, err := p.cmd.CombinedOutput(); err != nil {
			fmt.Fprintln(os.Stderr, "running Prometheus failed", err)
			fmt.Fprintln(os.Stderr, string(b))
		}
	}()
	time.Sleep(2 * time.Second)

	return nil
}

func (p *Prometheus) WaitPrometheusUp(ctx context.Context, logger log.Logger) error {
	if !p.running {
		return errors.New("method Start was not invoked.")
	}
	return runutil.Retry(time.Second, ctx.Done(), func() error {
		r, err := http.Get(fmt.Sprintf("http://%s/-/ready", p.addr))
		if err != nil {
			return err
		}
		defer runutil.ExhaustCloseWithLogOnErr(logger, r.Body, "failed to exhaust and close body")

		if r.StatusCode != 200 {
			return errors.Errorf("Got non 200 response: %v", r.StatusCode)
		}
		return nil
	})
}

func (p *Prometheus) Restart() error {
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrap(err, "failed to kill Prometheus. Kill it manually")
	}
	_ = p.cmd.Wait()
	return p.start()
}

// Dir returns TSDB dir.
func (p *Prometheus) Dir() string {
	return p.dir
}

// Addr returns correct address after Start method.
func (p *Prometheus) Addr() string {
	return p.addr + p.prefix
}

func (p *Prometheus) DisableCompaction() {
	p.disabledCompaction = true
}

// SetConfig updates the contents of the config.
func (p *Prometheus) SetConfig(s string) {
	p.config = s
}

// writeConfig writes the Prometheus config to the config file.
func (p *Prometheus) writeConfig(config string) (err error) {
	f, err := os.Create(filepath.Join(p.dir, "prometheus.yml"))
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, f, "prometheus config")
	_, err = f.Write([]byte(config))
	return err
}

// Stop terminates Prometheus and clean up its data directory.
func (p *Prometheus) Stop() error {
	if !p.running {
		return nil
	}

	if p.cmd.Process != nil {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return errors.Wrapf(err, "failed to Prometheus. Kill it manually and clean %s dir", p.db.Dir())
		}
	}
	time.Sleep(time.Second / 2)
	return p.cleanup()
}

func (p *Prometheus) cleanup() error {
	p.running = false
	return os.RemoveAll(p.db.Dir())
}

// Appender returns a new appender to populate the Prometheus instance with data.
// All appenders must be closed before Start is called and no new ones must be opened
// afterwards.
func (p *Prometheus) Appender() storage.Appender {
	if p.running {
		panic("Appender must not be called after start")
	}
	return p.db.Appender(context.Background())
}

// CreateEmptyBlock produces empty block like it was the case before fix: https://github.com/prometheus/tsdb/pull/374.
// (Prometheus pre v2.7.0).
func CreateEmptyBlock(dir string, mint, maxt int64, extLset labels.Labels, resolution int64) (ulid.ULID, error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	if err := os.Mkdir(path.Join(dir, uid.String()), os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	if err := os.Mkdir(path.Join(dir, uid.String(), "chunks"), os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	w, err := index.NewWriter(context.Background(), path.Join(dir, uid.String(), "index"))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "new index")
	}

	if err := w.Close(); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	m := tsdb.BlockMeta{
		Version: 1,
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{uid},
		},
	}
	b, err := json.Marshal(&m)
	if err != nil {
		return ulid.ULID{}, err
	}

	if err := ioutil.WriteFile(path.Join(dir, uid.String(), "meta.json"), b, os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "saving meta.json")
	}

	if _, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(dir, uid.String()), metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
	}, nil); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "finalize block")
	}

	return uid, nil
}

// CreateBlock writes a block with the given series and numSamples samples each.
// Samples will be in the time range [mint, maxt).
func CreateBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
	hashFunc metadata.HashFunc,
) (id ulid.ULID, err error) {
	return createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, false, hashFunc)
}

// CreateBlockWithTombstone is same as CreateBlock but leaves tombstones which mimics the Prometheus local block.
func CreateBlockWithTombstone(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
	hashFunc metadata.HashFunc,
) (id ulid.ULID, err error) {
	return createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, true, hashFunc)
}

// CreateBlockWithBlockDelay writes a block with the given series and numSamples samples each.
// Samples will be in the time range [mint, maxt)
// Block ID will be created with a delay of time duration blockDelay.
func CreateBlockWithBlockDelay(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	blockDelay time.Duration,
	extLset labels.Labels,
	resolution int64,
	hashFunc metadata.HashFunc,
) (ulid.ULID, error) {
	blockID, err := createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, false, hashFunc)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "block creation")
	}

	id, err := ulid.New(uint64(timestamp.FromTime(timestamp.Time(int64(blockID.Time())).Add(-blockDelay))), bytes.NewReader(blockID.Entropy()))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "create block id")
	}

	m, err := metadata.ReadFromDir(path.Join(dir, blockID.String()))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "open meta file")
	}

	m.ULID = id
	m.Compaction.Sources = []ulid.ULID{id}

	if err := m.WriteToDir(log.NewNopLogger(), path.Join(dir, blockID.String())); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "write meta.json file")
	}

	return id, os.Rename(path.Join(dir, blockID.String()), path.Join(dir, id.String()))
}

func createBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
	tombstones bool,
	hashFunc metadata.HashFunc,
) (id ulid.ULID, err error) {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = 10000000000
	h, err := tsdb.NewHead(nil, nil, nil, headOpts)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer func() {
		runutil.CloseWithErrCapture(&err, h, "TSDB Head")
		if e := os.RemoveAll(headOpts.ChunkDirRoot); e != nil {
			err = errors.Wrap(e, "delete chunks dir")
		}
	}()

	var g errgroup.Group
	var timeStepSize = (maxt - mint) / int64(numSamples+1)
	var batchSize = len(series) / runtime.GOMAXPROCS(0)

	for len(series) > 0 {
		l := batchSize
		if len(series) < 1000 {
			l = len(series)
		}
		batch := series[:l]
		series = series[l:]

		g.Go(func() error {
			t := mint

			for i := 0; i < numSamples; i++ {
				app := h.Appender(ctx)

				for _, lset := range batch {
					_, err := app.Append(0, lset, t, rand.Float64())
					if err != nil {
						if rerr := app.Rollback(); rerr != nil {
							err = errors.Wrapf(err, "rollback failed: %v", rerr)
						}

						return errors.Wrap(err, "add sample")
					}
				}
				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
				t += timeStepSize
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return id, err
	}
	c, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewNopLogger(), []int64{maxt - mint}, nil, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	id, err = c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	if id.Compare(ulid.ULID{}) == 0 {
		return id, errors.Errorf("nothing to write, asked for %d samples", numSamples)
	}

	blockDir := filepath.Join(dir, id.String())

	files := []metadata.File{}
	if hashFunc != metadata.NoneFunc {
		paths := []string{}
		if err := filepath.Walk(blockDir, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			paths = append(paths, path)
			return nil
		}); err != nil {
			return id, errors.Wrapf(err, "walking %s", dir)
		}

		for _, p := range paths {
			pHash, err := metadata.CalculateHash(p, metadata.SHA256Func, log.NewNopLogger())
			if err != nil {
				return id, errors.Wrapf(err, "calculating hash of %s", blockDir+p)
			}
			files = append(files, metadata.File{
				RelPath: strings.TrimPrefix(p, blockDir+"/"),
				Hash:    &pHash,
			})
		}
	}

	if _, err = metadata.InjectThanos(log.NewNopLogger(), blockDir, metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
		Files:      files,
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if !tombstones {
		if err = os.Remove(filepath.Join(dir, id.String(), "tombstones")); err != nil {
			return id, errors.Wrap(err, "remove tombstones")
		}
	}

	return id, nil
}
