// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2eutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	defaultPrometheusVersion   = "v0.54.1"
	defaultAlertmanagerVersion = "v0.20.0"
	defaultMinioVersion        = "RELEASE.2022-07-30T05-21-40Z"

	// Space delimited list of versions.
	promPathsEnvVar       = "THANOS_TEST_PROMETHEUS_PATHS"
	alertmanagerBinEnvVar = "THANOS_TEST_ALERTMANAGER_PATH"
	minioBinEnvVar        = "THANOS_TEST_MINIO_PATH"

	// A placeholder for actual Prometheus instance address in the scrape config.
	PromAddrPlaceHolder = "PROMETHEUS_ADDRESS"
)

var (
	histogramSample = histogram.Histogram{
		Schema:        0,
		Count:         20,
		Sum:           -3.1415,
		ZeroCount:     12,
		ZeroThreshold: 0.001,
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 4},
			{Offset: 1, Length: 1},
		},
		NegativeBuckets: []int64{1, 2, -2, 1, -1},
	}

	floatHistogramSample = histogram.FloatHistogram{
		ZeroThreshold: 0.01,
		ZeroCount:     5.5,
		Count:         15,
		Sum:           11.5,
		PositiveSpans: []histogram.Span{
			{Offset: -2, Length: 2},
			{Offset: 1, Length: 3},
		},
		PositiveBuckets: []float64{0.5, 0, 1.5, 2, 3.5},
		NegativeSpans: []histogram.Span{
			{Offset: 3, Length: 2},
			{Offset: 3, Length: 2},
		},
		NegativeBuckets: []float64{1.5, 0.5, 2.5, 3},
	}
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

	stdout, stderr bytes.Buffer
}

func NewTSDB() (*tsdb.DB, error) {
	dir, err := os.MkdirTemp("", "prometheus-test")
	if err != nil {
		return nil, err
	}
	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = math.MaxInt64
	return tsdb.Open(dir, nil, nil, opts, nil)
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

	f, err := os.Create(filepath.Join(db.Dir(), "prometheus.yml"))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Some well-known external labels so that we can test label resorting
	if _, err = io.WriteString(f, "global:\n  external_labels:\n    region: eu-west"); err != nil {
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
func (p *Prometheus) Start(ctx context.Context, l log.Logger) error {
	if p.running {
		return errors.New("Already started")
	}

	if err := p.db.Close(); err != nil {
		return err
	}
	if err := p.start(); err != nil {
		return err
	}
	if err := p.waitPrometheusUp(ctx, l, p.prefix); err != nil {
		return err
	}
	return nil
}

func (p *Prometheus) start() error {
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

	p.stderr.Reset()
	p.stdout.Reset()

	p.cmd.Stdout = &p.stdout
	p.cmd.Stderr = &p.stderr

	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("starting Prometheus failed: %w", err)
	}

	p.running = true
	return nil
}

func (p *Prometheus) waitPrometheusUp(ctx context.Context, logger log.Logger, prefix string) error {
	if !p.running {
		return errors.New("method Start was not invoked.")
	}
	return runutil.RetryWithLog(logger, time.Second, ctx.Done(), func() error {
		r, err := http.Get(fmt.Sprintf("http://%s%s/-/ready", p.addr, prefix))
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

func (p *Prometheus) Restart(ctx context.Context, l log.Logger) error {
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrap(err, "failed to kill Prometheus. Kill it manually")
	}
	_ = p.cmd.Wait()
	if err := p.start(); err != nil {
		return err
	}
	return p.waitPrometheusUp(ctx, l, p.prefix)
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
func (p *Prometheus) Stop() (rerr error) {
	if !p.running {
		return nil
	}

	if p.cmd.Process != nil {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return errors.Wrapf(err, "failed to Prometheus. Kill it manually and clean %s dir", p.db.Dir())
		}

		err := p.cmd.Wait()
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if exitErr.ExitCode() != -1 {
					fmt.Fprintln(os.Stderr, "Prometheus exited with", exitErr.ExitCode())
					fmt.Fprintln(os.Stderr, "stdout:\n", p.stdout.String(), "\nstderr:\n", p.stderr.String())
				} else {
					err = nil
				}
			}
		}

		if err != nil {
			return fmt.Errorf("waiting for Prometheus to exit: %w", err)
		}
	}

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

	if err := os.WriteFile(path.Join(dir, uid.String(), "meta.json"), b, os.ModePerm); err != nil {
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
	return createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, false, hashFunc, chunkenc.ValFloat)
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
	return createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, true, hashFunc, chunkenc.ValFloat)
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
	return createBlockWithDelay(ctx, dir, series, numSamples, mint, maxt, blockDelay, extLset, resolution, hashFunc, chunkenc.ValFloat)
}

// CreateHistogramBlockWithDelay writes a block with the given native histogram series and numSamples samples each.
// Samples will be in the time range [mint, maxt).
func CreateHistogramBlockWithDelay(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	blockDelay time.Duration,
	extLset labels.Labels,
	resolution int64,
	hashFunc metadata.HashFunc,
) (id ulid.ULID, err error) {
	return createBlockWithDelay(ctx, dir, series, numSamples, mint, maxt, blockDelay, extLset, resolution, hashFunc, chunkenc.ValHistogram)
}

// CreateFloatHistogramBlockWithDelay writes a block with the given float native histogram series and numSamples samples each.
// Samples will be in the time range [mint, maxt).
func CreateFloatHistogramBlockWithDelay(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	blockDelay time.Duration,
	extLset labels.Labels,
	resolution int64,
	hashFunc metadata.HashFunc,
) (id ulid.ULID, err error) {
	return createBlockWithDelay(ctx, dir, series, numSamples, mint, maxt, blockDelay, extLset, resolution, hashFunc, chunkenc.ValFloatHistogram)
}

func createBlockWithDelay(ctx context.Context, dir string, series []labels.Labels, numSamples int, mint int64, maxt int64, blockDelay time.Duration, extLset labels.Labels, resolution int64, hashFunc metadata.HashFunc, samplesType chunkenc.ValueType) (ulid.ULID, error) {
	blockID, err := createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, false, hashFunc, samplesType)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "block creation")
	}

	id, err := ulid.New(uint64(timestamp.FromTime(timestamp.Time(int64(blockID.Time())).Add(-blockDelay))), bytes.NewReader(blockID.Entropy()))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "create block id")
	}

	bdir := path.Join(dir, blockID.String())
	m, err := metadata.ReadFromDir(bdir)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "open meta file")
	}

	logger := log.NewNopLogger()
	m.ULID = id
	m.Compaction.Sources = []ulid.ULID{id}
	if err := m.WriteToDir(logger, path.Join(dir, blockID.String())); err != nil {
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
	sampleType chunkenc.ValueType,
) (id ulid.ULID, err error) {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = 10000000000
	headOpts.EnableNativeHistograms = *atomic.NewBool(true)
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
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
	r := rand.New(rand.NewSource(int64(numSamples)))
	var randMutex sync.Mutex

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
					var err error
					if sampleType == chunkenc.ValFloat {
						randMutex.Lock()
						_, err = app.Append(0, lset, t, r.Float64())
						randMutex.Unlock()
					} else if sampleType == chunkenc.ValHistogram {
						_, err = app.AppendHistogram(0, lset, t, &histogramSample, nil)
					} else if sampleType == chunkenc.ValFloatHistogram {
						_, err = app.AppendHistogram(0, lset, t, nil, &floatHistogramSample)
					}
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

	ids, err := c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}
	if len(ids) == 0 {
		return id, errors.Errorf("nothing to write, asked for %d samples", numSamples)
	}
	id = ids[0]

	blockDir := filepath.Join(dir, id.String())
	logger := log.NewNopLogger()
	seriesSize, err := gatherMaxSeriesSize(ctx, filepath.Join(blockDir, "index"))
	if err != nil {
		return id, errors.Wrap(err, "gather max series size")
	}

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

	if _, err = metadata.InjectThanos(logger, blockDir, metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
		Files:      files,
		// For simplicity, use series size for all series size fields.
		IndexStats: metadata.IndexStats{
			SeriesMaxSize:   seriesSize,
			SeriesP90Size:   seriesSize,
			SeriesP99Size:   seriesSize,
			SeriesP999Size:  seriesSize,
			SeriesP9999Size: seriesSize,
		},
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

func gatherMaxSeriesSize(ctx context.Context, fn string) (int64, error) {
	r, err := index.NewFileReader(fn)
	if err != nil {
		return 0, errors.Wrap(err, "open index file")
	}
	defer runutil.CloseWithErrCapture(&err, r, "gather index issue file reader")

	key, value := index.AllPostingsKey()
	p, err := r.Postings(ctx, key, value)
	if err != nil {
		return 0, errors.Wrap(err, "get all postings")
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	offsetMultiplier := 1
	version := r.Version()
	if version >= 2 {
		offsetMultiplier = 16
	}

	// Per series.
	var (
		prevId        storage.SeriesRef
		maxSeriesSize int64
	)
	for p.Next() {
		id := p.At()
		if prevId != 0 {
			// Approximate size.
			seriesSize := int64(id-prevId) * int64(offsetMultiplier)
			if seriesSize > maxSeriesSize {
				maxSeriesSize = seriesSize
			}
		}
		prevId = id
	}
	if p.Err() != nil {
		return 0, errors.Wrap(err, "walk postings")
	}

	return maxSeriesSize, nil
}

// CreateBlockWithChurn writes a block with the given series. Start time of each series
// will be randomized in the given time window to create churn. Only float chunk is supported right now.
func CreateBlockWithChurn(
	ctx context.Context,
	rnd *rand.Rand,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
	scrapeInterval int64,
	seriesSize int64,
) (id ulid.ULID, err error) {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = 10000000000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer func() {
		runutil.CloseWithErrCapture(&err, h, "TSDB Head")
		if e := os.RemoveAll(headOpts.ChunkDirRoot); e != nil {
			err = errors.Wrap(e, "delete chunks dir")
		}
	}()

	app := h.Appender(ctx)
	for i := 0; i < len(series); i++ {

		var ref storage.SeriesRef
		start := RandRange(rnd, mint, maxt)
		for j := 0; j < numSamples; j++ {
			if ref == 0 {
				ref, err = app.Append(0, series[i], start, float64(i+j))
			} else {
				ref, err = app.Append(ref, series[i], start, float64(i+j))
			}
			if err != nil {
				if rerr := app.Rollback(); rerr != nil {
					err = errors.Wrapf(err, "rollback failed: %v", rerr)
				}
				return id, errors.Wrap(err, "add sample")
			}
			start += scrapeInterval
			if start > maxt {
				break
			}
		}
	}
	if err := app.Commit(); err != nil {
		return id, errors.Wrap(err, "commit")
	}

	c, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewNopLogger(), []int64{maxt - mint}, nil, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	ids, err := c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	if len(ids) == 0 {
		return id, errors.Errorf("nothing to write, asked for %d samples", numSamples)
	}
	id = ids[0]

	blockDir := filepath.Join(dir, id.String())
	logger := log.NewNopLogger()

	if _, err = metadata.InjectThanos(logger, blockDir, metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
		IndexStats: metadata.IndexStats{SeriesMaxSize: seriesSize},
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	return id, nil
}

// AddDelay rewrites a given block with delay.
func AddDelay(blockID ulid.ULID, dir string, blockDelay time.Duration) (ulid.ULID, error) {
	id, err := ulid.New(uint64(timestamp.FromTime(timestamp.Time(int64(blockID.Time())).Add(-blockDelay))), bytes.NewReader(blockID.Entropy()))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "create block id")
	}

	bdir := path.Join(dir, blockID.String())
	m, err := metadata.ReadFromDir(bdir)
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "open meta file")
	}

	logger := log.NewNopLogger()
	m.ULID = id
	m.Compaction.Sources = []ulid.ULID{id}
	if err := m.WriteToDir(logger, path.Join(dir, blockID.String())); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "write meta.json file")
	}

	return id, os.Rename(path.Join(dir, blockID.String()), path.Join(dir, id.String()))
}
