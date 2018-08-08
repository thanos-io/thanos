package testutil

import (
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/sync/errgroup"
)

// Prometheus represents a test instance for integration testing.
// It can be populated with data before being started.
type Prometheus struct {
	dir string
	db  *tsdb.DB

	running bool
	cmd     *exec.Cmd
	addr    string
}

func NewTSDB() (*tsdb.DB, error) {
	dir, err := ioutil.TempDir("", "prometheus-test")
	if err != nil {
		return nil, err
	}
	return tsdb.Open(dir, nil, nil, &tsdb.Options{
		BlockRanges:       []int64{2 * 3600 * 1000},
		RetentionDuration: math.MaxInt64,
	})
}

// NewPrometheus creates a new test Prometheus instance that will listen on address.
func NewPrometheus() (*Prometheus, error) {
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
		dir:  db.Dir(),
		db:   db,
		addr: "<prometheus-not-started>",
	}, nil
}

// Start running the Prometheus instance and return.
func (p *Prometheus) Start() error {
	p.running = true

	if err := p.db.Close(); err != nil {
		return err
	}

	port, err := FreePort()
	if err != nil {
		return err
	}

	p.addr = fmt.Sprintf("localhost:%d", port)
	p.cmd = exec.Command(
		"prometheus",
		"--storage.tsdb.path="+p.db.Dir(),
		"--web.listen-address="+p.addr,
		"--config.file="+filepath.Join(p.db.Dir(), "prometheus.yml"),
	)
	go func() {
		if b, err := p.cmd.CombinedOutput(); err != nil {
			fmt.Fprintln(os.Stderr, "running Prometheus failed", err)
			fmt.Fprintln(os.Stderr, string(b))
		}
	}()
	time.Sleep(2 * time.Second)

	return nil
}

// Addr gets correct address after Start method.
func (p *Prometheus) Addr() string {
	return p.addr
}

// SetConfig updates the contents of the config file.
func (p *Prometheus) SetConfig(s string) (err error) {
	f, err := os.Create(filepath.Join(p.dir, "prometheus.yml"))
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(nil, &err, f, "prometheus config")

	_, err = f.Write([]byte(s))
	return err
}

// Stop terminates Prometheus and clean up its data directory.
func (p *Prometheus) Stop() error {
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return errors.Wrapf(err, "failed to Prometheus. Kill it manually and cleanr %s dir", p.db.Dir())
	}

	time.Sleep(time.Second / 2)
	return p.cleanup()
}

func (p *Prometheus) cleanup() error {
	return os.RemoveAll(p.db.Dir())
}

// Appender returns a new appender to populate the Prometheus instance with data.
// All appenders must be closed before Start is called and no new ones must be opened
// afterwards.
func (p *Prometheus) Appender() tsdb.Appender {
	if p.running {
		panic("Appender must not be called after start")
	}
	return p.db.Appender()
}

// CreateBlock writes a block with the given series and numSamples samples each.
// Samples will be in the time range [mint, maxt).
func CreateBlock(
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
) (id ulid.ULID, err error) {
	h, err := tsdb.NewHead(nil, nil, tsdb.NopWAL(), 10000000000)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer runutil.CloseWithErrCapture(log.NewNopLogger(), &err, h, "TSDB Head")

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
				app := h.Appender()

				for _, lset := range batch {
					_, err := app.Add(lset, t, rand.Float64())
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
	c, err := tsdb.NewLeveledCompactor(nil, log.NewNopLogger(), []int64{maxt - mint}, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	id, err = c.Write(dir, h, mint, maxt)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	if _, err = block.InjectThanosMeta(log.NewNopLogger(), filepath.Join(dir, id.String()), block.ThanosMeta{
		Labels:     extLset.Map(),
		Downsample: block.ThanosDownsampleMeta{Resolution: resolution},
		Source:     block.TestSource,
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if err = os.Remove(filepath.Join(dir, id.String(), "tombstones")); err != nil {
		return id, errors.Wrap(err, "remove tombstones")
	}

	return id, nil
}
