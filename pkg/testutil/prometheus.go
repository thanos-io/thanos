package testutil

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/tsdb"
)

// Prometheus represents a test instance for integration testing.
// It can be populated with data before being started.
type Prometheus struct {
	addr    string
	running bool
	db      *tsdb.DB
	cmd     *exec.Cmd
}

func NewTSDB() (*tsdb.DB, error) {
	dir, err := ioutil.TempDir("", "prometheus-test")
	if err != nil {
		return nil, err
	}

	return tsdb.Open(dir, nil, nil, &tsdb.Options{
		WALFlushInterval:  10 * time.Millisecond,
		BlockRanges:       []int64{2 * 3600 * 1000},
		RetentionDuration: math.MaxInt64,
	})
}

// NewPrometheus creates a new test Prometheus instance that will listen on address.
func NewPrometheus(address string) (*Prometheus, error) {
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
		addr: address,
		db:   db,
	}, nil
}

// Start running the Prometheus instance and return.
func (p *Prometheus) Start() error {
	p.running = true
	time.Sleep(time.Second / 2)

	if err := p.db.Close(); err != nil {
		return err
	}

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

// Stop terminates Prometheus and clean up its data directory.
func (p *Prometheus) Stop() error {
	p.cmd.Process.Signal(syscall.SIGTERM)
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
