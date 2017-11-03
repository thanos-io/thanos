package testutil

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/prometheus/tsdb"
)

// Prometheus represents a test instance for integration testing.
// It can be populated with data before being started.
type Prometheus struct {
	dir     string
	addr    string
	running bool
	db      *tsdb.DB
}

// NewPrometheus creates a new test Prometheus instance that will listen on address.
func NewPrometheus(address string) (*Prometheus, error) {
	dir, err := ioutil.TempDir("", "prometheus-test")
	if err != nil {
		return nil, err
	}
	// Just touch an empty config file. We don't need to actually scrape anything.
	_, err = os.Create(filepath.Join(dir, "prometheus.yml"))
	if err != nil {
		return nil, err
	}

	db, err := tsdb.Open(dir, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	return &Prometheus{
		dir:  dir,
		addr: address,
		db:   db,
	}, nil
}

// Start runs the Prometheus instance until the context is canceled.
func (p *Prometheus) Start(ctx context.Context) error {
	// /// debug
	// q, _ := p.db.Querier(0, 100000000000000)
	// ss := q.Select(labels.NewEqualMatcher("a", "b"))
	// for ss.Next() {
	// 	fmt.Println(ss.At().Labels())
	// 	it := ss.At().Iterator()

	// 	for it.Next() {
	// 		fmt.Println(it.At())
	// 	}
	// }
	////

	p.running = true
	if err := p.db.Close(); err != nil {
		return err
	}
	// TODO(fabxc): Something needs to get debugged and fixed upstream.
	time.Sleep(5 * time.Second)

	fmt.Println(p.dir)
	// return nil

	go func() {
		// defer p.cleanup()

		b, err := exec.CommandContext(ctx,
			"prometheus",
			"--storage.tsdb.path="+p.dir,
			"--web.listen-address="+p.addr,
			"--config.file="+filepath.Join(p.dir, "prometheus.yml"),
		).CombinedOutput()

		// if err != nil {
		fmt.Fprintln(os.Stderr, "running Prometheus failed", err)
		fmt.Fprintln(os.Stderr, string(b))
		// }
	}()
	time.Sleep(2 * time.Second)
	return nil
}

func (p *Prometheus) cleanup() error {
	return os.RemoveAll(p.dir)
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
