package discovery

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
)

const (
	testDir      = "fixtures"
	fileContent1 = "localhost:9090"
	fileContent2 = "example.org:443"
)

func TestFileSD(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	defer removeWithLogOnErr(logger, filepath.Join(testDir, "_test_valid.yml"))
	defer removeWithLogOnErr(logger, filepath.Join(testDir, "_test_valid.json"))
	defer removeWithLogOnErr(logger, filepath.Join(testDir, "_test_invalid_nil.json"))
	defer removeWithLogOnErr(logger, filepath.Join(testDir, "_test_invalid_nil.yml"))
	testFileSD(t, "valid", ".yml", true)
	testFileSD(t, "valid", ".json", true)
	testFileSD(t, "invalid_nil", ".json", false)
	testFileSD(t, "invalid_nil", ".yml", false)
}

func testFileSD(t *testing.T, prefix, ext string, expect bool) {
	// As interval refreshing is more of a fallback, we only want to test
	// whether file watches work as expected.
	var conf SDConfig
	conf.Files = []string{filepath.Join(testDir, "_*"+ext)}
	conf.RefreshInterval = time.Duration(1 * time.Hour)

	var (
		discoverer  = NewFileDiscoverer(&conf, nil)
		ch          = make(chan *Discoverable)
		ctx, cancel = context.WithCancel(context.Background())
		logger      = log.NewLogfmtLogger(os.Stderr)
	)
	go discoverer.Run(ctx, ch)

	select {
	case <-time.After(25 * time.Millisecond):
		// Expected.
	case tgs := <-ch:
		t.Fatalf("Unexpected target groups in file discovery: %s", tgs)
	}

	// To avoid empty group struct sent from the discovery caused by invalid fsnotify updates,
	// drain the channel until we are ready with the test files.
	fileReady := make(chan struct{})
	drainReady := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
			case <-fileReady:
				close(drainReady)
				return
			}
		}
	}()

	newf, err := os.Create(filepath.Join(testDir, "_test_"+prefix+ext))
	if err != nil {
		t.Fatal(err)
	}
	defer runutil.CloseWithLogOnErr(logger, newf, "test file sd")

	f, err := os.Open(filepath.Join(testDir, prefix+ext))
	if err != nil {
		t.Fatal(err)
	}
	defer runutil.CloseWithLogOnErr(logger, f, "file sd")
	_, err = io.Copy(newf, f)
	if err != nil {
		t.Fatal(err)
	}

	// Test file is ready so stop draining the discovery channel.
	// It contains two target groups.
	close(fileReady)
	<-drainReady
	_, err = newf.WriteString(" ") // One last meaningless write to trigger fsnotify and a new loop of the discovery service.
	if err != nil {
		t.Fatal(err)
	}

	timeout := time.After(15 * time.Second)
retry:
	for {
		select {
		case <-timeout:
			if expect {
				t.Fatalf("Expected new target group but got none")
			} else {
				// Invalid type fsd should always break down.
				break retry
			}
		case discoverable := <-ch:
			if !expect {
				t.Fatalf("Unexpected file content, we expected a failure here.")
			}
			if len(discoverable.Services) != 2 {
				continue retry // Potentially a partial write, just retry.
			}

			discovered := discoverable.Services[0]
			if discovered != fileContent1 {
				t.Fatalf("Unexpected file content. Got %v, but expected %v", discovered, fileContent1)
			}

			discovered = discoverable.Services[1]
			if discovered != fileContent2 {
				t.Fatalf("Unexpected file content. Got %v, but expected %v", discovered, fileContent2)
			}

			break retry
		}
	}

	// Based on unknown circumstances, sometimes fsnotify will trigger more events in
	// some runs (which might be empty, chains of different operations etc.).
	// We have to drain those to avoid deadlocking and must
	// not try to make sense of it all...
	drained := make(chan struct{})
	go func() {
		for {
			select {
			case discoverable := <-ch:
				// Below we will change the file to a bad syntax. Previously extracted target
				// groups must not be deleted via sending an empty target group.
				if len(discoverable.Services) == 0 {
					t.Errorf("Unexpected empty file content received")
				}
			case <-time.After(500 * time.Millisecond):
				close(drained)
				return
			}
		}
	}()

	newf, err = os.Create(filepath.Join(testDir, "_test.new"))
	if err != nil {
		t.Fatal(err)
	}
	defer removeWithLogOnErr(logger, newf.Name())

	if _, err := newf.Write([]byte("]gibberish\n][")); err != nil {
		t.Fatal(err)
	}
	runutil.CloseWithLogOnErr(logger, newf, "test file sd")

	if err := os.Rename(newf.Name(), filepath.Join(testDir, "_test_"+prefix+ext)); err != nil {
		t.Fatal(err)
	}

	cancel()
	<-drained
}

func removeWithLogOnErr(logger log.Logger, name string) {
	if err := os.Remove(name); err != nil {
		level.Warn(logger).Log("msg", "detected remove error", "err", errors.Wrap(err, fmt.Sprintf("failed removing %v", name)))
	}
}
