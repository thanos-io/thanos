package receive

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"gopkg.in/fsnotify.v1"
)

// HashringConfig represents the configuration for a hashring
// a receive node knows about.
type HashringConfig struct {
	Hashring  string   `json:"hashring"`
	Tenants   []string `json:"tenants"`
	Endpoints []string `json:"endpoints"`
}

// ConfigWatcher is able to watch a file containing a hashring configuration
// for updates.
type ConfigWatcher struct {
	ch       chan []HashringConfig
	path     string
	interval time.Duration
	logger   log.Logger
	watcher  *fsnotify.Watcher

	changesCounter prometheus.Counter
	errorCounter   prometheus.Counter
	refreshCounter prometheus.Counter

	// last is the last known configuration.
	last []HashringConfig
}

// NewConfigWatcher creates a new ConfigWatcher.
func NewConfigWatcher(logger log.Logger, r prometheus.Registerer, path string, interval model.Duration) (*ConfigWatcher, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "creating file watcher")
	}
	if err := watcher.Add(path); err != nil {
		return nil, errors.Wrap(err, "adding path to file watcher")
	}
	c := &ConfigWatcher{
		ch:       make(chan []HashringConfig),
		path:     path,
		interval: time.Duration(interval),
		logger:   logger,
		watcher:  watcher,
		changesCounter: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_changes_total",
				Help: "The number of times the hashrings configuration file has changed.",
			}),
		errorCounter: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_errors_total",
				Help: "The number of errors watching the hashrings configuration file.",
			}),
		refreshCounter: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "thanos_receive_hashrings_file_refreshes_total",
				Help: "The number of refreshes of the hashrings configuration file.",
			}),
	}

	if r != nil {
		r.MustRegister(
			c.changesCounter,
			c.errorCounter,
			c.refreshCounter,
		)
	}

	return c, nil
}

// Run starts the ConfigWatcher until the given context is cancelled.
func (cw *ConfigWatcher) Run(ctx context.Context) {
	defer cw.stop()

	cw.refresh(ctx)

	ticker := time.NewTicker(cw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case event := <-cw.watcher.Events:
			// fsnotify sometimes sends a bunch of events without name or operation.
			// It's unclear what they are and why they are sent - filter them out.
			if len(event.Name) == 0 {
				break
			}
			// Everything but a chmod requires rereading.
			if event.Op^fsnotify.Chmod == 0 {
				break
			}
			// Changes to a file can spawn various sequences of events with
			// different combinations of operations. For all practical purposes
			// this is inaccurate.
			// The most reliable solution is to reload everything if anything happens.
			cw.refresh(ctx)

		case <-ticker.C:
			// Setting a new watch after an update might fail. Make sure we don't lose
			// those files forever.
			cw.refresh(ctx)

		case err := <-cw.watcher.Errors:
			if err != nil {
				cw.errorCounter.Inc()
				level.Error(cw.logger).Log("msg", "error watching file", "err", err)
			}
		}
	}
}

// readFile reads the configured file and returns a configuration.
func (cw *ConfigWatcher) readFile() ([]HashringConfig, error) {
	fd, err := os.Open(cw.path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := fd.Close(); err != nil {
			level.Error(cw.logger).Log("msg", "failed to close file", "err", err, "path", cw.path)
		}
	}()

	content, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}

	var config []HashringConfig
	err = json.Unmarshal(content, &config)
	return config, err
}

// refresh reads the configured file and sends the hashring configuration on the channel.
func (cw *ConfigWatcher) refresh(ctx context.Context) {
	cw.refreshCounter.Inc()
	config, err := cw.readFile()
	if err != nil {
		cw.errorCounter.Inc()
		level.Error(cw.logger).Log("msg", "failed to read configuration file", "err", err, "path", cw.path)
		return
	}

	// If there was no change to the configuration, return early.
	if reflect.DeepEqual(cw.last, config) {
		return
	}
	cw.changesCounter.Inc()
	// Save the last known configuration.
	cw.last = config

	select {
	case <-ctx.Done():
		return
	case cw.ch <- config:
		return
	}
}

// stop shuts down the config watcher.
func (cw *ConfigWatcher) stop() {
	level.Debug(cw.logger).Log("msg", "stopping hashring configuration watcher...", "path", cw.path)

	done := make(chan struct{})
	defer close(done)

	// Closing the watcher will deadlock unless all events and errors are drained.
	go func() {
		for {
			select {
			case <-cw.watcher.Errors:
			case <-cw.watcher.Events:
			// Drain all events and errors.
			case <-done:
				return
			}
		}
	}()
	if err := cw.watcher.Close(); err != nil {
		level.Error(cw.logger).Log("msg", "error closing file watcher", "path", cw.path, "err", err)
	}

	close(cw.ch)
	level.Debug(cw.logger).Log("msg", "hashring configuration watcher stopped")
}

// C returns a chan that gets hashring configuration updates.
func (cw *ConfigWatcher) C() <-chan []HashringConfig {
	return cw.ch
}
