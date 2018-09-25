package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Discoverer interface {
	// Run hands a channel to the discovery provider through which it can send updated service discoveries.
	// Must returns if the context gets canceled. It should not close the update
	// channel on returning.
	Run(ctx context.Context, up chan<- []Discoverable)
}

//TODO(ivan): rename (maybe Target is a better name)? agree on the format of the files(currently using only a list of strings)?
// Discoverable is the representation of what a Discoverer finds.
// It contains the source of the discovery and the set of services that were found.
type Discoverable struct {
	Source   string
	Services []string
}

// SDConfig is the configuration for file based service discovery
type SDConfig struct {
	Files           []string
	RefreshInterval time.Duration
}

// Discovery provides service discovery functionality based
// on files that contain target groups in JSON or YAML format. Refreshing
// happens using file watches and periodic refreshes.
type FileDiscoverer struct {
	paths    []string
	watcher  *fsnotify.Watcher
	interval time.Duration
	//TODO(ivan): add timestamp metrics like in prometheus

	// lastRefresh stores which files were found during the last refresh
	// This is used to detect deleted files.
	lastRefresh map[string]struct{}
	logger      log.Logger
}

// NewFileDiscoverer returns a new file discoverer for the given paths.
func NewFileDiscoverer(conf *SDConfig, logger log.Logger) *FileDiscoverer {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &FileDiscoverer{
		paths:    conf.Files,
		interval: conf.RefreshInterval,
		logger:   logger,
	}
}

// Run implements the Discoverer interface.
func (d *FileDiscoverer) Run(ctx context.Context, ch chan<- *Discoverable) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		level.Error(d.logger).Log("msg", "Error adding file watcher", "err", err)
		return
	}
	d.watcher = watcher
	defer d.stop()

	d.refresh(ctx, ch)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case event := <-d.watcher.Events:
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
			d.refresh(ctx, ch)

		case <-ticker.C:
			// Setting a new watch after an update might fail. Make sure we don't lose
			// those files forever.
			d.refresh(ctx, ch)

		case err := <-d.watcher.Errors:
			if err != nil {
				level.Error(d.logger).Log("msg", "Error watching file", "err", err)
			}
		}
	}
}

// watchFiles sets watches on all full paths or directories that were configured for
// this file discovery.
func (d *FileDiscoverer) watchFiles() {
	if d.watcher == nil {
		panic("no watcher configured")
	}
	for _, p := range d.paths {
		if idx := strings.LastIndex(p, "/"); idx > -1 {
			p = p[:idx]
		} else {
			p = "./"
		}
		if err := d.watcher.Add(p); err != nil {
			level.Error(d.logger).Log("msg", "Error adding file watch", "path", p, "err", err)
		}
	}
}

// listFiles returns a list of all files that match the configured patterns.
func (d *FileDiscoverer) listFiles() []string {
	var paths []string
	for _, p := range d.paths {
		files, err := filepath.Glob(p)
		if err != nil {
			level.Error(d.logger).Log("msg", "Error expanding glob", "glob", p, "err", err)
			continue
		}
		paths = append(paths, files...)
	}
	return paths
}

// refresh reads all files matching the discovery's patterns and sends the respective
// updated discoverables through the channel.
func (d *FileDiscoverer) refresh(ctx context.Context, ch chan<- *Discoverable) {
	ref := map[string]struct{}{}
	for _, p := range d.listFiles() {
		discoverable, err := d.readFile(p)
		if err != nil {
			level.Error(d.logger).Log("msg", "Error reading file", "path", p, "err", err)
			// Prevent deletion down below. Failed read might be caused by refreshing during a partial write of the file.
			ref[p] = struct{}{}
			continue
		}
		select {
		case ch <- discoverable:
		case <-ctx.Done():
			return
		}

		ref[p] = struct{}{}
	}
	// Send empty updates for sources that disappeared.
	for f := range d.lastRefresh {
		_, ok := ref[f]
		if !ok {
			level.Debug(d.logger).Log("msg", "file_sd refresh found file that should be removed", "file", f)
			select {
			case ch <- &Discoverable{Source: f}:
			case <-ctx.Done():
				return
			}
		}
	}
	d.lastRefresh = ref

	d.watchFiles()
}

// stop shuts down the file watcher.
func (d *FileDiscoverer) stop() {
	level.Debug(d.logger).Log("msg", "Stopping file discovery...", "paths", fmt.Sprintf("%v", d.paths))

	done := make(chan struct{})
	defer close(done)

	// Closing the watcher will deadlock unless all events and errors are drained.
	go func() {
		for {
			select {
			case <-d.watcher.Errors:
			case <-d.watcher.Events:
				// Drain all events and errors.
			case <-done:
				return
			}
		}
	}()
	if err := d.watcher.Close(); err != nil {
		level.Error(d.logger).Log("msg", "Error closing file watcher", "paths", fmt.Sprintf("%v", d.paths), "err", err)
	}

	level.Debug(d.logger).Log("msg", "File discovery stopped")
}

// readFile reads a JSON or YAML list of discoverables from the file, depending on its
// file extension.
func (d *FileDiscoverer) readFile(filename string) (*Discoverable, error) {
	fmt.Printf("reading file %v \n", filename)
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	content, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}

	discoverable := &Discoverable{
		Source:   filename,
		Services: []string{},
	}

	switch ext := filepath.Ext(filename); strings.ToLower(ext) {
	case ".json":
		if err := json.Unmarshal(content, &discoverable.Services); err != nil {
			return nil, err
		}
	case ".yml", ".yaml":
		if err := yaml.UnmarshalStrict(content, &discoverable.Services); err != nil {
			return nil, err
		}
	default:
		panic(fmt.Errorf("discovery.File.readFile: unhandled file extension %q", ext))
	}

	if discoverable.Services == nil {
		return nil, errors.New(fmt.Sprintf("nil services find in file %v", filename))
	}

	return discoverable, nil
}
