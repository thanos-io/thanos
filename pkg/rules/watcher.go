// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DefaultFileWatcherInterval is the fallback poll interval used by FileWatcher
// to recover from missed fsnotify events (e.g. when a watched directory is
// removed and recreated). It mirrors the default refresh interval used by the
// thanos receive hashring config watcher.
const DefaultFileWatcherInterval = 5 * time.Minute

// FileWatcher watches a set of rule file glob patterns and signals on a
// channel whenever the resolved file set or any file content changes. It
// combines fsnotify for prompt detection with a periodic refresh as a safety
// net, mirroring the approach in pkg/receive/config.go.
//
// The receive watcher is not reused directly because it is shaped around a
// single file and a typed payload channel (parsed hashring config), whereas
// rule reload is driven from multiple glob patterns and only needs a signal
// (the reload itself is performed by the existing reloadRules path). Sharing
// the underlying primitive across components is left for a future refactor.
//
// Parent directories are watched rather than individual files so that
// Kubernetes ConfigMap mounts (which atomically swap a `..data` symlink) and
// glob patterns matching newly-added files are handled correctly.
type FileWatcher struct {
	patterns []string
	interval time.Duration
	logger   log.Logger
	watcher  *fsnotify.Watcher

	ch chan struct{}

	watchedDirs map[string]struct{}
	lastHash    uint64

	changesCounter prometheus.Counter
	errorCounter   prometheus.Counter
}

// NewFileWatcher creates a FileWatcher for the given rule file glob patterns.
// The returned watcher must be started with Run.
func NewFileWatcher(logger log.Logger, reg prometheus.Registerer, patterns []string, interval time.Duration) (*FileWatcher, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "creating file watcher")
	}
	fw := &FileWatcher{
		patterns:    patterns,
		interval:    interval,
		logger:      logger,
		watcher:     w,
		ch:          make(chan struct{}, 1),
		watchedDirs: make(map[string]struct{}),
		changesCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_rule_config_files_changes_total",
			Help: "The number of times the rule files have been detected as changed by the file watcher.",
		}),
		errorCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_rule_config_files_errors_total",
			Help: "The number of errors encountered while watching rule files.",
		}),
	}
	fw.refreshWatchedDirs()
	// Seed lastHash so the initial reloadRules() call by the consumer is not
	// immediately followed by a redundant reload triggered by this watcher.
	// Seeding here (rather than in Run) means changes that happen between
	// construction and Run starting are still detected via the fsnotify
	// events that have queued up in the meantime.
	fw.lastHash = fw.computeHash()
	return fw, nil
}

// C returns a channel that emits a signal whenever the resolved rule file set
// or any file content has changed since the previous notification.
func (w *FileWatcher) C() <-chan struct{} { return w.ch }

// Run drives the watcher until ctx is canceled.
func (w *FileWatcher) Run(ctx context.Context) {
	defer w.stop()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case event := <-w.watcher.Events:
			// fsnotify can emit empty events; ignore them.
			if event.Name == "" {
				continue
			}
			// CHMOD-only events are not relevant to rule content.
			if event.Op^fsnotify.Chmod == 0 {
				continue
			}
			w.refreshWatchedDirs()
			w.maybeNotify()

		case <-ticker.C:
			// Safety net: re-add directories dropped after a remove and
			// catch changes that fsnotify did not report.
			w.refreshWatchedDirs()
			w.maybeNotify()

		case err := <-w.watcher.Errors:
			if err != nil {
				w.errorCounter.Inc()
				level.Error(w.logger).Log("msg", "error watching rule files", "err", err)
			}
		}
	}
}

// resolveFiles expands the configured glob patterns to a deduplicated, sorted
// list of file paths.
func (w *FileWatcher) resolveFiles() []string {
	seen := make(map[string]struct{})
	var files []string
	for _, pat := range w.patterns {
		matches, err := filepath.Glob(pat)
		if err != nil {
			level.Warn(w.logger).Log("msg", "invalid rule file glob pattern", "pattern", pat, "err", err)
			continue
		}
		for _, m := range matches {
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			files = append(files, m)
		}
	}
	sort.Strings(files)
	return files
}

// refreshWatchedDirs registers fsnotify against the parent directory of every
// currently-resolved rule file, plus the directory of each configured glob
// pattern. Directories that no longer match are removed.
func (w *FileWatcher) refreshWatchedDirs() {
	desired := make(map[string]struct{})

	add := func(path string) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return
		}
		desired[filepath.Dir(abs)] = struct{}{}
	}

	for _, pat := range w.patterns {
		add(pat)
	}
	for _, f := range w.resolveFiles() {
		add(f)
	}

	for d := range desired {
		if _, ok := w.watchedDirs[d]; ok {
			continue
		}
		if err := w.watcher.Add(d); err != nil {
			// Directory may not exist yet (e.g. mount not ready). The
			// periodic tick will retry.
			level.Debug(w.logger).Log("msg", "failed to watch directory", "dir", d, "err", err)
			continue
		}
		w.watchedDirs[d] = struct{}{}
	}

	for d := range w.watchedDirs {
		if _, ok := desired[d]; ok {
			continue
		}
		if err := w.watcher.Remove(d); err != nil {
			level.Debug(w.logger).Log("msg", "failed to unwatch directory", "dir", d, "err", err)
		}
		delete(w.watchedDirs, d)
	}
}

// computeHash returns a hash deterministic over the resolved rule file set
// and the contents of those files. Read errors are folded into the hash so
// transient failures do not repeatedly fire change notifications.
func (w *FileWatcher) computeHash() uint64 {
	h := sha256.New()
	for _, f := range w.resolveFiles() {
		_, _ = h.Write([]byte(f))
		_, _ = h.Write([]byte{0})
		b, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			_, _ = h.Write([]byte("error:"))
			_, _ = h.Write([]byte(err.Error()))
			_, _ = h.Write([]byte{0})
			continue
		}
		_, _ = h.Write(b)
		_, _ = h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8])
}

// maybeNotify emits a signal on C() if the rule file state has changed since
// the previous notification. The channel is buffered (size 1) so that rapid
// successive changes coalesce into a single signal.
func (w *FileWatcher) maybeNotify() {
	cur := w.computeHash()
	if cur == w.lastHash {
		return
	}
	w.lastHash = cur
	w.changesCounter.Inc()
	select {
	case w.ch <- struct{}{}:
	default:
		// A signal is already pending.
	}
}

// stop closes the underlying fsnotify watcher and the signal channel. Pending
// events are drained to avoid deadlocking inside fsnotify on Close.
func (w *FileWatcher) stop() {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-w.watcher.Events:
			case <-w.watcher.Errors:
			case <-done:
				return
			}
		}
	}()
	if err := w.watcher.Close(); err != nil {
		level.Error(w.logger).Log("msg", "error closing rule file watcher", "err", err)
	}
	close(done)
	close(w.ch)
}
