// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package filewatch provides a generic primitive for watching a set of files
// (single path or glob patterns) and emitting a signal whenever the resolved
// file set or any file content changes. It is shared by components that need
// "reload on config-file change" semantics, such as thanos receive's hashring
// config and thanos rule's rule files.
//
// The primitive owns:
//   - fsnotify wiring with safe Stop drain
//   - a periodic safety-net tick (recovers from missed events; re-adds
//     directories that were removed and recreated)
//   - parent-directory watching (so Kubernetes ConfigMap/Secret mounts which
//     atomically swap a `..data` symlink are handled correctly)
//   - a SHA-256 content-hash gate (suppresses mtime-only or no-op events)
//
// It is intentionally *not* responsible for parsing file content or for any
// payload-specific metrics; those belong to the caller's wrapper layer.
package filewatch

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// DefaultInterval is the fallback poll interval used by Watcher to recover
// from missed fsnotify events (e.g. when a watched directory is removed and
// recreated).
const DefaultInterval = 5 * time.Minute

// Options configures a Watcher.
type Options struct {
	// Logger is used for non-fatal messages emitted by the watcher.
	// If nil, a no-op logger is used.
	Logger log.Logger
	// Patterns is the set of paths or glob patterns to watch. A single
	// literal path is just []string{path}. Required.
	Patterns []string
	// Interval is the safety-net tick interval. Zero means DefaultInterval.
	Interval time.Duration
	// ChangesCounter is incremented whenever a real (hash-differing) change
	// is detected. The caller registers it so different components can pick
	// metric names that fit their existing observability surface. May be
	// nil if the caller prefers to count changes from its own wrapper logic
	// (e.g. only after downstream parsing succeeds, to preserve the original
	// "successful reload" semantics of a pre-existing metric); the primitive
	// nil-checks at the single call site rather than substituting a
	// placeholder, so the change is not observed at this layer in that case.
	ChangesCounter prometheus.Counter
	// ErrorsCounter is incremented on fsnotify errors and file-read errors.
	// Callers may also increment it from their own wrapper logic (e.g. for
	// parse errors) to keep "any error" semantics on a single counter.
	// Required.
	ErrorsCounter prometheus.Counter
}

// Watcher watches a set of paths or glob patterns and signals on a channel
// whenever the resolved file set or any file content changes since the
// previous signal.
//
// Parent directories are watched rather than individual files so that
// Kubernetes ConfigMap mounts (which atomically swap a `..data` symlink) and
// glob patterns matching newly-added files are handled correctly.
type Watcher struct {
	patterns []string
	interval time.Duration
	logger   log.Logger
	watcher  *fsnotify.Watcher

	ch chan []string

	watchedDirs map[string]struct{}
	lastHash    uint64

	changesCounter prometheus.Counter
	errorCounter   prometheus.Counter

	stopOnce sync.Once
}

// New constructs a Watcher. The returned watcher must be started with Run
// and torn down with Stop (or by canceling the context passed to Run).
func New(opts Options) (*Watcher, error) {
	if len(opts.Patterns) == 0 {
		return nil, errors.New("filewatch: at least one pattern is required")
	}
	if opts.ErrorsCounter == nil {
		return nil, errors.New("filewatch: ErrorsCounter is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	interval := opts.Interval
	if interval == 0 {
		interval = DefaultInterval
	}
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "creating file watcher")
	}
	// changesCounter may be nil; it is nil-checked at the single call site
	// in maybeNotify rather than substituted with a placeholder, so we do
	// not have to register a counter just to discard its increments.
	w := &Watcher{
		patterns:       append([]string(nil), opts.Patterns...),
		interval:       interval,
		logger:         logger,
		watcher:        fsw,
		ch:             make(chan []string, 1),
		watchedDirs:    make(map[string]struct{}),
		changesCounter: opts.ChangesCounter,
		errorCounter:   opts.ErrorsCounter,
	}

	// Resolve once and thread through so refreshWatchedDirs and the seed
	// hash derive from a single consistent filesystem snapshot.
	files := w.resolveFiles()
	w.refreshWatchedDirs(files)
	// Seed lastHash so the first tick does not falsely report a change. If a
	// watched file is currently unreadable, seeding fails. We do not fail
	// construction over that: the periodic tick will retry, and at most one
	// possibly-spurious notification may fire on the first successful read
	// (callers are expected to be idempotent).
	if hash, herr := w.computeHash(files); herr != nil {
		level.Warn(w.logger).Log("msg", "could not seed file hash; will retry from the watcher loop", "err", herr)
	} else {
		w.lastHash = hash
	}
	return w, nil
}

// C returns a channel that emits the resolved file list whenever the watched
// state has changed since the previous emission. The channel is buffered
// (size 1) so rapid successive changes coalesce into a single signal. It is
// closed by Stop after Run exits, so consumers should select on a comma-ok
// receive to detect shutdown.
func (w *Watcher) C() <-chan []string { return w.ch }

// Run drives the watcher until ctx is canceled or Stop is called.
func (w *Watcher) Run(ctx context.Context) {
	defer w.Stop()

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
			// CHMOD-only events are not relevant to file content.
			if event.Op^fsnotify.Chmod == 0 {
				continue
			}
			files := w.resolveFiles()
			w.refreshWatchedDirs(files)
			w.maybeNotify(files)

		case <-ticker.C:
			// Safety net: re-add directories dropped after a remove and
			// catch changes that fsnotify did not report.
			files := w.resolveFiles()
			w.refreshWatchedDirs(files)
			w.maybeNotify(files)

		case err := <-w.watcher.Errors:
			if err != nil {
				w.errorCounter.Inc()
				level.Error(w.logger).Log("msg", "error watching files", "err", err)
			}
		}
	}
}

// resolveFiles expands the configured patterns to a deduplicated, sorted
// list of file paths.
func (w *Watcher) resolveFiles() []string {
	seen := make(map[string]struct{})
	var files []string
	for _, pat := range w.patterns {
		matches, err := filepath.Glob(pat)
		if err != nil {
			level.Warn(w.logger).Log("msg", "invalid glob pattern", "pattern", pat, "err", err)
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
// currently-resolved file plus the directory of each configured pattern.
// Directories that no longer match are removed. The caller passes in the
// result of a single resolveFiles() call so this and maybeNotify operate on
// the same filesystem snapshot.
func (w *Watcher) refreshWatchedDirs(files []string) {
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
	for _, f := range files {
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

// computeHash returns a hash deterministic over the resolved file set and
// the contents of those files. If any file cannot be read the function
// returns the error without producing a partial hash; callers must not
// signal a reload in that case, so consumers are never asked to reload from
// an inconsistent snapshot of the filesystem.
func (w *Watcher) computeHash(files []string) (uint64, error) {
	h := sha256.New()
	for _, f := range files {
		_, _ = h.Write([]byte(f))
		_, _ = h.Write([]byte{0})
		b, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			return 0, errors.Wrapf(err, "reading file %s", f)
		}
		_, _ = h.Write(b)
		_, _ = h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8]), nil
}

// maybeNotify emits the resolved file list on C() if the state has changed
// since the previous emission. The channel is buffered (size 1) so rapid
// successive changes coalesce into a single signal.
//
// If computeHash fails (a watched file is unreadable), no signal is emitted
// and the error counter is incremented. lastHash is preserved so that a
// successful read in a later tick correctly compares against the last good
// snapshot and detects any real change that happened during the outage.
func (w *Watcher) maybeNotify(files []string) {
	cur, err := w.computeHash(files)
	if err != nil {
		w.errorCounter.Inc()
		level.Warn(w.logger).Log("msg", "skipping reload signal due to file read error", "err", err)
		return
	}
	if cur == w.lastHash {
		return
	}
	w.lastHash = cur
	if w.changesCounter != nil {
		w.changesCounter.Inc()
	}
	select {
	case w.ch <- files:
	default:
		// A signal is already pending; coalesce. The pending signal will
		// carry the previous file list, but consumers are expected to
		// re-resolve from the current filesystem (the signal is just a
		// "something changed" trigger).
	}
}

// Stop releases the underlying fsnotify watcher and closes the signal
// channel. Pending events are drained to avoid deadlocking inside fsnotify
// on Close. Stop is called automatically by Run on shutdown; callers may
// also invoke it directly (e.g. to release resources after a construction-
// time validation failure before Run is started). Subsequent calls are no-ops.
func (w *Watcher) Stop() {
	w.stopOnce.Do(func() {
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
			level.Error(w.logger).Log("msg", "error closing file watcher", "err", err)
		}
		close(done)
		close(w.ch)
	})
}
