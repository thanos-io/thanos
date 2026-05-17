// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/efficientgo/core/testutil"
)

// waitForSignal blocks until the watcher emits a signal or the timeout
// expires, returning whether a signal was received.
func waitForSignal(t *testing.T, w *FileWatcher, timeout time.Duration) bool {
	t.Helper()
	select {
	case _, ok := <-w.C():
		return ok
	case <-time.After(timeout):
		return false
	}
}

// newTestWatcher constructs a FileWatcher and starts its Run loop. It
// registers a t.Cleanup that cancels the context and waits for Run to exit,
// ensuring no watcher goroutine outlives the test (which would otherwise race
// with t.TempDir cleanup and trip goroutine-leak detectors).
func newTestWatcher(t *testing.T, patterns []string, interval time.Duration) *FileWatcher {
	t.Helper()
	// NewFileWatcher seeds the content hash before returning, so subsequent
	// file mutations are observed regardless of when Run() is scheduled.
	w, err := NewFileWatcher(log.NewNopLogger(), prometheus.NewRegistry(), patterns, interval)
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return w
}

// TestFileWatcher_DetectsContentChange asserts that a content change to a
// watched rule file triggers a single notification.
func TestFileWatcher_DetectsContentChange(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	w := newTestWatcher(t, []string{file}, 10*time.Second)

	testutil.Ok(t, os.WriteFile(file, []byte("groups:\n- name: g1\n"), 0644))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second), "expected change notification")
}

// TestFileWatcher_NoSignalWhenContentUnchanged asserts that touching a file
// (rewriting identical content) does not produce a spurious notification —
// the SHA-256 hash gate must filter it out.
func TestFileWatcher_NoSignalWhenContentUnchanged(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	content := []byte("groups:\n- name: g1\n")
	testutil.Ok(t, os.WriteFile(file, content, 0644))

	// Short interval so the periodic safety-net would fire if the hash gate
	// were broken.
	w := newTestWatcher(t, []string{file}, 200*time.Millisecond)

	// Rewrite identical content; mtime updates but hash does not.
	testutil.Ok(t, os.WriteFile(file, content, 0644))

	testutil.Equals(t, false, waitForSignal(t, w, 1*time.Second), "expected no notification for identical content")
}

// TestFileWatcher_DetectsNewFileMatchingGlob asserts that adding a file that
// matches a configured glob pattern is reported, even though no file existed
// at watcher start.
func TestFileWatcher_DetectsNewFileMatchingGlob(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "*.yaml")

	w := newTestWatcher(t, []string{pattern}, 10*time.Second)

	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "new.yaml"), []byte("groups: []\n"), 0644))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second), "expected notification for new file matching glob")
}

// TestFileWatcher_DetectsAtomicReplace asserts that a Kubernetes
// ConfigMap-style atomic replace (write to temp file then rename over the
// target) is detected. Watching the parent directory rather than the file
// inode is what makes this work.
func TestFileWatcher_DetectsAtomicReplace(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	w := newTestWatcher(t, []string{file}, 10*time.Second)

	tmp := filepath.Join(dir, ".rules.yaml.tmp")
	testutil.Ok(t, os.WriteFile(tmp, []byte("groups:\n- name: replaced\n"), 0644))
	testutil.Ok(t, os.Rename(tmp, file))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second), "expected notification on atomic rename")
}

// TestFileWatcher_StopClosesChannel asserts that canceling the context causes
// Run to return and the signal channel to close. Consumers must be able to
// detect the close (via the ok value) to avoid a busy-loop on shutdown.
func TestFileWatcher_StopClosesChannel(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	w, err := NewFileWatcher(log.NewNopLogger(), prometheus.NewRegistry(), []string{file}, 10*time.Second)
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	// Cancel without sleeping: even if Run has not yet entered its select
	// loop, ctx.Done is sticky, so the loop exits on the first iteration.
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit within 2s after ctx cancel")
	}

	// After Run exits, C() must be closed so consumers can detect shutdown
	// via the ok value rather than spinning on zero-value receives.
	_, ok := <-w.C()
	testutil.Equals(t, false, ok, "C() should be closed after Run exits")
}

// TestFileWatcher_DeletedFileFoldedIntoHash asserts that removing a file
// matched by a glob is detected as a change (not silently ignored).
func TestFileWatcher_DeletedFileFoldedIntoHash(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "*.yaml")
	a := filepath.Join(dir, "a.yaml")
	b := filepath.Join(dir, "b.yaml")
	testutil.Ok(t, os.WriteFile(a, []byte("groups: []\n"), 0644))
	testutil.Ok(t, os.WriteFile(b, []byte("groups: []\n"), 0644))

	w := newTestWatcher(t, []string{pattern}, 10*time.Second)

	testutil.Ok(t, os.Remove(b))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second), "expected notification when a matched file is removed")
}

// TestFileWatcher_NoNotifyWhenFileUnreadable asserts that a watched file
// becoming unreadable does NOT trigger a reload, since the rule manager
// would otherwise be asked to reload from an inconsistent snapshot. Once
// readability is restored and content changes, reloads resume normally.
func TestFileWatcher_NoNotifyWhenFileUnreadable(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	// Short interval so the periodic safety-net tick exercises the
	// unreadable-file path within the test timeout.
	w := newTestWatcher(t, []string{file}, 200*time.Millisecond)

	testutil.Ok(t, os.Chmod(file, 0o000))
	defer func() { _ = os.Chmod(file, 0o644) }()

	// Sanity check: if the test process can still read the file with mode
	// 0o000 (e.g. running as root), the unreadable-file path is not
	// actually exercised. Skip rather than report a false positive.
	if _, err := os.ReadFile(file); err == nil {
		t.Skip("test process can still read file with mode 0o000; skipping")
	}

	testutil.Equals(t, false, waitForSignal(t, w, 1*time.Second),
		"expected no notification while a watched file is unreadable")

	// Restore readability and change content; reloads should resume.
	testutil.Ok(t, os.Chmod(file, 0o644))
	testutil.Ok(t, os.WriteFile(file, []byte("groups:\n- name: g1\n"), 0644))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second),
		"expected notification after readability restored and content changed")
}
