// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filewatch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/efficientgo/core/testutil"
)

// testCounters returns a pair of (changes, errors) counters registered in a
// fresh registry. Each test gets its own registry to avoid collisions on the
// (deliberately fixed) counter names.
func testCounters() (prometheus.Counter, prometheus.Counter) {
	reg := prometheus.NewRegistry()
	return promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_changes_total"}),
		promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_errors_total"})
}

// waitForSignal blocks until the watcher emits a signal or the timeout
// expires. It returns (received, ok) where ok is the channel-close indicator.
func waitForSignal(t *testing.T, w *Watcher, timeout time.Duration) bool {
	t.Helper()
	select {
	case _, ok := <-w.C():
		return ok
	case <-time.After(timeout):
		return false
	}
}

// newTestWatcher constructs a Watcher and starts its Run loop. The watcher
// is shut down via t.Cleanup so no goroutine outlives the test.
func newTestWatcher(t *testing.T, patterns []string, interval time.Duration) *Watcher {
	t.Helper()
	changes, errs := testCounters()
	w, err := New(Options{
		Logger:         log.NewNopLogger(),
		Patterns:       patterns,
		Interval:       interval,
		ChangesCounter: changes,
		ErrorsCounter:  errs,
	})
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

func TestWatcher_DetectsContentChange(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	w := newTestWatcher(t, []string{file}, 10*time.Second)

	testutil.Ok(t, os.WriteFile(file, []byte("groups:\n- name: g1\n"), 0644))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second), "expected change notification")
}

func TestWatcher_NoSignalWhenContentUnchanged(t *testing.T) {
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

func TestWatcher_DetectsNewFileMatchingGlob(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "*.yaml")

	w := newTestWatcher(t, []string{pattern}, 10*time.Second)

	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "new.yaml"), []byte("groups: []\n"), 0644))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second), "expected notification for new file matching glob")
}

func TestWatcher_DetectsAtomicReplace(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	w := newTestWatcher(t, []string{file}, 10*time.Second)

	tmp := filepath.Join(dir, ".rules.yaml.tmp")
	testutil.Ok(t, os.WriteFile(tmp, []byte("groups:\n- name: replaced\n"), 0644))
	testutil.Ok(t, os.Rename(tmp, file))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second), "expected notification on atomic rename")
}

func TestWatcher_StopClosesChannel(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	changes, errs := testCounters()
	w, err := New(Options{
		Logger:         log.NewNopLogger(),
		Patterns:       []string{file},
		Interval:       10 * time.Second,
		ChangesCounter: changes,
		ErrorsCounter:  errs,
	})
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit within 2s after ctx cancel")
	}

	_, ok := <-w.C()
	testutil.Equals(t, false, ok, "C() should be closed after Run exits")
}

func TestWatcher_DeletedFileFoldedIntoHash(t *testing.T) {
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

// TestWatcher_NoNotifyWhenFileUnreadable asserts that a watched file becoming
// unreadable does NOT trigger a reload, since the consumer would otherwise
// be asked to reload from an inconsistent snapshot. Once readability is
// restored and content changes, reloads resume normally.
func TestWatcher_NoNotifyWhenFileUnreadable(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	w := newTestWatcher(t, []string{file}, 200*time.Millisecond)

	testutil.Ok(t, os.Chmod(file, 0o000))
	defer func() { _ = os.Chmod(file, 0o644) }()

	if _, err := os.ReadFile(file); err == nil {
		t.Skip("test process can still read file with mode 0o000; skipping")
	}

	testutil.Equals(t, false, waitForSignal(t, w, 1*time.Second),
		"expected no notification while a watched file is unreadable")

	testutil.Ok(t, os.Chmod(file, 0o644))
	testutil.Ok(t, os.WriteFile(file, []byte("groups:\n- name: g1\n"), 0644))

	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second),
		"expected notification after readability restored and content changed")
}

// TestWatcher_RequiresErrorsCounter asserts the constructor rejects a missing
// ErrorsCounter, since errors should always be observable.
func TestWatcher_RequiresErrorsCounter(t *testing.T) {
	_, err := New(Options{
		Patterns: []string{"some.yaml"},
	})
	testutil.NotOk(t, err)
}

// TestWatcher_NilChangesCounterIsAllowed asserts that callers may opt out of
// "change detected" observability at the primitive layer (e.g. when a
// wrapper exposes a more meaningful counter such as successful reloads).
// The hot path must not panic in that case.
func TestWatcher_NilChangesCounterIsAllowed(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "rules.yaml")
	testutil.Ok(t, os.WriteFile(file, []byte("groups: []\n"), 0644))

	_, errs := testCounters()
	w, err := New(Options{
		Logger:        log.NewNopLogger(),
		Patterns:      []string{file},
		Interval:      200 * time.Millisecond,
		ErrorsCounter: errs,
		// ChangesCounter intentionally nil.
	})
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

	// Mutate content; primitive should still emit on its channel (just
	// without bumping any caller-visible changes counter).
	testutil.Ok(t, os.WriteFile(file, []byte("groups:\n- name: g1\n"), 0644))
	testutil.Equals(t, true, waitForSignal(t, w, 3*time.Second),
		"expected signal even when ChangesCounter is nil")
}

// TestWatcher_RequiresPatterns asserts the constructor rejects a zero-length
// pattern list, which would otherwise produce a watcher that never fires.
func TestWatcher_RequiresPatterns(t *testing.T) {
	changes, errs := testCounters()
	_, err := New(Options{
		ChangesCounter: changes,
		ErrorsCounter:  errs,
	})
	testutil.NotOk(t, err)
}
