// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package reloader

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/atomic"
	"go.uber.org/goleak"

	"github.com/efficientgo/core/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestReloader_ConfigApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	l, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(t, err)

	reloads := &atomic.Value{}
	reloads.Store(0)
	i := 0
	srv := &http.Server{}
	srv.Handler = http.HandlerFunc(func(resp http.ResponseWriter, r *http.Request) {
		i++
		if i%2 == 0 {
			// Every second request, fail to ensure that retry works.
			resp.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		reloads.Store(reloads.Load().(int) + 1) // The only writer.
		resp.WriteHeader(http.StatusOK)
	})
	go func() { _ = srv.Serve(l) }()
	defer func() { testutil.Ok(t, srv.Close()) }()

	reloadURL, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
	testutil.Ok(t, err)

	dir := t.TempDir()

	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "in"), os.ModePerm))
	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "out"), os.ModePerm))

	var (
		input  = filepath.Join(dir, "in", "cfg.yaml.tmpl")
		output = filepath.Join(dir, "out", "cfg.yaml")
	)
	reloader := New(nil, nil, &Options{
		ReloadURL:     reloadURL,
		CfgFile:       input,
		CfgOutputFile: output,
		WatchedDirs:   nil,
		WatchInterval: 9999 * time.Hour, // Disable interval to test watch logic only.
		RetryInterval: 100 * time.Millisecond,
		DelayInterval: 1 * time.Millisecond,
	})

	// Fail without config.
	err = reloader.Watch(ctx)
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasSuffix(err.Error(), "no such file or directory"), "expect error since there is no input config.")

	testutil.Ok(t, os.WriteFile(input, []byte(`
config:
  a: 1
  b: $(TEST_RELOADER_THANOS_ENV)
  c: $(TEST_RELOADER_THANOS_ENV2)
`), os.ModePerm))

	// Fail with config but without unset variables.
	err = reloader.Watch(ctx)
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasSuffix(err.Error(), `found reference to unset environment variable "TEST_RELOADER_THANOS_ENV"`), "expect error since there envvars are not set.")

	testutil.Ok(t, os.Setenv("TEST_RELOADER_THANOS_ENV", "2"))
	testutil.Ok(t, os.Setenv("TEST_RELOADER_THANOS_ENV2", "3"))

	rctx, cancel2 := context.WithCancel(ctx)
	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		testutil.Ok(t, reloader.Watch(rctx))
	}()

	reloadsSeen := 0
	attemptsCnt := 0
Outer:
	for {
		select {
		case <-ctx.Done():
			break Outer
		case <-time.After(300 * time.Millisecond):
		}

		rel := reloads.Load().(int)
		reloadsSeen = rel

		if reloadsSeen == 1 {
			// Initial apply seen (without doing nothing).
			f, err := os.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, `
config:
  a: 1
  b: 2
  c: 3
`, string(f))

			// Change config, expect reload in another iteration.
			testutil.Ok(t, os.WriteFile(input, []byte(`
config:
  a: changed
  b: $(TEST_RELOADER_THANOS_ENV)
  c: $(TEST_RELOADER_THANOS_ENV2)
`), os.ModePerm))
		} else if reloadsSeen == 2 {
			// Another apply, ensure we see change.
			f, err := os.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, `
config:
  a: changed
  b: 2
  c: 3
`, string(f))

			// Change the mode so reloader can't read the file.
			testutil.Ok(t, os.Chmod(input, os.ModeDir))
			attemptsCnt++
			// That was the second attempt to reload config. All good, break.
			if attemptsCnt == 2 {
				break
			}
		}
	}
	cancel2()
	g.Wait()

	testutil.Ok(t, os.Unsetenv("TEST_RELOADER_THANOS_ENV"))
	testutil.Ok(t, os.Unsetenv("TEST_RELOADER_THANOS_ENV2"))
}

func TestReloader_ConfigRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	l, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(t, err)

	correctConfig := []byte(`
config:
  a: 1
`)
	faultyConfig := []byte(`
faulty_config:
  a: 1
`)

	dir := t.TempDir()

	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "in"), os.ModePerm))
	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "out"), os.ModePerm))

	var (
		input  = filepath.Join(dir, "in", "cfg.yaml.tmpl")
		output = filepath.Join(dir, "out", "cfg.yaml")
	)

	reloads := &atomic.Value{}
	reloads.Store(0)
	srv := &http.Server{}

	srv.Handler = http.HandlerFunc(func(resp http.ResponseWriter, r *http.Request) {
		f, err := os.ReadFile(output)
		testutil.Ok(t, err)

		if string(f) == string(faultyConfig) {
			resp.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		reloads.Store(reloads.Load().(int) + 1) // The only writer.
		resp.WriteHeader(http.StatusOK)
	})
	go func() { _ = srv.Serve(l) }()
	defer func() { testutil.Ok(t, srv.Close()) }()

	reloadURL, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
	testutil.Ok(t, err)

	reloader := New(nil, nil, &Options{
		ReloadURL:     reloadURL,
		CfgFile:       input,
		CfgOutputFile: output,
		WatchedDirs:   nil,
		WatchInterval: 10 * time.Second, // 10 seconds to make the reload of faulty config fail quick
		RetryInterval: 100 * time.Millisecond,
		DelayInterval: 1 * time.Millisecond,
	})

	testutil.Ok(t, os.WriteFile(input, correctConfig, os.ModePerm))

	rctx, cancel2 := context.WithCancel(ctx)
	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		testutil.Ok(t, reloader.Watch(rctx))
	}()

	reloadsSeen := 0
	faulty := false

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout with faulty = %t, reloadsSeen = %d", faulty, reloadsSeen)
		case <-time.After(300 * time.Millisecond):
		}

		rel := reloads.Load().(int)
		reloadsSeen = rel

		if reloadsSeen == 1 && !faulty {
			// Initial apply seen (without doing anything).
			f, err := os.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, string(correctConfig), string(f))

			// Change to a faulty config
			testutil.Ok(t, os.WriteFile(input, faultyConfig, os.ModePerm))
			faulty = true
		} else if reloadsSeen == 1 && faulty {
			// Faulty config will trigger a reload, but reload failed
			f, err := os.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, string(faultyConfig), string(f))

			// Rollback config
			testutil.Ok(t, os.WriteFile(input, correctConfig, os.ModePerm))
		} else if reloadsSeen >= 2 {
			// Rollback to previous config should trigger a reload
			f, err := os.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, string(correctConfig), string(f))

			break
		}
	}
	cancel2()
	g.Wait()
}

func TestReloader_DirectoriesApply(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(t, err)

	i := 0
	reloads := 0
	reloadsMtx := sync.Mutex{}

	srv := &http.Server{}
	srv.Handler = http.HandlerFunc(func(resp http.ResponseWriter, r *http.Request) {
		reloadsMtx.Lock()
		defer reloadsMtx.Unlock()

		i++
		if i%2 == 0 {
			// Fail every second request to ensure that retry works.
			resp.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		reloads++
		resp.WriteHeader(http.StatusOK)
	})
	go func() {
		_ = srv.Serve(l)
	}()
	defer func() { testutil.Ok(t, srv.Close()) }()

	reloadURL, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
	testutil.Ok(t, err)

	ruleDir := t.TempDir()
	tempRule1File := path.Join(ruleDir, "rule1.yaml")
	tempRule3File := path.Join(ruleDir, "rule3.yaml")
	tempRule4File := path.Join(ruleDir, "rule4.yaml")

	testutil.Ok(t, os.WriteFile(tempRule1File, []byte("rule1-changed"), os.ModePerm))
	testutil.Ok(t, os.WriteFile(tempRule3File, []byte("rule3-changed"), os.ModePerm))
	testutil.Ok(t, os.WriteFile(tempRule4File, []byte("rule4-changed"), os.ModePerm))

	dir := t.TempDir()
	dir2 := t.TempDir()

	// dir
	// └─ rule-dir -> dir2/rule-dir
	// dir2
	// └─ rule-dir
	testutil.Ok(t, os.Mkdir(path.Join(dir2, "rule-dir"), os.ModePerm))
	testutil.Ok(t, os.Symlink(path.Join(dir2, "rule-dir"), path.Join(dir, "rule-dir")))

	logger := log.NewNopLogger()
	r := prometheus.NewRegistry()
	reloader := New(
		logger,
		r,
		&Options{
			ReloadURL:     reloadURL,
			CfgFile:       "",
			CfgOutputFile: "",
			WatchedDirs:   []string{dir, path.Join(dir, "rule-dir")},
			WatchInterval: 9999 * time.Hour, // Disable interval to test watch logic only.
			RetryInterval: 100 * time.Millisecond,
		})

	// dir
	// ├─ rule-dir -> dir2/rule-dir
	// └─ rule1.yaml
	// dir2
	// ├─ rule-dir
	// │  └─ rule4.yaml
	// ├─ rule3-001.yaml -> rule3-source.yaml
	// └─ rule3-source.yaml
	// The reloader watches 2 directories: dir and dir/rule-dir.
	testutil.Ok(t, os.WriteFile(path.Join(dir, "rule1.yaml"), []byte("rule"), os.ModePerm))
	testutil.Ok(t, os.WriteFile(path.Join(dir2, "rule3-source.yaml"), []byte("rule3"), os.ModePerm))
	testutil.Ok(t, os.Symlink(path.Join(dir2, "rule3-source.yaml"), path.Join(dir2, "rule3-001.yaml")))
	testutil.Ok(t, os.WriteFile(path.Join(dir2, "rule-dir", "rule4.yaml"), []byte("rule4"), os.ModePerm))

	stepFunc := func(rel int) {
		t.Log("Performing step number", rel)
		switch rel {
		case 0:
			// Create rule2.yaml.
			//
			// dir
			// ├─ rule-dir -> dir2/rule-dir
			// ├─ rule1.yaml
			// └─ rule2.yaml (*)
			// dir2
			// ├─ rule-dir
			// │  └─ rule4.yaml
			// ├─ rule3-001.yaml -> rule3-source.yaml
			// └─ rule3-source.yaml
			testutil.Ok(t, os.WriteFile(path.Join(dir, "rule2.yaml"), []byte("rule2"), os.ModePerm))
		case 1:
			// Update rule1.yaml.
			//
			// dir
			// ├─ rule-dir -> dir2/rule-dir
			// ├─ rule1.yaml (*)
			// └─ rule2.yaml
			// dir2
			// ├─ rule-dir
			// │  └─ rule4.yaml
			// ├─ rule3-001.yaml -> rule3-source.yaml
			// └─ rule3-source.yaml
			testutil.Ok(t, os.Rename(tempRule1File, path.Join(dir, "rule1.yaml")))
		case 2:
			// Create dir/rule3.yaml (symlink to rule3-001.yaml).
			//
			// dir
			// ├─ rule-dir -> dir2/rule-dir
			// ├─ rule1.yaml
			// ├─ rule2.yaml
			// └─ rule3.yaml -> dir2/rule3-001.yaml (*)
			// dir2
			// ├─ rule-dir
			// │  └─ rule4.yaml
			// ├─ rule3-001.yaml -> rule3-source.yaml
			// └─ rule3-source.yaml
			testutil.Ok(t, os.Symlink(path.Join(dir2, "rule3-001.yaml"), path.Join(dir2, "rule3.yaml")))
			testutil.Ok(t, os.Rename(path.Join(dir2, "rule3.yaml"), path.Join(dir, "rule3.yaml")))
		case 3:
			// Update the symlinked file and replace the symlink file to trigger fsnotify.
			//
			// dir
			// ├─ rule-dir -> dir2/rule-dir
			// ├─ rule1.yaml
			// ├─ rule2.yaml
			// └─ rule3.yaml -> dir2/rule3-002.yaml (*)
			// dir2
			// ├─ rule-dir
			// │  └─ rule4.yaml
			// ├─ rule3-002.yaml -> rule3-source.yaml (*)
			// └─ rule3-source.yaml (*)
			testutil.Ok(t, os.Rename(tempRule3File, path.Join(dir2, "rule3-source.yaml")))
			testutil.Ok(t, os.Symlink(path.Join(dir2, "rule3-source.yaml"), path.Join(dir2, "rule3-002.yaml")))
			testutil.Ok(t, os.Symlink(path.Join(dir2, "rule3-002.yaml"), path.Join(dir2, "rule3.yaml")))
			testutil.Ok(t, os.Rename(path.Join(dir2, "rule3.yaml"), path.Join(dir, "rule3.yaml")))
			testutil.Ok(t, os.Remove(path.Join(dir2, "rule3-001.yaml")))
		case 4:
			// Update rule4.yaml in the symlinked directory.
			//
			// dir
			// ├─ rule-dir -> dir2/rule-dir
			// ├─ rule1.yaml
			// ├─ rule2.yaml
			// └─ rule3.yaml -> rule3-source.yaml
			// dir2
			// ├─ rule-dir
			// │  └─ rule4.yaml (*)
			// └─ rule3-source.yaml
			testutil.Ok(t, os.Rename(tempRule4File, path.Join(dir2, "rule-dir", "rule4.yaml")))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		defer cancel()

		reloadsSeen := 0
		init := false
		for {
			runtime.Gosched() // Ensure during testing on small machine, other go routines have chance to continue.

			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}

			reloadsMtx.Lock()
			rel := reloads
			reloadsMtx.Unlock()
			if init && rel <= reloadsSeen {
				continue
			}

			// Catch up if reloader is step(s) ahead.
			for skipped := rel - reloadsSeen - 1; skipped > 0; skipped-- {
				stepFunc(rel - skipped)
			}

			stepFunc(rel)

			init = true
			reloadsSeen = rel

			if rel > 4 {
				// All good.
				return
			}
		}
	}()
	err = reloader.Watch(ctx)
	cancel()
	g.Wait()

	testutil.Ok(t, err)
	testutil.Equals(t, 6.0, promtest.ToFloat64(reloader.watcher.watchEvents))
	testutil.Equals(t, 0.0, promtest.ToFloat64(reloader.watcher.watchErrors))
	testutil.Equals(t, 4.0, promtest.ToFloat64(reloader.reloadErrors))
	testutil.Equals(t, 9.0, promtest.ToFloat64(reloader.reloads))
	testutil.Equals(t, 5, reloads)
}

func TestReloaderDirectoriesApplyBasedOnWatchInterval(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(t, err)

	reloads := &atomic.Value{}
	reloads.Store(0)
	srv := &http.Server{}
	srv.Handler = http.HandlerFunc(func(resp http.ResponseWriter, r *http.Request) {
		reloads.Store(reloads.Load().(int) + 1) // The only writer.
		resp.WriteHeader(http.StatusOK)
	})
	go func() {
		_ = srv.Serve(l)
	}()
	defer func() { testutil.Ok(t, srv.Close()) }()

	reloadURL, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
	testutil.Ok(t, err)

	dir := t.TempDir()
	dir2 := t.TempDir()

	// dir
	// └─ rule-dir -> dir2/rule-dir
	// dir2
	// └─ rule-dir
	testutil.Ok(t, os.Mkdir(path.Join(dir2, "rule-dir"), os.ModePerm))
	testutil.Ok(t, os.Symlink(path.Join(dir2, "rule-dir"), path.Join(dir, "rule-dir")))

	logger := log.NewNopLogger()
	reloader := New(
		logger,
		nil,
		&Options{
			ReloadURL:     reloadURL,
			CfgFile:       "",
			CfgOutputFile: "",
			WatchedDirs:   []string{dir, path.Join(dir, "rule-dir")},
			WatchInterval: 1 * time.Second, // use a small watch interval.
			RetryInterval: 9999 * time.Hour,
		},
	)

	// dir
	// ├─ rule-dir -> dir2/rule-dir
	// └─ rule1.yaml
	// dir2
	// ├─ rule-dir
	// │  └─ rule4.yaml
	// ├─ rule3-001.yaml -> rule3-source.yaml
	// └─ rule3-source.yaml
	//
	// The reloader watches 2 directories: dir and dir/rule-dir.
	testutil.Ok(t, os.WriteFile(path.Join(dir, "rule1.yaml"), []byte("rule"), os.ModePerm))
	testutil.Ok(t, os.WriteFile(path.Join(dir2, "rule3-source.yaml"), []byte("rule3"), os.ModePerm))
	testutil.Ok(t, os.Symlink(path.Join(dir2, "rule3-source.yaml"), path.Join(dir2, "rule3-001.yaml")))
	testutil.Ok(t, os.WriteFile(path.Join(dir2, "rule-dir", "rule4.yaml"), []byte("rule4"), os.ModePerm))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		defer cancel()

		reloadsSeen := 0
		init := false
		for {
			runtime.Gosched() // Ensure during testing on small machine, other go routines have chance to continue.

			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}

			rel := reloads.Load().(int)
			if init && rel <= reloadsSeen {
				continue
			}
			init = true
			reloadsSeen = rel

			t.Log("Performing step number", rel)
			switch rel {
			case 0:
				// Create rule3.yaml (symlink to rule3-001.yaml).
				//
				// dir
				// ├─ rule-dir -> dir2/rule-dir
				// ├─ rule1.yaml
				// ├─ rule2.yaml
				// └─ rule3.yaml -> dir2/rule3-001.yaml (*)
				// dir2
				// ├─ rule-dir
				// │  └─ rule4.yaml
				// ├─ rule3-001.yaml -> rule3-source.yaml
				// └─ rule3-source.yaml
				testutil.Ok(t, os.Symlink(path.Join(dir2, "rule3-001.yaml"), path.Join(dir2, "rule3.yaml")))
				testutil.Ok(t, os.Rename(path.Join(dir2, "rule3.yaml"), path.Join(dir, "rule3.yaml")))
			case 1:
				// Update the symlinked file but do not replace the symlink in dir.
				//
				// fsnotify shouldn't send any event because the change happens
				// in a directory that isn't watched but the reloader should detect
				// the update thanks to the watch interval.
				//
				// dir
				// ├─ rule-dir -> dir2/rule-dir
				// ├─ rule1.yaml
				// ├─ rule2.yaml
				// └─ rule3.yaml -> dir2/rule3-001.yaml
				// dir2
				// ├─ rule-dir
				// │  └─ rule4.yaml
				// ├─ rule3-001.yaml -> rule3-source.yaml
				// └─ rule3-source.yaml (*)
				testutil.Ok(t, os.WriteFile(path.Join(dir2, "rule3-source.yaml"), []byte("rule3-changed"), os.ModePerm))
			}

			if rel > 1 {
				// All good.
				return
			}
		}
	}()
	err = reloader.Watch(ctx)
	cancel()
	g.Wait()

	testutil.Ok(t, err)
	testutil.Equals(t, 2, reloads.Load().(int))
}

func TestReloader_ConfigApplyWithWatchIntervalEqualsZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	l, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(t, err)

	reloads := &atomic.Value{}
	reloads.Store(0)
	srv := &http.Server{}
	srv.Handler = http.HandlerFunc(func(resp http.ResponseWriter, r *http.Request) {
		reloads.Store(reloads.Load().(int) + 1)
		resp.WriteHeader(http.StatusOK)
	})
	go func() { _ = srv.Serve(l) }()
	defer func() { testutil.Ok(t, srv.Close()) }()

	reloadURL, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
	testutil.Ok(t, err)

	dir := t.TempDir()

	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "in"), os.ModePerm))
	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "out"), os.ModePerm))

	var (
		input  = filepath.Join(dir, "in", "cfg.yaml.tmpl")
		output = filepath.Join(dir, "out", "cfg.yaml")
	)
	reloader := New(nil, nil, &Options{
		ReloadURL:     reloadURL,
		CfgFile:       input,
		CfgOutputFile: output,
		WatchedDirs:   nil,
		WatchInterval: 0, // Set WatchInterval equals to 0
		RetryInterval: 100 * time.Millisecond,
		DelayInterval: 1 * time.Millisecond,
	})

	testutil.Ok(t, os.WriteFile(input, []byte(`
config:
  a: 1
  b: 2
  c: 3
`), os.ModePerm))

	rctx, cancel2 := context.WithCancel(ctx)
	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		testutil.Ok(t, reloader.Watch(rctx))
	}()

Outer:
	for {
		select {
		case <-ctx.Done():
			break Outer
		case <-time.After(300 * time.Millisecond):
		}
		if reloads.Load().(int) == 0 {
			// Initial apply seen (without doing nothing).
			f, err := os.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, `
config:
  a: 1
  b: 2
  c: 3
`, string(f))
			break
		}
	}
	cancel2()
	g.Wait()
	// Check no reload request made
	testutil.Equals(t, 0, reloads.Load().(int))
}
