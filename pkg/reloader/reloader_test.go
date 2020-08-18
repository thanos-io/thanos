// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package reloader

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/goleak"

	"github.com/thanos-io/thanos/pkg/testutil"
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

	dir, err := ioutil.TempDir("", "reloader-cfg-test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

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
		RuleDirs:      nil,
		WatchInterval: 9999 * time.Hour, // Disable interval to test watch logic only.
		RetryInterval: 100 * time.Millisecond,
	})

	// Fail without config.
	err = reloader.Watch(ctx)
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasSuffix(err.Error(), "no such file or directory"), "expect error since there is no input config.")

	testutil.Ok(t, ioutil.WriteFile(input, []byte(`
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
	for {
		select {
		case <-ctx.Done():
			break
		case <-time.After(300 * time.Millisecond):
		}

		rel := reloads.Load().(int)
		reloadsSeen = rel

		if reloadsSeen == 1 {
			// Initial apply seen (without doing nothing).
			f, err := ioutil.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, `
config:
  a: 1
  b: 2
  c: 3
`, string(f))

			// Change config, expect reload in another iteration.
			testutil.Ok(t, ioutil.WriteFile(input, []byte(`
config:
  a: changed
  b: $(TEST_RELOADER_THANOS_ENV)
  c: $(TEST_RELOADER_THANOS_ENV2)
`), os.ModePerm))
		} else if reloadsSeen == 2 {
			// Another apply, ensure we see change.
			f, err := ioutil.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, `
config:
  a: changed
  b: 2
  c: 3
`, string(f))

			// Change the mode so reloader can't read the file.
			testutil.Ok(t, os.Chmod(input, os.ModeDir))
			attemptsCnt += 1
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

func TestReloader_RuleApply(t *testing.T) {
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
	go func() {
		_ = srv.Serve(l)
	}()
	defer func() { testutil.Ok(t, srv.Close()) }()

	reloadURL, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
	testutil.Ok(t, err)

	dir, err := ioutil.TempDir("", "reloader-rules-test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	dir2, err := ioutil.TempDir("", "reload-rules-test2")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir2)) }()

	// Symlinked directory.
	testutil.Ok(t, os.Mkdir(path.Join(dir2, "rule-dir"), os.ModePerm))
	testutil.Ok(t, os.Symlink(path.Join(dir2, "rule-dir"), path.Join(dir, "rule-dir")))

	reloader := New(nil, nil, &Options{
		ReloadURL:     reloadURL,
		CfgFile:       "",
		CfgOutputFile: "",
		RuleDirs:      []string{dir, path.Join(dir, "rule-dir")},
		WatchInterval: 100 * time.Millisecond,
		RetryInterval: 100 * time.Millisecond,
	})

	// Some initial state.
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "rule1.yaml"), []byte("rule"), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir2, "rule3-source.yaml"), []byte("rule3"), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir2, "rule-dir", "rule4.yaml"), []byte("rule4"), os.ModePerm))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		defer cancel()

		reloadsSeen := 0
		init := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
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
				// Add new rule file.
				testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "rule2.yaml"), []byte("rule2"), os.ModePerm))
			case 1:
				// Change rule 1 in place.
				testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "rule1.yaml"), []byte("rule1-changed"), os.ModePerm))
			case 2:
				// Add new rule as symlink.
				testutil.Ok(t, os.Symlink(path.Join(dir2, "rule3-source.yaml"), path.Join(dir2, "rule3.yaml")))
				testutil.Ok(t, os.Rename(path.Join(dir2, "rule3.yaml"), path.Join(dir, "rule3.yaml")))
			case 3:
				// Change rule in symlink.
				testutil.Ok(t, ioutil.WriteFile(path.Join(dir2, "rule3-source.yaml"), []byte("rule3-changed"), os.ModePerm))
			case 4:
				// Change rule in symlinked directory..
				testutil.Ok(t, ioutil.WriteFile(path.Join(dir2, "rule-dir", "rule4.yaml"), []byte("rule4-changed"), os.ModePerm))
			}
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
	testutil.Equals(t, 5, reloads.Load().(int))
}
