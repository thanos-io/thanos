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

	"github.com/fortytw2/leaktest"
	"github.com/thanos-io/thanos/pkg/testutil"
	"go.uber.org/atomic"
)

func TestReloader_ConfigApply(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

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
	reloader := New(nil, nil, reloadURL, input, output, nil)
	reloader.watchInterval = 9999 * time.Hour // Disable interval to test watch logic only.
	reloader.retryInterval = 100 * time.Millisecond

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
	for {
		select {
		case <-ctx.Done():
			break
		case <-time.After(300 * time.Millisecond):
		}

		rel := reloads.Load().(int)
		if rel <= reloadsSeen {
			// Nothing new.
			continue
		}

		if reloadsSeen == 0 {
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
		} else {
			// Another apply, ensure we see change.
			f, err := ioutil.ReadFile(output)
			testutil.Ok(t, err)
			testutil.Equals(t, `
config:
  a: changed
  b: 2
  c: 3
`, string(f))
			// All good, break
			break
		}
		reloadsSeen = rel
	}
	cancel2()
	g.Wait()

	testutil.Ok(t, os.Unsetenv("TEST_RELOADER_THANOS_ENV"))
	testutil.Ok(t, os.Unsetenv("TEST_RELOADER_THANOS_ENV2"))
}

func TestReloader_RuleApply(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

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

	reloader := New(nil, nil, reloadURL, "", "", []string{dir, path.Join(dir, "rule-dir")})
	reloader.watchInterval = 100 * time.Millisecond
	reloader.retryInterval = 100 * time.Millisecond

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

func TestReloader_SymlinkWatch(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

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

	dir, err := ioutil.TempDir("", "reloader-watch-dir")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	// The following setup is similar to the one when k8s projects a configmap contents as a volume
	// Directory setup ( -> = symlink):
	// in/config.yaml -> in/data/config.yaml
	// in/rules.yaml -> in/data/rules.yaml
	// data -> first-config
	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "in"), os.ModePerm))
	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "out"), os.ModePerm))

	// Setup first config
	testutil.Ok(t, os.Mkdir(filepath.Join(dir, "in", "first-config"), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "in", "first-config", "cfg.yaml"), []byte(`
config:
  a: a
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "in", "first-config", "rule.yaml"), []byte(`rule1`), os.ModePerm))
	// Setup data
	testutil.Ok(t, os.Symlink(path.Join(dir, "in", "first-config"), path.Join(dir, "in", "data")))

	// Setup config symlinks to data
	testutil.Ok(t, os.Symlink(path.Join(dir, "in", "data", "cfg.yaml"), path.Join(dir, "in", "cfg.yaml")))
	testutil.Ok(t, os.Symlink(path.Join(dir, "in", "data", "rule.yaml"), path.Join(dir, "in", "rule.yaml")))

	reloader := New(nil, nil, reloadURL, path.Join(dir, "in", "cfg.yaml"), path.Join(dir, "out", "cfg.yaml"), []string{dir, path.Join(dir, "in")})

	reloader.watchInterval = 100 * time.Millisecond
	reloader.retryInterval = 100 * time.Millisecond

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
			case 1:
				// The following steps try to reproduce the file change behavior when k8s projects a configmap as a volume
				// and the configmap changes. https://github.com/kubernetes/kubernetes/blob/release-1.19/pkg/volume/util/atomic_writer.go#L87-L120
				// Setup second config
				testutil.Ok(t, os.Mkdir(filepath.Join(dir, "in", "second-config"), os.ModePerm))
				testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "in", "second-config", "cfg.yaml"), []byte(`
config:
  b: b
`), os.ModePerm))
				testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "in", "second-config", "rule.yaml"), []byte(`rule2`), os.ModePerm))

				// Create temp data dir
				testutil.Ok(t, os.Symlink(path.Join(dir, "in", "second-config"), path.Join(dir, "in", "tmp_data")))

				// Rename data
				testutil.Ok(t, os.Rename(path.Join(dir, "in", "tmp_data"), path.Join(dir, "in", "data")))

				// Remove first config
				testutil.Ok(t, os.RemoveAll(path.Join(dir, "in", "first-config", "rule.yaml")))
				testutil.Ok(t, os.RemoveAll(path.Join(dir, "in", "first-config", "cfg.yaml")))
				testutil.Ok(t, os.RemoveAll(path.Join(dir, "in", "first-config")))
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
	output, err := ioutil.ReadFile(path.Join(dir, "out", "cfg.yaml"))
	testutil.Ok(t, err)
	testutil.Equals(t, output, []byte(`
config:
  b: b
`))
}
