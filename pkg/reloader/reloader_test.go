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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestReloader_ConfigApply(t *testing.T) {
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

	dir, err := ioutil.TempDir("", "reloader-cfg-test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	testutil.Ok(t, os.Mkdir(dir+"/in", os.ModePerm))
	testutil.Ok(t, os.Mkdir(dir+"/out", os.ModePerm))

	var (
		input  = path.Join(dir, "in", "cfg.yaml.tmpl")
		output = path.Join(dir, "out", "cfg.yaml")
	)
	reloader := New(nil, reloadURL, input, output, nil)
	reloader.watchInterval = 9999 * time.Hour // Disable interval to test watch logic only.
	reloader.retryInterval = 100 * time.Millisecond

	testNoConfig(t, reloader)

	testutil.Ok(t, ioutil.WriteFile(input, []byte(`
config:
  a: 1
  b: $(TEST_RELOADER_THANOS_ENV)
  c: $(TEST_RELOADER_THANOS_ENV2)
`), os.ModePerm))

	testUnsetVariables(t, reloader)

	testutil.Ok(t, os.Setenv("TEST_RELOADER_THANOS_ENV", "2"))
	testutil.Ok(t, os.Setenv("TEST_RELOADER_THANOS_ENV2", "3"))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	g := sync.WaitGroup{}
	g.Add(1)
	go func() {
		defer g.Done()
		defer cancel()

		reloadsSeen := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
			}

			rel := reloads.Load().(int)
			if rel <= reloadsSeen {
				continue
			}
			reloadsSeen = rel

			switch rel {
			case 1:
				// Initial apply seen (without doing nothing)

				// Output looks as expected?
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
			case 2:
				f, err := ioutil.ReadFile(output)
				testutil.Ok(t, err)

				testutil.Equals(t, `
config:
  a: changed
  b: 2
  c: 3
`, string(f))
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
	testutil.Equals(t, 2, reloads.Load().(int))
}

func testNoConfig(t *testing.T, reloader *Reloader) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	err := reloader.Watch(ctx)
	cancel()
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasSuffix(err.Error(), "no such file or directory"), "expect error since there is no input config.")
}

func testUnsetVariables(t *testing.T, reloader *Reloader) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	err := reloader.Watch(ctx)
	cancel()
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasSuffix(err.Error(), `found reference to unset environment variable "TEST_RELOADER_THANOS_ENV"`), "expect error since there envvars are not set.")
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

	reloader := New(nil, reloadURL, "", "", []string{dir, path.Join(dir, "rule-dir")})
	reloader.watchInterval = 100 * time.Millisecond
	reloader.retryInterval = 100 * time.Millisecond

	// Some initial state.
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, "rule1.yaml"), []byte("rule"), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir2, "rule3-source.yaml"), []byte("rule3"), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(path.Join(dir2, "rule-dir", "rule4.yaml"), []byte("rule4"), os.ModePerm))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
