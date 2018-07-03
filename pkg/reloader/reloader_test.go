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
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

func TestReloader_ConfigApply(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	l, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(t, err)

	reloads := 0
	i := 0
	promHandlerMu := sync.Mutex{}
	srv := &http.Server{}
	srv.Handler = http.HandlerFunc(func(resp http.ResponseWriter, r *http.Request) {
		promHandlerMu.Lock()
		defer promHandlerMu.Unlock()
		i++
		if i%2 == 0 {
			// Every second request, fail to ensure that retry works.
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

	dir, err := ioutil.TempDir("", "reloader-cfg-test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	testutil.Ok(t, os.Mkdir(dir+"/in", os.ModePerm))
	testutil.Ok(t, os.Mkdir(dir+"/out", os.ModePerm))

	var (
		input  = path.Join(dir, "in", "cfg.yaml.tmpl")
		output = path.Join(dir, "out", "cfg.yaml")
	)
	reloader := New(nil, reloadURL, input, output, "")
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

	reloadsFn := func() int {
		promHandlerMu.Lock()
		promHandlerMu.Unlock()
		return reloads
	}
	testInitialApply(t, reloader, reloadsFn, output)

	reloads = 0
	reloader.lastCfgHash = []byte{}
	reloader.lastRuleHash = []byte{}
	testOnChangeApply(t, reloader, reloadsFn, input, output)
}

func testNoConfig(t *testing.T, reloader *Reloader) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := reloader.Watch(ctx)
	cancel()
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasSuffix(err.Error(), "no such file or directory"), "expect error since there is no input config.")
}

func testUnsetVariables(t *testing.T, reloader *Reloader) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := reloader.Watch(ctx)
	cancel()
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.HasSuffix(err.Error(), `found reference to unset environment variable "TEST_RELOADER_THANOS_ENV"`), "expect error since there envvars are not set.")
}

func testInitialApply(t *testing.T, reloader *Reloader, reloadsFn func() int, output string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}

			if ctx.Err() != nil {
				break
			}

			if reloadsFn() > 0 {
				break
			}
		}
	}()
	err := reloader.Watch(ctx)
	cancel()
	testutil.Ok(t, err)

	testutil.Equals(t, 1, reloadsFn())
	f, err := ioutil.ReadFile(output)
	testutil.Ok(t, err)

	testutil.Equals(t, `
config:
  a: 1
  b: 2
  c: 3
`, string(f))
}

func testOnChangeApply(t *testing.T, reloader *Reloader, reloadsFn func() int, input string, output string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}

			if ctx.Err() != nil {
				break
			}

			if reloadsFn() == 1 {
				testutil.Ok(t, ioutil.WriteFile(input, []byte(`
config:
  a: changed
  b: $(TEST_RELOADER_THANOS_ENV)
  c: $(TEST_RELOADER_THANOS_ENV2)
`), os.ModePerm))
				continue
			}
			if reloadsFn() > 1 {
				break
			}
		}
	}()
	err := reloader.Watch(ctx)
	cancel()
	testutil.Ok(t, err)

	testutil.Equals(t, 2, reloadsFn())
	f, err := ioutil.ReadFile(output)
	testutil.Ok(t, err)

	testutil.Equals(t, `
config:
  a: changed
  b: 2
  c: 3
`, string(f))
}

func TestReloader_RuleApply(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	l, err := net.Listen("tcp", "localhost:0")
	testutil.Ok(t, err)

	reloads := 0
	i := 0
	promHandlerMu := sync.Mutex{}
	srv := &http.Server{}
	srv.Handler = http.HandlerFunc(func(resp http.ResponseWriter, r *http.Request) {
		promHandlerMu.Lock()
		defer promHandlerMu.Unlock()

		i++
		if i%2 == 0 {
			// Every second request, fail to ensure that retry works.
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

	dir, err := ioutil.TempDir("", "reloader-rules-test")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	reloader := New(nil, reloadURL, "", "", dir)
	reloader.ruleInterval = 100 * time.Millisecond
	reloader.retryInterval = 100 * time.Millisecond

	reloadsFn := func() int {
		promHandlerMu.Lock()
		promHandlerMu.Unlock()
		return reloads
	}

	testutil.Ok(t, ioutil.WriteFile(dir+"/rule1.yaml", []byte("rule"), os.ModePerm))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
			case <-time.After(300 * time.Millisecond):
			}

			if ctx.Err() != nil {
				break
			}

			if reloadsFn() == 1 {
				testutil.Ok(t, ioutil.WriteFile(dir+"/rule2.yaml", []byte("rule2"), os.ModePerm))
				continue
			}
			if reloadsFn() == 2 {
				// Change rule 1.
				testutil.Ok(t, ioutil.WriteFile(dir+"/rule1.yaml", []byte("rule1-changed"), os.ModePerm))
				continue
			}
			if reloadsFn() > 2 {
				break
			}
		}
	}()
	err = reloader.Watch(ctx)
	cancel()
	testutil.Ok(t, err)

	testutil.Equals(t, 3, reloadsFn())
}
