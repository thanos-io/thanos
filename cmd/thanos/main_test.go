package main

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/prober"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
)

type TestComponent struct {
	name string
}

func (c TestComponent) String() string {
	return c.name
}

func queryHTTPGetEndpoint(ctx context.Context, t *testing.T, logger log.Logger, url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s", url), nil)
	testutil.Ok(t, err)
	return http.DefaultClient.Do(req.WithContext(ctx))
}

func TestGenericHttpEndpoints(t *testing.T) {
	var g run.Group
	logger := log.NewNopLogger()
	metricsRegistry := prometheus.NewRegistry()
	component := TestComponent{name: "sidecar"}
	ctx := context.Background()

	freePort, err := testutil.FreePort()
	testutil.Ok(t, err)

	serverAddress := fmt.Sprintf("127.0.0.1:%d", freePort)

	p := prober.NewProber(component, logger)
	err = metricHTTPListenGroup(&g, logger, metricsRegistry, serverAddress, *p)
	testutil.Ok(t, err)
	go func() { _ = g.Run() }()

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, "/metrics"))
		testutil.Ok(t, err)
		testutil.Equals(t, 200, resp.StatusCode)
		return err
	}))

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, "/debug/pprof/"))
		testutil.Ok(t, err)
		testutil.Equals(t, 200, resp.StatusCode)
		return err
	}))
}
