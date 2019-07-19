package prober

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func queryHTTPGetEndpoint(ctx context.Context, t *testing.T, logger log.Logger, url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s", url), nil)
	testutil.Ok(t, err)
	return http.DefaultClient.Do(req.WithContext(ctx))
}

type TestComponent struct {
	name string
}

func (c TestComponent) String() string {
	return c.name
}

func TestProberHealthInitialState(t *testing.T) {
	component := TestComponent{name: "test"}
	expectedErrorMessage := fmt.Sprintf(initialErrorFmt, component)
	p := NewProber(component, log.NewNopLogger(), nil)

	err := p.IsHealthy()
	testutil.NotOk(t, err)
	testutil.Equals(t, err.Error(), expectedErrorMessage)
}

func TestProberReadinessInitialState(t *testing.T) {
	component := TestComponent{name: "test"}
	expectedErrorMessage := fmt.Sprintf(initialErrorFmt, component)
	p := NewProber(component, log.NewNopLogger(), nil)

	err := p.IsReady()
	testutil.NotOk(t, err)
	testutil.Equals(t, err.Error(), expectedErrorMessage)
}

func TestProberReadyStatusSetting(t *testing.T) {
	component := TestComponent{name: "test"}
	testError := fmt.Errorf("test error")
	p := NewProber(component, log.NewNopLogger(), nil)

	p.SetReady()
	err := p.IsReady()
	testutil.Equals(t, err, nil)
	p.SetNotReady(testError)
	err = p.IsReady()
	testutil.NotOk(t, err)
}

func TestProberHeatlthyStatusSetting(t *testing.T) {
	component := TestComponent{name: "test"}
	testError := fmt.Errorf("test error")
	p := NewProber(component, log.NewNopLogger(), nil)

	p.SetHealthy()
	err := p.IsHealthy()
	testutil.Equals(t, err, nil)
	p.SetNotHealthy(testError)
	err = p.IsHealthy()
	testutil.NotOk(t, err)
}

func TestProberMuxRegistering(t *testing.T) {
	component := TestComponent{name: "test"}
	ctx := context.Background()
	var g run.Group
	mux := http.NewServeMux()

	freePort, err := testutil.FreePort()
	testutil.Ok(t, err)
	serverAddress := fmt.Sprintf("localhost:%d", freePort)

	l, err := net.Listen("tcp", serverAddress)
	testutil.Ok(t, err)
	g.Add(func() error {
		return errors.Wrap(http.Serve(l, mux), "serve probes")
	}, func(error) {})

	p := NewProber(component, log.NewNopLogger(), nil)
	p.RegisterInMux(mux)

	go func() { _ = g.Run() }()

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, healthyEndpointPath))
		testutil.Ok(t, err)
		testutil.Equals(t, probeErrorHTTPStatus, resp.StatusCode)
		return err
	}))

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, readyEndpointPath))
		testutil.Ok(t, err)
		testutil.Equals(t, probeErrorHTTPStatus, resp.StatusCode)
		return err
	}))

	p.SetHealthy()

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, healthyEndpointPath))
		testutil.Ok(t, err)
		testutil.Equals(t, 200, resp.StatusCode)
		return err
	}))

}

func TestProberRouterRegistering(t *testing.T) {
	component := TestComponent{name: "test"}
	router := route.New()
	ctx := context.Background()
	var g run.Group
	mux := http.NewServeMux()

	freePort, err := testutil.FreePort()
	testutil.Ok(t, err)
	serverAddress := fmt.Sprintf("localhost:%d", freePort)

	l, err := net.Listen("tcp", serverAddress)
	testutil.Ok(t, err)
	g.Add(func() error {
		return errors.Wrap(http.Serve(l, mux), "serve probes")
	}, func(error) {})

	p := NewProber(component, log.NewNopLogger(), nil)
	p.RegisterInRouter(router)
	mux.Handle("/", router)

	go func() { _ = g.Run() }()

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, healthyEndpointPath))
		testutil.Ok(t, err)
		testutil.Equals(t, probeErrorHTTPStatus, resp.StatusCode)
		return err
	}))

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, readyEndpointPath))
		testutil.Ok(t, err)
		testutil.Equals(t, probeErrorHTTPStatus, resp.StatusCode)
		return err
	}))

	p.SetHealthy()

	testutil.Ok(t, runutil.Retry(time.Second, ctx.Done(), func() error {
		resp, err := queryHTTPGetEndpoint(ctx, t, log.NewNopLogger(), path.Join(serverAddress, healthyEndpointPath))
		testutil.Ok(t, err)
		testutil.Equals(t, 200, resp.StatusCode)
		return err
	}))

}
