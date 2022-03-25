// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestHTTPProberHealthInitialState(t *testing.T) {
	p := NewHTTP()

	testutil.Assert(t, !p.isHealthy(), "initially should not be healthy")
}

func TestHTTPProberReadinessInitialState(t *testing.T) {
	p := NewHTTP()

	testutil.Assert(t, !p.IsReady(), "initially should not be ready")
}

func TestHTTPProberHealthyStatusSetting(t *testing.T) {
	testError := errors.New("test error")
	p := NewHTTP()

	p.Healthy()

	testutil.Assert(t, p.isHealthy(), "should be healthy")

	p.NotHealthy(testError)

	testutil.Assert(t, !p.isHealthy(), "should not be healthy")
}

func TestHTTPProberReadyStatusSetting(t *testing.T) {
	testError := errors.New("test error")
	p := NewHTTP()

	p.Ready()

	testutil.Assert(t, p.IsReady(), "should be ready")

	p.NotReady(testError)

	testutil.Assert(t, !p.IsReady(), "should not be ready")
}

func TestHTTPProberMuxRegistering(t *testing.T) {
	serverAddress := fmt.Sprintf("localhost:%d", 8081)

	l, err := net.Listen("tcp", serverAddress)
	testutil.Ok(t, err)

	p := NewHTTP()

	healthyEndpointPath := "/-/healthy"
	readyEndpointPath := "/-/ready"

	mux := http.NewServeMux()
	mux.HandleFunc(healthyEndpointPath, p.HealthyHandler(log.NewNopLogger()))
	mux.HandleFunc(readyEndpointPath, p.ReadyHandler(log.NewNopLogger()))

	var g run.Group
	g.Add(func() error {
		return errors.Wrap(http.Serve(l, mux), "serve probes")
	}, func(err error) {
		t.Fatalf("server failed: %v", err)
	})

	go func() { _ = g.Run() }()

	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resp, err := doGet(ctx, path.Join(serverAddress, healthyEndpointPath))
		testutil.Ok(t, err)
		defer resp.Body.Close()

		testutil.Equals(t, resp.StatusCode, http.StatusServiceUnavailable, "should not be healthy, response code: %d", resp.StatusCode)
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resp, err := doGet(ctx, path.Join(serverAddress, readyEndpointPath))
		testutil.Ok(t, err)
		defer resp.Body.Close()

		testutil.Equals(t, resp.StatusCode, http.StatusServiceUnavailable, "should not be ready, response code: %d", resp.StatusCode)
	}
	{
		p.Healthy()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resp, err := doGet(ctx, path.Join(serverAddress, healthyEndpointPath))
		testutil.Ok(t, err)
		defer resp.Body.Close()

		testutil.Equals(t, resp.StatusCode, http.StatusOK, "should be healthy, response code: %d", resp.StatusCode)
	}
	{
		p.Ready()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resp, err := doGet(ctx, path.Join(serverAddress, readyEndpointPath))
		testutil.Ok(t, err)
		defer resp.Body.Close()

		testutil.Equals(t, resp.StatusCode, http.StatusOK, "should be ready, response code: %d", resp.StatusCode)
	}
}

func doGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s", url), nil)
	if err != nil {
		return nil, err
	}

	return http.DefaultClient.Do(req.WithContext(ctx))
}
