// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/thanos-io/thanos/pkg/component"
)

func Test_extractOrgId(t *testing.T) {
	var testData = []struct {
		configuredHeaders []string
		requestHeaders    map[string]string
		expectOrgId       string
	}{
		{
			configuredHeaders: []string{"x-grafana-user", "x-server-id"},
			requestHeaders:    map[string]string{"x-grafana-user": "imagrafanauser", "x-server-id": "iamserverid"},
			expectOrgId:       "imagrafanauser",
		},
		{
			configuredHeaders: []string{"x-server-id", "x-grafana-user"},
			requestHeaders:    map[string]string{"x-grafana-user": "imagrafanauser", "x-server-id": "iamserverid"},
			expectOrgId:       "iamserverid",
		},
		{
			configuredHeaders: []string{},
			requestHeaders:    map[string]string{"another-header": "another"},
			expectOrgId:       "anonymous",
		},
	}
	for _, data := range testData {
		config := queryFrontendConfig{
			orgIdHeaders: data.configuredHeaders,
		}
		req, _ := http.NewRequest("", "", nil)
		for k, v := range data.requestHeaders {
			req.Header.Set(k, v)
		}
		id := extractOrgId(&config, req)
		testutil.Equals(t, data.expectOrgId, id)
	}
}

func TestAddQueryFrontendGRPCServer(t *testing.T) {
	t.Parallel()

	// Reserve a free port for the gRPC server.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	testutil.Ok(t, err)
	addr := l.Addr().String()
	testutil.Ok(t, l.Close())

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Path", r.URL.Path)
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("hello " + r.URL.Query().Get("query")))
	})

	var g run.Group
	testutil.Ok(t, addQueryFrontendGRPCServer(&g, log.NewNopLogger(), prometheus.NewRegistry(), opentracing.NoopTracer{},
		component.QueryFrontend, grpcConfig{bindAddress: addr}, handler))

	var resp *httpgrpc.HTTPResponse
	g.Add(func() error {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err = httpgrpc.NewHTTPClient(conn).Handle(ctx, &httpgrpc.HTTPRequest{
			Method: http.MethodGet,
			Url:    "/api/v1/query?query=up",
		}, grpc.WaitForReady(true))
		return err
	}, func(error) {})

	testutil.Ok(t, g.Run())
	testutil.Equals(t, int32(http.StatusTeapot), resp.Code)
	testutil.Equals(t, "hello up", string(resp.Body))

	var path string
	for _, h := range resp.Headers {
		if h.Key == "X-Path" {
			path = h.Values[0]
		}
	}
	testutil.Equals(t, "/api/v1/query", path)
}

func TestAddQueryFrontendGRPCServer_InvalidTLSConfig(t *testing.T) {
	t.Parallel()

	var g run.Group
	err := addQueryFrontendGRPCServer(&g, log.NewNopLogger(), prometheus.NewRegistry(), opentracing.NoopTracer{},
		component.QueryFrontend, grpcConfig{
			bindAddress: "127.0.0.1:0",
			tlsSrvCert:  "/does/not/exist.crt",
			tlsSrvKey:   "/does/not/exist.key",
		}, http.NotFoundHandler())
	testutil.NotOk(t, err)
}
