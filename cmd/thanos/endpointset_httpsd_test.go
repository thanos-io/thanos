// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/cache"
)

type staticContent struct {
	content []byte
}

func (s staticContent) Content() ([]byte, error) { return s.content, nil }
func (s staticContent) Path() string             { return "" }

func TestEndpointConfigParsesHTTPSDConfigs(t *testing.T) {
	cfg := staticContent{content: []byte(`
endpoints:
  - address: store-1:10901
http_sd_configs:
  - url: http://sd.example.com/endpoints
    refresh_interval: 30s
`)}
	provider, err := newEndpointConfigProvider(log.NewNopLogger(), cfg, 0, nil, nil, nil, nil)
	testutil.Ok(t, err)

	got := provider.config()
	testutil.Equals(t, 1, len(got.HTTPSDConfigs))
	testutil.Equals(t, "http://sd.example.com/endpoints", got.HTTPSDConfigs[0].URL)
	testutil.Equals(t, 30*time.Second, time.Duration(got.HTTPSDConfigs[0].RefreshInterval))
}

func TestEndpointConfigRejectsInvalidHTTPSDURL(t *testing.T) {
	cfg := staticContent{content: []byte(`
http_sd_configs:
  - url: ftp://sd.example.com/endpoints
`)}
	_, err := newEndpointConfigProvider(log.NewNopLogger(), cfg, 0, nil, nil, nil, nil)
	testutil.NotOk(t, err)
}

func TestRunHTTPSDDiscoversEndpoints(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `[{"targets": ["store-a:10901", "store-b:10901"], "labels": {"zone": "a"}}]`)
	}))
	defer srv.Close()

	cfg := staticContent{content: []byte(fmt.Sprintf(`
http_sd_configs:
  - url: %s
    refresh_interval: 1s
`, srv.URL))}
	provider, err := newEndpointConfigProvider(log.NewNopLogger(), cfg, 0, nil, nil, nil, nil)
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var g run.Group
	httpSDCache := cache.New()
	testutil.Ok(t, runHTTPSD(ctx, &g, component.Query, prometheus.NewRegistry(), log.NewNopLogger(), provider, 0, httpSDCache))
	g.Add(func() error {
		<-ctx.Done()
		return nil
	}, func(error) { cancel() })

	done := make(chan error, 1)
	go func() { done <- g.Run() }()

	deadline := time.Now().Add(15 * time.Second)
	for {
		addrs := httpSDCache.Addresses()
		slices.Sort(addrs)
		if slices.Equal(addrs, []string{"store-a:10901", "store-b:10901"}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("endpoints not discovered, got %v", addrs)
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancel()
	<-done
}
