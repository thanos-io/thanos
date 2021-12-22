// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"strings"
	"testing"

	"github.com/efficientgo/e2e"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func CleanScenario(t testing.TB, e *e2e.DockerEnvironment) func() {
	return func() {
		// Make sure Clean can properly delete everything.
		testutil.Ok(t, exec.Command("chmod", "-R", "777", e.SharedDir()).Run())
		e.Close()
	}
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// NewSingleHostReverseProxy is almost same as httputil.NewSingleHostReverseProxy
// but it performs a url path rewrite.
func NewSingleHostReverseProxy(target *url.URL, externalPrefix string) *httputil.ReverseProxy {
	targetQuery := target.RawQuery
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.URL.Path = singleJoiningSlash(target.Path, strings.TrimPrefix(req.URL.Path, "/"+externalPrefix))

		if targetQuery == "" || req.URL.RawQuery == "" {
			req.URL.RawQuery = targetQuery + req.URL.RawQuery
		} else {
			req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		}
	}
	return &httputil.ReverseProxy{Director: director}
}
