// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-kit/log"

	"github.com/efficientgo/core/testutil"
)

func TestHTTPServerMiddleware(t *testing.T) {
	b := bytes.Buffer{}

	m := NewHTTPServerMiddleware(log.NewLogfmtLogger(io.Writer(&b)))
	handler := func(w http.ResponseWriter, r *http.Request) {
		_, err := io.WriteString(w, "Test Works")
		if err != nil {
			testutil.Ok(t, err)
		}
	}
	hm := m.HTTPMiddleware("test", http.HandlerFunc(handler))

	// Regression test for Cortex way - https://github.com/thanos-io/thanos/pull/4041
	u, err := url.Parse("http://example.com:5555/foo")
	testutil.Ok(t, err)
	req := &http.Request{
		Method: "GET",
		URL:    u,
		Body:   nil,
	}

	w := httptest.NewRecorder()

	hm(w, req)

	resp := w.Result()
	body, err := io.ReadAll(resp.Body)
	testutil.Ok(t, err)

	testutil.Equals(t, 200, resp.StatusCode)
	testutil.Equals(t, "Test Works", string(body))
	testutil.Assert(t, !strings.Contains(b.String(), "err="))

	// Typical way:
	req = httptest.NewRequest("GET", "http://example.com:5555/foo", nil)
	b.Reset()

	w = httptest.NewRecorder()
	hm(w, req)

	resp = w.Result()
	body, err = io.ReadAll(resp.Body)
	testutil.Ok(t, err)

	testutil.Equals(t, 200, resp.StatusCode)
	testutil.Equals(t, "Test Works", string(body))
	testutil.Assert(t, !strings.Contains(b.String(), "err="))

	// URL with no explicit port number in the format- hostname:port
	req = httptest.NewRequest("GET", "http://example.com/foo", nil)
	b.Reset()

	w = httptest.NewRecorder()
	hm(w, req)

	resp = w.Result()
	body, err = io.ReadAll(resp.Body)
	testutil.Ok(t, err)

	testutil.Equals(t, 200, resp.StatusCode)
	testutil.Equals(t, "Test Works", string(body))
	testutil.Assert(t, !strings.Contains(b.String(), "err="))
}
