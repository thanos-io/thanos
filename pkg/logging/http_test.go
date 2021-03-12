// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestHTTPServerMiddleware(t *testing.T) {
	m := NewHTTPServerMiddleware(log.NewNopLogger())
	handler := func(w http.ResponseWriter, r *http.Request) {
		_, err := io.WriteString(w, "Test Works")
		if err != nil {
			t.Log(err)
		}
	}
	hm := m.HTTPMiddleware("test", http.HandlerFunc(handler))

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	w := httptest.NewRecorder()

	hm(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	testutil.Equals(t, 200, resp.StatusCode)
	testutil.Equals(t, "Test Works", string(body))
}
