// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/route"
	promLabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	promApiV1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/efficientgo/core/testutil"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

func TestMarshallMatrixNull(t *testing.T) {
	var m promql.Matrix
	result := response{
		Status: StatusSuccess,
		Data: promApiV1.QueryData{
			ResultType: parser.ValueTypeMatrix,
			Result:     m, // null
		},
	}

	b1, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Error marshaling response body: %s", err)
	}

	exp := response{
		Status: StatusSuccess,
		Data: promApiV1.QueryData{
			ResultType: parser.ValueTypeMatrix,
			Result:     promql.Matrix{},
		},
	}

	b2, err := json.Marshal(exp)
	if err != nil {
		t.Fatalf("Error marshaling response body: %s", err)
	}

	if !reflect.DeepEqual(b1, b2) {
		t.Fatalf("Expected response \n%v\n but got \n%v\n", string(b1), string(b2))
	}
}

func TestRespondSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Respond(w, "test", nil, log.NewNopLogger())
	}))
	defer s.Close()

	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("Error on test request: %s", err)
	}
	body, err := io.ReadAll(resp.Body)
	defer func() { testutil.Ok(t, resp.Body.Close()) }()
	if err != nil {
		t.Fatalf("Error reading response body: %s", err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("Return code %d expected in success response but got %d", 200, resp.StatusCode)
	}
	if h := resp.Header.Get("Content-Type"); h != "application/json" {
		t.Fatalf("Expected Content-Type %q but got %q", "application/json", h)
	}

	var res response
	if err = json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("Error unmarshaling JSON body: %s", err)
	}

	exp := &response{
		Status: StatusSuccess,
		Data:   "test",
	}
	if !reflect.DeepEqual(&res, exp) {
		t.Fatalf("Expected response \n%v\n but got \n%v\n", res, exp)
	}
}

func TestRespondError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		RespondError(w, &ApiError{ErrorTimeout, errors.New("message")}, "test", log.NewNopLogger())
	}))
	defer s.Close()

	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("Error on test request: %s", err)
	}
	body, err := io.ReadAll(resp.Body)
	defer func() { testutil.Ok(t, resp.Body.Close()) }()
	if err != nil {
		t.Fatalf("Error reading response body: %s", err)
	}

	if want, have := http.StatusServiceUnavailable, resp.StatusCode; want != have {
		t.Fatalf("Return code %d expected in error response but got %d", want, have)
	}
	if h := resp.Header.Get("Content-Type"); h != "application/json" {
		t.Fatalf("Expected Content-Type %q but got %q", "application/json", h)
	}

	var res response
	if err = json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("Error unmarshaling JSON body: %s", err)
	}

	exp := &response{
		Status:    StatusError,
		Data:      "test",
		ErrorType: ErrorTimeout,
		Error:     "message",
	}
	if !reflect.DeepEqual(&res, exp) {
		t.Fatalf("Expected response \n%v\n but got \n%v\n", res, exp)
	}
}

func TestOptionsMethod(t *testing.T) {
	r := route.New()
	api := &BaseAPI{}
	logMiddleware := logging.NewHTTPServerMiddleware(log.NewNopLogger())
	api.Register(r, &opentracing.NoopTracer{}, log.NewNopLogger(), extpromhttp.NewNopInstrumentationMiddleware(), logMiddleware)

	s := httptest.NewServer(r)
	defer s.Close()

	req, err := http.NewRequest("OPTIONS", s.URL+"/any_path", nil)
	if err != nil {
		t.Fatalf("Error creating OPTIONS request: %s", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Error executing OPTIONS request: %s", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Expected status %d, got %d", http.StatusNoContent, resp.StatusCode)
	}

	for h, v := range corsHeaders {
		if resp.Header.Get(h) != v {
			t.Fatalf("Expected %q for header %q, got %q", v, h, resp.Header.Get(h))
		}
	}
}

type nilWriter struct{}

var _ = http.ResponseWriter(&nilWriter{})

func (nilWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (nilWriter) Header() http.Header {
	return http.Header{}
}

func (nilWriter) WriteHeader(statusCode int) {}

func BenchmarkRespond(b *testing.B) {
	floats := []promql.FPoint{}

	for i := 0; i < 10000; i++ {
		floats = append(floats, promql.FPoint{T: 1435781451 + int64(i), F: 1234.123 + float64(i)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Respond(&nilWriter{}, promql.Matrix{
			promql.Series{
				Metric: promLabels.FromMap(map[string]string{"__name__": "up", "job": "prometheus"}),
				Floats: floats,
			},
		}, nil, log.NewNopLogger())
	}
}
