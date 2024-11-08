// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package transport

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type fakeRoundTripper struct {
	response       *http.Response
	err            error
	requestLatency time.Duration
}

func (f *fakeRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	time.Sleep(f.requestLatency)
	return f.response, f.err
}

func TestHandler_SlowQueryLog(t *testing.T) {
	t.Parallel()

	cfg := HandlerConfig{
		QueryStatsEnabled:    true,
		LogQueriesLongerThan: 1 * time.Microsecond,
	}

	tests := []struct {
		name     string
		url      string
		logParts []string
	}{
		{
			name: "Basic query",
			url:  "/api/v1/query?query=absent(up)&start=1714262400&end=1714266000",
			logParts: []string{
				"slow query detected",
				"time_taken=",
				"path=/api/v1/query",
				"param_query=absent(up)",
				"param_start=1714262400",
				"param_end=1714266000",
			},
		},
		{
			name: "Series call",
			url:  "/api/v1/series?match[]={__name__=~\"up\"}",
			logParts: []string{
				"slow query detected",
				"time_taken=",
				"path=/api/v1/series",
			},
		},
		{
			name: "Query with different parameters",
			url:  "/api/v1/query_range?query=rate(http_requests_total[5m])&start=1714262400&end=1714266000&step=15",
			logParts: []string{
				"slow query detected",
				"time_taken=",
				"path=/api/v1/query_range",
				"param_query=rate(http_requests_total[5m])",
				"param_start=1714262400",
				"param_end=1714266000",
				"param_step=15",
			},
		},
		{
			name: "Non-query endpoint",
			url:  "/favicon.ico",
			// No slow query log for non-query endpoints
			logParts: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fakeRoundTripper := &fakeRoundTripper{
				requestLatency: 2 * time.Microsecond,
				response: &http.Response{
					StatusCode: http.StatusOK,
					Header: http.Header{
						"Content-Type":  []string{"application/json"},
						"Server-Timing": []string{"querier;dur=1.23"},
					},
					Body: io.NopCloser(bytes.NewBufferString(`{}`)),
				},
			}

			logWriter := &bytes.Buffer{}
			logger := log.NewLogfmtLogger(log.NewSyncWriter(logWriter))

			handler := NewHandler(cfg, fakeRoundTripper, logger, prometheus.NewRegistry())

			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", tt.url, nil))

			for _, part := range tt.logParts {
				require.Contains(t, logWriter.String(), part)
			}
		})
	}
}
