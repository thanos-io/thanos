// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"

	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	httputil "github.com/thanos-io/thanos/pkg/server/http"
)

type HTTPServerMiddleware struct {
	opts   *options
	logger log.Logger
}

func (m *HTTPServerMiddleware) preCall(start time.Time) {
	level.Debug(m.logger).Log("http.start_time", start.String(), "msg", "started call")
}

func (m *HTTPServerMiddleware) postCall(name string, start time.Time, wrapped *httputil.ResponseWriterWithStatus, r *http.Request) {
	status := wrapped.Status()
	logger := log.With(m.logger, "http.method", name, "http.request.id", r.Header.Get("X-Request-ID"), "http.code", fmt.Sprintf("%d", status),
		"http.time_ms", fmt.Sprintf("%v", durationToMilliseconds(time.Since(start))))

	if status >= 500 && status < 600 {
		logger = log.With(logger, "http.error", fmt.Sprintf("%v", status))
	}
	m.opts.levelFunc(logger, status).Log("msg", "finished call")
}

func (m *HTTPServerMiddleware) HTTPMiddleware(name string, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		wrapped := httputil.WrapResponseWriterWithStatus(w)
		start := time.Now()
		decision := m.opts.shouldLog()

		switch decision {
		case NoLogCall:
			next.ServeHTTP(w, r)

		case LogStartAndFinishCall:
			m.preCall(start)
			next.ServeHTTP(wrapped, r)
			m.postCall(name, start, wrapped, r)

		case LogFinishCall:
			next.ServeHTTP(wrapped, r)
			m.postCall(name, start, wrapped, r)
		}
	}
}

func NewHTTPServerMiddleware(logger log.Logger, opts ...Option) *HTTPServerMiddleware {
	o := evaluateOpt(opts)
	return &HTTPServerMiddleware{
		logger: log.With(logger, "protocol", "http", "http.component", "server"),
		opts:   o,
	}
}
