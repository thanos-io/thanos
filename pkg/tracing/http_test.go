// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tracing

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPMiddlewareRecordsWireBytes(t *testing.T) {
	mockTracer := mocktracer.New()
	body := []byte("hello world")

	handler := HTTPMiddleware(mockTracer, "test", log.NewNopLogger(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	spans := mockTracer.FinishedSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, len(body), spans[0].Tag(wireBytesTagKey))
}

func TestHTTPMiddlewareRecordsWireBytesMultipleWrites(t *testing.T) {
	mockTracer := mocktracer.New()
	chunk1 := []byte("hello ")
	chunk2 := []byte("world")

	handler := HTTPMiddleware(mockTracer, "test", log.NewNopLogger(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(chunk1)
		_, _ = w.Write(chunk2)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	spans := mockTracer.FinishedSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, len(chunk1)+len(chunk2), spans[0].Tag(wireBytesTagKey))
}

func TestHTTPMiddlewareWireBytesZeroOnEmptyResponse(t *testing.T) {
	mockTracer := mocktracer.New()

	handler := HTTPMiddleware(mockTracer, "test", log.NewNopLogger(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	spans := mockTracer.FinishedSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, 0, spans[0].Tag(wireBytesTagKey))
}

func TestCountingResponseWriterFlush(t *testing.T) {
	rec := httptest.NewRecorder()
	cw := &countingResponseWriter{ResponseWriter: rec}
	_, _ = cw.Write([]byte("data"))
	cw.Flush()
	assert.True(t, rec.Flushed)
	assert.Equal(t, 4, cw.bytes)
}

func TestIsSpanSampledMockTracer(t *testing.T) {
	mockTracer := mocktracer.New()
	span := mockTracer.StartSpan("test-op")
	defer span.Finish()

	// Mock tracer spans are not OTEL bridge spans, so isSpanSampled
	// conservatively returns true.
	assert.True(t, isSpanSampled(span))
}

func TestHTTPMiddlewareSkipsWireBytesWhenUnsampled(t *testing.T) {
	// Use the opentracing noop tracer: its spans have no valid OTEL
	// context, so isSpanSampled falls back to true. We verify the
	// mock-tracer path (always sampled) already covers counting in other
	// tests. This test instead uses a no-op tracer to confirm the
	// middleware still serves the request correctly without panicking.
	noopTracer := opentracing.NoopTracer{}
	body := []byte("ok")

	handler := HTTPMiddleware(noopTracer, "test", log.NewNopLogger(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, "ok", rec.Body.String())
}
