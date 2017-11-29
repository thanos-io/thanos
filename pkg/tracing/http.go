package tracing

import (
	"net"
	"net/http"
	"strconv"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func HTTPMiddleware(tracer opentracing.Tracer, operationName string, logger log.Logger, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var span opentracing.Span
		wireContext, err := tracer.Extract(
			opentracing.TextMap,
			opentracing.HTTPHeadersCarrier(r.Header),
		)
		if err != nil && err != opentracing.ErrSpanContextNotFound {
			level.Error(logger).Log("msg", "failed to extract tracer from request", "operationName", operationName, "err", err)
		}

		span = tracer.StartSpan(operationName, ext.RPCServerOption(wireContext))
		ext.HTTPMethod.Set(span, r.Method)
		ext.HTTPUrl.Set(span, r.URL.String())

		next(w, r.WithContext(opentracing.ContextWithSpan(ContextWithTracer(r.Context(), tracer), span)))
		span.Finish()
		return
	}
}

type tripperware struct {
	operationName string
	logger        log.Logger
	next          http.RoundTripper
}

func (t *tripperware) RoundTrip(r *http.Request) (*http.Response, error) {
	tracer := tracerFromContext(r.Context())
	if tracer == nil {
		// No tracer, programmatic mistake.
		level.Warn(t.logger).Log("msg", "Tracer not found in context.")
		return t.next.RoundTrip(r)
	}

	span := opentracing.SpanFromContext(r.Context())
	if span == nil {
		return t.next.RoundTrip(r)
	}

	span = tracer.StartSpan(t.operationName, opentracing.ChildOf(span.Context()))
	ext.HTTPMethod.Set(span, r.Method)
	ext.HTTPUrl.Set(span, r.URL.String())
	host, portString, err := net.SplitHostPort(r.URL.Host)
	if err == nil {
		ext.PeerHostname.Set(span, host)
		if port, err := strconv.Atoi(portString); err != nil {
			ext.PeerPort.Set(span, uint16(port))
		}
	} else {
		ext.PeerHostname.Set(span, r.URL.Host)
	}

	// There's nothing we can do with any errors here.
	if err = tracer.Inject(
		span.Context(),
		opentracing.TextMap,
		opentracing.HTTPHeadersCarrier(r.Header),
	); err != nil {
		level.Warn(t.logger).Log("msg", "failed to inject trace", "err", err)
	}

	resp, err := t.next.RoundTrip(r)
	span.Finish()
	return resp, err
}

func HTTPTripperware(logger log.Logger, operationName string, next http.RoundTripper) http.RoundTripper {
	return &tripperware{
		operationName: operationName,
		logger:        logger,
		next:          next,
	}
}
