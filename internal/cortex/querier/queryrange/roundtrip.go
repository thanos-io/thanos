// Copyright (c) The Cortex Authors.
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
//
// Mostly lifted from prometheus/web/api/v1/api.go.

package queryrange

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go/ext"
	"io"
	"net/http"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/thanos-io/thanos/internal/cortex/querier"
	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
)

const day = 24 * time.Hour

var (
	// PassthroughMiddleware is a noop middleware
	PassthroughMiddleware = MiddlewareFunc(func(next Handler) Handler {
		return next
	})

	errInvalidMinShardingLookback = errors.New("a non-zero value is required for querier.query-ingesters-within when -querier.parallelise-shardable-queries is enabled")
)

// Config for query_range middleware chain.
type Config struct {
	SplitQueriesByInterval time.Duration `yaml:"split_queries_by_interval"`
	AlignQueriesWithStep   bool          `yaml:"align_queries_with_step"`
	ResultsCacheConfig     `yaml:"results_cache"`
	CacheResults           bool `yaml:"cache_results"`
	MaxRetries             int  `yaml:"max_retries"`
	ShardedQueries         bool `yaml:"parallelise_shardable_queries"`
	// List of headers which query_range middleware chain would forward to downstream querier.
	ForwardHeaders flagext.StringSlice `yaml:"forward_headers_list"`
}

// Validate validates the config.
func (cfg *Config) Validate(qCfg querier.Config) error {
	if cfg.CacheResults {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("querier.cache-results may only be enabled in conjunction with querier.split-queries-by-interval. Please set the latter")
		}
		if err := cfg.ResultsCacheConfig.Validate(qCfg); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config")
		}
	}
	return nil
}

// HandlerFunc is like http.HandlerFunc, but for Handler.
type HandlerFunc func(context.Context, Request) (Response, error)

// Do implements Handler.
func (q HandlerFunc) Do(ctx context.Context, req Request) (Response, error) {
	return q(ctx, req)
}

// Handler is like http.Handle, but specifically for Prometheus query_range calls.
type Handler interface {
	Do(context.Context, Request) (Response, error)
}

// MiddlewareFunc is like http.HandlerFunc, but for Middleware.
type MiddlewareFunc func(Handler) Handler

// Wrap implements Middleware.
func (q MiddlewareFunc) Wrap(h Handler) Handler {
	return q(h)
}

// Middleware is a higher order Handler.
type Middleware interface {
	Wrap(Handler) Handler
}

// MergeMiddlewares produces a middleware that applies multiple middleware in turn;
// ie Merge(f,g,h).Wrap(handler) == f.Wrap(g.Wrap(h.Wrap(handler)))
func MergeMiddlewares(middleware ...Middleware) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		for i := len(middleware) - 1; i >= 0; i-- {
			next = middleware[i].Wrap(next)
		}
		return next
	})
}

// Tripperware is a signature for all http client-side middleware.
type Tripperware func(http.RoundTripper) http.RoundTripper

// RoundTripFunc is to http.RoundTripper what http.HandlerFunc is to http.Handler.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (f RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type roundTripper struct {
	next    http.RoundTripper
	handler Handler
	codec   Codec
	headers []string
}

// NewRoundTripper merges a set of middlewares into an handler, then inject it into the `next` roundtripper
// using the codec to translate requests and responses.
func NewRoundTripper(next http.RoundTripper, codec Codec, headers []string, middlewares ...Middleware) http.RoundTripper {
	transport := roundTripper{
		next:    next,
		codec:   codec,
		headers: headers,
	}
	transport.handler = MergeMiddlewares(middlewares...).Wrap(&transport)
	return transport
}

func (q roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {

	// include the headers specified in the roundTripper during decoding the request.
	request, err := q.codec.DecodeRequest(r.Context(), r, q.headers)
	if err != nil {
		return nil, err
	}

	if span := opentracing.SpanFromContext(r.Context()); span != nil {
		request.LogToSpan(span)
	}

	response, err := q.handler.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return q.codec.EncodeResponse(r.Context(), response)
}

// Do implements Handler.
func (q roundTripper) Do(ctx context.Context, r Request) (Response, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "HTTP Request", ext.SpanKindRPCClient)
	defer span.Finish()

	request, err := q.codec.EncodeRequest(ctx, r)

	if err != nil {
		span.SetTag("error", true)
		span.LogKV("event", "error", "message", err.Error())
		return nil, err
	}

	span.SetOperationName(fmt.Sprintf("HTTP %s %s", request.Method, request.URL.String()))
	span.SetTag("http.method", request.Method)
	span.SetTag("http.url", request.URL.String())

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, request); err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err.Error())
	}

	newReqCtx := opentracing.ContextWithSpan(request.Context(), span)
	request = request.WithContext(newReqCtx)

	response, err := q.next.RoundTrip(request)

	span.SetTag("http.host", request.URL.Host)

	if err != nil {
		span.SetTag("error", true)
		span.LogKV(
			"event", "error",
			"message", err.Error(),
		)
		return nil, err
	}

	if response.StatusCode >= 400 {
		span.SetTag("error", true)
		span.LogKV(
			"event", "error",
			"http.status_code", response.StatusCode,
			"message", response.Status,
			"http.response_size", response.ContentLength,
		)
	} else {
		span.LogKV(
			"http.status_code", response.StatusCode,
			"http.response_size", response.ContentLength,
		)
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
		_ = response.Body.Close()
	}()

	return q.codec.DecodeResponse(ctx, response, r)
}
