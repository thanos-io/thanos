package cache

import (
	"context"

	"github.com/opentracing/opentracing-go"

	"github.com/thanos-io/thanos/pkg/tracing"
)

type TracingCache struct {
	Cache
}

func (t TracingCache) Fetch(ctx context.Context, keys []string) (result map[string][]byte) {
	tracing.DoWithSpan(ctx, "cache_fetch", func(spanCtx context.Context, span opentracing.Span) {
		result = t.Cache.Fetch(spanCtx, keys)
		span.LogKV("requested", len(keys), "returned", len(result))
	})
	return
}
