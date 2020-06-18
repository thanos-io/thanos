// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package middleware

import (
	"context"
	"math/rand"
	"net/http"
	"time"

	"github.com/oklog/ulid"
)

type ctxKey int

const reqIDKey = ctxKey(0)

// newContextWithRequestID creates a context with a request id.
func newContextWithRequestID(ctx context.Context, rid string) context.Context {
	return context.WithValue(ctx, reqIDKey, rid)
}

// RequestIDFromContext returns the request id from context.
func RequestIDFromContext(ctx context.Context) (string, bool) {
	rid, ok := ctx.Value(reqIDKey).(string)
	return rid, ok
}

// RequestID sets a unique request id for each request.
func RequestID(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
			reqID := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)
			r.Header.Set("X-Request-ID", reqID.String())
		}
		ctx := newContextWithRequestID(r.Context(), reqID)
		h.ServeHTTP(w, r.WithContext(ctx))
	}
}
