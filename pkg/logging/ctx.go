package logging

import (
	"context"
	uuid "github.com/satori/go.uuid"
)

const header = "X-Request-ID"

type contextKey struct{}

var requestIDKey = contextKey{}

func GetRequestID(ctx context.Context) (string, bool) {
	val := ctx.Value(requestIDKey)
	if reqID, ok := val.(string); ok {
		return reqID, true
	}
	return "", false
}

func injectNewRequestID(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestIDKey, uuid.NewV4())
}


