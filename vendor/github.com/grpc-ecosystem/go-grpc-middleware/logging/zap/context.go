package grpc_zap

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/tags/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/context"
)

// AddFields adds zap fields to the logger.
// Deprecated: should use the ctx_zap.AddFields instead
func AddFields(ctx context.Context, fields ...zapcore.Field) {
	ctx_zap.AddFields(ctx, fields...)
}

// Extract takes the call-scoped Logger from grpc_zap middleware.
// Deprecated: should use the ctx_zap.Extract instead
func Extract(ctx context.Context) *zap.Logger {
	return ctx_zap.Extract(ctx)
}
