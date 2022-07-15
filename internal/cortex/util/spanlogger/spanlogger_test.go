// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package spanlogger

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestSpanLogger_Log(t *testing.T) {
	span, ctx := New(context.Background(), "test", "bar")
	_ = span.Log("foo")
	newSpan := FromContext(ctx)
	require.Equal(t, span.Span, newSpan.Span)
	_ = newSpan.Log("bar")
	noSpan := FromContext(context.Background())
	_ = noSpan.Log("foo")
	require.Error(t, noSpan.Error(errors.New("err")))
	require.NoError(t, noSpan.Error(nil))
}

func TestSpanLogger_CustomLogger(t *testing.T) {
	var logged [][]interface{}
	var logger funcLogger = func(keyvals ...interface{}) error {
		logged = append(logged, keyvals)
		return nil
	}
	span, ctx := NewWithLogger(context.Background(), logger, "test")
	_ = span.Log("msg", "original spanlogger")

	span = FromContextWithFallback(ctx, log.NewNopLogger())
	_ = span.Log("msg", "restored spanlogger")

	span = FromContextWithFallback(context.Background(), logger)
	_ = span.Log("msg", "fallback spanlogger")

	expect := [][]interface{}{
		{"method", "test", "msg", "original spanlogger"},
		{"msg", "restored spanlogger"},
		{"msg", "fallback spanlogger"},
	}
	require.Equal(t, expect, logged)
}

func TestSpanCreatedWithTenantTag(t *testing.T) {
	mockSpan := createSpan(user.InjectOrgID(context.Background(), "team-a"))

	require.Equal(t, []string{"team-a"}, mockSpan.Tag(TenantIDTagName))
}

func TestSpanCreatedWithoutTenantTag(t *testing.T) {
	mockSpan := createSpan(context.Background())

	_, exist := mockSpan.Tags()[TenantIDTagName]
	require.False(t, exist)
}

func createSpan(ctx context.Context) *mocktracer.MockSpan {
	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)

	logger, _ := New(ctx, "name")
	return logger.Span.(*mocktracer.MockSpan)
}

type funcLogger func(keyvals ...interface{}) error

func (f funcLogger) Log(keyvals ...interface{}) error {
	return f(keyvals...)
}
