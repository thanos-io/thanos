// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package grpcclient_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/internal/cortex/util/grpcclient"
)

func TestRateLimiterFailureResultsInResourceExhaustedError(t *testing.T) {
	config := grpcclient.Config{
		RateLimitBurst: 0,
		RateLimit:      0,
	}
	conn := grpc.ClientConn{}
	invoker := func(currentCtx context.Context, currentMethod string, currentReq, currentRepl interface{}, currentConn *grpc.ClientConn, currentOpts ...grpc.CallOption) error {
		return nil
	}

	limiter := grpcclient.NewRateLimiter(&config)
	err := limiter(context.Background(), "methodName", "", "expectedReply", &conn, invoker)

	if se, ok := err.(interface {
		GRPCStatus() *status.Status
	}); ok {
		assert.Equal(t, se.GRPCStatus().Code(), codes.ResourceExhausted)
		assert.Equal(t, se.GRPCStatus().Message(), "rate: Wait(n=1) exceeds limiter's burst 0")
	} else {
		assert.Fail(t, "Could not convert error into expected Status type")
	}
}
