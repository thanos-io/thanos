// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

/*
This was copied over from https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2.0.0-rc.3
and modified to support tracing in Thanos till migration to Otel is supported.
*/

package interceptors

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
)

type GRPCType string

const (
	Unary        GRPCType = "unary"
	ClientStream GRPCType = "client_stream"
	ServerStream GRPCType = "server_stream"
	BidiStream   GRPCType = "bidi_stream"
)

var (
	AllCodes = []codes.Code{
		codes.OK, codes.Canceled, codes.Unknown, codes.InvalidArgument, codes.DeadlineExceeded, codes.NotFound,
		codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated, codes.ResourceExhausted,
		codes.FailedPrecondition, codes.Aborted, codes.OutOfRange, codes.Unimplemented, codes.Internal,
		codes.Unavailable, codes.DataLoss,
	}
)

func splitMethodName(fullMethod string) (string, string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/") // remove leading slash
	if i := strings.Index(fullMethod, "/"); i >= 0 {
		return fullMethod[:i], fullMethod[i+1:]
	}
	return "unknown", "unknown"
}

func FullMethod(service, method string) string {
	return fmt.Sprintf("/%s/%s", service, method)
}

type ClientReportable interface {
	ClientReporter(ctx context.Context, reqProtoOrNil any, typ GRPCType, service string, method string) (Reporter, context.Context)
}

type ServerReportable interface {
	ServerReporter(ctx context.Context, reqProtoOrNil any, typ GRPCType, service string, method string) (Reporter, context.Context)
}

type Reporter interface {
	PostCall(err error, rpcDuration time.Duration)

	PostMsgSend(reqProto any, err error, sendDuration time.Duration)
	PostMsgReceive(replyProto any, err error, recvDuration time.Duration)
}

var _ Reporter = NoopReporter{}

type NoopReporter struct{}

func (NoopReporter) PostCall(error, time.Duration)            {}
func (NoopReporter) PostMsgSend(any, error, time.Duration)    {}
func (NoopReporter) PostMsgReceive(any, error, time.Duration) {}

type report struct {
	rpcType   GRPCType
	service   string
	method    string
	startTime time.Time
}

func newReport(typ GRPCType, fullMethod string) report {
	r := report{
		startTime: time.Now(),
		rpcType:   typ,
	}
	r.service, r.method = splitMethodName(fullMethod)
	return r
}
