// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type mockReadinessChecker struct {
	ready bool
}

func (m *mockReadinessChecker) IsReady() bool {
	return m.ready
}

func (m *mockReadinessChecker) SetReady(ready bool) {
	m.ready = ready
}

func TestNewReadinessGRPCOptions(t *testing.T) {
	readyChecker := &mockReadinessChecker{ready: true}
	options := NewReadinessGRPCOptions(readyChecker)
	testutil.Equals(t, 2, len(options))
}

func TestReadinessInterceptorUnary(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()

	checker := &mockReadinessChecker{ready: false}
	unaryInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if !checker.IsReady() {
			return nil, nil
		}
		return handler(ctx, req)
	}

	s := grpc.NewServer(grpc.UnaryInterceptor(unaryInterceptor))

	mockSrv := &mockWriteableStoreServer{}
	storepb.RegisterWriteableStoreServer(s, mockSrv)
	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer s.Stop()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	testutil.Ok(t, err)
	defer conn.Close()

	client := storepb.NewWriteableStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.RemoteWrite(ctx, &storepb.WriteRequest{})
	testutil.Ok(t, err)
	testutil.Assert(t, resp != nil)
	testutil.Equals(t, 0, mockSrv.callCount)
	checker.SetReady(true)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	resp2, err2 := client.RemoteWrite(ctx2, &storepb.WriteRequest{})
	testutil.Ok(t, err2)
	testutil.Assert(t, resp2 != nil)
	testutil.Equals(t, 1, mockSrv.callCount)
}

func TestReadinessInterceptorStream(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()

	checker := &mockReadinessChecker{ready: false}
	streamInterceptor := func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !checker.IsReady() {
			return nil
		}
		return handler(srv, ss)
	}

	s := grpc.NewServer(grpc.StreamInterceptor(streamInterceptor))

	mockSrv := &mockWriteableStoreServer{}
	storepb.RegisterWriteableStoreServer(s, mockSrv)
	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("Server failed: %v", err)
		}
	}()
	defer s.Stop()

	testutil.Assert(t, true)
}

func TestReadinessCheckerInterface(t *testing.T) {
	checker := &mockReadinessChecker{ready: false}
	var _ ReadinessChecker = checker
	testutil.Equals(t, false, checker.IsReady())

	checker.SetReady(true)
	testutil.Equals(t, true, checker.IsReady())

	checker.SetReady(false)
	testutil.Equals(t, false, checker.IsReady())
}
