// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// TestCapNProtoClient_UnresponsiveServerDuringRestart verifies that the client
// can recover after encountering an unresponsive server (e.g., during pod restart).
// This is a regression test for: https://github.com/thanos-io/thanos/issues/8254
func TestCapNProtoClient_UnresponsiveServerDuringRestart(t *testing.T) {
	t.Parallel()
	logger := log.NewNopLogger()

	// Start an unresponsive server (accepts connections but never responds)
	unresponsiveListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := unresponsiveListener.Addr().String()

	var unresponsiveWg sync.WaitGroup
	unresponsiveWg.Add(1)
	go func() {
		defer unresponsiveWg.Done()
		for {
			conn, err := unresponsiveListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
					if _, err := c.Read(buf); err != nil {
						if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
							continue
						}
						return
					}
				}
			}(conn)
		}
	}()

	// Create client pointing to unresponsive server
	client := writecapnp.NewRemoteWriteClient(writecapnp.NewTCPDialer(addr), logger)

	// First request times out (expected)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = client.RemoteWrite(ctx1, &storepb.WriteRequest{Tenant: "test"})
	cancel1()
	require.Error(t, err)
	require.Contains(t, err.Error(), "context deadline exceeded")

	// Shutdown unresponsive server and start working server on same address
	unresponsiveListener.Close()
	unresponsiveWg.Wait()
	time.Sleep(100 * time.Millisecond)

	workingListener, err := net.Listen("tcp", addr)
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		workingListener, err = net.Listen("tcp", addr)
	}
	require.NoError(t, err)
	defer workingListener.Close()

	handler := NewCapNProtoHandler(logger, NewCapNProtoWriter(
		logger,
		newFakeTenantAppendable(&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
		&CapNProtoWriterOptions{},
	))
	srv := NewCapNProtoServer(workingListener, handler, logger)
	go srv.ListenAndServe()
	defer srv.Shutdown()
	time.Sleep(50 * time.Millisecond)

	// Second request should succeed - client must recover by reconnecting
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = client.RemoteWrite(ctx2, &storepb.WriteRequest{Tenant: "test"})
	cancel2()

	require.NoError(t, err, "client should recover by reconnecting to working server")
	client.Close()
}
