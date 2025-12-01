// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
	"google.golang.org/grpc/test/bufconn"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestCapNProtoServer_SingleConcurrentClient(t *testing.T) {
	t.Parallel()

	var (
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(log.NewNopLogger(), writer)
		srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for range 1000 {
		client := writecapnp.NewRemoteWriteClient(listener, log.NewLogfmtLogger(os.Stdout))
		_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
			Tenant: "default",
		})
		require.NoError(t, err)
		require.NoError(t, client.Close())
	}
	require.NoError(t, listener.Close())
}

func TestCapNProtoServer_MultipleConcurrentClients(t *testing.T) {
	t.Parallel()

	var (
		writer = NewCapNProtoWriter(
			log.NewNopLogger(),
			newFakeTenantAppendable(
				&fakeAppendable{appender: newFakeAppender(nil, nil, nil)}),
			&CapNProtoWriterOptions{},
		)
		listener = bufconn.Listen(1024)
		handler  = NewCapNProtoHandler(log.NewNopLogger(), writer)
		srv      = NewCapNProtoServer(listener, handler, log.NewNopLogger())
	)
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown()

	for range 1000 {
		client := writecapnp.NewRemoteWriteClient(listener, log.NewLogfmtLogger(os.Stdout))
		_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
			Tenant: "default",
		})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, client.Close())
		}()
	}

	require.NoError(t, listener.Close())
}

func TestCapNProtoServer_MultipleSerialClientsWithReconnect(t *testing.T) {
	custom.TolerantVerifyLeak(t)
	var (
		logger   = log.NewNopLogger()
		listener = bufconn.Listen(1024)
		handler  = newFaultyHandler(2)
	)

	srv := NewCapNProtoServer(listener, handler, logger)
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(srv.Shutdown)

	client := writecapnp.NewRemoteWriteClient(listener, logger)
	const numRuns = 100
	const numRequests = 10
	for range numRuns {
		handler.numFailures = 2
		var wg sync.WaitGroup
		for range numRequests {
			wg.Go(func() {
				_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{
					Tenant: "default",
				})
				require.NoError(t, err)
			})
		}
		wg.Wait()
	}
	require.NoError(t, client.Close())
	require.NoError(t, errors.Join(listener.Close()))
}

type faultyHandler struct {
	mu          sync.Mutex
	numReqs     int
	numFailures int
}

func newFaultyHandler(failEach int) *faultyHandler {
	return &faultyHandler{numFailures: failEach}
}

func (f *faultyHandler) Write(ctx context.Context, call writecapnp.Writer_write) error {
	call.Go()
	if err := f.checkFailures(); err != nil {
		return err
	}

	arg, err := call.Args().Wr()
	if err != nil {
		return err
	}
	if _, err := arg.Tenant(); err != nil {
		return err
	}
	results, err := call.AllocResults()
	if err != nil {
		return err
	}
	results.SetError(writecapnp.WriteError_none)
	return nil
}

func (f *faultyHandler) checkFailures() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.numFailures--
	if f.numFailures > 0 {
		return errors.New("handler failure")
	}
	return nil
}
