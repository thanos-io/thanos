package writecapnp

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type writerFunc func(context.Context, Writer_write) error

func (w writerFunc) Write(ctx context.Context, call Writer_write) error {
	return w(ctx, call)
}

func newPipeConn(t *testing.T, server Writer_Server) (net.Conn, func()) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		writerClient := Writer_ServerToClient(server)
		rpcConn := rpc.NewConn(rpc.NewPackedStreamTransport(serverConn), &rpc.Options{
			BootstrapClient: capnp.Client(writerClient).AddRef(),
		})
		<-rpcConn.Done()
		writerClient.Release()
		close(done)
	}()

	cleanup := func() {
		_ = clientConn.Close()
		<-done
	}

	return clientConn, cleanup
}

type sequenceDialer struct {
	mu     sync.Mutex
	conns  []net.Conn
	clean  []func()
	dialed int
}

func (d *sequenceDialer) Dial() (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.conns) == 0 {
		return nil, errors.New("no more connections")
	}
	conn := d.conns[0]
	d.conns = d.conns[1:]
	d.dialed++
	return conn, nil
}

func (d *sequenceDialer) cleanup() {
	for _, c := range d.clean {
		c()
	}
}

func TestRemoteWriteClientReconnectsOnDeadline(t *testing.T) {
	t.Parallel()

	firstConn, firstCleanup := newPipeConn(t, writerFunc(func(ctx context.Context, call Writer_write) error {
		call.Go()
		return context.DeadlineExceeded
	}))
	secondConn, secondCleanup := newPipeConn(t, writerFunc(func(ctx context.Context, call Writer_write) error {
		call.Go()
		_, err := call.AllocResults()
		return err
	}))

	dialer := &sequenceDialer{
		conns: []net.Conn{firstConn, secondConn},
		clean: []func(){firstCleanup, secondCleanup},
	}
	defer dialer.cleanup()

	client := NewRemoteWriteClient(dialer, log.NewNopLogger())
	defer func() { _ = client.Close() }()

	_, err := client.RemoteWrite(context.Background(), &storepb.WriteRequest{})
	testutil.Ok(t, err)

	testutil.Equals(t, 2, dialer.dialed)
}
