// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type CapNProtoServer struct {
	listener net.Listener
	server   writecapnp.Writer
	logger   log.Logger
}

func NewCapNProtoServer(listener net.Listener, handler *CapNProtoHandler, logger log.Logger) *CapNProtoServer {
	return &CapNProtoServer{
		listener: listener,
		server:   writecapnp.Writer_ServerToClient(handler),
		logger:   logger,
	}
}

func (c *CapNProtoServer) ListenAndServe() error {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			return err
		}

		go func() {
			defer runutil.CloseWithLogOnErr(c.logger, conn, "receive capnp conn")
			rpcConn := rpc.NewConn(rpc.NewPackedStreamTransport(conn), &rpc.Options{
				// The BootstrapClient is the RPC interface that will be made available
				// to the remote endpoint by default.
				BootstrapClient: capnp.Client(c.server).AddRef(),
			})
			<-rpcConn.Done()
		}()
	}
}

func (c *CapNProtoServer) Shutdown() {
	c.server.Release()
}

type CapNProtoHandler struct {
	writer *CapNProtoWriter
	logger log.Logger
}

func NewCapNProtoHandler(logger log.Logger, writer *CapNProtoWriter) *CapNProtoHandler {
	return &CapNProtoHandler{logger: logger, writer: writer}
}

func (c CapNProtoHandler) Write(ctx context.Context, call writecapnp.Writer_write) error {
	call.Go()
	wr, err := call.Args().Wr()
	if err != nil {
		return err
	}
	t, err := wr.Tenant()
	if err != nil {
		return err
	}
	req, err := writecapnp.NewRequest(wr)
	if err != nil {
		return err
	}
	defer req.Close()

	var errs writeErrors
	errs.Add(c.writer.Write(ctx, t, req))
	if err := errs.ErrOrNil(); err != nil {
		level.Debug(c.logger).Log("msg", "failed to handle request", "err", err)
		result, allocErr := call.AllocResults()
		if allocErr != nil {
			return allocErr
		}

		switch errors.Cause(err) {
		case nil:
			return nil
		case errNotReady:
			result.SetError(writecapnp.WriteError_unavailable)
		case errUnavailable:
			result.SetError(writecapnp.WriteError_unavailable)
		case errConflict:
			result.SetError(writecapnp.WriteError_alreadyExists)
		case errBadReplica:
			result.SetError(writecapnp.WriteError_invalidArgument)
		default:
			result.SetError(writecapnp.WriteError_internal)
		}
	}
	return nil
}

type BufferedListener struct {
	ctx    context.Context
	cancel context.CancelFunc

	conns chan net.Conn
}

func (b BufferedListener) Accept() (net.Conn, error) {
	select {
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	case c := <-b.conns:
		return c, nil
	}
}

func (b BufferedListener) Close() error {
	b.cancel()
	return nil
}

func (b BufferedListener) Addr() net.Addr {
	return addr{}
}

type addr struct{}

func (addr) Network() string { return "bufconn" }
func (addr) String() string  { return "bufconn" }
