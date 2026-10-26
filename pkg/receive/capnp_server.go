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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
)

type CapNProtoServer struct {
	listener net.Listener
	server   writecapnp.Writer
	logger   log.Logger

	compressionConcurrency int
}

type CapNProtoServerOption func(*CapNProtoServer)

// WithCapNProtoCompressionConcurrency sets the zstd encoder concurrency shared
// with the replication clients of this process.
func WithCapNProtoCompressionConcurrency(n int) CapNProtoServerOption {
	return func(s *CapNProtoServer) { s.compressionConcurrency = n }
}

func NewCapNProtoServer(listener net.Listener, handler *CapNProtoHandler, logger log.Logger, opts ...CapNProtoServerOption) *CapNProtoServer {
	s := &CapNProtoServer{
		listener:               listener,
		server:                 writecapnp.Writer_ServerToClient(handler),
		logger:                 logger,
		compressionConcurrency: writecapnp.DefaultCompressionConcurrency,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (c *CapNProtoServer) ListenAndServe() error {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			return err
		}

		go func() {
			transport, err := writecapnp.NewServerTransport(conn, c.compressionConcurrency)
			if err != nil {
				level.Debug(c.logger).Log("msg", "failed to set up capnproto transport", "peer", conn.RemoteAddr(), "err", err)
				_ = conn.Close()
				return
			}
			rpcConn := rpc.NewConn(transport, &rpc.Options{
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

func NewCapNProtoHandler(reg prometheus.Registerer, logger log.Logger, writer *CapNProtoWriter) *CapNProtoHandler {
	return &CapNProtoHandler{
		logger: logger,
		writer: writer,
	}
}

func (c CapNProtoHandler) Write(ctx context.Context, call writecapnp.Writer_write) error {
	call.Go()
	wr, err := call.Args().Wr()
	if err != nil {
		return err
	}

	var errs writeErrors

	if wr.HasTimeSeries() {
		t, err := wr.Tenant()
		if err != nil {
			return err
		}

		req, err := writecapnp.NewSingleTenantRequest(wr, t)
		if err != nil {
			return err
		}

		errs.Add(c.writer.Write(ctx, req))
		errs.Add(req.Close())
	} else {
		data, err := wr.Data()
		if err != nil {
			return err
		}

		symTable, err := wr.Symbols()
		if err != nil {
			return err
		}

		for i := 0; i < data.Len(); i++ {
			d := data.At(i)

			tenant, err := d.Tenant()
			if err != nil {
				return err
			}

			req, err := writecapnp.NewRequest(d, symTable, tenant)
			if err != nil {
				return err
			}

			errs.Add(c.writer.Write(ctx, req))
			errs.Add(req.Close())
		}
	}

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
			if cerr := result.SetExtraErrorContext(err.Error()); cerr != nil {
				level.Error(c.logger).Log("msg", "failed to set extra error context", "err", cerr)
			}
		}
	}

	return nil
}
