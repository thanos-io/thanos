// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"net"
	"unsafe"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

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
	writer       *CapNProtoWriter
	logger       log.Logger
	handledTotal prometheus.Counter
}

func NewCapNProtoHandler(reg prometheus.Registerer, logger log.Logger, writer *CapNProtoWriter) *CapNProtoHandler {
	handledTotal := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_receive_capnproto_handled_total",
		Help: "Total number of handled CapNProto requests.",
	})

	return &CapNProtoHandler{handledTotal: handledTotal, logger: logger, writer: writer}
}

func (c *CapNProtoHandler) Write(ctx context.Context, call writecapnp.Writer_write) error {
	c.handledTotal.Inc()

	call.Go()
	wr, err := call.Args().Wr()
	if err != nil {
		return err
	}

	data, err := wr.Data()
	if err != nil {
		return err
	}

	var errs writeErrors
	for i := 0; i < data.Len(); i++ {
		d := data.At(i)
		req, err := writecapnp.NewRequest(d)
		if err != nil {
			return err
		}
		defer req.Close()

		tenant, err := d.TenantBytes()
		if err != nil {
			return err
		}

		errs.Add(c.writer.Write(ctx, unsafe.String(&tenant[0], len(tenant)), req))
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
		}
	}

	return nil
}
