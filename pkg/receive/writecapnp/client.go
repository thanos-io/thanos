// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"context"
	"io"
	"net"
	"sync"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/runutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type connState int

const (
	connStateDisconnected = iota
	connStateError
	connStateConnected
)

type conn struct {
	mu sync.RWMutex

	state  connState
	closer io.Closer
	writer Writer
}

type Dialer interface {
	Dial() (net.Conn, error)
}

type TCPDialer struct {
	address string
}

func NewTCPDialer(address string) *TCPDialer {
	return &TCPDialer{address: address}
}

func (t TCPDialer) Dial() (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", t.address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial peer %s", t.address)
	}
	return conn, nil
}

type RemoteWriteClient struct {
	dialer Dialer
	conn   *conn

	logger log.Logger
}

func NewRemoteWriteClient(dialer Dialer, logger log.Logger) *RemoteWriteClient {
	return &RemoteWriteClient{
		dialer: dialer,
		logger: logger,
		conn:   &conn{},
	}
}

func (r *RemoteWriteClient) RemoteWrite(ctx context.Context, in *storepb.WriteRequest, _ ...grpc.CallOption) (*storepb.WriteResponse, error) {
	const numAttempts = 3
	var (
		resp    Writer_write_Results
		release func()
		err     error
	)
	for range numAttempts {
		if err := r.conn.connect(ctx, r.logger, r.dialer); err != nil {
			return nil, err
		}
		if resp, release, err = r.write(ctx, in); err == nil {
			break
		}
		r.conn.setStateError()
		level.Warn(r.logger).Log("msg", "rpc failed, reconnecting", "err", err.Error())
	}
	if err != nil {
		return nil, err
	}
	defer release()

	switch resp.Error() {
	case WriteError_unavailable:
		return nil, status.Error(codes.Unavailable, "rpc failed")
	case WriteError_alreadyExists:
		return nil, status.Error(codes.AlreadyExists, "rpc failed")
	case WriteError_invalidArgument:
		return nil, status.Error(codes.InvalidArgument, "rpc failed")
	case WriteError_internal:
		return nil, status.Error(codes.Internal, "rpc failed")
	default:
		return &storepb.WriteResponse{}, nil
	}
}

func (r *RemoteWriteClient) write(ctx context.Context, in *storepb.WriteRequest) (Writer_write_Results, func(), error) {
	r.conn.mu.RLock()
	defer r.conn.mu.RUnlock()

	arena := capnp.SingleSegment(nil)
	defer arena.Release()

	result, release := r.conn.writer.Write(ctx, func(params Writer_write_Params) error {
		_, seg, err := capnp.NewMessage(arena)
		if err != nil {
			return err
		}
		wr, err := NewRootWriteRequest(seg)
		if err != nil {
			return err
		}
		if err := params.SetWr(wr); err != nil {
			return err
		}
		wr, err = params.Wr()
		if err != nil {
			return err
		}
		return BuildInto(wr, in.Tenant, in.Timeseries)
	})

	resp, err := result.Struct()
	return resp, release, err
}

func (r *conn) setStateError() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = connStateError
}

func (r *conn) connect(ctx context.Context, logger log.Logger, dialer Dialer) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case connStateConnected:
		return nil
	case connStateError:
		r.close(logger)
		fallthrough
	case connStateDisconnected:
		cc, err := dialer.Dial()
		if err != nil {
			return errors.Wrap(err, "failed to dial peer")
		}
		codec := rpc.NewPackedStreamTransport(cc)
		r.closer = codec

		rpcConn := rpc.NewConn(codec, nil)
		r.writer = Writer(rpcConn.Bootstrap(ctx))
		r.state = connStateConnected
	}
	return nil
}

func (r *RemoteWriteClient) Close() error {
	r.conn.closeLocked(r.logger)
	return nil
}

func (r *conn) closeLocked(logger log.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.close(logger)
}

func (r *conn) close(logger log.Logger) {
	if r.state != connStateDisconnected {
		codec := r.closer
		r.closer = nil
		go func() {
			runutil.CloseWithLogOnErr(logger, codec, "capnp closer")
		}()
	}
	r.state = connStateDisconnected
}
