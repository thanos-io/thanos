// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"context"
	"fmt"
	"io"
	"iter"
	"runtime/debug"
	"sync"

	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type inProcessServer struct {
	Store_SeriesServer
	ctx   context.Context
	yield func(response *SeriesResponse, err error) bool
}

func newInProcessServer(ctx context.Context, yield func(*SeriesResponse, error) bool) *inProcessServer {
	return &inProcessServer{
		ctx:   ctx,
		yield: yield,
	}
}

func (s *inProcessServer) Send(resp *SeriesResponse) error {
	s.yield(resp, nil)
	return nil
}

func (s *inProcessServer) Context() context.Context {
	return s.ctx
}

type inProcessClient struct {
	Store_SeriesClient
	ctx  context.Context
	next func() (*SeriesResponse, error, bool)
	stop func()
	mu   sync.Mutex // protects next and stop
}

func newInProcessClient(ctx context.Context, next func() (*SeriesResponse, error, bool), stop func()) *inProcessClient {
	return &inProcessClient{
		ctx:  ctx,
		next: next,
		stop: stop,
		mu:   sync.Mutex{},
	}
}

func (c *inProcessClient) Recv() (*SeriesResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	resp, err, ok := c.next()
	if err != nil {
		c.stop()
		return nil, err
	}
	if !ok {
		if c.ctx.Err() != nil {
			return nil, c.ctx.Err()
		}
		return nil, io.EOF
	}
	return resp, err
}

func (c *inProcessClient) Context() context.Context {
	return c.ctx
}

func (c *inProcessClient) CloseSend() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stop()
	return nil
}

func ServerAsClient(srv StoreServer, readOnly atomic.Bool) StoreClient {
	return &serverAsClient{srv: srv, readOnly: readOnly}
}

// serverAsClient allows to use servers as clients.
// NOTE: Passing CallOptions does not work - it would be needed to be implemented in grpc itself (before, after are private).
type serverAsClient struct {
	srv      StoreServer
	readOnly atomic.Bool
}

func (s serverAsClient) LabelNames(ctx context.Context, in *LabelNamesRequest, _ ...grpc.CallOption) (*LabelNamesResponse, error) {
	return s.srv.LabelNames(ctx, in)
}

func (s serverAsClient) LabelValues(ctx context.Context, in *LabelValuesRequest, _ ...grpc.CallOption) (*LabelValuesResponse, error) {
	return s.srv.LabelValues(ctx, in)
}

type readOnlySeriesClient struct {
	ctx context.Context
}

var _ Store_SeriesClient = &readOnlySeriesClient{}

func (r *readOnlySeriesClient) Recv() (*SeriesResponse, error) {
	return nil, io.EOF
}

func (r *readOnlySeriesClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (r *readOnlySeriesClient) Trailer() metadata.MD {
	return nil
}

func (r *readOnlySeriesClient) CloseSend() error {
	return nil
}

func (r *readOnlySeriesClient) Context() context.Context {
	return r.ctx
}

func (r *readOnlySeriesClient) SendMsg(m interface{}) error {
	return nil
}

func (r *readOnlySeriesClient) RecvMsg(m interface{}) error {
	return io.EOF
}

func (s serverAsClient) Series(ctx context.Context, in *SeriesRequest, _ ...grpc.CallOption) (Store_SeriesClient, error) {
	if s.readOnly.Load() {
		return &readOnlySeriesClient{ctx: ctx}, nil
	}
	var srvIter iter.Seq2[*SeriesResponse, error] = func(yield func(*SeriesResponse, error) bool) {
		defer func() {
			if r := recover(); r != nil {
				st := debug.Stack()
				panic(fmt.Sprintf("panic %v in server iterator: %s", r, st))
			}
		}()
		srv := newInProcessServer(ctx, yield)
		err := s.srv.Series(in, srv)
		if err != nil {
			yield(nil, err)
			return
		}
	}

	clientIter, stop := iter.Pull2(srvIter)
	return newInProcessClient(ctx, clientIter, stop), nil
}
