// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"context"
	"io"
	"iter"

	"google.golang.org/grpc"
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
}

func newInProcessClient(ctx context.Context, next func() (*SeriesResponse, error, bool), stop func()) *inProcessClient {
	return &inProcessClient{
		ctx:  ctx,
		next: next,
		stop: stop,
	}
}

func (c *inProcessClient) Recv() (*SeriesResponse, error) {
	resp, err, ok := c.next()
	if err != nil {
		c.stop()
		return nil, err
	}
	if !ok {
		return nil, io.EOF
	}
	return resp, err
}

func (c *inProcessClient) Context() context.Context {
	return c.ctx
}

func (c *inProcessClient) CloseSend() error {
	c.stop()
	return nil
}

func ServerAsClient(srv StoreServer) StoreClient {
	return &serverAsClient{srv: srv}
}

// serverAsClient allows to use servers as clients.
// NOTE: Passing CallOptions does not work - it would be needed to be implemented in grpc itself (before, after are private).
type serverAsClient struct {
	srv StoreServer
}

func (s serverAsClient) LabelNames(ctx context.Context, in *LabelNamesRequest, _ ...grpc.CallOption) (*LabelNamesResponse, error) {
	return s.srv.LabelNames(ctx, in)
}

func (s serverAsClient) LabelValues(ctx context.Context, in *LabelValuesRequest, _ ...grpc.CallOption) (*LabelValuesResponse, error) {
	return s.srv.LabelValues(ctx, in)
}

func (s serverAsClient) Series(ctx context.Context, in *SeriesRequest, _ ...grpc.CallOption) (Store_SeriesClient, error) {
	var srvIter iter.Seq2[*SeriesResponse, error] = func(yield func(*SeriesResponse, error) bool) {
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
