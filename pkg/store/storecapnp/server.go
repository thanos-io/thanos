// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecapnp

import (
	"context"
	"net"

	"capnproto.org/go/capnp/v3"
	"capnproto.org/go/capnp/v3/rpc"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// Server serves a Cap'n Proto Store API.
type Server struct {
	listener net.Listener
	server   Store
	logger   log.Logger
}

// NewServer creates a new Cap'n Proto Store server.
func NewServer(listener net.Listener, handler *Handler, logger log.Logger) *Server {
	return &Server{
		listener: listener,
		server:   Store_ServerToClient(handler),
		logger:   logger,
	}
}

// ListenAndServe starts accepting connections.
func (s *Server) ListenAndServe() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		go func() {
			rpcConn := rpc.NewConn(rpc.NewPackedStreamTransport(conn), &rpc.Options{
				BootstrapClient: capnp.Client(s.server).AddRef(),
			})
			<-rpcConn.Done()
		}()
	}
}

// Shutdown releases the server resources.
func (s *Server) Shutdown() {
	s.server.Release()
}

// Handler implements the Cap'n Proto Store_Server interface.
type Handler struct {
	store      storepb.StoreServer
	infoServer infopb.InfoServer
	logger     log.Logger
}

// NewHandler creates a new Handler wrapping a storepb.StoreServer and infopb.InfoServer.
func NewHandler(logger log.Logger, store storepb.StoreServer, infoServer infopb.InfoServer) *Handler {
	return &Handler{logger: logger, store: store, infoServer: infoServer}
}

// Series implements Store_Server.
func (h *Handler) Series(ctx context.Context, call Store_series) error {
	call.Go()

	args := call.Args()
	capnpReq, err := args.Req()
	if err != nil {
		return errors.Wrap(err, "get request")
	}

	// Decode symbols
	symbols, err := capnpReq.Symbols()
	if err != nil {
		return errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	// Parse request
	pbReq, err := ParseSeriesRequestFrom(capnpReq, decoder)
	if err != nil {
		return errors.Wrap(err, "parse request")
	}

	// Create the stream implementation
	streamImpl := newSeriesStreamHandler(h.store, pbReq, h.logger)

	// Allocate results and set the stream capability
	results, err := call.AllocResults()
	if err != nil {
		return errors.Wrap(err, "alloc results")
	}

	return results.SetStream(SeriesStream_ServerToClient(streamImpl))
}

// LabelNames implements Store_Server.
func (h *Handler) LabelNames(ctx context.Context, call Store_labelNames) error {
	call.Go()

	args := call.Args()
	capnpReq, err := args.Req()
	if err != nil {
		return errors.Wrap(err, "get request")
	}

	// Decode symbols
	symbols, err := capnpReq.Symbols()
	if err != nil {
		return errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	// Parse request
	pbReq, err := ParseLabelNamesRequestFrom(capnpReq, decoder)
	if err != nil {
		return errors.Wrap(err, "parse request")
	}

	// Call the underlying store
	pbResp, err := h.store.LabelNames(ctx, pbReq)

	// Allocate results
	results, err2 := call.AllocResults()
	if err2 != nil {
		return errors.Wrap(err2, "alloc results")
	}

	if err != nil {
		storeErr, errCtx := grpcErrorToStore(err)
		results.SetError(storeErr)
		if errCtx != "" {
			if setErr := results.SetErrorContext(errCtx); setErr != nil {
				level.Error(h.logger).Log("msg", "failed to set error context", "err", setErr)
			}
		}
		return nil
	}

	// Build response
	capnpResp, err := results.NewResp()
	if err != nil {
		return errors.Wrap(err, "new response")
	}

	if err := BuildLabelNamesResponseInto(capnpResp, pbResp); err != nil {
		return errors.Wrap(err, "build response")
	}

	results.SetError(StoreError_none)
	return nil
}

// LabelValues implements Store_Server.
func (h *Handler) LabelValues(ctx context.Context, call Store_labelValues) error {
	call.Go()

	args := call.Args()
	capnpReq, err := args.Req()
	if err != nil {
		return errors.Wrap(err, "get request")
	}

	// Decode symbols
	symbols, err := capnpReq.Symbols()
	if err != nil {
		return errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	// Parse request
	pbReq, err := ParseLabelValuesRequestFrom(capnpReq, decoder)
	if err != nil {
		return errors.Wrap(err, "parse request")
	}

	// Call the underlying store
	pbResp, err := h.store.LabelValues(ctx, pbReq)

	// Allocate results
	results, err2 := call.AllocResults()
	if err2 != nil {
		return errors.Wrap(err2, "alloc results")
	}

	if err != nil {
		storeErr, errCtx := grpcErrorToStore(err)
		results.SetError(storeErr)
		if errCtx != "" {
			if setErr := results.SetErrorContext(errCtx); setErr != nil {
				level.Error(h.logger).Log("msg", "failed to set error context", "err", setErr)
			}
		}
		return nil
	}

	// Build response
	capnpResp, err := results.NewResp()
	if err != nil {
		return errors.Wrap(err, "new response")
	}

	if err := BuildLabelValuesResponseInto(capnpResp, pbResp); err != nil {
		return errors.Wrap(err, "build response")
	}

	results.SetError(StoreError_none)
	return nil
}

// Info implements Store_Server.
func (h *Handler) Info(ctx context.Context, call Store_info) error {
	call.Go()

	// Allocate results
	results, err := call.AllocResults()
	if err != nil {
		return errors.Wrap(err, "alloc results")
	}

	if h.infoServer == nil {
		results.SetError(StoreError_unavailable)
		if setErr := results.SetErrorContext("info server not available"); setErr != nil {
			level.Error(h.logger).Log("msg", "failed to set error context", "err", setErr)
		}
		return nil
	}

	// Call the gRPC Info server
	pbResp, err := h.infoServer.Info(ctx, &infopb.InfoRequest{})
	if err != nil {
		storeErr, errCtx := grpcErrorToStore(err)
		results.SetError(storeErr)
		if errCtx != "" {
			if setErr := results.SetErrorContext(errCtx); setErr != nil {
				level.Error(h.logger).Log("msg", "failed to set error context", "err", setErr)
			}
		}
		return nil
	}

	// Build response
	capnpResp, err := results.NewResp()
	if err != nil {
		return errors.Wrap(err, "new response")
	}

	if err := BuildInfoResponseFromProto(capnpResp, pbResp); err != nil {
		return errors.Wrap(err, "build response")
	}

	results.SetError(StoreError_none)
	return nil
}

// seriesStreamHandler implements SeriesStream_Server.
type seriesStreamHandler struct {
	store    storepb.StoreServer
	req      *storepb.SeriesRequest
	logger   log.Logger
	respChan chan *storepb.SeriesResponse
	errChan  chan error
	started  bool
	cancel   context.CancelFunc
}

func newSeriesStreamHandler(store storepb.StoreServer, req *storepb.SeriesRequest, logger log.Logger) *seriesStreamHandler {
	return &seriesStreamHandler{
		store:    store,
		req:      req,
		logger:   logger,
		respChan: make(chan *storepb.SeriesResponse, 100),
		errChan:  make(chan error, 1),
	}
}

// Next implements SeriesStream_Server.
func (s *seriesStreamHandler) Next(ctx context.Context, call SeriesStream_next) error {
	call.Go()

	// Start the underlying Series call on first Next()
	if !s.started {
		s.started = true
		// Use a background context for the series goroutine since the streaming
		// should continue even when individual Next() contexts complete.
		// The series will be canceled when the client stops calling Next().
		seriesCtx, cancel := context.WithCancel(context.Background())
		s.cancel = cancel
		go s.runSeries(seriesCtx)
	}

	// Allocate results
	results, err := call.AllocResults()
	if err != nil {
		return errors.Wrap(err, "alloc results")
	}

	// Try to get next response
	select {
	case resp, ok := <-s.respChan:
		if !ok {
			// Channel closed, check for error
			select {
			case err := <-s.errChan:
				if err != nil {
					storeErr, errCtx := grpcErrorToStore(err)
					results.SetError(storeErr)
					if errCtx != "" {
						results.SetErrorContext(errCtx)
					}
					results.SetDone(true)
					return nil
				}
			default:
			}
			results.SetDone(true)
			results.SetError(StoreError_none)
			return nil
		}

		// Build response
		capnpResp, err := results.NewResp()
		if err != nil {
			return errors.Wrap(err, "new response")
		}

		if err := BuildSeriesResponseInto(capnpResp, resp); err != nil {
			return errors.Wrap(err, "build response")
		}

		results.SetDone(false)
		results.SetError(StoreError_none)
		return nil

	case <-ctx.Done():
		// Cancel the series goroutine if the client context is done
		if s.cancel != nil {
			s.cancel()
		}
		storeErr, errCtx := grpcErrorToStore(ctx.Err())
		results.SetError(storeErr)
		if errCtx != "" {
			results.SetErrorContext(errCtx)
		}
		results.SetDone(true)
		return nil
	}
}

func (s *seriesStreamHandler) runSeries(ctx context.Context) {
	sender := &seriesSender{
		ctx:      ctx,
		respChan: s.respChan,
	}

	err := s.store.Series(s.req, sender)
	s.errChan <- err
	close(s.respChan)
}

// seriesSender implements storepb.Store_SeriesServer for channel-based sending.
type seriesSender struct {
	ctx      context.Context
	respChan chan<- *storepb.SeriesResponse
}

func (s *seriesSender) Send(resp *storepb.SeriesResponse) error {
	select {
	case s.respChan <- resp:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *seriesSender) Context() context.Context {
	return s.ctx
}

func (s *seriesSender) SetHeader(metadata.MD) error {
	return nil
}

func (s *seriesSender) SendHeader(metadata.MD) error {
	return nil
}

func (s *seriesSender) SetTrailer(metadata.MD) {
}

func (s *seriesSender) SendMsg(m any) error {
	if resp, ok := m.(*storepb.SeriesResponse); ok {
		return s.Send(resp)
	}
	return errors.New("invalid message type")
}

func (s *seriesSender) RecvMsg(m any) error {
	return errors.New("RecvMsg not supported on server stream")
}

// grpcErrorToStore converts a gRPC error to a Cap'n Proto StoreError.
func grpcErrorToStore(err error) (StoreError, string) {
	if err == nil {
		return StoreError_none, ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return StoreError_internal, err.Error()
	}

	var storeErr StoreError
	switch st.Code() {
	case codes.OK:
		storeErr = StoreError_none
	case codes.Unavailable:
		storeErr = StoreError_unavailable
	case codes.InvalidArgument:
		storeErr = StoreError_invalidArgument
	case codes.NotFound:
		storeErr = StoreError_notFound
	case codes.ResourceExhausted:
		storeErr = StoreError_resourceExhausted
	default:
		storeErr = StoreError_internal
	}

	return storeErr, st.Message()
}
