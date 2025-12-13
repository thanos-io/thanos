// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecapnp

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
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// Dialer is an interface for creating network connections.
type Dialer interface {
	Dial() (net.Conn, error)
}

// TCPDialer implements Dialer for TCP connections.
type TCPDialer struct {
	address string
}

// NewTCPDialer creates a new TCPDialer.
func NewTCPDialer(address string) *TCPDialer {
	return &TCPDialer{address: address}
}

// Dial creates a new TCP connection to the configured address.
func (t *TCPDialer) Dial() (net.Conn, error) {
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

// StoreClient is a Cap'n Proto Store client that implements storepb.StoreClient.
type StoreClient struct {
	mu sync.Mutex

	dialer Dialer
	conn   *rpc.Conn
	store  Store
	logger log.Logger

	// Metadata about the store - these should be set after connecting
	// and obtaining info from the store.
	labelSets                    []labels.Labels
	minTime, maxTime             int64
	tsdbInfos                    []infopb.TSDBInfo
	supportsSharding             bool
	supportsWithoutReplicaLabels bool
	addr                         string
	isLocal                      bool
}

// NewStoreClient creates a new Cap'n Proto Store client.
func NewStoreClient(dialer Dialer, logger log.Logger, addr string, isLocal bool) *StoreClient {
	return &StoreClient{
		dialer:  dialer,
		logger:  logger,
		addr:    addr,
		isLocal: isLocal,
	}
}

// Series implements storepb.StoreClient.
func (c *StoreClient) Series(ctx context.Context, req *storepb.SeriesRequest, opts ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return c.seriesWithReconnect(ctx, 2, req)
}

func (c *StoreClient) seriesWithReconnect(ctx context.Context, numReconnects int, req *storepb.SeriesRequest) (storepb.Store_SeriesClient, error) {
	if err := c.connect(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	// Call Series on the capnp store, which returns a SeriesStream capability
	result, release := c.store.Series(ctx, func(params Store_series_Params) error {
		capnpReq, err := params.NewReq()
		if err != nil {
			return err
		}
		return BuildSeriesRequestInto(capnpReq, req)
	})

	// Get the result struct
	resultStruct, err := result.Struct()
	if err != nil {
		release()
		if numReconnects > 0 && capnp.IsDisconnected(err) {
			level.Warn(c.logger).Log("msg", "rpc failed, reconnecting")
			if err := c.Close(); err != nil {
				return nil, err
			}
			return c.seriesWithReconnect(ctx, numReconnects-1, req)
		}
		return nil, errors.Wrap(err, "failed to call Series")
	}

	// Get the stream capability
	stream := resultStruct.Stream()

	// Create a streaming client that wraps the capnp stream
	return &seriesStreamClient{
		ctx:     ctx,
		stream:  stream,
		release: release,
		logger:  c.logger,
	}, nil
}

// seriesStreamClient implements storepb.Store_SeriesClient.
type seriesStreamClient struct {
	ctx     context.Context
	stream  SeriesStream
	release capnp.ReleaseFunc
	logger  log.Logger
	done    bool
}

// Recv implements storepb.Store_SeriesClient.
func (s *seriesStreamClient) Recv() (*storepb.SeriesResponse, error) {
	if s.done {
		return nil, io.EOF
	}

	// Call next() on the stream
	result, release := s.stream.Next(s.ctx, nil)
	defer release()

	resultStruct, err := result.Struct()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get next result")
	}

	// Check if done
	if resultStruct.Done() {
		s.done = true
		return nil, io.EOF
	}

	// Check for errors
	storeErr := resultStruct.Error()
	if storeErr != StoreError_none {
		errCtx, _ := resultStruct.ErrorContext()
		return nil, storeErrorToGRPC(storeErr, errCtx)
	}

	// Parse the response
	if !resultStruct.HasResp() {
		return nil, errors.New("response has no data")
	}

	capnpResp, err := resultStruct.Resp()
	if err != nil {
		return nil, errors.Wrap(err, "get response")
	}

	symbols, err := capnpResp.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}

	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseSeriesResponseFrom(capnpResp, decoder)
}

// Header implements grpc.ClientStream.
func (s *seriesStreamClient) Header() (metadata.MD, error) {
	return nil, nil
}

// Trailer implements grpc.ClientStream.
func (s *seriesStreamClient) Trailer() metadata.MD {
	return nil
}

// CloseSend implements grpc.ClientStream.
func (s *seriesStreamClient) CloseSend() error {
	return nil
}

// Context implements grpc.ClientStream.
func (s *seriesStreamClient) Context() context.Context {
	return s.ctx
}

// SendMsg implements grpc.ClientStream.
func (s *seriesStreamClient) SendMsg(m any) error {
	return nil
}

// RecvMsg implements grpc.ClientStream.
func (s *seriesStreamClient) RecvMsg(m any) error {
	resp, err := s.Recv()
	if err != nil {
		return err
	}
	// Type assert and copy
	if target, ok := m.(*storepb.SeriesResponse); ok {
		*target = *resp
	}
	return nil
}

// LabelNames implements storepb.StoreClient.
func (c *StoreClient) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return c.labelNamesWithReconnect(ctx, 2, req)
}

func (c *StoreClient) labelNamesWithReconnect(ctx context.Context, numReconnects int, req *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	if err := c.connect(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	result, release := c.store.LabelNames(ctx, func(params Store_labelNames_Params) error {
		capnpReq, err := params.NewReq()
		if err != nil {
			return err
		}
		return BuildLabelNamesRequestInto(capnpReq, req)
	})
	defer release()

	resultStruct, err := result.Struct()
	if err != nil {
		if numReconnects > 0 && capnp.IsDisconnected(err) {
			level.Warn(c.logger).Log("msg", "rpc failed, reconnecting")
			if err := c.Close(); err != nil {
				return nil, err
			}
			return c.labelNamesWithReconnect(ctx, numReconnects-1, req)
		}
		return nil, errors.Wrap(err, "failed to call LabelNames")
	}

	// Check for errors
	storeErr := resultStruct.Error()
	if storeErr != StoreError_none {
		errCtx, _ := resultStruct.ErrorContext()
		return nil, storeErrorToGRPC(storeErr, errCtx)
	}

	// Parse response
	capnpResp, err := resultStruct.Resp()
	if err != nil {
		return nil, errors.Wrap(err, "get response")
	}

	symbols, err := capnpResp.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}

	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseLabelNamesResponseFrom(capnpResp, decoder)
}

// LabelValues implements storepb.StoreClient.
func (c *StoreClient) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return c.labelValuesWithReconnect(ctx, 2, req)
}

func (c *StoreClient) labelValuesWithReconnect(ctx context.Context, numReconnects int, req *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	if err := c.connect(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	result, release := c.store.LabelValues(ctx, func(params Store_labelValues_Params) error {
		capnpReq, err := params.NewReq()
		if err != nil {
			return err
		}
		return BuildLabelValuesRequestInto(capnpReq, req)
	})
	defer release()

	resultStruct, err := result.Struct()
	if err != nil {
		if numReconnects > 0 && capnp.IsDisconnected(err) {
			level.Warn(c.logger).Log("msg", "rpc failed, reconnecting")
			if err := c.Close(); err != nil {
				return nil, err
			}
			return c.labelValuesWithReconnect(ctx, numReconnects-1, req)
		}
		return nil, errors.Wrap(err, "failed to call LabelValues")
	}

	// Check for errors
	storeErr := resultStruct.Error()
	if storeErr != StoreError_none {
		errCtx, _ := resultStruct.ErrorContext()
		return nil, storeErrorToGRPC(storeErr, errCtx)
	}

	// Parse response
	capnpResp, err := resultStruct.Resp()
	if err != nil {
		return nil, errors.Wrap(err, "get response")
	}

	symbols, err := capnpResp.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}

	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseLabelValuesResponseFrom(capnpResp, decoder)
}

// FetchInfo calls the Info API to get store metadata.
func (c *StoreClient) FetchInfo(ctx context.Context) (*InfoResult, error) {
	return c.fetchInfoWithReconnect(ctx, 2)
}

// Info implements infopb.InfoClient.
func (c *StoreClient) Info(ctx context.Context, req *infopb.InfoRequest, opts ...grpc.CallOption) (*infopb.InfoResponse, error) {
	result, err := c.FetchInfo(ctx)
	if err != nil {
		return nil, err
	}

	// Convert InfoResult to infopb.InfoResponse
	resp := &infopb.InfoResponse{
		LabelSets: make([]labelpb.ZLabelSet, len(result.LabelSets)),
		Store: &infopb.StoreInfo{
			MinTime:                      result.MinTime,
			MaxTime:                      result.MaxTime,
			SupportsSharding:             result.SupportsSharding,
			SupportsWithoutReplicaLabels: result.SupportsWithoutReplicaLabels,
			TsdbInfos:                    result.TSDBInfos,
		},
	}

	for i, ls := range result.LabelSets {
		resp.LabelSets[i] = labelpb.ZLabelSet{
			Labels: labelpb.ZLabelsFromPromLabels(ls),
		}
	}

	return resp, nil
}

func (c *StoreClient) fetchInfoWithReconnect(ctx context.Context, numReconnects int) (*InfoResult, error) {
	if err := c.connect(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	result, release := c.store.Info(ctx, func(params Store_info_Params) error {
		// InfoRequest is empty, nothing to set
		return nil
	})
	defer release()

	resultStruct, err := result.Struct()
	if err != nil {
		if numReconnects > 0 && capnp.IsDisconnected(err) {
			level.Warn(c.logger).Log("msg", "rpc failed, reconnecting")
			if err := c.Close(); err != nil {
				return nil, err
			}
			return c.fetchInfoWithReconnect(ctx, numReconnects-1)
		}
		return nil, errors.Wrap(err, "failed to call Info")
	}

	// Check for errors
	storeErr := resultStruct.Error()
	if storeErr != StoreError_none {
		errCtx, _ := resultStruct.ErrorContext()
		return nil, storeErrorToGRPC(storeErr, errCtx)
	}

	// Parse response
	capnpResp, err := resultStruct.Resp()
	if err != nil {
		return nil, errors.Wrap(err, "get response")
	}

	symbols, err := capnpResp.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}

	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseInfoResponseFrom(capnpResp, decoder)
}

// connect establishes a connection to the store if not already connected.
func (c *StoreClient) connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	conn, err := c.dialer.Dial()
	if err != nil {
		return errors.Wrap(err, "failed to dial peer")
	}

	c.conn = rpc.NewConn(rpc.NewPackedStreamTransport(conn), nil)
	store := Store(c.conn.Bootstrap(ctx))
	if err := store.Resolve(ctx); err != nil {
		level.Warn(c.logger).Log("msg", "failed to bootstrap capnp store, closing connection", "err", err)
		c.closeUnlocked()
		return errors.Wrap(err, "failed to bootstrap capnp store")
	}

	c.store = store
	return nil
}

// Close closes the connection.
func (c *StoreClient) Close() error {
	c.mu.Lock()
	c.closeUnlocked()
	c.mu.Unlock()
	return nil
}

func (c *StoreClient) closeUnlocked() {
	if c.conn != nil {
		conn := c.conn
		c.conn = nil
		go conn.Close()
	}
}

// Methods implementing the store.Client interface

// LabelSets returns the label sets of the store.
func (c *StoreClient) LabelSets() []labels.Labels {
	return c.labelSets
}

// SetLabelSets sets the label sets of the store.
func (c *StoreClient) SetLabelSets(ls []labels.Labels) {
	c.labelSets = ls
}

// TimeRange returns the time range of the store.
func (c *StoreClient) TimeRange() (int64, int64) {
	return c.minTime, c.maxTime
}

// SetTimeRange sets the time range of the store.
func (c *StoreClient) SetTimeRange(minTime, maxTime int64) {
	c.minTime = minTime
	c.maxTime = maxTime
}

// TSDBInfos returns TSDB info for the store.
func (c *StoreClient) TSDBInfos() []infopb.TSDBInfo {
	return c.tsdbInfos
}

// SetTSDBInfos sets the TSDB infos.
func (c *StoreClient) SetTSDBInfos(infos []infopb.TSDBInfo) {
	c.tsdbInfos = infos
}

// SupportsSharding returns whether the store supports sharding.
func (c *StoreClient) SupportsSharding() bool {
	return c.supportsSharding
}

// SetSupportsSharding sets whether the store supports sharding.
func (c *StoreClient) SetSupportsSharding(supports bool) {
	c.supportsSharding = supports
}

// SupportsWithoutReplicaLabels returns whether the store supports without replica labels.
func (c *StoreClient) SupportsWithoutReplicaLabels() bool {
	return c.supportsWithoutReplicaLabels
}

// SetSupportsWithoutReplicaLabels sets whether the store supports without replica labels.
func (c *StoreClient) SetSupportsWithoutReplicaLabels(supports bool) {
	c.supportsWithoutReplicaLabels = supports
}

// String returns a string representation of the client.
func (c *StoreClient) String() string {
	return c.addr
}

// Addr returns the address of the store.
func (c *StoreClient) Addr() (string, bool) {
	return c.addr, c.isLocal
}

// Matches checks if the store matches the given matchers.
// By default, this returns true. Actual filtering should be done at a higher level.
func (c *StoreClient) Matches(_ []*labels.Matcher) bool {
	return true
}

// storeErrorToGRPC converts a capnp StoreError to a gRPC error.
func storeErrorToGRPC(err StoreError, ctx string) error {
	if ctx == "" {
		ctx = "capnp store error"
	}
	switch err {
	case StoreError_none:
		return nil
	case StoreError_unavailable:
		return status.Error(codes.Unavailable, ctx)
	case StoreError_invalidArgument:
		return status.Error(codes.InvalidArgument, ctx)
	case StoreError_notFound:
		return status.Error(codes.NotFound, ctx)
	case StoreError_internal:
		return status.Error(codes.Internal, ctx)
	case StoreError_resourceExhausted:
		return status.Error(codes.ResourceExhausted, ctx)
	default:
		return status.Error(codes.Unknown, ctx)
	}
}
