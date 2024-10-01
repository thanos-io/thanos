// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"hash"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/filter"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	RemoteReadFrameLimit      = 1048576
	cuckooStoreFilterCapacity = 1000000
	storeFilterUpdateInterval = 15 * time.Second
)

type TSDBReader interface {
	storage.ChunkQueryable
	StartTime() (int64, error)
}

// TSDBStoreOption is a functional option for TSDBStore.
type TSDBStoreOption func(s *TSDBStore)

// WithCuckooMetricNameStoreFilter returns a TSDBStoreOption that enables the Cuckoo filter for metric names.
func WithCuckooMetricNameStoreFilter() TSDBStoreOption {
	return func(s *TSDBStore) {
		s.storeFilter = filter.NewCuckooMetricNameStoreFilter(cuckooStoreFilterCapacity)
		s.startStoreFilterUpdate = true
	}
}

// TSDBStore implements the store API against a local TSDB instance.
// It attaches the provided external labels to all results. It only responds with raw data
// and does not support downsampling.
type TSDBStore struct {
	logger           log.Logger
	db               TSDBReader
	component        component.StoreAPI
	buffers          sync.Pool
	maxBytesPerFrame int

	extLset                labels.Labels
	startStoreFilterUpdate bool
	storeFilter            filter.StoreFilter
	mtx                    sync.RWMutex
	close                  func()
	storepb.UnimplementedStoreServer
}

func (s *TSDBStore) Close() {
	s.close()
}

func RegisterWritableStoreServer(storeSrv storepb.WriteableStoreServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		storepb.RegisterWriteableStoreServer(s, storeSrv)
	}
}

// ReadWriteTSDBStore is a TSDBStore that can also be written to.
type ReadWriteTSDBStore struct {
	storepb.StoreServer
	storepb.WriteableStoreServer
}

// NewTSDBStore creates a new TSDBStore.
// NOTE: Given lset has to be sorted.
func NewTSDBStore(
	logger log.Logger,
	db TSDBReader,
	component component.StoreAPI,
	extLset labels.Labels,
	options ...TSDBStoreOption,
) *TSDBStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	st := &TSDBStore{
		logger:           logger,
		db:               db,
		component:        component,
		extLset:          extLset,
		maxBytesPerFrame: RemoteReadFrameLimit,
		storeFilter:      filter.AllowAllStoreFilter{},
		close:            func() {},
		buffers: sync.Pool{New: func() interface{} {
			b := make([]byte, 0, initialBufSize)
			return &b
		}},
	}

	for _, option := range options {
		option(st)
	}

	if st.startStoreFilterUpdate {
		ctx, cancel := context.WithCancel(context.Background())

		updateFilter := func(ctx context.Context) {
			vals, err := st.LabelValues(ctx, &storepb.LabelValuesRequest{
				Label: model.MetricNameLabel,
				End:   math.MaxInt64,
			})
			if err != nil {
				level.Error(logger).Log("msg", "failed to update metric names", "err", err)
				return
			}

			st.storeFilter.ResetAndSet(vals.Values...)
		}
		st.close = cancel
		updateFilter(ctx)

		t := time.NewTicker(storeFilterUpdateInterval)

		go func() {
			for {
				select {
				case <-t.C:
					updateFilter(ctx)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return st
}

func (s *TSDBStore) SetExtLset(extLset labels.Labels) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.extLset = extLset
}

func (s *TSDBStore) getExtLset() labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.extLset
}

func (s *TSDBStore) LabelSet() []labelpb.ZLabelSet {
	labels := labelpb.ZLabelSetsFromPromLabels(s.getExtLset())
	labelSets := []labelpb.ZLabelSet{}
	if len(labels) > 0 {
		labelSets = append(labelSets, labels...)
	}

	return labelSets
}

func (p *TSDBStore) TSDBInfos() []infopb.TSDBInfo {
	labels := p.LabelSet()
	if len(labels) == 0 {
		return []infopb.TSDBInfo{}
	}

	mint, maxt := p.TimeRange()
	return []infopb.TSDBInfo{
		{
			Labels: labelpb.ZLabelSet{
				Labels: labels[0].Labels,
			},
			MinTime: mint,
			MaxTime: maxt,
		},
	}
}

func (s *TSDBStore) TimeRange() (int64, int64) {
	var minTime int64 = math.MinInt64
	startTime, err := s.db.StartTime()
	if err == nil {
		// Since we always use tsdb.DB  implementation,
		// StartTime should never return error.
		minTime = startTime
	}

	return minTime, math.MaxInt64
}

func (s *TSDBStore) Matches(matchers []*labels.Matcher) bool {
	return s.storeFilter.Matches(matchers)
}

// CloseDelegator allows to delegate close (releasing resources used by request to the server).
// This is useful when we invoke StoreAPI within another StoreAPI and results are ephemeral until copied.
type CloseDelegator interface {
	Delegate(io.Closer)
}

type noopUpstream struct {
	ctx context.Context
	storepb.Store_SeriesServer
}

func (n *noopUpstream) Context() context.Context {
	return n.ctx
}

func (s *TSDBStore) SeriesLocal(ctx context.Context, r *storepb.SeriesRequest) ([]*storepb.Series, error) {
	srv := newFlushableServer(&noopUpstream{ctx: ctx}, sortingStrategyStoreSendNoop)
	if err := s.Series(r, srv); err != nil {
		return nil, err
	}

	rs := srv.(*resortingServer)

	return rs.series, nil
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (s *TSDBStore) Series(r *storepb.SeriesRequest, seriesSrv storepb.Store_SeriesServer) error {
	var srv flushableServer
	if fs, ok := seriesSrv.(flushableServer); !ok {
		srv = newFlushableServer(seriesSrv, sortingStrategyStore)
	} else {
		srv = fs
	}

	match, matchers, err := matchesExternalLabels(r.Matchers, s.getExtLset())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if !match {
		return nil
	}

	if len(matchers) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding external labels)").Error())
	}

	q, err := s.db.ChunkQuerier(r.MinTime, r.MaxTime)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	if cd, ok := srv.(CloseDelegator); ok {
		cd.Delegate(q)
	} else {
		defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb chunk querier series")
	}

	hints := &storage.SelectHints{
		Start: r.MinTime,
		End:   r.MaxTime,
		Limit: int(r.Limit),
	}
	set := q.Select(srv.Context(), true, hints, matchers...)

	shardMatcher := r.ShardInfo.Matcher(&s.buffers)
	defer shardMatcher.Close()
	hasher := hashPool.Get().(hash.Hash64)
	defer hashPool.Put(hasher)

	extLsetToRemove := map[string]struct{}{}
	for _, lbl := range r.WithoutReplicaLabels {
		extLsetToRemove[lbl] = struct{}{}
	}
	finalExtLset := rmLabels(s.extLset.Copy(), extLsetToRemove)

	// Stream at most one series per frame; series may be split over multiple frames according to maxBytesInFrame.
	for set.Next() {
		series := set.At()

		completeLabelset := labelpb.ExtendSortedLabels(rmLabels(series.Labels(), extLsetToRemove), finalExtLset)
		if !shardMatcher.MatchesLabels(completeLabelset) {
			continue
		}

		storeSeries := storepb.Series{Labels: labelpb.ZLabelsFromPromLabels(completeLabelset)}
		if r.SkipChunks {
			if err := srv.Send(storepb.NewSeriesResponse(&storeSeries)); err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
			continue
		}

		bytesLeftForChunks := s.maxBytesPerFrame
		for _, lbl := range storeSeries.Labels {
			bytesLeftForChunks -= lbl.Size()
		}
		frameBytesLeft := bytesLeftForChunks

		seriesChunks := []storepb.AggrChunk{}
		chIter := series.Iterator(nil)
		isNext := chIter.Next()
		for isNext {
			chk := chIter.At()
			if chk.Chunk == nil {
				return status.Errorf(codes.Internal, "TSDBStore: found not populated chunk returned by SeriesSet at ref: %v", chk.Ref)
			}

			chunkBytes := make([]byte, len(chk.Chunk.Bytes()))
			copy(chunkBytes, chk.Chunk.Bytes())
			c := storepb.AggrChunk{
				MinTime: chk.MinTime,
				MaxTime: chk.MaxTime,
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_Encoding(chk.Chunk.Encoding() - 1), // Proto chunk encoding is one off to TSDB one.
					Data: chunkBytes,
					Hash: hashChunk(hasher, chunkBytes, enableChunkHashCalculation),
				},
			}
			frameBytesLeft -= c.Size()
			seriesChunks = append(seriesChunks, c)

			// We are fine with minor inaccuracy of max bytes per frame. The inaccuracy will be max of full chunk size.
			isNext = chIter.Next()
			if frameBytesLeft > 0 && isNext {
				continue
			}
			if err := srv.Send(storepb.NewSeriesResponse(&storepb.Series{Labels: storeSeries.Labels, Chunks: seriesChunks})); err != nil {
				return status.Error(codes.Aborted, err.Error())
			}

			if isNext {
				frameBytesLeft = bytesLeftForChunks
				seriesChunks = make([]storepb.AggrChunk, 0, len(seriesChunks))
			}
		}
		if err := chIter.Err(); err != nil {
			return status.Error(codes.Internal, errors.Wrap(err, "chunk iter").Error())
		}

	}
	if err := set.Err(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	for _, w := range set.Warnings() {
		if err := srv.Send(storepb.NewWarnSeriesResponse(w)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return srv.Flush()
}

// LabelNames returns all known label names constrained with the given matchers.
func (s *TSDBStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	match, matchers, err := matchesExternalLabels(r.Matchers, s.getExtLset())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if !match {
		return &storepb.LabelNamesResponse{Names: nil}, nil
	}

	q, err := s.db.ChunkQuerier(r.Start, r.End)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb querier label names")

	hints := &storage.LabelHints{
		Limit: int(r.Limit),
	}
	res, _, err := q.LabelNames(ctx, hints, matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	extLsetToRemove := map[string]struct{}{}
	for _, lbl := range r.WithoutReplicaLabels {
		extLsetToRemove[lbl] = struct{}{}
	}

	if len(res) > 0 {
		s.getExtLset().Range(func(l labels.Label) {
			if _, ok := extLsetToRemove[l.Name]; !ok {
				res = append(res, l.Name)
			}
		})
		sort.Strings(res)
	}

	// Label values can come from a postings table of a memory-mapped block which can be deleted during
	// head compaction. Since we close the block querier before we return from the function,
	// we need to copy label values to make sure the client still has access to the data when
	// a block is deleted.
	values := make([]string, len(res))
	for i := range res {
		values[i] = strings.Clone(res[i])
	}

	return &storepb.LabelNamesResponse{Names: values}, nil
}

// LabelValues returns all known label values for a given label name.
func (s *TSDBStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	if r.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}

	for i := range r.WithoutReplicaLabels {
		if r.Label == r.WithoutReplicaLabels[i] {
			return &storepb.LabelValuesResponse{}, nil
		}
	}

	match, matchers, err := matchesExternalLabels(r.Matchers, s.getExtLset())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelValuesResponse{}, nil
	}

	q, err := s.db.ChunkQuerier(r.Start, r.End)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer runutil.CloseWithLogOnErr(s.logger, q, "close tsdb querier label values")

	// If we request label values for an external label while selecting an additional matcher for other label values
	if val := s.getExtLset().Get(r.Label); val != "" {
		if len(matchers) == 0 {
			return &storepb.LabelValuesResponse{Values: []string{val}}, nil
		}

		hints := &storage.SelectHints{
			Start: r.Start,
			End:   r.End,
			Func:  "series",
			Limit: int(r.Limit),
		}
		set := q.Select(ctx, false, hints, matchers...)

		for set.Next() {
			return &storepb.LabelValuesResponse{Values: []string{val}}, nil
		}
		return &storepb.LabelValuesResponse{}, nil
	}

	hints := &storage.LabelHints{
		Limit: int(r.Limit),
	}
	res, _, err := q.LabelValues(ctx, r.Label, hints, matchers...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Label values can come from a postings table of a memory-mapped block which can be deleted during
	// head compaction. Since we close the block querier before we return from the function,
	// we need to copy label values to make sure the client still has access to the data when
	// a block is deleted.
	values := make([]string, len(res))
	for i := range res {
		values[i] = strings.Clone(res[i])
	}

	return &storepb.LabelValuesResponse{Values: values}, nil
}
