// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/responseset"
	grpc_opentracing "github.com/thanos-io/thanos/pkg/tracing/tracing_middleware"

	"github.com/thanos-io/thanos/pkg/losertree"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type seriesStream interface {
	Next() bool
	At() *storepb.SeriesResponse
	Error() error
}

type responseDeduplicator struct {
	h seriesStream

	bufferedSameSeries []*storepb.SeriesResponse

	bufferedResp []*storepb.SeriesResponse
	buffRespI    int

	prev *storepb.SeriesResponse
	ok   bool

	chunkDedupMap map[uint64]storepb.AggrChunk
}

func (d *responseDeduplicator) Error() error {
	return d.h.Error()
}

// NewResponseDeduplicator returns a wrapper around a loser tree that merges duplicated series messages into one.
// It also deduplicates identical chunks identified by the same checksum from each series message.
func NewResponseDeduplicator(h seriesStream) *responseDeduplicator {
	ok := h.Next()
	var prev *storepb.SeriesResponse
	if ok {
		prev = h.At()
	}
	return &responseDeduplicator{
		h:             h,
		ok:            ok,
		prev:          prev,
		chunkDedupMap: make(map[uint64]storepb.AggrChunk),
	}
}

func (d *responseDeduplicator) Next() bool {
	if d.buffRespI+1 < len(d.bufferedResp) {
		d.buffRespI++
		return true
	}

	if !d.ok && d.prev == nil {
		return false
	}

	d.buffRespI = 0
	d.bufferedResp = d.bufferedResp[:0]
	d.bufferedSameSeries = d.bufferedSameSeries[:0]

	var s *storepb.SeriesResponse
	for {
		if d.prev == nil {
			d.ok = d.h.Next()
			if !d.ok {
				if len(d.bufferedSameSeries) > 0 {
					d.bufferedResp = append(d.bufferedResp, d.chainSeriesAndRemIdenticalChunks(d.bufferedSameSeries))
				}
				return len(d.bufferedResp) > 0
			}
			s = d.h.At()
		} else {
			s = d.prev
			d.prev = nil
		}

		if s.GetSeries() == nil {
			d.bufferedResp = append(d.bufferedResp, s)
			continue
		}

		if len(d.bufferedSameSeries) == 0 {
			d.bufferedSameSeries = append(d.bufferedSameSeries, s)
			continue
		}

		lbls := d.bufferedSameSeries[0].GetSeries().Labels
		atLbls := s.GetSeries().Labels

		if labels.Compare(labelpb.ZLabelsToPromLabels(lbls), labelpb.ZLabelsToPromLabels(atLbls)) == 0 {
			d.bufferedSameSeries = append(d.bufferedSameSeries, s)
			continue
		}

		d.bufferedResp = append(d.bufferedResp, d.chainSeriesAndRemIdenticalChunks(d.bufferedSameSeries))
		d.prev = s

		return true
	}
}

func (d *responseDeduplicator) chainSeriesAndRemIdenticalChunks(series []*storepb.SeriesResponse) *storepb.SeriesResponse {
	clear(d.chunkDedupMap)

	for _, s := range series {
		for _, chk := range s.GetSeries().Chunks {
			for _, field := range []*storepb.Chunk{
				chk.Raw, chk.Count, chk.Max, chk.Min, chk.Sum, chk.Counter,
			} {
				if field == nil {
					continue
				}
				hash := field.Hash
				if hash == 0 {
					hash = xxhash.Sum64(field.Data)
				}

				if _, ok := d.chunkDedupMap[hash]; !ok {
					chk := chk
					d.chunkDedupMap[hash] = chk
					break
				}
			}
		}
	}

	// If no chunks were requested.
	if len(d.chunkDedupMap) == 0 {
		return series[0]
	}

	finalChunks := make([]storepb.AggrChunk, 0, len(d.chunkDedupMap))
	for _, chk := range d.chunkDedupMap {
		finalChunks = append(finalChunks, chk)
	}

	sort.Slice(finalChunks, func(i, j int) bool {
		return finalChunks[i].Compare(finalChunks[j]) > 0
	})

	return storepb.NewSeriesResponse(&storepb.Series{
		Labels: series[0].GetSeries().Labels,
		Chunks: finalChunks,
	})
}

func (d *responseDeduplicator) At() *storepb.SeriesResponse {
	return d.bufferedResp[d.buffRespI]
}

type loserTreeError struct {
	seriesSets []responseset.ResponseSet[storepb.SeriesResponse]
	*losertree.Tree[*storepb.SeriesResponse, responseset.ResponseSet[storepb.SeriesResponse]]
}

func (e *loserTreeError) Error() error {
	for _, s := range e.seriesSets {
		if err := s.Error(); err != nil {
			return err
		}
	}
	return nil
}

// NewProxyResponseLoserTree returns heap that k-way merge series together.
// It's agnostic to duplicates and overlaps, it forwards all duplicated series in random order.
func NewProxyResponseLoserTree(seriesSets ...responseset.ResponseSet[storepb.SeriesResponse]) *loserTreeError {
	var maxVal *storepb.SeriesResponse = storepb.NewSeriesResponse(nil)

	less := func(a, b *storepb.SeriesResponse) bool {
		if a == maxVal && b != maxVal {
			return false
		}
		if a != maxVal && b == maxVal {
			return true
		}
		if a == maxVal && b == maxVal {
			return true
		}
		if a.GetSeries() != nil && b.GetSeries() != nil {
			iLbls := labelpb.ZLabelsToPromLabels(a.GetSeries().Labels)
			jLbls := labelpb.ZLabelsToPromLabels(b.GetSeries().Labels)

			return labels.Compare(iLbls, jLbls) < 0
		} else if a.GetSeries() == nil && b.GetSeries() != nil {
			return true
		} else if a.GetSeries() != nil && b.GetSeries() == nil {
			return false
		}
		return false
	}

	return &loserTreeError{
		seriesSets: seriesSets,
		Tree: losertree.New(seriesSets, maxVal, func(s responseset.ResponseSet[storepb.SeriesResponse]) *storepb.SeriesResponse {
			return s.At()
		}, less, func(s responseset.ResponseSet[storepb.SeriesResponse]) {
			s.Close()
		}),
	}
}

func newLazyRespSet(
	span opentracing.Span,
	frameTimeout time.Duration,
	storeName string,
	storeLabelSets []labels.Labels,
	closeSeries context.CancelFunc,
	cl storepb.Store_SeriesClient,
	shardMatcher *storepb.ShardMatcher,
	applySharding bool,
	emptyStreamResponses prometheus.Counter,
) responseset.ResponseSet[storepb.SeriesResponse] {
	seriesStats := &storepb.SeriesStatsCounter{}
	numResponses := 0
	bytesProcessed := 0

	ret := responseset.NewLazyResponseSet(
		span,
		frameTimeout,
		storeName,
		storeLabelSets,
		closeSeries,
		cl,
		func(_ []*storepb.SeriesResponse) {
			span.SetTag("processed.series", seriesStats.Series)
			span.SetTag("processed.chunks", seriesStats.Chunks)
			span.SetTag("processed.samples", seriesStats.Samples)
			span.SetTag("processed.bytes", bytesProcessed)
			if numResponses == 0 {
				emptyStreamResponses.Inc()
			}
		},
		func(data *storepb.SeriesResponse) bool {
			numResponses++
			bytesProcessed += data.Size()
			if data.GetSeries() != nil && applySharding && !shardMatcher.MatchesZLabels(data.GetSeries().Labels) {
				return false
			}
			if data.GetSeries() != nil {
				seriesStats.Count(data.GetSeries())
			}
			return true
		},
		func() {
			shardMatcher.Close()
		},
	)

	return ret
}

// RetrievalStrategy stores what kind of retrieval strategy
// shall be used for the async response set.
type RetrievalStrategy string

const (
	// LazyRetrieval allows readers (e.g. PromQL engine) to use (stream) data as soon as possible.
	LazyRetrieval RetrievalStrategy = "lazy"
	// EagerRetrieval is optimized to read all into internal buffer before returning to readers (e.g. PromQL engine).
	// This currently preferred because:
	// * Both PromQL engines (old and new) want all series ASAP to make decisions.
	// * Querier buffers all responses when using StoreAPI internally.
	EagerRetrieval RetrievalStrategy = "eager"
)

func newAsyncRespSet(
	ctx context.Context,
	st Client,
	req *storepb.SeriesRequest,
	frameTimeout time.Duration,
	retrievalStrategy RetrievalStrategy,
	buffers *sync.Pool,
	shardInfo *storepb.ShardInfo,
	logger log.Logger,
	emptyStreamResponses prometheus.Counter,
) (responseset.ResponseSet[storepb.SeriesResponse], error) {

	var (
		span   opentracing.Span
		cancel context.CancelFunc
	)

	storeID, storeAddr, isLocalStore := storeInfo(st)
	seriesCtx := grpc_opentracing.ClientAddContextTags(ctx, opentracing.Tags{
		"target": storeAddr,
	})
	span, seriesCtx = tracing.StartSpan(seriesCtx, "proxy.series", tracing.Tags{
		"store.id":       storeID,
		"store.is_local": isLocalStore,
		"store.addr":     storeAddr,
	})

	seriesCtx, cancel = context.WithCancel(seriesCtx)

	shardMatcher := shardInfo.Matcher(buffers)

	applySharding := shardInfo != nil && !st.SupportsSharding()
	if applySharding {
		level.Debug(logger).Log("msg", "Applying series sharding in the proxy since there is not support in the underlying store", "store", st.String())
	}

	cl, err := st.Series(seriesCtx, req)
	if err != nil {
		err = errors.Wrapf(err, "fetch series for %s %s", storeID, st)

		span.SetTag("err", err.Error())
		span.Finish()
		cancel()
		return nil, err
	}

	var labelsToRemove map[string]struct{}
	if !st.SupportsWithoutReplicaLabels() && len(req.WithoutReplicaLabels) > 0 {
		level.Warn(logger).Log("msg", "detecting store that does not support without replica label setting. "+
			"Falling back to eager retrieval with additional sort. Make sure your storeAPI supports it to speed up your queries", "store", st.String())
		retrievalStrategy = EagerRetrieval

		labelsToRemove = make(map[string]struct{})
		for _, replicaLabel := range req.WithoutReplicaLabels {
			labelsToRemove[replicaLabel] = struct{}{}
		}
	}

	switch retrievalStrategy {
	case LazyRetrieval:
		return newLazyRespSet(
			span,
			frameTimeout,
			st.String(),
			st.LabelSets(),
			cancel,
			cl,
			shardMatcher,
			applySharding,
			emptyStreamResponses,
		), nil
	case EagerRetrieval:
		return newEagerRespSet(
			span,
			frameTimeout,
			st.String(),
			st.LabelSets(),
			cancel,
			cl,
			shardMatcher,
			applySharding,
			emptyStreamResponses,
			labelsToRemove,
		), nil
	default:
		panic(fmt.Sprintf("unsupported retrieval strategy %s", retrievalStrategy))
	}
}

func newEagerRespSet(
	span opentracing.Span,
	frameTimeout time.Duration,
	storeName string,
	storeLabelSets []labels.Labels,
	closeSeries context.CancelFunc,
	cl storepb.Store_SeriesClient,
	shardMatcher *storepb.ShardMatcher,
	applySharding bool,
	emptyStreamResponses prometheus.Counter,
	removeLabels map[string]struct{},
) responseset.ResponseSet[storepb.SeriesResponse] {
	seriesStats := &storepb.SeriesStatsCounter{}
	numResponses := 0
	bytesProcessed := 0

	ret := responseset.NewEagerResponseSet(
		span,
		frameTimeout,
		storeName,
		storeLabelSets,
		closeSeries,
		cl,
		func(data []*storepb.SeriesResponse) {
			span.SetTag("processed.series", seriesStats.Series)
			span.SetTag("processed.chunks", seriesStats.Chunks)
			span.SetTag("processed.samples", seriesStats.Samples)
			span.SetTag("processed.bytes", bytesProcessed)
			if numResponses == 0 {
				emptyStreamResponses.Inc()
			}
			sortWithoutLabels(data, removeLabels)
		},
		func(data *storepb.SeriesResponse) bool {
			numResponses++
			bytesProcessed += data.Size()
			if data.GetSeries() != nil && applySharding && !shardMatcher.MatchesZLabels(data.GetSeries().Labels) {
				return false
			}
			if data.GetSeries() != nil {
				seriesStats.Count(data.GetSeries())
			}
			return true
		},
		func() {
			shardMatcher.Close()
		},
	)

	return ret
}

func rmLabels(l labels.Labels, labelsToRemove map[string]struct{}) labels.Labels {
	b := labels.NewBuilder(l)
	for k := range labelsToRemove {
		b.Del(k)
	}
	return b.Labels()
}

// sortWithoutLabels removes given labels from series and re-sorts the series responses that the same
// series with different labels are coming right after each other. Other types of responses are moved to front.
func sortWithoutLabels(set []*storepb.SeriesResponse, labelsToRemove map[string]struct{}) {
	for _, s := range set {
		ser := s.GetSeries()
		if ser == nil {
			continue
		}

		if len(labelsToRemove) > 0 {
			ser.Labels = labelpb.ZLabelsFromPromLabels(rmLabels(labelpb.ZLabelsToPromLabels(ser.Labels), labelsToRemove))
		}
	}

	// With the re-ordered label sets, re-sorting all series aligns the same series
	// from different replicas sequentially.
	sort.Slice(set, func(i, j int) bool {
		si := set[i].GetSeries()
		if si == nil {
			return true
		}
		sj := set[j].GetSeries()
		if sj == nil {
			return false
		}
		return labels.Compare(labelpb.ZLabelsToPromLabels(si.Labels), labelpb.ZLabelsToPromLabels(sj.Labels)) < 0
	})
}
