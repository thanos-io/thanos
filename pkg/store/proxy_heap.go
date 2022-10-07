// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log/level"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// dedupResponseHeap is a wrapper around ProxyResponseHeap
// that removes duplicated identical chunks identified by the same labelset and checksum.
// It uses a hashing function to do that.
type dedupResponseHeap struct {
	h *ProxyResponseHeap

	responses []*storepb.SeriesResponse

	previousResponse *storepb.SeriesResponse
	previousNext     bool
}

func NewDedupResponseHeap(h *ProxyResponseHeap) *dedupResponseHeap {
	return &dedupResponseHeap{
		h:            h,
		previousNext: h.Next(),
	}
}

func (d *dedupResponseHeap) Next() bool {
	d.responses = d.responses[:0]

	// If there is something buffered that is *not* a series.
	if d.previousResponse != nil && d.previousResponse.GetSeries() == nil {
		d.responses = append(d.responses, d.previousResponse)
		d.previousResponse = nil
		d.previousNext = d.h.Next()
		return len(d.responses) > 0 || d.previousNext
	}

	var resp *storepb.SeriesResponse
	var nextHeap bool

	// If buffered then use it.
	if d.previousResponse != nil {
		resp = d.previousResponse
		d.previousResponse = nil
	} else {
		// If not buffered then check whether there is anything.
		nextHeap = d.h.Next()
		if !nextHeap {
			return false
		}
		resp = d.h.At()
	}

	// Append buffered or retrieved response.
	d.responses = append(d.responses, resp)

	// Update previousNext.
	defer func(next *bool) {
		d.previousNext = *next
	}(&nextHeap)

	if resp.GetSeries() == nil {
		return len(d.responses) > 0 || d.previousNext
	}

	for {
		nextHeap = d.h.Next()
		if !nextHeap {
			break
		}
		resp = d.h.At()
		if resp.GetSeries() == nil {
			d.previousResponse = resp
			break
		}

		lbls := resp.GetSeries().Labels
		lastLbls := d.responses[len(d.responses)-1].GetSeries().Labels

		if labels.Compare(labelpb.ZLabelsToPromLabels(lbls), labelpb.ZLabelsToPromLabels(lastLbls)) == 0 {
			d.responses = append(d.responses, resp)
		} else {
			// This one is different. It will be taken care of via the next Next() call.
			d.previousResponse = resp
			break
		}
	}

	return len(d.responses) > 0 || d.previousNext
}

func (d *dedupResponseHeap) At() *storepb.SeriesResponse {
	if len(d.responses) == 0 {
		panic("BUG: At() called with no responses; please call At() only if Next() returns true")
	} else if len(d.responses) == 1 {
		return d.responses[0]
	}

	chunkDedupMap := map[uint64]*storepb.AggrChunk{}

	for _, resp := range d.responses {
		if resp.GetSeries() == nil {
			continue
		}
		for _, chk := range resp.GetSeries().Chunks {
			for _, field := range []*storepb.Chunk{
				chk.Raw, chk.Count, chk.Max, chk.Min, chk.Sum, chk.Counter,
			} {
				if field == nil {
					continue
				}
				h := xxhash.Sum64(field.Data)

				if _, ok := chunkDedupMap[h]; !ok {
					chk := chk

					chunkDedupMap[h] = &chk
				}
			}

		}
	}

	// If no chunks were requested.
	if len(chunkDedupMap) == 0 {
		return d.responses[0]
	}

	finalChunks := make([]storepb.AggrChunk, 0, len(chunkDedupMap))
	for _, chk := range chunkDedupMap {
		finalChunks = append(finalChunks, *chk)
	}

	sort.Slice(finalChunks, func(i, j int) bool {
		return finalChunks[i].Compare(finalChunks[j]) > 0
	})

	// Guaranteed to be a series because Next() only buffers one
	// warning at a time that gets handled in the beginning.
	lbls := d.responses[0].GetSeries().Labels

	return storepb.NewSeriesResponse(&storepb.Series{
		Labels: lbls,
		Chunks: finalChunks,
	})
}

// ProxyResponseHeap is a heap for storepb.SeriesSets.
// It performs k-way merge between all of those sets.
// TODO(GiedriusS): can be improved with a tournament tree.
// This is O(n*logk) but can be Theta(n*logk). However,
// tournament trees need n-1 auxiliary nodes so there
// might not be much of a difference.
type ProxyResponseHeap []ProxyResponseHeapNode

func (h *ProxyResponseHeap) Less(i, j int) bool {
	iResp := (*h)[i].rs.At()
	jResp := (*h)[j].rs.At()

	if iResp.GetSeries() != nil && jResp.GetSeries() != nil {
		iLbls := labelpb.ZLabelsToPromLabels(iResp.GetSeries().Labels)
		jLbls := labelpb.ZLabelsToPromLabels(jResp.GetSeries().Labels)
		return labels.Compare(iLbls, jLbls) < 0
	} else if iResp.GetSeries() == nil && jResp.GetSeries() != nil {
		return true
	} else if iResp.GetSeries() != nil && jResp.GetSeries() == nil {
		return false
	}

	// If it is not a series then the order does not matter. What matters
	// is that we get different types of responses one after another.
	return false
}

func (h *ProxyResponseHeap) Len() int {
	return len(*h)
}

func (h *ProxyResponseHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *ProxyResponseHeap) Push(x interface{}) {
	*h = append(*h, x.(ProxyResponseHeapNode))
}

func (h *ProxyResponseHeap) Pop() (v interface{}) {
	*h, v = (*h)[:h.Len()-1], (*h)[h.Len()-1]
	return
}

func (h *ProxyResponseHeap) Empty() bool {
	return h.Len() == 0
}

func (h *ProxyResponseHeap) Min() *ProxyResponseHeapNode {
	return &(*h)[0]
}

type ProxyResponseHeapNode struct {
	rs respSet
}

func NewProxyResponseHeap(seriesSets ...respSet) *ProxyResponseHeap {
	ret := make(ProxyResponseHeap, 0, len(seriesSets))

	for _, ss := range seriesSets {
		if ss.Empty() {
			continue
		}
		ss := ss
		ret.Push(ProxyResponseHeapNode{rs: ss})
	}

	heap.Init(&ret)

	return &ret
}

func (h *ProxyResponseHeap) Next() bool {
	return !h.Empty()
}

func (h *ProxyResponseHeap) At() *storepb.SeriesResponse {
	min := h.Min().rs

	atResp := min.At()

	if min.Next() {
		heap.Fix(h, 0)
	} else {
		heap.Remove(h, 0)
	}

	return atResp
}

func (l *lazyRespSet) StoreID() string {
	return l.st.String()
}

func (l *lazyRespSet) Labelset() string {
	return labelpb.PromLabelSetsToString(l.st.LabelSets())
}

// lazyRespSet is a lazy storepb.SeriesSet that buffers
// everything as fast as possible while at the same it permits
// reading response-by-response. It blocks if there is no data
// in Next().
type lazyRespSet struct {
	// Generic parameters.
	span         opentracing.Span
	cl           storepb.Store_SeriesClient
	closeSeries  context.CancelFunc
	st           Client
	frameTimeout time.Duration
	ctx          context.Context

	// Internal bookkeeping.
	dataOrFinishEvent    *sync.Cond
	bufferedResponses    []*storepb.SeriesResponse
	bufferedResponsesMtx *sync.Mutex
	lastResp             *storepb.SeriesResponse

	noMoreData  bool
	initialized bool

	shardMatcher *storepb.ShardMatcher
}

func (l *lazyRespSet) Empty() bool {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	// NOTE(GiedriusS): need to wait here for at least one
	// response so that we could build the heap properly.
	if l.noMoreData && len(l.bufferedResponses) == 0 {
		return true
	}

	for len(l.bufferedResponses) == 0 {
		l.dataOrFinishEvent.Wait()
		if l.noMoreData && len(l.bufferedResponses) == 0 {
			break
		}
	}

	return len(l.bufferedResponses) == 0 && l.noMoreData
}

// Next either blocks until more data is available or reads
// the next response. If it is not lazy then it waits for everything
// to finish.
func (l *lazyRespSet) Next() bool {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	if l.noMoreData && len(l.bufferedResponses) == 0 {
		l.lastResp = nil

		return false
	}

	for len(l.bufferedResponses) == 0 {
		l.dataOrFinishEvent.Wait()
		if l.noMoreData && len(l.bufferedResponses) == 0 {
			break
		}
	}

	if len(l.bufferedResponses) > 0 {
		l.lastResp = l.bufferedResponses[0]
		l.bufferedResponses = l.bufferedResponses[1:]
		return true
	}

	l.lastResp = nil
	return false
}

func (l *lazyRespSet) At() *storepb.SeriesResponse {
	// We need to wait for at least one response so that we would be able to properly build the heap.
	if !l.initialized {
		l.Next()
		l.initialized = true
		return l.lastResp
	}

	// Next() was called previously.
	return l.lastResp
}

func newLazyRespSet(
	ctx context.Context,
	span opentracing.Span,
	frameTimeout time.Duration,
	st Client,
	closeSeries context.CancelFunc,
	cl storepb.Store_SeriesClient,
	shardMatcher *storepb.ShardMatcher,
	applySharding bool,
	emptyStreamResponses prometheus.Counter,

) respSet {
	bufferedResponses := []*storepb.SeriesResponse{}
	bufferedResponsesMtx := &sync.Mutex{}
	dataAvailable := sync.NewCond(bufferedResponsesMtx)

	respSet := &lazyRespSet{
		frameTimeout:         frameTimeout,
		cl:                   cl,
		st:                   st,
		closeSeries:          closeSeries,
		span:                 span,
		ctx:                  ctx,
		dataOrFinishEvent:    dataAvailable,
		bufferedResponsesMtx: bufferedResponsesMtx,
		bufferedResponses:    bufferedResponses,
		shardMatcher:         shardMatcher,
	}

	go func(st Client, l *lazyRespSet) {
		bytesProcessed := 0
		seriesStats := &storepb.SeriesStatsCounter{}

		defer func() {
			l.span.SetTag("processed.series", seriesStats.Series)
			l.span.SetTag("processed.chunks", seriesStats.Chunks)
			l.span.SetTag("processed.samples", seriesStats.Samples)
			l.span.SetTag("processed.bytes", bytesProcessed)
			l.span.Finish()
		}()

		numResponses := 0
		defer func() {
			if numResponses == 0 {
				emptyStreamResponses.Inc()
			}
		}()

		handleRecvResponse := func(t *time.Timer) bool {
			if t != nil {
				defer t.Reset(frameTimeout)
			}

			select {
			case <-l.ctx.Done():
				err := errors.Wrapf(l.ctx.Err(), "failed to receive any data from %s", st.String())
				l.span.SetTag("err", err.Error())

				l.bufferedResponsesMtx.Lock()
				l.bufferedResponses = append(l.bufferedResponses, storepb.NewWarnSeriesResponse(err))
				l.noMoreData = true
				l.dataOrFinishEvent.Signal()
				l.bufferedResponsesMtx.Unlock()
				return false
			default:
				resp, err := cl.Recv()
				if err == io.EOF {
					l.bufferedResponsesMtx.Lock()
					l.noMoreData = true
					l.dataOrFinishEvent.Signal()
					l.bufferedResponsesMtx.Unlock()
					return false
				}

				if err != nil {
					var rerr error
					if t != nil && !t.Stop() && errors.Is(err, context.Canceled) {
						// Most likely the per-Recv timeout has been reached.
						// There's a small race between canceling and the Recv()
						// but this is most likely true.
						rerr = errors.Wrapf(err, "failed to receive any data in %s from %s", l.frameTimeout, st.String())
					} else {
						rerr = errors.Wrapf(err, "receive series from %s", st.String())
					}

					l.span.SetTag("err", rerr.Error())

					l.bufferedResponsesMtx.Lock()
					l.bufferedResponses = append(l.bufferedResponses, storepb.NewWarnSeriesResponse(rerr))
					l.noMoreData = true
					l.dataOrFinishEvent.Signal()
					l.bufferedResponsesMtx.Unlock()
					return false
				}

				numResponses++
				bytesProcessed += resp.Size()

				if resp.GetSeries() != nil && applySharding && !shardMatcher.MatchesZLabels(resp.GetSeries().Labels) {
					return true
				}

				if resp.GetSeries() != nil {
					seriesStats.Count(resp.GetSeries())
				}

				l.bufferedResponsesMtx.Lock()
				l.bufferedResponses = append(l.bufferedResponses, resp)
				l.dataOrFinishEvent.Signal()
				l.bufferedResponsesMtx.Unlock()
				return true
			}
		}

		var t *time.Timer
		if frameTimeout > 0 {
			t = time.AfterFunc(frameTimeout, closeSeries)
			defer t.Stop()
		}
		for {
			if !handleRecvResponse(t) {
				return
			}
		}
	}(st, respSet)

	return respSet
}

// RetrievalStrategy stores what kind of retrieval strategy
// shall be used for the async response set.
type RetrievalStrategy string

const (
	LazyRetrieval RetrievalStrategy = "lazy"

	// TODO(GiedriusS): remove eager retrieval once
	// https://github.com/prometheus/prometheus/blob/ce6a643ee88fba7c02fbd0459c4d0ac498f512dd/promql/engine.go#L877-L902
	// is removed.
	EagerRetrieval RetrievalStrategy = "eager"
)

func newAsyncRespSet(ctx context.Context,
	st Client,
	req *storepb.SeriesRequest,
	frameTimeout time.Duration,
	retrievalStrategy RetrievalStrategy,
	storeSupportsSharding bool,
	buffers *sync.Pool,
	shardInfo *storepb.ShardInfo,
	logger log.Logger,
	emptyStreamResponses prometheus.Counter) (respSet, error) {

	var span opentracing.Span
	var closeSeries context.CancelFunc

	storeAddr, isLocalStore := st.Addr()
	storeID := labelpb.PromLabelSetsToString(st.LabelSets())
	if storeID == "" {
		storeID = "Store Gateway"
	}

	seriesCtx := grpc_opentracing.ClientAddContextTags(ctx, opentracing.Tags{
		"target": storeAddr,
	})

	span, seriesCtx = tracing.StartSpan(seriesCtx, "proxy.series", tracing.Tags{
		"store.id":       storeID,
		"store.is_local": isLocalStore,
		"store.addr":     storeAddr,
	})

	seriesCtx, closeSeries = context.WithCancel(seriesCtx)

	shardMatcher := shardInfo.Matcher(buffers)

	applySharding := shardInfo != nil && !storeSupportsSharding
	if applySharding {
		msg := "Applying series sharding in the proxy since there is not support in the underlying store"
		level.Debug(logger).Log("msg", msg, "store", st.String())
	}

	cl, err := st.Series(seriesCtx, req)
	if err != nil {
		err = errors.Wrapf(err, "fetch series for %s %s", storeID, st)

		span.SetTag("err", err.Error())
		span.Finish()
		closeSeries()
		return nil, err
	}

	switch retrievalStrategy {
	case LazyRetrieval:
		return newLazyRespSet(
			seriesCtx,
			span,
			frameTimeout,
			st,
			closeSeries,
			cl,
			shardMatcher,
			applySharding,
			emptyStreamResponses,
		), nil
	case EagerRetrieval:
		return newEagerRespSet(
			seriesCtx,
			span,
			frameTimeout,
			st,
			closeSeries,
			cl,
			shardMatcher,
			applySharding,
			emptyStreamResponses,
		), nil
	default:
		panic(fmt.Sprintf("unsupported retrieval strategy %s", retrievalStrategy))
	}
}

func (l *lazyRespSet) Close() {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	l.closeSeries()
	l.noMoreData = true
	l.dataOrFinishEvent.Signal()

	l.shardMatcher.Close()
}

// eagerRespSet is a SeriesSet that blocks until all data is retrieved from
// the StoreAPI.
type eagerRespSet struct {
	// Generic parameters.
	span opentracing.Span
	cl   storepb.Store_SeriesClient
	ctx  context.Context

	closeSeries  context.CancelFunc
	st           Client
	frameTimeout time.Duration

	shardMatcher *storepb.ShardMatcher

	// Internal bookkeeping.
	bufferedResponses []*storepb.SeriesResponse
	wg                *sync.WaitGroup
	i                 int
}

func newEagerRespSet(
	ctx context.Context,
	span opentracing.Span,
	frameTimeout time.Duration,
	st Client,
	closeSeries context.CancelFunc,
	cl storepb.Store_SeriesClient,
	shardMatcher *storepb.ShardMatcher,
	applySharding bool,
	emptyStreamResponses prometheus.Counter,
) respSet {
	ret := &eagerRespSet{
		span:              span,
		st:                st,
		closeSeries:       closeSeries,
		cl:                cl,
		frameTimeout:      frameTimeout,
		ctx:               ctx,
		bufferedResponses: []*storepb.SeriesResponse{},
		wg:                &sync.WaitGroup{},
		shardMatcher:      shardMatcher,
	}

	ret.wg.Add(1)

	// Start a goroutine and immediately buffer everything.
	go func(st Client, l *eagerRespSet) {
		seriesStats := &storepb.SeriesStatsCounter{}
		bytesProcessed := 0

		defer func() {
			l.span.SetTag("processed.series", seriesStats.Series)
			l.span.SetTag("processed.chunks", seriesStats.Chunks)
			l.span.SetTag("processed.samples", seriesStats.Samples)
			l.span.SetTag("processed.bytes", bytesProcessed)
			l.span.Finish()
			ret.wg.Done()
		}()

		numResponses := 0
		defer func() {
			if numResponses == 0 {
				emptyStreamResponses.Inc()
			}
		}()

		handleRecvResponse := func(t *time.Timer) bool {
			if t != nil {
				defer t.Reset(frameTimeout)
			}

			select {
			case <-l.ctx.Done():
				err := errors.Wrapf(l.ctx.Err(), "failed to receive any data from %s", st.String())
				l.bufferedResponses = append(l.bufferedResponses, storepb.NewWarnSeriesResponse(err))
				l.span.SetTag("err", err.Error())
				return false
			default:
				resp, err := cl.Recv()
				if err == io.EOF {
					return false
				}
				if err != nil {
					var rerr error
					if t != nil && !t.Stop() && errors.Is(err, context.Canceled) {
						// Most likely the per-Recv timeout has been reached.
						// There's a small race between canceling and the Recv()
						// but this is most likely true.
						rerr = errors.Wrapf(err, "failed to receive any data in %s from %s", l.frameTimeout, st.String())
					} else {
						rerr = errors.Wrapf(err, "receive series from %s", st.String())
					}
					l.bufferedResponses = append(l.bufferedResponses, storepb.NewWarnSeriesResponse(rerr))
					l.span.SetTag("err", rerr.Error())
					return false
				}

				numResponses++
				bytesProcessed += resp.Size()

				if resp.GetSeries() != nil && applySharding && !shardMatcher.MatchesZLabels(resp.GetSeries().Labels) {
					return true
				}

				if resp.GetSeries() != nil {
					seriesStats.Count(resp.GetSeries())
				}

				l.bufferedResponses = append(l.bufferedResponses, resp)
				return true
			}
		}
		var t *time.Timer
		if frameTimeout > 0 {
			t = time.AfterFunc(frameTimeout, closeSeries)
			defer t.Stop()
		}

		for {
			if !handleRecvResponse(t) {
				return
			}
		}
	}(st, ret)

	return ret
}

func (l *eagerRespSet) Close() {
	l.shardMatcher.Close()
}

func (l *eagerRespSet) At() *storepb.SeriesResponse {
	l.wg.Wait()

	if len(l.bufferedResponses) == 0 {
		return nil
	}

	return l.bufferedResponses[l.i]
}

func (l *eagerRespSet) Next() bool {
	l.wg.Wait()

	l.i++

	return l.i < len(l.bufferedResponses)
}

func (l *eagerRespSet) Empty() bool {
	l.wg.Wait()

	return len(l.bufferedResponses) == 0
}

func (l *eagerRespSet) StoreID() string {
	return l.st.String()
}

func (l *eagerRespSet) Labelset() string {
	return labelpb.PromLabelSetsToString(l.st.LabelSets())
}

type respSet interface {
	Close()
	At() *storepb.SeriesResponse
	Next() bool
	StoreID() string
	Labelset() string
	Empty() bool
}
