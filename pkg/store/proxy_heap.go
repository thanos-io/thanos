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

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
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

type dedupResponseHeap struct {
	h *ProxyResponseHeap

	bufferedSameSeries []*storepb.SeriesResponse

	bufferedResp []*storepb.SeriesResponse
	buffRespI    int

	prev *storepb.SeriesResponse
	ok   bool
}

// NewDedupResponseHeap returns a wrapper around ProxyResponseHeap that merged duplicated series messages into one.
// It also deduplicates identical chunks identified by the same checksum from each series message.
func NewDedupResponseHeap(h *ProxyResponseHeap) *dedupResponseHeap {
	ok := h.Next()
	var prev *storepb.SeriesResponse
	if ok {
		prev = h.At()
	}
	return &dedupResponseHeap{
		h:    h,
		ok:   ok,
		prev: prev,
	}
}

func (d *dedupResponseHeap) Next() bool {
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
					d.bufferedResp = append(d.bufferedResp, chainSeriesAndRemIdenticalChunks(d.bufferedSameSeries))
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

		d.bufferedResp = append(d.bufferedResp, chainSeriesAndRemIdenticalChunks(d.bufferedSameSeries))
		d.prev = s

		return true
	}
}

func chainSeriesAndRemIdenticalChunks(series []*storepb.SeriesResponse) *storepb.SeriesResponse {
	chunkDedupMap := map[uint64]*storepb.AggrChunk{}

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

				if _, ok := chunkDedupMap[hash]; !ok {
					chk := chk
					chunkDedupMap[hash] = &chk
					break
				}
			}
		}
	}

	// If no chunks were requested.
	if len(chunkDedupMap) == 0 {
		return series[0]
	}

	finalChunks := make([]storepb.AggrChunk, 0, len(chunkDedupMap))
	for _, chk := range chunkDedupMap {
		finalChunks = append(finalChunks, *chk)
	}

	sort.Slice(finalChunks, func(i, j int) bool {
		return finalChunks[i].Compare(finalChunks[j]) > 0
	})

	return storepb.NewSeriesResponse(&storepb.Series{
		Labels: series[0].GetSeries().Labels,
		Chunks: finalChunks,
	})
}

func (d *dedupResponseHeap) At() *storepb.SeriesResponse {
	return d.bufferedResp[d.buffRespI]
}

// ProxyResponseHeap is a heap for storepb.SeriesSets.
// It performs k-way merge between all of those sets.
// TODO(GiedriusS): can be improved with a tournament tree.
// This is O(n*logk) but can be Theta(n*logk). However,
// tournament trees need n-1 auxiliary nodes so there
// might not be much of a difference.
type ProxyResponseHeap struct {
	nodes []*ProxyResponseHeapNode

	replicaLabels    []string
	replicaLabelsMap map[string]struct{}
	baseToNodes      map[uint64]*ProxyResponseHeapNode
	responseSets     []respSet

	cur                            *storepb.SeriesResponse
	curBaseLabels, curSeriesLabels labels.Labels

	hashBuf []byte
}

func first[E any](s []E) (E, bool) {
	if len(s) == 0 {
		var zero E
		return zero, false
	}
	return s[0], true
}

func (h *ProxyResponseHeap) Less(i, j int) bool {
	iResp, oki := first(h.nodes[i].responses)
	jResp, okj := first(h.nodes[j].responses)

	if oki && !okj {
		return true
	}
	if !oki && okj {
		return false
	}

	if !oki && !okj {
		return false
	}

	if iResp.sr.GetSeries() != nil && jResp.sr.GetSeries() != nil {
		c := labels.Compare(iResp.sr.GetSeries().PromLabels(), jResp.sr.GetSeries().PromLabels())
		if c != 0 {
			return c < 0
		}

		return labels.Compare(h.nodes[i].baseLabels, h.nodes[j].baseLabels) < 0
	} else if iResp.sr.GetSeries() == nil && jResp.sr.GetSeries() != nil {
		return false
	} else if iResp.sr.GetSeries() != nil && jResp.sr.GetSeries() == nil {
		return true
	}

	// If it is not a series then the order does not matter. What matters
	// is that we get different types of responses one after another.
	return false
}

func (h *ProxyResponseHeap) Len() int {
	return len(h.nodes)
}

func (h *ProxyResponseHeap) Swap(i, j int) {
	h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i]
	h.nodes[j].pos = j
	h.nodes[i].pos = i
}

func (h *ProxyResponseHeap) Push(x interface{}) {
	h.nodes = append(h.nodes, x.(*ProxyResponseHeapNode))
}

func (h *ProxyResponseHeap) Pop() (v interface{}) {
	h.nodes, v = h.nodes[:h.Len()-1], h.nodes[h.Len()-1]
	return
}

func (h *ProxyResponseHeap) Empty() bool {
	return h.Len() == 0
}

func (h *ProxyResponseHeap) Min() *ProxyResponseHeapNode {
	if h.Len() == 0 {
		return nil
	}
	return h.nodes[0]
}

type responseRespSetContainer struct {
	sr                       *storepb.SeriesResponse
	rs                       respSet
	baseLabels, seriesLabels labels.Labels
	pos                      int
}

type responseRespSetHeap []responseRespSetContainer

func (h *responseRespSetHeap) Len() int { return len(*h) }

func (h *responseRespSetHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	(*h)[i].pos = i
	(*h)[j].pos = j
}

func (h *responseRespSetHeap) Less(i, j int) bool {
	oki := !((*h)[i].sr == nil || (*h)[i].sr.GetSeries() == nil)
	okj := !((*h)[j].sr == nil || (*h)[j].sr.GetSeries() == nil)
	if oki && !okj {
		return false
	}
	if !oki && okj {
		return true
	}

	if !oki && !okj {
		return false
	}
	return labels.Compare((*h)[i].sr.GetSeries().PromLabels(), (*h)[j].sr.GetSeries().PromLabels()) < 0
}

func (h *responseRespSetHeap) Push(x interface{}) {
	// a failed type assertion here will panic
	*h = append(*h, x.(responseRespSetContainer))
}

func (h *responseRespSetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type ProxyResponseHeapNode struct {
	responses  responseRespSetHeap
	baseLabels labels.Labels
	pos        int
}

func (h *ProxyResponseHeap) getBaseLabels(replicaLabels []string, lbls labels.Labels) (uint64, labels.Labels) {
	h.hashBuf = h.hashBuf[:0]

	for _, rl := range replicaLabels {
		if lbls.Get(rl) == "" {
			continue
		}
		h.hashBuf = append(h.hashBuf, []byte(lbls.Get(rl))...)
		h.hashBuf = append(h.hashBuf, 0xff)
	}

	hash := xxhash.Sum64(h.hashBuf)
	for bh, node := range h.baseToNodes {
		if hash != bh {
			continue
		}
		return bh, node.baseLabels
	}

	l := labels.Labels{}

	for _, rl := range replicaLabels {
		if lbls.Get(rl) == "" {
			continue
		}
		l = append(l, labels.Label{Name: rl, Value: lbls.Get(rl)})
	}

	return hash, l
}

func (h *ProxyResponseHeap) addSeriesResponse(ss respSet) (bool, labels.Labels) {
	var seriesLabels labels.Labels

	sr := ss.At()
	if sr == nil {
		return false, seriesLabels
	}
	s := sr.GetSeries()
	if s == nil {
		// If not series then the order doesn't matter. Put it in the 0 bucket.
		if h.baseToNodes[0] == nil {
			prh := &ProxyResponseHeapNode{
				responses: []responseRespSetContainer{
					{rs: ss, sr: sr},
				},
			}
			h.baseToNodes[0] = prh
			heap.Push(h, prh)
		} else {
			heap.Push(&h.baseToNodes[0].responses, responseRespSetContainer{
				sr: sr, rs: ss,
			})
		}
		return false, seriesLabels
	}

	bh, baseLabels := h.getBaseLabels(h.replicaLabels, s.PromLabels())

	s.Labels = labelpb.ZLabelsFromPromLabels(rmLabels(s.PromLabels(), h.replicaLabelsMap))
	seriesLabels = s.PromLabels()
	if b, ok := h.baseToNodes[bh]; ok {
		b.responses = append(b.responses, responseRespSetContainer{
			rs: ss, sr: sr, baseLabels: baseLabels, seriesLabels: seriesLabels,
		})
		heap.Fix(&b.responses, len(b.responses)-1)
		// The b.responses had been empty before. Perhaps our position has changed?
		heap.Fix(h, b.responses[0].pos)
	} else {
		prh := &ProxyResponseHeapNode{
			responses:  []responseRespSetContainer{{rs: ss, sr: sr, baseLabels: baseLabels, seriesLabels: seriesLabels}},
			baseLabels: baseLabels,
		}
		h.baseToNodes[bh] = prh
		heap.Push(h, prh)
	}

	return true, seriesLabels
}

// NewProxyResponseHeap returns heap that k-way merge series together.
// It's agnostic to duplicates and overlaps, it forwards all duplicated series in random order.
func NewProxyResponseHeap(replicaLabels []string, responseSets ...respSet) *ProxyResponseHeap {
	ret := ProxyResponseHeap{
		nodes:         make([]*ProxyResponseHeapNode, 0, len(responseSets)),
		replicaLabels: replicaLabels,
		responseSets:  responseSets,
		baseToNodes:   make(map[uint64]*ProxyResponseHeapNode),
	}

	rls := map[string]struct{}{}
	for _, rl := range replicaLabels {
		rls[rl] = struct{}{}
	}
	ret.replicaLabelsMap = rls

	for _, ss := range responseSets {
		if ss.Empty() {
			continue
		}

		ret.addSeriesResponse(ss)
	}

	heap.Init(&ret)

	return &ret
}

// Next returns true if there are more responses.
func (h *ProxyResponseHeap) Next() bool {
	/*
		if h.stickyRs != nil {
			if h.stickyRs.Next() {

			} else {
				h.stickyRs = nil
			}
		}
	*/
	min := h.Min()
	if min == nil {
		return false
	}
	if len(min.responses) == 0 {
		return false
	}

	var popped responseRespSetContainer
	min.responses.Swap(0, min.responses.Len()-1)
	popped = min.responses[len(min.responses)-1]
	min.responses = min.responses[:len(min.responses)-1]
	heap.Fix(&min.responses, 0)

	h.cur = popped.sr
	h.curBaseLabels = popped.baseLabels
	h.curSeriesLabels = popped.seriesLabels

	// Read from the same popped until series labels are the same or smaller.
	// We need this to avoid the situation like this with replica label "a":
	// a=1,b=1
	// a=1,b=2
	// a=2,b=1
	// a=2,b=2
	// TODO(GiedriusS): we can probably do better somehow to avoid needlessly enlarging slice.
	added := false
	for {
		if !popped.rs.Next() {
			break
		}

		added = true
		gotSeries, seriesLabels := h.addSeriesResponse(popped.rs)
		if gotSeries && labels.Compare(seriesLabels, h.curSeriesLabels) <= 0 {
			break
		}
	}

	if !added {
		heap.Init(h)
	}

	return true
}

func (h *ProxyResponseHeap) At() *storepb.SeriesResponse {
	return h.cur
}

func (l *lazyRespSet) StoreID() string {
	return l.storeName
}

func (l *lazyRespSet) Labelset() string {
	return labelpb.PromLabelSetsToString(l.storeLabelSets)
}

func (l *lazyRespSet) StoreLabels() map[string]struct{} {
	return l.storeLabels
}

// lazyRespSet is a lazy storepb.SeriesSet that buffers
// everything as fast as possible while at the same it permits
// reading response-by-response. It blocks if there is no data
// in Next().
type lazyRespSet struct {
	// Generic parameters.
	span           opentracing.Span
	cl             storepb.Store_SeriesClient
	closeSeries    context.CancelFunc
	storeName      string
	storeLabelSets []labels.Labels
	storeLabels    map[string]struct{}
	frameTimeout   time.Duration
	ctx            context.Context

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
	storeName string,
	storeLabelSets []labels.Labels,
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
		storeName:            storeName,
		storeLabelSets:       storeLabelSets,
		closeSeries:          closeSeries,
		span:                 span,
		ctx:                  ctx,
		dataOrFinishEvent:    dataAvailable,
		bufferedResponsesMtx: bufferedResponsesMtx,
		bufferedResponses:    bufferedResponses,
		shardMatcher:         shardMatcher,
	}
	respSet.storeLabels = make(map[string]struct{})
	for _, ls := range storeLabelSets {
		for _, l := range ls {
			respSet.storeLabels[l.Name] = struct{}{}
		}
	}

	go func(st string, l *lazyRespSet) {
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
				err := errors.Wrapf(l.ctx.Err(), "failed to receive any data from %s", st)
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
					// TODO(bwplotka): Return early on error. Don't wait of dedup, merge and sort if partial response is disabled.
					var rerr error
					if t != nil && !t.Stop() && errors.Is(err, context.Canceled) {
						// Most likely the per-Recv timeout has been reached.
						// There's a small race between canceling and the Recv()
						// but this is most likely true.
						rerr = errors.Wrapf(err, "failed to receive any data in %s from %s", l.frameTimeout, st)
					} else {
						rerr = errors.Wrapf(err, "receive series from %s", st)
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
	}(storeName, respSet)

	return respSet
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
) (respSet, error) {

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

	applySharding := shardInfo != nil && !st.SupportsSharding()
	if applySharding {
		level.Debug(logger).Log("msg", "Applying series sharding in the proxy since there is not support in the underlying store", "store", st.String())
	}

	cl, err := st.Series(seriesCtx, req)
	if err != nil {
		err = errors.Wrapf(err, "fetch series for %s %s", storeID, st)

		span.SetTag("err", err.Error())
		span.Finish()
		closeSeries()
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
			seriesCtx,
			span,
			frameTimeout,
			st.String(),
			st.LabelSets(),
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
			labelsToRemove,
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
// NOTE(bwplotka): It also resorts the series (and emits warning) if the client.SupportsWithoutReplicaLabels() is false.
type eagerRespSet struct {
	// Generic parameters.
	span opentracing.Span
	cl   storepb.Store_SeriesClient
	ctx  context.Context

	closeSeries  context.CancelFunc
	st           Client
	frameTimeout time.Duration

	shardMatcher *storepb.ShardMatcher
	removeLabels map[string]struct{}

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
	removeLabels map[string]struct{},
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
		removeLabels:      removeLabels,
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

		// TODO(bwplotka): Consider improving readability by getting rid of anonymous functions and merging eager and
		// lazyResponse into one struct.
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
					// TODO(bwplotka): Return early on error. Don't wait of dedup, merge and sort if partial response is disabled.
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
				break
			}
		}

		// This should be used only for stores that does not support doing this on server side.
		// See docs/proposals-accepted/20221129-avoid-global-sort.md for details.
		if len(l.removeLabels) > 0 {
			sortWithoutLabels(l.bufferedResponses, l.removeLabels)
		}

	}(st, ret)

	return ret
}

func rmLabels(l labels.Labels, labelsToRemove map[string]struct{}) labels.Labels {
	for i := 0; i < len(l); i++ {
		if _, ok := labelsToRemove[l[i].Name]; !ok {
			continue
		}
		l = append(l[:i], l[i+1:]...)
		i--
	}
	return l
}

// sortWithoutLabels removes given labels from series and re-sorts the series responses that the same
// series with different labels are coming right after each other. Other types of responses are moved to front.
func sortWithoutLabels(set []*storepb.SeriesResponse, labelsToRemove map[string]struct{}) {
	for _, s := range set {
		ser := s.GetSeries()
		if ser == nil {
			continue
		}

		ser.Labels = labelpb.ZLabelsFromPromLabels(rmLabels(labelpb.ZLabelsToPromLabels(ser.Labels), labelsToRemove))
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
