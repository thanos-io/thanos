// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"io"
	"math"
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
	grpc_opentracing "github.com/thanos-io/thanos/pkg/tracing/tracing_middleware"

	"github.com/thanos-io/thanos/pkg/losertree"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type seriesStream interface {
	Next() bool
	At() *storepb.SeriesResponse
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

// NewProxyResponseLoserTree returns heap that k-way merge series together.
// It's agnostic to duplicates and overlaps, it forwards all duplicated series in random order.
func NewProxyResponseLoserTree(seriesSets ...respSet) *losertree.Tree[*storepb.SeriesResponse, respSet] {
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

		if a.GetWarning() != "" && b.GetWarning() != "" {
			return len(a.GetWarning()) < len(b.GetWarning())
		}

		return false
	}

	return losertree.New[*storepb.SeriesResponse, respSet](seriesSets, maxVal, func(s respSet) *storepb.SeriesResponse {
		return s.At()
	}, less, func(s respSet) {
		s.Close()
	})
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

type ringBuffer struct {
	// This event firing means the buffer has a slot for more data.
	bufferSlotEvent *sync.Cond
	fixedBufferSize int
	// This a ring buffer of size fixedBufferSize.
	// A ring buffer of size N can hold N - 1 elements at most in order to distinguish being empty from being full.
	bufferedResponses []*storepb.SeriesResponse
	ringHead          int
	ringTail          int
	closed            bool
}

// NB: A call site of any method of ringBuffer must hold the mtx lock.
func newRingBuffer(fixedBufferSize int, mtx *sync.Mutex) *ringBuffer {
	return &ringBuffer{
		bufferedResponses: make([]*storepb.SeriesResponse, fixedBufferSize+1),
		fixedBufferSize:   fixedBufferSize + 1,
		bufferSlotEvent:   sync.NewCond(mtx),
		ringHead:          0,
		ringTail:          0,
		closed:            false,
	}
}

// Can block until there is a slot for more data or the ring buffer is closed.
func (rb *ringBuffer) append(resp *storepb.SeriesResponse) bool {
	for rb.isFull() && !rb.closed {
		rb.bufferSlotEvent.Wait()
	}
	if !rb.closed {
		rb.bufferedResponses[rb.ringTail] = resp
		rb.ringTail = (rb.ringTail + 1) % rb.fixedBufferSize
		return true
	}
	return false
}

func (rb *ringBuffer) close() {
	rb.closed = true
	rb.bufferSlotEvent.Signal()
}

func (rb *ringBuffer) pop() *storepb.SeriesResponse {
	defer rb.bufferSlotEvent.Signal()

	resp := rb.bufferedResponses[rb.ringHead]
	rb.ringHead = (rb.ringHead + 1) % rb.fixedBufferSize
	return resp
}

func (rb *ringBuffer) isEmpty() bool {
	return rb.ringHead == rb.ringTail
}

func (rb *ringBuffer) isFull() bool {
	return (rb.ringTail+1)%rb.fixedBufferSize == rb.ringHead
}

// lazyRespSet is a lazy storepb.SeriesSet that buffers
// everything as fast as possible while at the same it permits
// reading response-by-response. It blocks if there is no data
// in Next().
// NB: It is not thread-safe, so its metholds must be called from the same goroutine.
type lazyRespSet struct {
	// Generic parameters.
	span           opentracing.Span
	cl             storepb.Store_SeriesClient
	closeSeries    context.CancelFunc
	storeName      string
	storeLabelSets []labels.Labels
	storeLabels    map[string]struct{}
	frameTimeout   time.Duration

	// Internal bookkeeping.
	dataOrFinishEvent *sync.Cond

	// bufferedResponsMtx protects all the following fields.
	bufferedResponsesMtx *sync.Mutex
	rb                   *ringBuffer
	initialized          bool
	noMoreData           bool
	lastResp             *storepb.SeriesResponse

	shardMatcher *storepb.ShardMatcher
}

func (l *lazyRespSet) Empty() bool {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	// NOTE(GiedriusS): need to wait here for at least one
	// response so that we could build the heap properly.
	if l.noMoreData && l.rb.isEmpty() {
		return true
	}

	for l.rb.isEmpty() {
		l.dataOrFinishEvent.Wait()
		if l.noMoreData && l.rb.isEmpty() {
			break
		}
	}

	return l.rb.isEmpty() && l.noMoreData
}

// Next either blocks until more data is available or reads
// the next response. If it is not lazy then it waits for everything
// to finish.
func (l *lazyRespSet) Next() bool {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	l.initialized = true

	if l.noMoreData && l.rb.isEmpty() {
		l.lastResp = nil

		return false
	}

	for l.rb.isEmpty() {
		l.dataOrFinishEvent.Wait()
		if l.noMoreData && l.rb.isEmpty() {
			break
		}
	}

	if !l.rb.isEmpty() {
		l.lastResp = l.rb.pop()
		return true
	}

	l.lastResp = nil
	return false
}

func (l *lazyRespSet) At() *storepb.SeriesResponse {
	// NB: don't need hold l.responsesMtx lock here, because a call site must not call At() and Next() concurrently.
	if !l.initialized {
		panic("please call Next before At")
	}

	return l.lastResp
}

func (l *lazyRespSet) Close() {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	l.closeSeries()
	l.rb.close()
	l.noMoreData = true
	l.dataOrFinishEvent.Signal()

	l.shardMatcher.Close()
	_ = l.cl.CloseSend()
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
	fixedBufferSize int,
) respSet {
	bufferedResponsesMtx := &sync.Mutex{}

	respSet := &lazyRespSet{
		frameTimeout:         frameTimeout,
		storeName:            storeName,
		storeLabelSets:       storeLabelSets,
		cl:                   cl,
		closeSeries:          closeSeries,
		span:                 span,
		dataOrFinishEvent:    sync.NewCond(bufferedResponsesMtx),
		bufferedResponsesMtx: bufferedResponsesMtx,
		rb:                   newRingBuffer(fixedBufferSize, bufferedResponsesMtx),
		initialized:          false,
		noMoreData:           false,
		shardMatcher:         shardMatcher,
	}
	respSet.storeLabels = make(map[string]struct{})
	for _, ls := range storeLabelSets {
		ls.Range(func(l labels.Label) {
			respSet.storeLabels[l.Name] = struct{}{}
		})
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

			resp, err := cl.Recv()
			if err != nil {
				if err == io.EOF {
					l.bufferedResponsesMtx.Lock()
					l.noMoreData = true
					l.dataOrFinishEvent.Signal()
					l.bufferedResponsesMtx.Unlock()
					return false
				}

				var rerr error
				// If timer is already stopped
				if t != nil && !t.Stop() {
					if t.C != nil {
						<-t.C // Drain the channel if it was already stopped.
					}
					rerr = errors.Wrapf(err, "failed to receive any data in %s from %s", l.frameTimeout, st)
				} else {
					rerr = errors.Wrapf(err, "receive series from %s", st)
				}
				l.span.SetTag("err", rerr.Error())

				l.bufferedResponsesMtx.Lock()
				l.rb.append(storepb.NewWarnSeriesResponse(rerr))
				l.noMoreData = true
				l.dataOrFinishEvent.Signal()
				l.bufferedResponsesMtx.Unlock()
				return false
			}
			if t != nil {
				// frameTimeout only applies to cl.Recv() gRPC call because the goroutine may be blocked on waiting for an empty buffer slot.
				// Set the timeout to the largest possible value to avoid triggering it.
				t.Reset(time.Duration(math.MaxInt64))
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
			if l.rb.append(resp) {
				l.dataOrFinishEvent.Signal()
			}
			l.bufferedResponsesMtx.Unlock()
			return true
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
	lazyRetrievalMaxBufferedResponses int,
) (respSet, error) {

	var (
		span   opentracing.Span
		cancel context.CancelFunc
	)

	storeID, storeAddr, isLocalStore := storeInfo(st)
	seriesCtx := grpc_opentracing.ClientAddContextTags(ctx, opentracing.Tags{
		"target": storeAddr,
	})
	span, seriesCtx = tracing.StartSpan(seriesCtx, "proxy.series", tracing.Tags{
		"store.id":           storeID,
		"store.is_local":     isLocalStore,
		"store.addr":         storeAddr,
		"retrieval_strategy": retrievalStrategy,
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
		if lazyRetrievalMaxBufferedResponses < 1 {
			// Some unit and e2e tests hit this path.
			lazyRetrievalMaxBufferedResponses = 1
		}

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
			lazyRetrievalMaxBufferedResponses,
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

// eagerRespSet is a SeriesSet that blocks until all data is retrieved from
// the StoreAPI.
// NOTE(bwplotka): It also resorts the series (and emits warning) if the client.SupportsWithoutReplicaLabels() is false.
type eagerRespSet struct {
	// Generic parameters.
	span opentracing.Span

	cl           storepb.Store_SeriesClient
	closeSeries  context.CancelFunc
	frameTimeout time.Duration

	shardMatcher *storepb.ShardMatcher
	removeLabels map[string]struct{}

	storeName      string
	storeLabels    map[string]struct{}
	storeLabelSets []labels.Labels

	// Internal bookkeeping.
	bufferedResponses []*storepb.SeriesResponse
	wg                *sync.WaitGroup
	i                 int
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
) respSet {
	ret := &eagerRespSet{
		span:              span,
		cl:                cl,
		closeSeries:       closeSeries,
		frameTimeout:      frameTimeout,
		bufferedResponses: []*storepb.SeriesResponse{},
		wg:                &sync.WaitGroup{},
		shardMatcher:      shardMatcher,
		removeLabels:      removeLabels,
		storeName:         storeName,
		storeLabelSets:    storeLabelSets,
	}
	ret.storeLabels = make(map[string]struct{})
	for _, ls := range storeLabelSets {
		ls.Range(func(l labels.Label) {
			ret.storeLabels[l.Name] = struct{}{}
		})
	}

	ret.wg.Add(1)

	// Start a goroutine and immediately buffer everything.
	go func(l *eagerRespSet) {
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

			resp, err := cl.Recv()
			if err != nil {
				if err == io.EOF {
					return false
				}

				var rerr error
				// If timer is already stopped
				if t != nil && !t.Stop() {
					if t.C != nil {
						<-t.C // Drain the channel if it was already stopped.
					}
					rerr = errors.Wrapf(err, "failed to receive any data in %s from %s", l.frameTimeout, storeName)
				} else {
					rerr = errors.Wrapf(err, "receive series from %s", storeName)
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
		// NOTE. Client is not guaranteed to give a sorted response when extLset is added
		// Generally we need to resort here.
		sortWithoutLabels(l.bufferedResponses, l.removeLabels)

	}(ret)

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

func (l *eagerRespSet) Close() {
	if l.closeSeries != nil {
		l.closeSeries()
	}
	l.shardMatcher.Close()
	_ = l.cl.CloseSend()
}

func (l *eagerRespSet) At() *storepb.SeriesResponse {
	l.wg.Wait()

	if len(l.bufferedResponses) == 0 {
		return nil
	}

	return l.bufferedResponses[l.i-1]
}

func (l *eagerRespSet) Next() bool {
	l.wg.Wait()

	l.i++

	return l.i <= len(l.bufferedResponses)
}

func (l *eagerRespSet) Empty() bool {
	l.wg.Wait()

	return len(l.bufferedResponses) == 0
}

func (l *eagerRespSet) StoreID() string {
	return l.storeName
}

func (l *eagerRespSet) Labelset() string {
	return labelpb.PromLabelSetsToString(l.storeLabelSets)
}

func (l *eagerRespSet) StoreLabels() map[string]struct{} {
	return l.storeLabels
}

type respSet interface {
	Close()
	At() *storepb.SeriesResponse
	Next() bool
	StoreID() string
	Labelset() string
	StoreLabels() map[string]struct{}
	Empty() bool
}
