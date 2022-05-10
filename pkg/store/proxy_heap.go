// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"container/heap"
	"context"
	"crypto/md5"
	"io"
	"sort"
	"sync"
	"time"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

// dedupResponseHeap is a wrapper around ProxyResponseHeap
// that deduplicates identical chunks identified by the same labelset.
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

func (d *dedupResponseHeap) Err() error {
	return d.h.Error()
}

func (d *dedupResponseHeap) At() *storepb.SeriesResponse {
	defer func() {
		d.responses = d.responses[:0]
	}()
	if len(d.responses) == 0 {
		return nil
	} else if len(d.responses) == 1 {
		return d.responses[0]
	}

	chunkDedupMap := map[string]*storepb.AggrChunk{}

	md5Hash := md5.New()

	for _, resp := range d.responses {
		for _, chk := range resp.GetSeries().Chunks {
			h := chk.Hash(md5Hash)

			if _, ok := chunkDedupMap[h]; !ok {
				chk := chk

				chunkDedupMap[h] = &chk
			}
		}
	}

	// If no chunks were requested.
	if len(chunkDedupMap) == 0 {
		return storepb.NewSeriesResponse(&storepb.Series{
			Labels: d.responses[0].GetSeries().Labels,
			Chunks: d.responses[0].GetSeries().Chunks,
		})
	}

	finalChunks := make([]storepb.AggrChunk, 0, len(chunkDedupMap))
	for _, chk := range chunkDedupMap {
		finalChunks = append(finalChunks, *chk)
	}

	sort.Slice(finalChunks, func(i, j int) bool {
		return finalChunks[i].Compare(finalChunks[j]) > 0
	})

	lbls := d.responses[0].GetSeries().Labels

	return storepb.NewSeriesResponse(&storepb.Series{
		Labels: lbls,
		Chunks: finalChunks,
	})
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
	rs *lazyRespSet
}

func NewProxyResponseHeap(seriesSets ...*lazyRespSet) *ProxyResponseHeap {
	ret := make(ProxyResponseHeap, 0, len(seriesSets))

	for _, ss := range seriesSets {
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

func (h *ProxyResponseHeap) Error() error {
	if len(*h) == 0 || h.Min() == nil {
		return nil
	}
	return h.Min().rs.Err()
}

// lazyRespSet is a lazy storepb.SeriesSet that buffers
// everything as fast as possible while at the same it permits
// reading response-by-response. It blocks if there is no data
// in Next().
type lazyRespSet struct {
	st Client
	cl storepb.Store_SeriesClient

	closeSeries  context.CancelFunc
	span         opentracing.Span
	ctx          context.Context
	frameTimeout time.Duration

	dataOrEndEvent       *sync.Cond
	bufferedResponses    []*storepb.SeriesResponse
	bufferedResponsesMtx *sync.Mutex
	readThrough          int
	lastResp             *storepb.SeriesResponse

	errs       errutil.MultiError
	errsMtx    sync.Mutex
	noMoreData bool
}

func (l *lazyRespSet) Err() error {
	l.errsMtx.Lock()
	defer l.errsMtx.Unlock()
	return l.errs.Err()
}

// Next either blocks until more data is available or reads
// the next response.
func (l *lazyRespSet) Next() bool {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	if l.noMoreData && len(l.bufferedResponses) == 0 {
		l.lastResp = nil

		return false
	}

	for len(l.bufferedResponses) == 0 {
		l.dataOrEndEvent.Wait()
		if l.noMoreData && len(l.bufferedResponses) == 0 {
			break
		}
	}

	if len(l.bufferedResponses) > 0 {
		l.lastResp = l.bufferedResponses[0]
		l.bufferedResponses = l.bufferedResponses[1:]
		l.readThrough++
		return true
	}

	l.lastResp = nil
	return false
}

func (l *lazyRespSet) At() *storepb.SeriesResponse {
	return l.lastResp
}

func newLazyRespSet(ctx context.Context, st Client, req *storepb.SeriesRequest, frameTimeout time.Duration) *lazyRespSet {
	storeID := labelpb.PromLabelSetsToString(st.LabelSets())
	if storeID == "" {
		storeID = "Store Gateway"
	}

	seriesCtx, closeSeries := context.WithCancel(ctx)
	seriesCtx = grpc_opentracing.ClientAddContextTags(seriesCtx, opentracing.Tags{
		"target": st.Addr(),
	})

	span, seriesCtx := tracing.StartSpan(seriesCtx, "proxy.series", tracing.Tags{
		"store.id":   storeID,
		"store.addr": st.Addr(),
	})

	errs := errutil.MultiError{}
	cl, err := st.Series(seriesCtx, req)
	if err != nil {
		errs.Add(err)
		span.SetTag("err", err.Error())
		span.Finish()
	}

	bufferedResponses := []*storepb.SeriesResponse{}
	bufferedResponsesMtx := &sync.Mutex{}
	dataAvailable := sync.NewCond(bufferedResponsesMtx)

	respSet := &lazyRespSet{
		frameTimeout:         frameTimeout,
		cl:                   cl,
		st:                   st,
		closeSeries:          closeSeries,
		span:                 span,
		ctx:                  seriesCtx,
		errs:                 errs,
		dataOrEndEvent:       dataAvailable,
		bufferedResponsesMtx: bufferedResponsesMtx,
		bufferedResponses:    bufferedResponses,
	}

	if errs.Err() != nil {
		respSet.noMoreData = true
		return respSet
	}

	// Start a goroutine and immediately buffer everything.
	go func(st Client, l *lazyRespSet) {
		handleRecvResponse := func() bool {
			frameTimeoutCtx, cancel := frameCtx(l.ctx, frameTimeout)
			defer cancel()

			select {
			case <-l.ctx.Done():
				err := errors.Wrapf(l.ctx.Err(), "failed to receive any data from %s", st.String())
				l.errsMtx.Lock()
				l.errs.Add(err)
				l.errsMtx.Unlock()
				l.span.SetTag("err", err.Error())
				l.span.Finish()

				l.bufferedResponsesMtx.Lock()
				l.noMoreData = true
				l.dataOrEndEvent.Signal()
				l.bufferedResponsesMtx.Unlock()
				return false
			case <-frameTimeoutCtx.Done():
				err := errors.Wrapf(frameTimeoutCtx.Err(), "failed to receive any data in %v from %s", frameTimeout, st.String())
				l.errsMtx.Lock()
				l.errs.Add(err)
				l.errsMtx.Unlock()
				l.span.SetTag("err", err.Error())
				l.span.Finish()

				l.bufferedResponsesMtx.Lock()
				l.noMoreData = true
				l.dataOrEndEvent.Signal()
				l.bufferedResponsesMtx.Unlock()
				return false
			default:
				resp, err := cl.Recv()
				if err == io.EOF {
					l.span.Finish()

					l.bufferedResponsesMtx.Lock()
					l.noMoreData = true
					l.dataOrEndEvent.Signal()
					l.bufferedResponsesMtx.Unlock()
					return false
				}

				if err != nil {
					err := errors.Wrapf(err, "receive series from %s", st.String())
					l.errsMtx.Lock()
					l.errs.Add(err)
					l.errsMtx.Unlock()

					l.span.SetTag("err", err.Error())
					l.span.Finish()

					l.bufferedResponsesMtx.Lock()
					l.noMoreData = true
					l.dataOrEndEvent.Signal()
					l.bufferedResponsesMtx.Unlock()
					return false
				}

				l.bufferedResponsesMtx.Lock()
				l.bufferedResponses = append(l.bufferedResponses, resp)
				l.dataOrEndEvent.Signal()
				l.bufferedResponsesMtx.Unlock()
				return true
			}
		}
		for {
			if !handleRecvResponse() {
				return
			}
		}
	}(st, respSet)

	return respSet
}

func (l *lazyRespSet) Close() {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	l.closeSeries()
	l.noMoreData = true
	l.dataOrEndEvent.Signal()
}
