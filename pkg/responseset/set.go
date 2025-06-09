// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package responseset

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// ResponseSet is a generic abstraction over a set of responses.
// It allows to iterate over responses, close the set and get some metadata about the set.
type ResponseSet[T any] interface {
	Close()
	At() *T
	Next() bool
	StoreID() string
	Labelset() string
	StoreLabels() map[string]struct{}
	Empty() bool
	Error() error
}

type Client[T any] interface {
	Recv() (*T, error)
	CloseSend() error
}

type EagerResponseSet[T any] struct {
	// Generic parameters.
	span opentracing.Span

	cl           Client[T]
	closeSeries  context.CancelFunc
	frameTimeout time.Duration

	// postStreamHook allows performing some manipulations on data
	// after it is fully received.
	postStreamHook func(data []*T)
	// postReceiveHook returns true if data needs to be kept.
	postReceiveHook func(data *T) bool

	storeName      string
	storeLabels    map[string]struct{}
	storeLabelSets []labels.Labels

	// Internal bookkeeping.
	bufferedResponses []*T
	wg                *sync.WaitGroup
	i                 int
	err               error

	closeHook func()
}

func (l *EagerResponseSet[T]) Close() {
	if l.closeSeries != nil {
		l.closeSeries()
	}
	_ = l.cl.CloseSend()
	l.closeHook()
}

func (l *EagerResponseSet[T]) At() *T {
	l.wg.Wait()

	if len(l.bufferedResponses) == 0 {
		return nil
	}

	return l.bufferedResponses[l.i-1]
}

func (l *EagerResponseSet[T]) Next() bool {
	l.wg.Wait()

	l.i++

	return l.i <= len(l.bufferedResponses)
}

func (l *EagerResponseSet[T]) Empty() bool {
	l.wg.Wait()

	return len(l.bufferedResponses) == 0
}

func (l *EagerResponseSet[T]) StoreID() string {
	return l.storeName
}

func (l *EagerResponseSet[T]) Labelset() string {
	return labelpb.PromLabelSetsToString(l.storeLabelSets)
}

func (l *EagerResponseSet[T]) StoreLabels() map[string]struct{} {
	return l.storeLabels
}

func (l *EagerResponseSet[T]) Error() error {
	return l.err
}

func NewEagerResponseSet[T any](
	span opentracing.Span,
	frameTimeout time.Duration,
	storeName string,
	storeLabelSets []labels.Labels,
	closeSeries context.CancelFunc,
	cl Client[T],
	postStreamHook func(data []*T),
	postReceiveHook func(data *T) bool,
	closeHook func(),
) ResponseSet[T] {
	ret := &EagerResponseSet[T]{
		span:              span,
		cl:                cl,
		closeSeries:       closeSeries,
		frameTimeout:      frameTimeout,
		bufferedResponses: []*T{},
		wg:                &sync.WaitGroup{},
		storeName:         storeName,
		storeLabelSets:    storeLabelSets,
		closeHook:         closeHook,
		postStreamHook:    postStreamHook,
		postReceiveHook:   postReceiveHook,
	}
	ret.storeLabels = make(map[string]struct{})
	for _, ls := range storeLabelSets {
		ls.Range(func(l labels.Label) {
			ret.storeLabels[l.Name] = struct{}{}
		})
	}

	ret.wg.Add(1)

	// Start a goroutine and immediately buffer everything.
	go func(l *EagerResponseSet[T]) {
		defer ret.wg.Done()

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

				l.err = rerr
				l.span.SetTag("err", rerr.Error())
				return false
			}

			if !l.postReceiveHook(resp) {
				return true
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

		l.postStreamHook(l.bufferedResponses)
		l.span.Finish()

	}(ret)

	return ret
}

// lazyRespSet is a lazy storepb.SeriesSet that buffers
// everything as fast as possible while at the same it permits
// reading response-by-response. It blocks if there is no data
// in Next().
type lazyRespSet[T any] struct {
	// Generic parameters.
	span           opentracing.Span
	cl             Client[T]
	closeSeries    context.CancelFunc
	storeName      string
	storeLabelSets []labels.Labels
	storeLabels    map[string]struct{}
	frameTimeout   time.Duration

	// Internal bookkeeping.
	dataOrFinishEvent    *sync.Cond
	bufferedResponses    []*T
	bufferedResponsesMtx *sync.Mutex
	lastResp             *T
	err                  error

	noMoreData  bool
	initialized bool

	// postReceiveHook returns true if data needs to be kept.
	postReceiveHook func(data *T) bool

	postStreamHook func(data []*T)
	closeHook      func()
}

func (l *lazyRespSet[T]) Error() error {
	return l.err
}

func NewLazyResponseSet[T any](
	span opentracing.Span,
	frameTimeout time.Duration,
	storeName string,
	storeLabelSets []labels.Labels,
	closeSeries context.CancelFunc,
	cl Client[T],
	postStreamHook func(data []*T),
	postReceiveHook func(data *T) bool,
	closeHook func(),
) ResponseSet[T] {
	bufferedResponses := make([]*T, 0)
	bufferedResponsesMtx := &sync.Mutex{}
	dataAvailable := sync.NewCond(bufferedResponsesMtx)

	respSet := &lazyRespSet[T]{
		frameTimeout:         frameTimeout,
		storeName:            storeName,
		storeLabelSets:       storeLabelSets,
		cl:                   cl,
		closeSeries:          closeSeries,
		span:                 span,
		dataOrFinishEvent:    dataAvailable,
		bufferedResponsesMtx: bufferedResponsesMtx,
		bufferedResponses:    bufferedResponses,
		postReceiveHook:      postReceiveHook,
		postStreamHook:       postStreamHook,
		closeHook:            closeHook,
	}
	respSet.storeLabels = make(map[string]struct{})
	for _, ls := range storeLabelSets {
		ls.Range(func(l labels.Label) {
			respSet.storeLabels[l.Name] = struct{}{}
		})
	}

	go func(st string, l *lazyRespSet[T]) {
		defer l.postStreamHook(nil)

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
				l.err = rerr
				l.noMoreData = true
				l.dataOrFinishEvent.Signal()
				l.bufferedResponsesMtx.Unlock()
				return false
			}

			if !l.postReceiveHook(resp) {
				return true
			}

			l.bufferedResponsesMtx.Lock()
			l.bufferedResponses = append(l.bufferedResponses, resp)
			l.dataOrFinishEvent.Signal()
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

func (l *lazyRespSet[T]) Close() {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	l.closeSeries()
	l.noMoreData = true
	l.dataOrFinishEvent.Signal()

	_ = l.cl.CloseSend()
	l.closeHook()
}

func (l *lazyRespSet[T]) StoreID() string {
	return l.storeName
}

func (l *lazyRespSet[T]) Labelset() string {
	return labelpb.PromLabelSetsToString(l.storeLabelSets)
}

func (l *lazyRespSet[T]) StoreLabels() map[string]struct{} {
	return l.storeLabels
}

func (l *lazyRespSet[T]) Empty() bool {
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
func (l *lazyRespSet[T]) Next() bool {
	l.bufferedResponsesMtx.Lock()
	defer l.bufferedResponsesMtx.Unlock()

	l.initialized = true

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
		if l.initialized {
			l.bufferedResponses = l.bufferedResponses[1:]
		}
		return true
	}

	l.lastResp = nil
	return false
}

func (l *lazyRespSet[T]) At() *T {
	if !l.initialized {
		panic("please call Next before At")
	}

	return l.lastResp
}
