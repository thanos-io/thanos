package usagepb

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	EstLabelMatcherSize = 8 + 30 + 30 + 30 + 50 // FastRegexp can be heavy.
)

type Tracker interface {
	MemoryBytesAllocated(b int)
	NetworkBytesRecv(b int)
	NetworkBytesSent(b int)
	GoRoutinesSpawned(b int)
	GoRoutinesFinished(b int)
}

// resourceTracker allows to track basic resource usage.
type resourceTracker struct {
	res Resources

	currentGoRoutines uint64
}

func NewResourceTracker() *resourceTracker {
	return &resourceTracker{}
}

func (r *resourceTracker) MemoryBytesAllocated(b int) {
	atomic.AddUint64(&r.res.MemoryAllocBytes, uint64(b))
}

func (r *resourceTracker) NetworkBytesRecv(b int) {
	atomic.AddUint64(&r.res.NetworkRecvBytes, uint64(b))
}

func (r *resourceTracker) NetworkBytesSent(b int) {
	atomic.AddUint64(&r.res.NetworkSentBytes, uint64(b))
}

func (r *resourceTracker) GoRoutinesSpawned(b int) {
	if curr := atomic.AddUint64(&r.currentGoRoutines, uint64(b)); curr > atomic.LoadUint64(&r.res.GoRoutinesMaxSpawned) {
		// Best effort, races possible.
		atomic.StoreUint64(&r.res.GoRoutinesMaxSpawned, curr)
	}
	atomic.AddUint64(&r.res.GoRoutinesSpawned, uint64(b))
}

func (r *resourceTracker) GoRoutinesFinished(b int) {
	atomic.AddUint64(&r.currentGoRoutines, ^uint64(b-1))
}

func (r *resourceTracker) String() string {
	return fmt.Sprintf("%#v", r.res)
}

func (r *resourceTracker) ToPBMessage() proto.Message {
	return &r.res
}

type spanTracker struct {
	total *resourceTracker
	inner *resourceTracker
}

func (r *spanTracker) MemoryBytesAllocated(b int) {
	r.total.MemoryBytesAllocated(b)
	r.inner.MemoryBytesAllocated(b)
}

func (r *spanTracker) NetworkBytesRecv(b int) {
	r.total.NetworkBytesRecv(b)
	r.inner.NetworkBytesRecv(b)
}

func (r *spanTracker) NetworkBytesSent(b int) {
	r.total.NetworkBytesSent(b)
	r.inner.NetworkBytesSent(b)
}

func (r *spanTracker) GoRoutinesSpawned(b int) {
	r.total.GoRoutinesSpawned(b)
	r.inner.GoRoutinesSpawned(b)
}

func (r *spanTracker) GoRoutinesFinished(b int) {
	r.total.GoRoutinesFinished(b)
	r.inner.GoRoutinesFinished(b)
}

func (m *Resources) ToOpenTracingTags() map[string]uint64 {
	return map[string]uint64{
		"memoryAllocBytes":     m.MemoryAllocBytes,
		"networkSentBytes":     m.NetworkSentBytes,
		"networkRecvBytes":     m.NetworkRecvBytes,
		"goRoutinesSpawned":    m.GoRoutinesSpawned,
		"goRoutinesMaxSpawned": m.GoRoutinesMaxSpawned,
	}
}
func (r *spanTracker) ToOpenTracingTags() map[string]uint64 {
	ret := r.inner.res.ToOpenTracingTags()
	for k, v := range r.total.res.ToOpenTracingTags() {
		ret["total."+k] = v
	}
	return ret
}

// TrackInSpan executes function doFn inside new span with `operationName` name and hooks that span as child to a
// span found within given context if any.
// On top of that it also tracks inner resource usage and reports status on finish as tracing tags.
// It uses opentracing.Tracer propagated in context. If no found, it uses noop tracer notification.
func (r *resourceTracker) TrackInSpan(ctx context.Context, operationName string, doFn func(context.Context, Tracker), opts ...opentracing.StartSpanOption) {
	span, newCtx := tracing.StartSpan(ctx, operationName, opts...)
	s := &spanTracker{total: r, inner: NewResourceTracker()}
	doFn(newCtx, s)
	for k, v := range s.ToOpenTracingTags() {
		span.SetTag(k, v)
	}
	span.Finish()

}
