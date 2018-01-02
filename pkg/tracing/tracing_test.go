// This file includes unit tests that test only tiny logic in this package, but are here mainly as a showcase on how tracing can
// be configured.

package tracing

import (
	"testing"

	"context"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/opentracing/basictracer-go"
)

// This test shows that if sample factor will enable tracing on client process, even when it would be disabled on server
// it will be still enabled for all spans within this request.
func TestContextTracing_ClientEnablesTracing(t *testing.T) {
	r := &basictracer.InMemorySpanRecorder{}

	clientTracer := &tracer{
		debugName: "Test",
		wrapped: basictracer.NewWithOptions(basictracer.Options{
			ShouldSample: func(traceID uint64) bool {
				return true
			},
			Recorder:       r,
			MaxLogsPerSpan: 100,
		}),
	}

	clientRoot, clientCtx := StartSpan(ContextWithTracer(context.Background(), clientTracer), "a")

	// Simulate Server process with different tracer, but with client span in context.
	srvTracer := &tracer{
		debugName: "Test",
		wrapped: basictracer.NewWithOptions(basictracer.Options{
			ShouldSample: func(traceID uint64) bool {
				return false
			},
			Recorder:       r,
			MaxLogsPerSpan: 100,
		}),
	}
	srvRoot, srvCtx := StartSpan(ContextWithTracer(clientCtx, srvTracer), "b")
	srvChild, _ := StartSpan(srvCtx, "bb")
	testutil.Equals(t, 0, len(r.GetSpans()))

	srvChild.Finish()
	testutil.Equals(t, 1, len(r.GetSpans()))
	testutil.Equals(t, 1, len(r.GetSampledSpans()))

	srvRoot.Finish()
	testutil.Equals(t, 2, len(r.GetSpans()))
	testutil.Equals(t, 2, len(r.GetSampledSpans()))

	clientRoot.Finish()
	testutil.Equals(t, 3, len(r.GetSpans()))
	testutil.Equals(t, 3, len(r.GetSampledSpans()))
}

// This test shows that if sample factor will disable tracing on client process, even when it would be enabled on server
// it will be still disabled for all spans within this request.
func TestContextTracing_ClientDisablesTracing(t *testing.T) {
	r := &basictracer.InMemorySpanRecorder{}

	clientTracer := &tracer{
		debugName: "Test",
		wrapped: basictracer.NewWithOptions(basictracer.Options{
			ShouldSample: func(traceID uint64) bool {
				return false
			},
			Recorder:       r,
			MaxLogsPerSpan: 100,
		}),
	}

	clientRoot, clientCtx := StartSpan(ContextWithTracer(context.Background(), clientTracer), "a")

	// Simulate Server process with different tracer, but with client span in context.
	srvTracer := &tracer{
		debugName: "Test",
		wrapped: basictracer.NewWithOptions(basictracer.Options{
			ShouldSample: func(traceID uint64) bool {
				return true
			},
			Recorder:       r,
			MaxLogsPerSpan: 100,
		}),
	}
	srvRoot, srvCtx := StartSpan(ContextWithTracer(clientCtx, srvTracer), "b")
	srvChild, _ := StartSpan(srvCtx, "bb")
	testutil.Equals(t, 0, len(r.GetSpans()))

	srvChild.Finish()
	testutil.Equals(t, 1, len(r.GetSpans()))
	testutil.Equals(t, 0, len(r.GetSampledSpans()))

	srvRoot.Finish()
	testutil.Equals(t, 2, len(r.GetSpans()))
	testutil.Equals(t, 0, len(r.GetSampledSpans()))

	clientRoot.Finish()
	testutil.Equals(t, 3, len(r.GetSpans()))
	testutil.Equals(t, 0, len(r.GetSampledSpans()))
}
