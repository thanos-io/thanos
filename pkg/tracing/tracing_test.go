// This file includes unit tests that test only tiny logic in this package, but are here mainly as a showcase on how tracing can
// be configured.

package tracing

import (
	"testing"

	"context"

	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/opentracing/basictracer-go"
)

// This test shows that if sample factor will enable tracing on client process, even when it would be disabled on server
// it will be still enabled for all spans within this span.
func TestContextTracing_ClientEnablesTracing(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	m := &basictracer.InMemorySpanRecorder{}
	r := &forceRecorder{wrapped: m}

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
	testutil.Equals(t, 0, len(m.GetSpans()))

	srvChild.Finish()
	testutil.Equals(t, 1, len(m.GetSpans()))
	testutil.Equals(t, 1, len(m.GetSampledSpans()))

	srvRoot.Finish()
	testutil.Equals(t, 2, len(m.GetSpans()))
	testutil.Equals(t, 2, len(m.GetSampledSpans()))

	clientRoot.Finish()
	testutil.Equals(t, 3, len(m.GetSpans()))
	testutil.Equals(t, 3, len(m.GetSampledSpans()))
}

// This test shows that if sample factor will disable tracing on client process, even when it would be enabled on server
// it will be still disabled for all spans within this span.
func TestContextTracing_ClientDisablesTracing(t *testing.T) {
	m := &basictracer.InMemorySpanRecorder{}
	r := &forceRecorder{wrapped: m}

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
	testutil.Equals(t, 0, len(m.GetSpans()))

	srvChild.Finish()
	testutil.Equals(t, 1, len(m.GetSpans()))
	testutil.Equals(t, 0, len(m.GetSampledSpans()))

	srvRoot.Finish()
	testutil.Equals(t, 2, len(m.GetSpans()))
	testutil.Equals(t, 0, len(m.GetSampledSpans()))

	clientRoot.Finish()
	testutil.Equals(t, 3, len(m.GetSpans()))
	testutil.Equals(t, 0, len(m.GetSampledSpans()))
}

// This test shows that if span will contain special baggage (for example from special HTTP header), even when sample
// factor will disable client & server tracing, it will be still enabled for all spans within this span.
func TestContextTracing_ForceTracing(t *testing.T) {
	m := &basictracer.InMemorySpanRecorder{}
	r := &forceRecorder{wrapped: m}

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

	// Force tracing for this span and its children.
	clientRoot.SetBaggageItem(ForceTracingBaggageKey, "Go for it")

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
	testutil.Equals(t, 0, len(m.GetSpans()))

	srvChild.Finish()
	testutil.Equals(t, 1, len(m.GetSpans()))
	testutil.Equals(t, 1, len(m.GetSampledSpans()))

	srvRoot.Finish()
	testutil.Equals(t, 2, len(m.GetSpans()))
	testutil.Equals(t, 2, len(m.GetSampledSpans()))

	clientRoot.Finish()
	testutil.Equals(t, 3, len(m.GetSpans()))
	testutil.Equals(t, 3, len(m.GetSampledSpans()))
}
