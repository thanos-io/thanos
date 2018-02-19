package gcloudtracer

import (
	"testing"
	"time"

	trace "cloud.google.com/go/trace/apiv1"
	"github.com/golang/protobuf/ptypes/timestamp"
	gax "github.com/googleapis/gax-go"
	basictracer "github.com/opentracing/basictracer-go"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	cloudtracepb "google.golang.org/genproto/googleapis/devtools/cloudtrace/v1"
)

func TestTraceClientImplementation(t *testing.T) {
	var client TraceClient
	client, _ = trace.NewClient(context.Background())
	_ = client
}

func TestRecorderClosing(t *testing.T) {
	patchTracesCalled := false
	closeCalled := false

	client := &mockTraceClient{
		patchTraces: func(ctx context.Context, req *cloudtracepb.PatchTracesRequest, opts ...gax.CallOption) error {
			patchTracesCalled = true
			assert.Equal(t, &cloudtracepb.PatchTracesRequest{
				ProjectId: "test_project",
				Traces: &cloudtracepb.Traces{
					Traces: []*cloudtracepb.Trace{
						&cloudtracepb.Trace{
							ProjectId: "test_project",
							TraceId:   "00000000000000010000000000000001",
							Spans: []*cloudtracepb.TraceSpan{
								&cloudtracepb.TraceSpan{
									SpanId: 10,
									Kind:   cloudtracepb.TraceSpan_SPAN_KIND_UNSPECIFIED,
									Name:   "test/operation",
									StartTime: &timestamp.Timestamp{
										Seconds: 1480425868,
									},
									EndTime: &timestamp.Timestamp{
										Seconds: 1480425873,
									},
									ParentSpanId: 9,
									Labels: map[string]string{
										"foo": "10",
										"bar": "foo",
									},
								},
							},
						},
					},
				},
			}, req)
			return nil
		},
		close: func() error {
			closeCalled = true
			return nil
		},
	}

	r, err := NewRecorder(context.Background(), "test_project", client)
	assert.NoError(t, err)

	r.bundler.DelayThreshold = time.Hour

	r.RecordSpan(basictracer.RawSpan{
		Context: basictracer.SpanContext{
			Sampled: true,
			SpanID:  10,
			TraceID: 1,
		},
		Operation:    "test/operation",
		ParentSpanID: 9,
		Start:        time.Unix(1480425868, 0),
		Duration:     5 * time.Second,
		Tags: map[string]interface{}{
			"foo": 10,
			"bar": "foo",
		},
	})

	assert.False(t, patchTracesCalled)
	assert.NoError(t, r.Close())
	assert.True(t, closeCalled)
	assert.True(t, patchTracesCalled)
}

func TestRecorder(t *testing.T) {
	t.Run("recorder=success", func(t *testing.T) {
		patchTracesCalled := false
		loggerCalled := false

		client := &mockTraceClient{
			patchTraces: func(ctx context.Context, req *cloudtracepb.PatchTracesRequest, opts ...gax.CallOption) error {
				patchTracesCalled = true
				assert.Equal(t, &cloudtracepb.PatchTracesRequest{
					ProjectId: "test_project",
					Traces: &cloudtracepb.Traces{
						Traces: []*cloudtracepb.Trace{
							&cloudtracepb.Trace{
								ProjectId: "test_project",
								TraceId:   "00000000000000010000000000000001",
								Spans: []*cloudtracepb.TraceSpan{
									&cloudtracepb.TraceSpan{
										SpanId: 10,
										Kind:   cloudtracepb.TraceSpan_SPAN_KIND_UNSPECIFIED,
										Name:   "test/operation",
										StartTime: &timestamp.Timestamp{
											Seconds: 1480425868,
										},
										EndTime: &timestamp.Timestamp{
											Seconds: 1480425873,
										},
										ParentSpanId: 9,
										Labels: map[string]string{
											"foo": "10",
											"bar": "foo",
										},
									},
								},
							},
						},
					},
				}, req)
				return nil
			},
			close: func() error {
				panic("not implemented")
			},
		}

		recorder, err := NewRecorder(
			context.Background(),
			"test_project",
			client,
			WithLogger(testLogger(func(format string, args ...interface{}) {
				loggerCalled = true
			})),
		)
		assert.NoError(t, err)
		// BufferedByteLimit is set to 1 for test propose, to send the trace immediately
		recorder.bundler.BufferedByteLimit = 1

		recorder.RecordSpan(basictracer.RawSpan{
			Context: basictracer.SpanContext{
				Sampled: true,
				SpanID:  10,
				TraceID: 1,
			},
			Operation:    "test/operation",
			ParentSpanID: 9,
			Start:        time.Unix(1480425868, 0),
			Duration:     5 * time.Second,
			Tags: map[string]interface{}{
				"foo": 10,
				"bar": "foo",
			},
		})
		assert.False(t, loggerCalled, "logger should not be called")
		assert.True(t, patchTracesCalled, "patch traces should have been called")
	})
}

func TestRecorderMissingProjectID(t *testing.T) {
	client := &mockTraceClient{
		patchTraces: func(ctx context.Context, req *cloudtracepb.PatchTracesRequest, opts ...gax.CallOption) error {
			panic("unimplemented")
		},
		close: func() error {
			panic("unimplemented")
		},
	}

	r, err := NewRecorder(context.Background(), "", client)
	assert.Equal(t, ErrInvalidProjectID, err)
	assert.Nil(t, r)
}

type mockTraceClient struct {
	patchTraces func(context.Context, *cloudtracepb.PatchTracesRequest, ...gax.CallOption) error
	close       func() error
}

func (c *mockTraceClient) PatchTraces(ctx context.Context, req *cloudtracepb.PatchTracesRequest, opts ...gax.CallOption) error {
	return c.patchTraces(ctx, req)
}

func (c *mockTraceClient) Close() error {
	return c.close()
}

type testLogger func(format string, args ...interface{})

func (l testLogger) Errorf(format string, args ...interface{}) {
	l(format, args...)
}

func (l testLogger) Infof(format string, args ...interface{}) {
}
