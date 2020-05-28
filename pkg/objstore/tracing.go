package objstore

import (
	"context"
	"io"

	"github.com/opentracing/opentracing-go"

	"github.com/thanos-io/thanos/pkg/tracing"
)

type TracingBucket struct {
	Bucket
}

func (t TracingBucket) WithExpectedErrs(expectedFunc IsOpFailureExpectedFunc) Bucket {
	if ib, ok := t.Bucket.(InstrumentedBucket); ok {
		return TracingBucket{ib.WithExpectedErrs(expectedFunc)}
	}
	return t
}

func (t TracingBucket) ReaderWithExpectedErrs(expectedFunc IsOpFailureExpectedFunc) BucketReader {
	return t.WithExpectedErrs(expectedFunc)
}

func (t TracingBucket) Iter(ctx context.Context, dir string, f func(string) error) (err error) {
	tracing.DoWithSpan(ctx, "bucket_iter", func(spanCtx context.Context, span opentracing.Span) {
		span.LogKV("dir", dir)
		err = t.Bucket.Iter(spanCtx, dir, f)
	})
	return
}

func (t TracingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	span, spanCtx := tracing.StartSpan(ctx, "bucket_get")
	span.LogKV("name", name)

	r, err := t.Bucket.Get(spanCtx, name)
	if err != nil {
		span.LogKV("err", err)
		span.Finish()
		return nil, err
	}

	return &tracingReadCloser{r: r, s: span}, nil
}

func (t TracingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	span, spanCtx := tracing.StartSpan(ctx, "bucket_getrange")
	span.LogKV("name", name, "offset", off, "length", length)

	r, err := t.Bucket.GetRange(spanCtx, name, off, length)
	if err != nil {
		span.LogKV("err", err)
		span.Finish()
		return nil, err
	}

	return &tracingReadCloser{r: r, s: span}, nil
}

func (t TracingBucket) Exists(ctx context.Context, name string) (exists bool, err error) {
	tracing.DoWithSpan(ctx, "bucket_exists", func(spanCtx context.Context, span opentracing.Span) {
		span.LogKV("name", name)
		exists, err = t.Bucket.Exists(spanCtx, name)
	})
	return
}

func (t TracingBucket) Attributes(ctx context.Context, name string) (attrs ObjectAttributes, err error) {
	tracing.DoWithSpan(ctx, "bucket_attributes", func(spanCtx context.Context, span opentracing.Span) {
		span.LogKV("name", name)
		attrs, err = t.Bucket.Attributes(spanCtx, name)
	})
	return
}

func (t TracingBucket) Upload(ctx context.Context, name string, r io.Reader) (err error) {
	tracing.DoWithSpan(ctx, "bucket_upload", func(spanCtx context.Context, span opentracing.Span) {
		span.LogKV("name", name)
		err = t.Bucket.Upload(spanCtx, name, r)
	})
	return
}

func (t TracingBucket) Delete(ctx context.Context, name string) (err error) {
	tracing.DoWithSpan(ctx, "bucket_delete", func(spanCtx context.Context, span opentracing.Span) {
		span.LogKV("name", name)
		err = t.Bucket.Delete(spanCtx, name)
	})
	return
}

func (t TracingBucket) Name() string {
	return "tracing: " + t.Bucket.Name()
}

type tracingReadCloser struct {
	r    io.ReadCloser
	s    opentracing.Span
	read int
}

func (t *tracingReadCloser) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.read += n
	}
	if err != nil && err != io.EOF && t.s != nil {
		t.s.LogKV("err", err)
	}
	return n, err
}

func (t *tracingReadCloser) Close() error {
	err := t.r.Close()
	if t.s != nil {
		t.s.LogKV("read", t.read)
		if err != nil {
			t.s.LogKV("close err", err)
		}
		t.s.Finish()
		t.s = nil
	}
	return err
}
