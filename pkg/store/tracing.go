package store

import (
	"context"
	"io"
	"strings"

	"github.com/opentracing/opentracing-go/ext"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type TraceBucketReader struct {
	br objstore.BucketReader
}

func (tbr TraceBucketReader) Iter(ctx context.Context, dir string, f func(string) error) error {
	return tbr.br.Iter(ctx, dir, f)
}

// Get returns a reader for the given object name.
func (tbr TraceBucketReader) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return tbr.br.Get(ctx, name)
}

// GetRange returns a new range reader for the given object name and range.
func (tbr TraceBucketReader) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	var operationName string
	switch {
	case strings.Contains(name, "chunk"):
		operationName = "[chunk] GetRange"
	case strings.Contains(name, "index"):
		operationName = "[index] GetRange"
	default:
		operationName = "getRange"
	}

	span, ctx := tracing.StartSpan(ctx, operationName)
	defer span.Finish()

	ext.SpanKindRPCClient.Set(span)

	span.LogKV(
		"name", name,
		"off", off,
		"length", length,
	)

	return tbr.br.GetRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
// TODO(bplotka): Consider removing Exists in favor of helper that do Get & IsObjNotFoundErr (less code to maintain).
func (tbr TraceBucketReader) Exists(ctx context.Context, name string) (bool, error) {
	return tbr.br.Exists(ctx, name)
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (tbr TraceBucketReader) IsObjNotFoundErr(err error) bool {
	return tbr.br.IsObjNotFoundErr(err)
}

// ObjectSize returns the size of the specified object.
func (tbr TraceBucketReader) ObjectSize(ctx context.Context, name string) (uint64, error) {
	return tbr.br.ObjectSize(ctx, name)
}
