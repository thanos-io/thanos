// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

type erroringBucket struct {
	bkt objstore.InstrumentedBucket
}

func (b *erroringBucket) Close() error {
	return b.bkt.Close()
}

// WithExpectedErrs allows to specify a filter that marks certain errors as expected, so it will not increment
// thanos_objstore_bucket_operation_failures_total metric.
func (b *erroringBucket) WithExpectedErrs(f objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	return b.bkt.WithExpectedErrs(f)
}

// ReaderWithExpectedErrs allows to specify a filter that marks certain errors as expected, so it will not increment
// thanos_objstore_bucket_operation_failures_total metric.
func (b *erroringBucket) ReaderWithExpectedErrs(f objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b.bkt.ReaderWithExpectedErrs(f)
}

func (b *erroringBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return b.bkt.Iter(ctx, dir, f, options...)
}

// Get returns a reader for the given object name.
func (b *erroringBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.Contains(name, "chunk") {
		return nil, fmt.Errorf("some random error has occurred")
	}
	return b.bkt.Get(ctx, name)
}

// GetRange returns a new range reader for the given object name and range.
func (b *erroringBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if strings.Contains(name, "chunk") {
		return nil, fmt.Errorf("some random error has occurred")
	}
	return b.bkt.GetRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *erroringBucket) Exists(ctx context.Context, name string) (bool, error) {
	return b.bkt.Exists(ctx, name)
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *erroringBucket) IsObjNotFoundErr(err error) bool {
	return b.bkt.IsObjNotFoundErr(err)
}

// IsAccessDeniedErr returns true if error means that access to the object was denied.
func (b *erroringBucket) IsAccessDeniedErr(err error) bool {
	return b.bkt.IsAccessDeniedErr(err)
}

// Attributes returns information about the specified object.
func (b *erroringBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return b.bkt.Attributes(ctx, name)
}

// Upload the contents of the reader as an object into the bucket.
// Upload should be idempotent.
func (b *erroringBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	return b.bkt.Upload(ctx, name, r)
}

// Delete removes the object with the given name.
// If object does not exists in the moment of deletion, Delete should throw error.
func (b *erroringBucket) Delete(ctx context.Context, name string) error {
	return b.bkt.Delete(ctx, name)
}

// Name returns the bucket name for the provider.
func (b *erroringBucket) Name() string {
	return b.bkt.Name()
}

// Ensures that downsampleBucket() stops its work properly
// after an error occurs with some blocks in the backlog.
// Testing for https://github.com/thanos-io/thanos/issues/4960.
func TestRegression4960_Deadlock(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	dir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	bkt = &erroringBucket{bkt: bkt}
	var id, id2, id3 ulid.ULID
	var err error
	{
		id, err = e2eutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{{{Name: "a", Value: "1"}}},
			1, 0, downsample.ResLevel1DownsampleRange+1, // Pass the minimum ResLevel1DownsampleRange check.
			labels.Labels{{Name: "e1", Value: "1"}},
			downsample.ResLevel0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id.String()), metadata.NoneFunc))
	}
	{
		id2, err = e2eutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{{{Name: "a", Value: "2"}}},
			1, 0, downsample.ResLevel1DownsampleRange+1, // Pass the minimum ResLevel1DownsampleRange check.
			labels.Labels{{Name: "e1", Value: "2"}},
			downsample.ResLevel0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id2.String()), metadata.NoneFunc))
	}
	{
		id3, err = e2eutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{{{Name: "a", Value: "2"}}},
			1, 0, downsample.ResLevel1DownsampleRange+1, // Pass the minimum ResLevel1DownsampleRange check.
			labels.Labels{{Name: "e1", Value: "2"}},
			downsample.ResLevel0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id3.String()), metadata.NoneFunc))
	}

	meta, err := block.DownloadMeta(ctx, logger, bkt, id)
	testutil.Ok(t, err)

	metrics := newDownsampleMetrics(prometheus.NewRegistry())
	testutil.Equals(t, 0.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(meta.Thanos.GroupKey())))
	metaFetcher, err := block.NewMetaFetcher(nil, block.FetcherConcurrency, bkt, "", nil, nil)
	testutil.Ok(t, err)

	metas, _, err := metaFetcher.Fetch(ctx)
	testutil.Ok(t, err)
	err = downsampleBucket(ctx, logger, metrics, bkt, metas, dir, 1, 1, metadata.NoneFunc, false)
	testutil.NotOk(t, err)

	testutil.Assert(t, strings.Contains(err.Error(), "some random error has occurred"))

}

func TestCleanupDownsampleCacheFolder(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	dir := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	var id ulid.ULID
	var err error
	{
		id, err = e2eutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{{{Name: "a", Value: "1"}}},
			1, 0, downsample.ResLevel1DownsampleRange+1, // Pass the minimum ResLevel1DownsampleRange check.
			labels.Labels{{Name: "e1", Value: "1"}},
			downsample.ResLevel0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id.String()), metadata.NoneFunc))
	}

	meta, err := block.DownloadMeta(ctx, logger, bkt, id)
	testutil.Ok(t, err)

	metrics := newDownsampleMetrics(prometheus.NewRegistry())
	testutil.Equals(t, 0.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(meta.Thanos.GroupKey())))
	metaFetcher, err := block.NewMetaFetcher(nil, block.FetcherConcurrency, bkt, "", nil, nil)
	testutil.Ok(t, err)

	metas, _, err := metaFetcher.Fetch(ctx)
	testutil.Ok(t, err)
	testutil.Ok(t, downsampleBucket(ctx, logger, metrics, bkt, metas, dir, 1, 1, metadata.NoneFunc, false))
	testutil.Equals(t, 1.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(meta.Thanos.GroupKey())))

	_, err = os.Stat(dir)
	testutil.Assert(t, os.IsNotExist(err), "index cache dir should not exist at the end of execution")
}
