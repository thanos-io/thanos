// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMetricBucket_Close(t *testing.T) {
	bkt := BucketWithMetrics("abc", NewInMemBucket(), nil)
	// Expected initialized metrics.
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))

	AcceptanceTest(t, bkt.WithExpectedErrs(bkt.IsObjNotFoundErr))
	testutil.Equals(t, float64(9), promtest.ToFloat64(bkt.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(2), promtest.ToFloat64(bkt.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(9), promtest.ToFloat64(bkt.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))
	lastUpload := promtest.ToFloat64(bkt.lastSuccessfulUploadTime)
	testutil.Assert(t, lastUpload > 0, "last upload not greater than 0, val: %f", lastUpload)

	// Clear bucket, but don't clear metrics to ensure we use same.
	bkt.bkt = NewInMemBucket()
	AcceptanceTest(t, bkt)
	testutil.Equals(t, float64(18), promtest.ToFloat64(bkt.ops.WithLabelValues(OpIter)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.ops.WithLabelValues(OpAttributes)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(4), promtest.ToFloat64(bkt.ops.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(18), promtest.ToFloat64(bkt.ops.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(6), promtest.ToFloat64(bkt.ops.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.ops))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpIter)))
	// Not expected not found error here.
	testutil.Equals(t, float64(1), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpAttributes)))
	// Not expected not found errors, this should increment failure metric on get for not found as well, so +2.
	testutil.Equals(t, float64(3), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGet)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpGetRange)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpExists)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpUpload)))
	testutil.Equals(t, float64(0), promtest.ToFloat64(bkt.opsFailures.WithLabelValues(OpDelete)))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsFailures))
	testutil.Equals(t, 7, promtest.CollectAndCount(bkt.opsDuration))
	testutil.Assert(t, promtest.ToFloat64(bkt.lastSuccessfulUploadTime) > lastUpload)
}

func TestTracingReader(t *testing.T) {
	r := bytes.NewReader([]byte("hello world"))
	tr := newTracingReadCloser(NopCloserWithSize(r), nil)

	size, err := TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)

	smallBuf := make([]byte, 4)
	n, err := io.ReadFull(tr, smallBuf)
	testutil.Ok(t, err)
	testutil.Equals(t, 4, n)

	// Verify that size is still the same, after reading 4 bytes.
	size, err = TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)
}

func TestDownloadUploadDirConcurrency(t *testing.T) {
	r := prometheus.NewRegistry()
	m := BucketWithMetrics("", NewInMemBucket(), r)
	tempDir := t.TempDir()

	testutil.Ok(t, m.Upload(context.Background(), "dir/obj1", bytes.NewReader([]byte("1"))))
	testutil.Ok(t, m.Upload(context.Background(), "dir/obj2", bytes.NewReader([]byte("2"))))
	testutil.Ok(t, m.Upload(context.Background(), "dir/obj3", bytes.NewReader([]byte("3"))))

	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE thanos_objstore_bucket_operations_total counter
        thanos_objstore_bucket_operations_total{bucket="",operation="attributes"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="delete"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="exists"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="get"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="get_range"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="iter"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="upload"} 3
		`), `thanos_objstore_bucket_operations_total`))

	testutil.Ok(t, DownloadDir(context.Background(), log.NewNopLogger(), m, "dir/", "dir/", tempDir, WithFetchConcurrency(10)))
	i, err := ioutil.ReadDir(tempDir)
	testutil.Ok(t, err)
	testutil.Assert(t, len(i) == 3)
	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE thanos_objstore_bucket_operations_total counter
        thanos_objstore_bucket_operations_total{bucket="",operation="attributes"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="delete"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="exists"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="get"} 3
        thanos_objstore_bucket_operations_total{bucket="",operation="get_range"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="iter"} 1
        thanos_objstore_bucket_operations_total{bucket="",operation="upload"} 3
		`), `thanos_objstore_bucket_operations_total`))

	testutil.Ok(t, UploadDir(context.Background(), log.NewNopLogger(), m, tempDir, "/dir-copy", WithUploadConcurrency(10)))

	testutil.Ok(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE thanos_objstore_bucket_operations_total counter
        thanos_objstore_bucket_operations_total{bucket="",operation="attributes"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="delete"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="exists"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="get"} 3
        thanos_objstore_bucket_operations_total{bucket="",operation="get_range"} 0
        thanos_objstore_bucket_operations_total{bucket="",operation="iter"} 1
        thanos_objstore_bucket_operations_total{bucket="",operation="upload"} 6
		`), `thanos_objstore_bucket_operations_total`))
}

func TestTimingTracingReader(t *testing.T) {
	m := BucketWithMetrics("", NewInMemBucket(), nil)
	r := bytes.NewReader([]byte("hello world"))

	tr := NopCloserWithSize(r)
	tr = newTimingReadCloser(tr, "", m.opsDuration, m.opsFailures, func(err error) bool {
		return false
	})
	tr = newTracingReadCloser(tr, nil)

	size, err := TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)

	smallBuf := make([]byte, 4)
	n, err := io.ReadFull(tr, smallBuf)
	testutil.Ok(t, err)
	testutil.Equals(t, 4, n)

	// Verify that size is still the same, after reading 4 bytes.
	size, err = TryToGetSize(tr)

	testutil.Ok(t, err)
	testutil.Equals(t, int64(11), size)
}

func TestDownloadDir_CleanUp(t *testing.T) {
	b := unreliableBucket{
		Bucket:  NewInMemBucket(),
		n:       3,
		current: atomic.NewInt32(0),
	}
	tempDir := t.TempDir()

	testutil.Ok(t, b.Upload(context.Background(), "dir/obj1", bytes.NewReader([]byte("1"))))
	testutil.Ok(t, b.Upload(context.Background(), "dir/obj2", bytes.NewReader([]byte("2"))))
	testutil.Ok(t, b.Upload(context.Background(), "dir/obj3", bytes.NewReader([]byte("3"))))

	// We exapect the third Get to fail
	testutil.NotOk(t, DownloadDir(context.Background(), log.NewNopLogger(), b, "dir/", "dir/", tempDir))
	_, err := os.Stat(tempDir)
	testutil.Assert(t, os.IsNotExist(err))
}

// unreliableBucket implements Bucket and returns an error on every n-th Get.
type unreliableBucket struct {
	Bucket

	n       int32
	current *atomic.Int32
}

func (b unreliableBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.current.Inc()%b.n == 0 {
		return nil, errors.Errorf("some error message")
	}
	return b.Bucket.Get(ctx, name)
}
