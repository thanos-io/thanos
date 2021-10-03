// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"bytes"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/pkg/errors"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

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

func TestTryToGetSize(t *testing.T) {
	tmpFile, err := os.OpenFile(path.Join(os.TempDir(), "test_try_get_size"), os.O_CREATE|os.O_RDWR, os.ModePerm)
	testutil.Ok(t, err)
	defer tmpFile.Close()
	_, err = tmpFile.WriteString("abc")
	testutil.Ok(t, err)

	buf := make([]byte, 5)
	bbuff := bytes.NewBuffer(buf)

	sreader := strings.NewReader("hello world")

	buf2 := make([]byte, 100)
	breader := bytes.NewReader(buf2)

	r := bytes.NewBuffer(make([]byte, 20))
	trc := newTimingReadCloser(NopCloserWithSize(r), "test", nil, nil, nil)
	for _, tcase := range []struct {
		name    string
		reader  io.Reader
		expSize int64
		expErr  error
	}{
		{
			name:    "local file",
			reader:  tmpFile,
			expSize: 3,
		},
		{
			name:    "bytes buffer",
			reader:  bbuff,
			expSize: 5,
		},
		{
			name:    "bytes reader",
			reader:  breader,
			expSize: 100,
		},
		{
			name:    "string reader",
			reader:  sreader,
			expSize: 11,
		},
		{
			name:    "timing read closer",
			reader:  trc,
			expSize: 20,
		},
		{
			name:   "other io.Reader implementation",
			reader: &testReader{},
			expErr: errors.Errorf("unsupported type of io.Reader: %T", &testReader{}),
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			size, err := TryToGetSize(tcase.reader)
			if tcase.expErr == nil {
				testutil.Ok(t, err)
				testutil.Equals(t, tcase.expSize, size)
			} else {
				testutil.Equals(t, tcase.expErr.Error(), err.Error())
			}
		})
	}
}

type testReader struct{}

func (r *testReader) Read([]byte) (n int, err error) { return 0, nil }
