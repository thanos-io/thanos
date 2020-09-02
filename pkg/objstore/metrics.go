// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"context"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// IsOpFailureExpectedFunc allows to mark certain errors as expected, so they will not increment thanos_objstore_bucket_operation_failures_total metric.
type IsOpFailureExpectedFunc func(error) bool

var _ InstrumentedBucket = &metricBucket{}

// BucketWithMetrics takes a bucket and registers metrics with the given registry for
// operations run against the bucket.
func BucketWithMetrics(name string, b Bucket, reg prometheus.Registerer) *metricBucket {
	bkt := &metricBucket{
		bkt:                 b,
		isOpFailureExpected: func(err error) bool { return false },
		ops: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_bucket_operations_total",
			Help:        "Total number of all attempted operations against a bucket.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),

		opsFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_bucket_operation_failures_total",
			Help:        "Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),

		opsDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:        "thanos_objstore_bucket_operation_duration_seconds",
			Help:        "Duration of successful operations against the bucket",
			ConstLabels: prometheus.Labels{"bucket": name},
			Buckets:     []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
		}, []string{"operation"}),

		lastSuccessfulUploadTime: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "thanos_objstore_bucket_last_successful_upload_time",
			Help: "Second timestamp of the last successful upload to the bucket.",
		}, []string{"bucket"}),
	}
	for _, op := range []string{
		OpIter,
		OpGet,
		OpGetRange,
		OpExists,
		OpUpload,
		OpDelete,
		OpAttributes,
	} {
		bkt.ops.WithLabelValues(op)
		bkt.opsFailures.WithLabelValues(op)
		bkt.opsDuration.WithLabelValues(op)
	}
	bkt.lastSuccessfulUploadTime.WithLabelValues(b.Name())
	return bkt
}

type metricBucket struct {
	bkt Bucket

	ops                 *prometheus.CounterVec
	opsFailures         *prometheus.CounterVec
	isOpFailureExpected IsOpFailureExpectedFunc

	opsDuration              *prometheus.HistogramVec
	lastSuccessfulUploadTime *prometheus.GaugeVec
}

func (b *metricBucket) WithExpectedErrs(fn IsOpFailureExpectedFunc) Bucket {
	return &metricBucket{
		bkt:                      b.bkt,
		ops:                      b.ops,
		opsFailures:              b.opsFailures,
		isOpFailureExpected:      fn,
		opsDuration:              b.opsDuration,
		lastSuccessfulUploadTime: b.lastSuccessfulUploadTime,
	}
}

func (b *metricBucket) ReaderWithExpectedErrs(fn IsOpFailureExpectedFunc) BucketReader {
	return b.WithExpectedErrs(fn)
}

func (b *metricBucket) Iter(ctx context.Context, dir string, f func(name string) error) error {
	const op = OpIter
	b.ops.WithLabelValues(op).Inc()

	err := b.bkt.Iter(ctx, dir, f)
	if err != nil && !b.isOpFailureExpected(err) {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	return err
}

func (b *metricBucket) Attributes(ctx context.Context, name string) (ObjectAttributes, error) {
	const op = OpAttributes
	b.ops.WithLabelValues(op).Inc()

	start := time.Now()
	attrs, err := b.bkt.Attributes(ctx, name)
	if err != nil {
		if !b.isOpFailureExpected(err) {
			b.opsFailures.WithLabelValues(op).Inc()
		}
		return attrs, err
	}
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return attrs, nil
}

func (b *metricBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	const op = OpGet
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.Get(ctx, name)
	if err != nil {
		if !b.isOpFailureExpected(err) {
			b.opsFailures.WithLabelValues(op).Inc()
		}
		return nil, err
	}
	return newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
		b.isOpFailureExpected,
	), nil
}

func (b *metricBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	const op = OpGetRange
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.GetRange(ctx, name, off, length)
	if err != nil {
		if !b.isOpFailureExpected(err) {
			b.opsFailures.WithLabelValues(op).Inc()
		}
		return nil, err
	}
	return newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
		b.isOpFailureExpected,
	), nil
}

func (b *metricBucket) Exists(ctx context.Context, name string) (bool, error) {
	const op = OpExists
	b.ops.WithLabelValues(op).Inc()

	start := time.Now()
	ok, err := b.bkt.Exists(ctx, name)
	if err != nil {
		if !b.isOpFailureExpected(err) {
			b.opsFailures.WithLabelValues(op).Inc()
		}
		return false, err
	}
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return ok, nil
}

func (b *metricBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	const op = OpUpload
	b.ops.WithLabelValues(op).Inc()

	start := time.Now()
	if err := b.bkt.Upload(ctx, name, r); err != nil {
		if !b.isOpFailureExpected(err) {
			b.opsFailures.WithLabelValues(op).Inc()
		}
		return err
	}
	b.lastSuccessfulUploadTime.WithLabelValues(b.bkt.Name()).SetToCurrentTime()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return nil
}

func (b *metricBucket) Delete(ctx context.Context, name string) error {
	const op = OpDelete
	b.ops.WithLabelValues(op).Inc()

	start := time.Now()
	if err := b.bkt.Delete(ctx, name); err != nil {
		if !b.isOpFailureExpected(err) {
			b.opsFailures.WithLabelValues(op).Inc()
		}
		return err
	}
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return nil
}

func (b *metricBucket) IsObjNotFoundErr(err error) bool {
	return b.bkt.IsObjNotFoundErr(err)
}

func (b *metricBucket) Close() error {
	return b.bkt.Close()
}

func (b *metricBucket) Name() string {
	return b.bkt.Name()
}
