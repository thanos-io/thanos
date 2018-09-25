package objstore

import (
	"context"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// BucketWithMetrics takes a bucket and registers metrics with the given registry for
// operations run against the bucket.
func BucketWithMetrics(name string, b Bucket, r prometheus.Registerer) Bucket {
	bkt := &metricBucket{
		bkt: b,

		ops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_bucket_operations_total",
			Help:        "Total number of operations against a bucket.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),

		opsFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_bucket_operation_failures_total",
			Help:        "Total number of operations against a bucket that failed.",
			ConstLabels: prometheus.Labels{"bucket": name},
		}, []string{"operation"}),

		opsDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "thanos_objstore_bucket_operation_duration_seconds",
			Help:        "Duration of operations against the bucket",
			ConstLabels: prometheus.Labels{"bucket": name},
			Buckets:     []float64{0.005, 0.01, 0.02, 0.04, 0.08, 0.15, 0.3, 0.6, 1, 1.5, 2.5, 5, 10, 20, 30},
		}, []string{"operation"}),
		lastSuccessfullUploadTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "thanos_objstore_bucket_last_successful_upload_time",
			Help:        "Second timestamp of the last successful upload to the bucket.",
			ConstLabels: prometheus.Labels{"bucket": name}}),
	}
	if r != nil {
		r.MustRegister(bkt.ops, bkt.opsFailures, bkt.opsDuration, bkt.lastSuccessfullUploadTime)
	}
	return bkt
}

type metricBucket struct {
	bkt Bucket

	ops                       *prometheus.CounterVec
	opsFailures               *prometheus.CounterVec
	opsDuration               *prometheus.HistogramVec
	lastSuccessfullUploadTime prometheus.Gauge
}

func (b *metricBucket) Iter(ctx context.Context, dir string, f func(name string) error) error {
	const op = "iter"

	err := b.bkt.Iter(ctx, dir, f)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()

	return err
}

func (b *metricBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	const op = "get"
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.Get(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
		return nil, err
	}
	rc = newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
	)

	return rc, nil
}

func (b *metricBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	const op = "get_range"
	b.ops.WithLabelValues(op).Inc()

	rc, err := b.bkt.GetRange(ctx, name, off, length)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
		return nil, err
	}
	rc = newTimingReadCloser(
		rc,
		op,
		b.opsDuration,
		b.opsFailures,
	)

	return rc, nil
}

func (b *metricBucket) Exists(ctx context.Context, name string) (bool, error) {
	const op = "exists"
	start := time.Now()

	ok, err := b.bkt.Exists(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return ok, err
}

func (b *metricBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	const op = "upload"
	start := time.Now()

	err := b.bkt.Upload(ctx, name, r)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	} else {
		//TODO: Use SetToCurrentTime() once we update the Prometheus client_golang
		b.lastSuccessfullUploadTime.Set(float64(time.Now().UnixNano()) / 1e9)
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return err
}

func (b *metricBucket) Delete(ctx context.Context, name string) error {
	const op = "delete"
	start := time.Now()

	err := b.bkt.Delete(ctx, name)
	if err != nil {
		b.opsFailures.WithLabelValues(op).Inc()
	}
	b.ops.WithLabelValues(op).Inc()
	b.opsDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	return err
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

type timingReadCloser struct {
	io.ReadCloser

	ok       bool
	start    time.Time
	op       string
	duration *prometheus.HistogramVec
	failed   *prometheus.CounterVec
}

func newTimingReadCloser(rc io.ReadCloser, op string, dur *prometheus.HistogramVec, failed *prometheus.CounterVec) *timingReadCloser {
	// Initialize the metrics with 0.
	dur.WithLabelValues(op)
	failed.WithLabelValues(op)
	return &timingReadCloser{
		ReadCloser: rc,
		ok:         true,
		start:      time.Now(),
		op:         op,
		duration:   dur,
		failed:     failed,
	}
}

func (rc *timingReadCloser) Close() error {
	err := rc.ReadCloser.Close()
	rc.duration.WithLabelValues(rc.op).Observe(time.Since(rc.start).Seconds())
	if rc.ok && err != nil {
		rc.failed.WithLabelValues(rc.op).Inc()
		rc.ok = false
	}
	return err
}

func (rc *timingReadCloser) Read(b []byte) (n int, err error) {
	n, err = rc.ReadCloser.Read(b)
	if rc.ok && err != nil && err != io.EOF {
		rc.failed.WithLabelValues(rc.op).Inc()
		rc.ok = false
	}
	return n, err
}
