package objstore

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type backoffBucket struct {
	Bucket
	runutil.Backoff
	logger        log.Logger
	ignoreObjects []string

	retries *prometheus.CounterVec
}

func NewBucketWithBackoff(bkt Bucket, backoff runutil.Backoff, logger log.Logger, reg prometheus.Registerer, ignoreObjects ...string) InstrumentedBucket {
	b := &backoffBucket{
		Bucket:        bkt,
		Backoff:       backoff,
		logger:        logger,
		ignoreObjects: ignoreObjects,
		retries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_bucket_backoff_retries_total",
			Help:        "Total number of retried operations against a bucket.",
			ConstLabels: prometheus.Labels{"bucket": bkt.Name()},
		}, []string{"operation"}),
	}

	for _, op := range []string{
		OpGet,
		OpGetRange,
		OpExists,
		OpUpload,
		OpDelete,
	} {
		b.retries.WithLabelValues(op)
	}
	return b
}

func (b *backoffBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	const op = OpGet

	r, err := b.Bucket.Get(ctx, name)
	// Don't retry in case of 404 or if it's an object worth ignoring.
	if err == nil || b.Bucket.IsObjNotFoundErr(err) || b.isIgnoreObject(name) {
		return r, err
	}

	for retry := 1; retry <= b.Backoff.Max(); retry++ {
		select {
		case <-ctx.Done():
			return r, err
		case <-b.Backoff.Done():
			return r, err
		case <-time.After(b.Backoff.Duration(retry)):
			// Retry again after backoff
		}

		r, err = b.Bucket.Get(ctx, name)
		// Don't retry in case of 404 or if it's an object worth ignoring.
		if err == nil || b.Bucket.IsObjNotFoundErr(err) || b.isIgnoreObject(name) {
			return r, err
		}

		level.Warn(b.logger).Log("msg", "get failed", "name", name, "retryAttempt", retry, "error", err)
		b.retries.WithLabelValues(op).Inc()
	}

	return r, err
}

func (b *backoffBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	const op = OpGetRange

	r, err := b.Bucket.GetRange(ctx, name, off, length)
	// Don't retry in case of 404 or if it's an object worth ignoring.
	if err == nil || b.Bucket.IsObjNotFoundErr(err) || b.isIgnoreObject(name) {
		return r, err
	}

	for retry := 1; retry <= b.Backoff.Max(); retry++ {
		select {
		case <-ctx.Done():
			return r, err
		case <-b.Backoff.Done():
			return r, err
		case <-time.After(b.Backoff.Duration(retry)):
			// Retry again after backoff
		}

		r, err = b.Bucket.GetRange(ctx, name, off, length)
		// Don't retry in case of 404 or if it's an object worth ignoring.
		if err == nil || b.Bucket.IsObjNotFoundErr(err) || b.isIgnoreObject(name) {
			return r, err
		}

		level.Warn(b.logger).Log("msg", "get range failed", "name", name, "retryAttempt", retry, "error", err)
		b.retries.WithLabelValues(op).Inc()
	}

	return r, err
}

func (b *backoffBucket) Exists(ctx context.Context, name string) (bool, error) {
	const op = OpExists
	exists, err := b.Bucket.Exists(ctx, name)
	// Don't retry in case of 404 or if it's an object worth ignoring.
	if err == nil || b.Bucket.IsObjNotFoundErr(err) || b.isIgnoreObject(name) {
		return exists, err
	}

	for retry := 1; retry <= b.Backoff.Max(); retry++ {
		select {
		case <-ctx.Done():
			return exists, err
		case <-b.Backoff.Done():
			return exists, err
		case <-time.After(b.Backoff.Duration(retry)):
			// Retry again after backoff
		}

		exists, err = b.Bucket.Exists(ctx, name)
		// Don't retry in case of 404 or if it's an object worth ignoring.
		if err == nil || b.Bucket.IsObjNotFoundErr(err) || b.isIgnoreObject(name) {
			return exists, err
		}

		level.Warn(b.logger).Log("msg", "exists failed", "name", name, "retryAttempt", retry, "error", err)
		b.retries.WithLabelValues(op).Inc()
	}

	return exists, err
}

func (b *backoffBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	const op = OpUpload

	err := b.Bucket.Upload(ctx, name, r)
	if err == nil || b.isIgnoreObject(name) {
		return nil
	}

	for retry := 1; retry <= b.Backoff.Max(); retry++ {
		select {
		case <-ctx.Done():
			return err
		case <-b.Backoff.Done():
			return err
		case <-time.After(b.Backoff.Duration(retry)):
			// Retry again after backoff
		}

		err = b.Bucket.Upload(ctx, name, r)
		if err == nil || b.isIgnoreObject(name) {
			return nil
		}

		level.Warn(b.logger).Log("msg", "upload failed", "name", name, "retryAttempt", retry, "error", err)
		b.retries.WithLabelValues(op).Inc()
	}

	return err
}

func (b *backoffBucket) Delete(ctx context.Context, name string) error {
	const op = OpDelete

	err := b.Bucket.Delete(ctx, name)
	if err == nil || b.isIgnoreObject(name) {
		return nil
	}

	for retry := 1; retry <= b.Backoff.Max(); retry++ {
		select {
		case <-ctx.Done():
			return err
		case <-b.Backoff.Done():
			return err
		case <-time.After(b.Backoff.Duration(retry)):
			// Retry again after backoff
		}

		err = b.Bucket.Delete(ctx, name)
		if err == nil || b.isIgnoreObject(name) {
			return nil
		}

		level.Warn(b.logger).Log("msg", "delete failed", "name", name, "retryAttempt", retry, "error", err)
		b.retries.WithLabelValues(op).Inc()
	}

	return err
}

func (b *backoffBucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	return b.Bucket.Iter(ctx, dir, f)
}

func (b *backoffBucket) Attributes(ctx context.Context, name string) (ObjectAttributes, error) {
	return b.Bucket.Attributes(ctx, name)
}

func (b *backoffBucket) IsObjNotFoundErr(err error) bool {
	return b.Bucket.IsObjNotFoundErr(err)
}

func (b *backoffBucket) Name() string {
	return b.Bucket.Name()
}

func (b *backoffBucket) WithExpectedErrs(expectedFunc IsOpFailureExpectedFunc) Bucket {
	if ib, ok := b.Bucket.(InstrumentedBucket); ok {
		// Make a copy, but replace bucket with instrumented one.
		res := &backoffBucket{}
		*res = *b
		res.Bucket = ib.WithExpectedErrs(expectedFunc)
		return res
	}

	return b
}

func (b *backoffBucket) ReaderWithExpectedErrs(expectedFunc IsOpFailureExpectedFunc) BucketReader {
	return b.WithExpectedErrs(expectedFunc)
}

func (b *backoffBucket) Close() error {
	return b.Bucket.Close()
}

func (b *backoffBucket) isIgnoreObject(name string) bool {
	for _, i := range b.ignoreObjects {
		if strings.HasSuffix(name, i) {
			return true
		}
	}
	return false
}
