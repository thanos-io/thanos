package client

import (
	"context"
	"fmt"
	"runtime"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"google.golang.org/api/option"
)

var ErrNotFound = errors.New("not found bucket")

// NewBucket initializes and returns new object storage clients.
func NewBucket(logger log.Logger, bucketConf objstore.BucketConfig, reg *prometheus.Registry, component string) (objstore.Bucket, error) {
	switch bucketConf.Provider {
	case objstore.GCS:
		if bucketConf.Bucket == "" {
			return nil, errors.New("missing Google Cloud Storage bucket name for stored blocks")
		}
		gcsOptions := option.WithUserAgent(fmt.Sprintf("thanos-%s/%s (%s)", component, version.Version, runtime.Version()))
		gcsClient, err := storage.NewClient(context.Background(), gcsOptions)
		if err != nil {
			return nil, errors.Wrap(err, "create GCS client")
		}
		return objstore.BucketWithMetrics(bucketConf.Bucket, gcs.NewBucket(bucketConf.Bucket, gcsClient, reg), reg), nil
	case objstore.S3:
		if err := s3.Validate(&bucketConf); err != nil {
			return nil, err
		}
		b, err := s3.NewBucket(logger, bucketConf, reg, component)
		if err != nil {
			return nil, errors.Wrap(err, "create s3 client")
		}
		return objstore.BucketWithMetrics(bucketConf.Bucket, b, reg), nil
	}
	return nil, ErrNotFound
}
