package client

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var ErrNotFound = errors.New("no valid GCS or S3 configuration supplied")

// NewBucket initializes and returns new object storage clients.
// TODO(bplotka): Use that in every command.
func NewBucket(gcsBucket *string, s3Config s3.Config, reg *prometheus.Registry) (objstore.Bucket, func() error, error) {
	if *gcsBucket != "" {
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return nil, nil, errors.Wrap(err, "create GCS client")
		}
		return gcs.NewBucket(*gcsBucket, gcsClient.Bucket(*gcsBucket), reg), gcsClient.Close, nil
	}

	if s3Config.Validate() == nil {
		b, err := s3.NewBucket(&s3Config, reg)
		if err != nil {
			return nil, nil, errors.Wrap(err, "create s3 client")
		}
		return b, func() error { return nil }, nil
	}

	return nil, nil, ErrNotFound
}
