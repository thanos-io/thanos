package client

import (
	"context"
	"fmt"
	"runtime"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"google.golang.org/api/option"
)

var ErrNotFound = errors.New("no valid GCS or S3 configuration supplied")

// NewBucket initializes and returns new object storage clients.
// TODO(bplotka): Use that in every command.
func NewBucket(gcsBucket *string, s3Config s3.Config, reg *prometheus.Registry, component string) (objstore.Bucket, func() error, error) {
	if *gcsBucket != "" {
		gcsOptions := option.WithUserAgent(fmt.Sprintf("thanos-%s/%s (%s)", component, version.Version, runtime.Version()))
		gcsClient, err := storage.NewClient(context.Background(), gcsOptions)
		if err != nil {
			return nil, nil, errors.Wrap(err, "create GCS client")
		}
		return gcs.NewBucket(*gcsBucket, gcsClient.Bucket(*gcsBucket), reg), gcsClient.Close, nil
	}

	if s3Config.Validate() == nil {
		b, err := s3.NewBucket(&s3Config, reg, component)
		if err != nil {
			return nil, nil, errors.Wrap(err, "create s3 client")
		}
		return b, func() error { return nil }, nil
	}

	return nil, nil, ErrNotFound
}
