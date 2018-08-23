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
	"github.com/vglafirov/thanos/pkg/objstore/azure"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"google.golang.org/api/option"
)

//ErrNotFound Config not found Error
var ErrNotFound = errors.New("no valid GCS, S3 or Azure configuration supplied")

// NewBucket initializes and returns new object storage clients.
func NewBucket(logger log.Logger, gcsBucket *string, s3Config s3.Config, azureConfig azure.Config, reg *prometheus.Registry, component string) (objstore.Bucket, error) {
	if *gcsBucket != "" {
		gcsOptions := option.WithUserAgent(fmt.Sprintf("thanos-%s/%s (%s)", component, version.Version, runtime.Version()))
		gcsClient, err := storage.NewClient(context.Background(), gcsOptions)
		if err != nil {
			return nil, errors.Wrap(err, "create GCS client")
		}
		return objstore.BucketWithMetrics(*gcsBucket, gcs.NewBucket(*gcsBucket, gcsClient, reg), reg), nil
	}

	if s3Config.Validate() == nil {
		b, err := s3.NewBucket(logger, &s3Config, reg, component)
		if err != nil {
			return nil, errors.Wrap(err, "create s3 client")
		}
		return objstore.BucketWithMetrics(s3Config.Bucket, b, reg), nil
	}

	if azureConfig.Validate() == nil {
		b, err := azure.NewBucket(logger, &azureConfig, reg, component)
		if err != nil {
			return nil, errors.Wrap(err, "create Azure client")
		}
		return objstore.BucketWithMetrics(azureConfig.StorageAccountName, b, reg), nil
	}

	return nil, ErrNotFound
}
