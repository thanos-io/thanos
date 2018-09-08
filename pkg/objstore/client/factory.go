package client

import (
	"context"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	yaml "gopkg.in/yaml.v2"
)

type objProvider string

const (
	GCS objProvider = "GCS"
	S3  objProvider = "S3"
)

type BucketConfig struct {
	Type   objProvider `yaml:"type"`
	Config interface{} `yaml:"config"`
}

var ErrNotFound = errors.New("not found bucket")

// NewBucket initializes and returns new object storage clients.
func NewBucket(logger log.Logger, conf string, reg *prometheus.Registry, component string) (objstore.Bucket, error) {
	var err error
	var bucketConf BucketConfig
	if conf == "" {
		return nil, ErrNotFound
	}
	if err := yaml.Unmarshal([]byte(conf), &bucketConf); err != nil {
		return nil, errors.Wrap(err, "unmarshal objstore.config")
	}

	config, err := yaml.Marshal(bucketConf.Config)
	if err != nil {
		return nil, err
	}

	var bucket objstore.Bucket
	switch bucketConf.Type {
	case GCS:
		bucket, err = gcs.NewBucket(logger, context.Background(), config, reg, component)
	case S3:
		bucket, err = s3.NewBucket(logger, config, reg, component)
	default:
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s client", bucketConf.Type))
	}
	return objstore.BucketWithMetrics(bucket.GetBucket(), bucket, reg), nil
}
