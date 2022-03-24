package objstoreutil

import (
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
)

func NewBucket(logger log.Logger, confContentYaml []byte, reg prometheus.Registerer, component string) (objstore.InstrumentedBucket, error) {
	bucket, err := client.NewBucket(logger, confContentYaml, component)
	if err != nil {
		return nil, err
	}

	return objstore.NewTracingBucket(objstore.BucketWithMetrics(bucket.Name(), bucket, reg)), nil
}
