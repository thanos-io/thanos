package dedup

import (
	"context"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type BucketDeduper struct {
	logger           log.Logger
	dedupDir         string
	replicaLabelName string
	bkt              objstore.Bucket

	metrics *DedupMetrics

	syncer *ReplicaSyncer
	merger *ReplicaMerger
}

func NewBucketDeduper(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, dedupDir, replicaLabelName string,
	consistencyDelay time.Duration, blockSyncConcurrency int) *BucketDeduper {
	metrics := NewDedupMetrics(reg)
	return &BucketDeduper{
		logger:           logger,
		dedupDir:         dedupDir,
		replicaLabelName: replicaLabelName,
		bkt:              bkt,
		metrics:          metrics,
		syncer:           NewReplicaSyncer(logger, metrics, bkt, replicaLabelName, consistencyDelay, blockSyncConcurrency),
		merger:           NewReplicaMerger(logger, metrics, bkt, dedupDir, replicaLabelName),
	}
}

func (d *BucketDeduper) Dedup(ctx context.Context) error {
	level.Info(d.logger).Log("msg", "start of deduplication")
	start := time.Now()
	if err := os.RemoveAll(d.dedupDir); err != nil {
		return errors.Wrap(err, "clean up the dedup temporary directory")
	}
	if err := os.MkdirAll(d.dedupDir, 0777); err != nil {
		return errors.Wrap(err, "create the dedup temporary directory")
	}

	replicas, err := d.syncer.Sync(ctx)
	if err != nil {
		return errors.Wrap(err, "sync replica metas")
	}

	groups := make(map[string]Replicas)
	for _, r := range replicas {
		group := r.Group()
		groups[group] = append(groups[group], r)
	}
	for k, v := range groups {
		level.Info(d.logger).Log("msg", "starting to dedup replicas", "group", k)
		d.metrics.deduplication.WithLabelValues(d.bkt.Name()).Inc()
		if len(v) == 0 {
			continue
		}
		resolution, err := v[0].Resolution()
		if err != nil {
			return errors.Wrapf(err, "merge replicas: %s", k)
		}
		if err := d.merger.Merge(ctx, resolution, v); err != nil {
			d.metrics.deduplicationFailures.WithLabelValues(d.bkt.Name(), k).Inc()
			return errors.Wrapf(err, "merge replicas: %s", k)
		}
		level.Info(d.logger).Log("msg", "completed to dedup replicas", "group", k)
	}
	level.Info(d.logger).Log("msg", "deduplication process done", "duration", time.Since(start))
	return nil
}
