// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"context"
	"time"

	"github.com/thanos-io/thanos/pkg/block"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type verifierMetrics struct {
	blocksMarkedForDeletion prometheus.Counter
}

func newVerifierMetrics(reg prometheus.Registerer) *verifierMetrics {
	var m verifierMetrics

	m.blocksMarkedForDeletion = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_verify_blocks_marked_for_deletion_total",
		Help: "Total number of blocks marked for deletion by verify.",
	})

	return &m
}

// Issue is an function that does verification and repair only if repair arg is true.
// It should log affected blocks using warn level logs. It should be safe for issue to run on healthy bucket.
type Issue func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, repair bool, idMatcher func(ulid.ULID) bool, fetcher block.MetadataFetcher, deleteDelay time.Duration, metrics *verifierMetrics) error

// Verifier runs given issues to verify if bucket is healthy.
type Verifier struct {
	logger      log.Logger
	bkt         objstore.Bucket
	backupBkt   objstore.Bucket
	issues      []Issue
	repair      bool
	fetcher     block.MetadataFetcher
	deleteDelay time.Duration
	metrics     *verifierMetrics
}

// New returns verifier that only logs affected blocks.
func New(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, fetcher block.MetadataFetcher, deleteDelay time.Duration, issues []Issue) *Verifier {
	return &Verifier{
		logger:      logger,
		bkt:         bkt,
		issues:      issues,
		fetcher:     fetcher,
		repair:      false,
		deleteDelay: deleteDelay,
		metrics:     newVerifierMetrics(reg),
	}
}

// NewWithRepair returns verifier that logs affected blocks and attempts to repair them.
func NewWithRepair(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, backupBkt objstore.Bucket, fetcher block.MetadataFetcher, deleteDelay time.Duration, issues []Issue) *Verifier {
	return &Verifier{
		logger:      logger,
		bkt:         bkt,
		backupBkt:   backupBkt,
		issues:      issues,
		fetcher:     fetcher,
		repair:      true,
		deleteDelay: deleteDelay,
		metrics:     newVerifierMetrics(reg),
	}
}

// Verify verifies registered issues.
func (v *Verifier) Verify(ctx context.Context, idMatcher func(ulid.ULID) bool) error {
	level.Warn(v.logger).Log(
		"msg", "GLOBAL COMPACTOR SHOULD __NOT__ BE RUNNING ON THE SAME BUCKET",
		"issues", len(v.issues),
		"repair", v.repair,
	)

	if len(v.issues) == 0 {
		return errors.New("nothing to verify. No issue registered")
	}

	// TODO(blotka): Wrap bucket with BucketWithMetrics and print metrics after each issue (e.g how many blocks where touched).
	// TODO(bplotka): Implement disk "bucket" to allow this verify to work on local disk space as well.
	for _, issueFn := range v.issues {
		err := issueFn(ctx, v.logger, v.bkt, v.backupBkt, v.repair, idMatcher, v.fetcher, v.deleteDelay, v.metrics)
		if err != nil {
			return errors.Wrap(err, "verify")
		}
	}

	level.Info(v.logger).Log("msg", "verify completed", "issues", len(v.issues), "repair", v.repair)
	return nil
}
