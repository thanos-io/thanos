package verifier

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// Issue is an function that does verification and repair only if repair arg is true.
// It should log affected blocks using warn level logs. It should be safe for issue to run on healthy bucket.
type Issue func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, repair bool, idMatcher func(ulid.ULID) bool) error

// Verifier runs given issues to verify if bucket is healthy.
type Verifier struct {
	logger    log.Logger
	bkt       objstore.Bucket
	backupBkt objstore.Bucket
	issues    []Issue
	repair    bool
}

// New returns verifier that only logs affected blocks.
func New(logger log.Logger, bkt objstore.Bucket, issues []Issue) *Verifier {
	return &Verifier{
		logger: logger,
		bkt:    bkt,
		issues: issues,
		repair: false,
	}
}

// NewWithRepair returns verifier that logs affected blocks and attempts to repair them.
func NewWithRepair(logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, issues []Issue) *Verifier {
	return &Verifier{
		logger:    logger,
		bkt:       bkt,
		backupBkt: backupBkt,
		issues:    issues,
		repair:    true,
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
		err := issueFn(ctx, v.logger, v.bkt, v.backupBkt, v.repair, idMatcher)
		if err != nil {
			return errors.Wrap(err, "verify")
		}
	}

	level.Info(v.logger).Log("msg", "verify completed", "issues", len(v.issues), "repair", v.repair)
	return nil
}
