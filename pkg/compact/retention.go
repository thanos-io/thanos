package compact

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/pkg/errors"
)

// RetentionPolicy describes retention policy.
type RetentionPolicy interface {
	Apply(context.Context) error
}

// TimeBasedRetentionPolicy is a retention policy based on blocks age.
type TimeBasedRetentionPolicy struct {
	logger            log.Logger
	bkt               objstore.Bucket
	retentionDuration time.Duration
}

// NewTimeBasedRetentionPolicy creates new TimeBasedRetentionPolicy.
func NewTimeBasedRetentionPolicy(logger log.Logger, bkt objstore.Bucket, retentionDuration time.Duration) *TimeBasedRetentionPolicy {
	return &TimeBasedRetentionPolicy{
		logger:            logger,
		bkt:               bkt,
		retentionDuration: retentionDuration,
	}
}

// Apply removes blocks older than rententionDuration based on blocks MaxTime.
func (tbr *TimeBasedRetentionPolicy) Apply(ctx context.Context) error {
	level.Info(tbr.logger).Log("msg", "start retention", "retentionDuration", tbr.retentionDuration)
	if err := tbr.bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		m, err := block.DownloadMeta(ctx, tbr.logger, tbr.bkt, id)
		if err != nil {
			return errors.Wrap(err, "download metadata")
		}

		maxTime := time.Unix(m.MaxTime/1000, 0)
		if time.Now().After(maxTime.Add(tbr.retentionDuration)) {
			level.Info(tbr.logger).Log("msg", "deleting block", "id", id, "maxTime", maxTime.String())
			if err := block.Delete(ctx, tbr.bkt, id); err != nil {
				return errors.Wrap(err, "delete block")
			}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "retention")
	}

	level.Info(tbr.logger).Log("msg", "retention done")
	return nil
}
