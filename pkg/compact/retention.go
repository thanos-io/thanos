package compact

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/pkg/errors"
)

// Apply removes blocks older than rententionDuration based on blocks MaxTime.
func ApplyDefaultRetentionPolicy(ctx context.Context, logger log.Logger, bkt objstore.Bucket, retentionDuration time.Duration) error {
	level.Info(logger).Log("msg", "start default retention")
	if err := bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		m, err := block.DownloadMeta(ctx, logger, bkt, id)
		if err != nil {
			return errors.Wrap(err, "download metadata")
		}

		maxTime := time.Unix(m.MaxTime/1000, 0)
		if time.Now().After(maxTime.Add(retentionDuration)) {
			level.Info(logger).Log("msg", "deleting block", "id", id, "maxTime", maxTime.String())
			if err := block.Delete(ctx, bkt, id); err != nil {
				return errors.Wrap(err, "delete block")
			}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "retention")
	}

	level.Info(logger).Log("msg", "default retention apply done")
	return nil
}

// Apply removes blocks of resolution older than rententionDuration based on blocks MaxTime.
func ApplyRetentionPolicyByResolution(ctx context.Context, logger log.Logger, bkt objstore.Bucket, retentionDuration time.Duration, resolution int64) error {
	level.Info(logger).Log("msg", fmt.Sprintf("start retention of resolution %d", resolution))
	if err := bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		m, err := block.DownloadMeta(ctx, logger, bkt, id)
		if err != nil {
			return errors.Wrap(err, "download metadata")
		}

		if m.Thanos.Downsample.Resolution == resolution {
			maxTime := time.Unix(m.MaxTime/1000, 0)
			if time.Now().After(maxTime.Add(retentionDuration)) {
				level.Info(logger).Log("msg", "deleting block", "id", id, "maxTime", maxTime.String())
				if err := block.Delete(ctx, bkt, id); err != nil {
					return errors.Wrap(err, "delete block")
				}
			}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "retention")
	}

	level.Info(logger).Log("msg", fmt.Sprintf("retention of resolution %d apply done", resolution))
	return nil
}
