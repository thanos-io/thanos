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

// Apply removes blocks depending on the specified retentionByResolution based on blocks MaxTime.
// A value of 0 disables the retention for its resolution.
func ApplyRetentionPolicyByResolution(ctx context.Context, logger log.Logger, bkt objstore.Bucket, retentionByResolution map[ResolutionLevel]time.Duration) error {
	level.Info(logger).Log("msg", "start retention")
	if err := bkt.Iter(ctx, "", func(name string) error {
		id, ok := block.IsBlockDir(name)
		if !ok {
			return nil
		}
		m, err := block.DownloadMeta(ctx, logger, bkt, id)
		if err != nil {
			return errors.Wrap(err, "download metadata")
		}

		retentionDuration, ok := retentionByResolution[ResolutionLevel(m.Thanos.Downsample.Resolution)]
		if !ok {
			level.Info(logger).Log("msg", "retention for resolution undefined", "resolution", m.Thanos.Downsample.Resolution)
			return nil
		}

		if retentionDuration.Seconds() == 0 {
			level.Info(logger).Log("msg", "skip retention for block", "id", id, "resolution", m.Thanos.Downsample.Resolution)
			return nil
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

	level.Info(logger).Log("msg", "retention apply done")
	return nil
}
