package compact

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// ApplyRetentionPolicyByResolution removes blocks depending on the specified retentionByResolution based on blocks MaxTime.
// A value of 0 disables the retention for its resolution.
func ApplyRetentionPolicyByResolution(ctx context.Context, logger log.Logger, bkt objstore.Bucket, retentionByResolution map[ResolutionLevel]time.Duration) error {
	level.Info(logger).Log("msg", "start optional retention")
	mdFetcher := block.NewMetadataFetcher(logger, 4, bkt)
	blockMDs, err := mdFetcher.Fetch(ctx)
	if err != nil {
		return errors.Wrap(err, "fetch block metadata")
	}

	for id, m := range blockMDs {
		if m == nil {
			continue
		}

		retentionDuration := retentionByResolution[ResolutionLevel(m.Thanos.Downsample.Resolution)]
		if retentionDuration.Seconds() == 0 {
			continue
		}

		maxTime := time.Unix(m.MaxTime/1000, 0)
		if time.Now().After(maxTime.Add(retentionDuration)) {
			level.Info(logger).Log("msg", "applying retention: deleting block", "id", id, "maxTime", maxTime.String())
			if err := block.Delete(ctx, logger, bkt, id); err != nil {
				return errors.Wrap(err, "delete block")
			}
		}
	}

	level.Info(logger).Log("msg", "optional retention apply done")
	return nil
}
