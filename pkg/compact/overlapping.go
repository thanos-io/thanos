package compact

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"os"
	"path/filepath"
)

type OverlappingCompactionLifecycleCallback struct {
	overlappingBlocks prometheus.Counter
	metaDir           string
}

func NewOverlappingCompactionLifecycleCallback() *OverlappingCompactionLifecycleCallback {
	return &OverlappingCompactionLifecycleCallback{}
}

// PreCompactionCallback given the assumption that toCompact is sorted by MinTime in ascending order from Planner
// (not guaranteed on MaxTime order), we will detect overlapping blocks and delete them while retaining all others.
func (c *OverlappingCompactionLifecycleCallback) PreCompactionCallback(ctx context.Context, logger log.Logger, cg *Group, toCompact []*metadata.Meta) error {
	if len(toCompact) == 0 {
		return nil
	}
	previous := 0
	for i, m := range toCompact {
		kept := toCompact[previous]
		if previous == 0 || m.Thanos.Source == metadata.ReceiveSource || kept.MaxTime <= m.MinTime {
			// no  overlapping with previous blocks, skip it
			previous = i
			continue
		} else if m.MinTime < kept.MinTime {
			// halt when the assumption is broken, need manual investigation
			return halt(errors.Errorf("later blocks has smaller minTime than previous block: %s -- %s", kept.String(), m.String()))
		}
		if kept.MaxTime >= m.MaxTime {
			level.Warn(logger).Log("msg", "found overlapping block in plan",
				"toKeep", kept.String(), "toDelete", m.String())
			cg.overlappingBlocks.Inc()
			if err := DeleteBlockNow(ctx, logger, cg.bkt, m, c.metaDir); err != nil {
				return retry(err)
			}
			toCompact[i] = nil
		} else {
			err := errors.Errorf("found partially overlapping block: %s -- %s", kept.String(), m.String())
			if cg.enableVerticalCompaction {
				level.Error(logger).Log("msg", "best effort to vertical compact", "err", err)
				previous = i // move to next block
			} else {
				return halt(err)
			}
		}
	}
	return nil
}

func (c *OverlappingCompactionLifecycleCallback) PostCompactionCallback(_ context.Context, _ log.Logger, _ *Group, _ ulid.ULID) error {
	return nil
}

func (c *OverlappingCompactionLifecycleCallback) GetBlockPopulator(_ context.Context, _ log.Logger, _ *Group) (tsdb.BlockPopulator, error) {
	return tsdb.DefaultBlockPopulator{}, nil
}

func FilterNilBlocks(blocks []*metadata.Meta) (res []*metadata.Meta) {
	for _, b := range blocks {
		if b != nil {
			res = append(res, b)
		}
	}
	return res
}

func DeleteBlockNow(ctx context.Context, logger log.Logger, bkt objstore.Bucket, m *metadata.Meta, dir string) error {
	level.Warn(logger).Log("msg", "delete polluted block immediately", "block", m.String(),
		"level", m.Compaction.Level, "source", m.Thanos.Source, "labels", m.Thanos.GetLabels())
	if err := block.Delete(ctx, logger, bkt, m.ULID); err != nil {
		return errors.Wrapf(err, "delete overlapping block %s", m.String())
	}
	if err := os.RemoveAll(filepath.Join(dir, m.ULID.String())); err != nil {
		return errors.Wrapf(err, "remove old block dir %s", m.String())
	}
	return nil
}
