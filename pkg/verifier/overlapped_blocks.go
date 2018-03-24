package verifier

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

const OverlappedBlocksIssueID = "overlapped_blocks"

// OverlappedBlocksIssue checks bucket for blocks with overlapped time ranges.
// No repair is available for this issue.
func OverlappedBlocksIssue() Issue {
	return func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, repair bool) error {
		level.Info(logger).Log("msg", "started verifying issue", "with-repair", repair, "issue", OverlappedBlocksIssueID)

		overlappedBlocks, err := findOverlappedBlocks(ctx, bkt)
		if err != nil {
			return errors.Wrap(err, OverlappedBlocksIssueID)
		}

		if len(overlappedBlocks) == 0 {
			// All good.
			return nil
		}

		for _, o := range overlappedBlocks {
			var info []string
			for _, m := range o {
				r := time.Duration((m.MaxTime-m.MinTime)/1000) * time.Second
				info = append(info, fmt.Sprintf(
					"[id: %s mint: %d maxt: %d <%s>]",
					m.ULID.String(),
					m.MinTime,
					m.MaxTime,
					r.String(),
				))
			}
			level.Warn(logger).Log("msg", "found overlapped blocks", "group", compact.GroupKey(o[0]), "blocks", strings.Join(info, ","))
		}

		if repair {
			level.Warn(logger).Log("msg", "repair is not implemented for this issue", "issue", OverlappedBlocksIssueID)
		}
		return nil
	}
}

func findOverlappedBlocks(ctx context.Context, bkt objstore.Bucket) (overlappedBlocks [][]block.Meta, err error) {
	var (
		// Sorted ranges from oldest time to newest.
		ranges        = map[string][]block.Meta{}
		overlappedMap = map[ulid.ULID][]block.Meta{}
		handleOverlap = func(g block.Meta, m block.Meta) {
			// We have overlap here.
			overlap := overlappedMap[g.ULID]
			if len(overlap) == 0 {
				overlap = append(overlap, g)
			}
			overlap = append(overlap, m)
			overlappedMap[g.ULID] = overlap
		}
	)

	return overlappedBlocks, compact.ForeachBlockID(ctx, bkt, func(id ulid.ULID) error {
		m, err := compact.DownloadMeta(ctx, bkt, id)
		if err != nil {
			return err
		}

		groupRanges := ranges[compact.GroupKey(m)]
		for _, g := range groupRanges {
			if m.MinTime >= g.MaxTime {
				continue
			}

			if m.MaxTime <= g.MinTime {
				// Prepend - all good.
				groupRanges = append([]block.Meta{m}, groupRanges...)
				return nil
			}


			// TODO(Bplotka): Find all of them!
			// This algorithm will not find all of them if there is at least one overlap. (WIP).
			handleOverlap(g, m)
		}

		// Addend - all good.
		groupRanges = append(groupRanges, m)
		return nil
	})
}
