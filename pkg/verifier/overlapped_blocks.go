package verifier

import (
	"context"
	"fmt"
	"sort"
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

		var metas []block.Meta
		err := block.Foreach(ctx, bkt, func(id ulid.ULID) error {
			m, err := block.DownloadMeta(ctx, bkt, id)
			if err != nil {
				return err
			}

			metas = append(metas, m)
			return nil
		})
		if err != nil {
			return errors.Wrap(err, OverlappedBlocksIssueID)
		}

		overlappedBlocks := findOverlappedBlocks(metas)
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

type timestampToMeta struct {
	meta *block.Meta
	t    int64
}

func findOverlappedBlocks(metas []block.Meta) (overlappedBlocks [][]block.Meta) {
	if len(metas) == 0 {
		return overlappedBlocks
	}

	// Add both edge timestamps for each meta.
	var timestamps []timestampToMeta
	for _, m := range metas {
		copy := m
		timestamps = append(timestamps, timestampToMeta{meta: &copy, t: m.MinTime}, timestampToMeta{meta: &copy, t: m.MaxTime})
	}
	sort.Slice(timestamps, func(i int, j int) bool {
		return timestamps[i].t < timestamps[j].t
	})

	// Blocks which we started but not yet finished.
	var rollingBlocks = map[ulid.ULID]*block.Meta{}
	for _, currT := range timestamps {
		_, ok := rollingBlocks[currT.meta.ULID]
		if ok {
			// Let's finish current block if rolling.
			delete(rollingBlocks, currT.meta.ULID)
			continue
		}

		// New block, let's see if it's overlapping with anything rolling.
		if len(rollingBlocks) > 0 {
			var overlaps []block.Meta
			for _, r := range rollingBlocks {
				// It can happen that blocks started or finished in the same time. That's not a collision.
				if r.MinTime == currT.t || r.MaxTime == currT.t {
					continue
				}

				// Overlapping blocks.
				overlaps = append(overlaps, *r)
			}

			if len(overlaps) > 0 {
				// Check if any overlap group contains same blocks. If yes, we can add to group
				// instead of creating new overlap slice.
				found := false
				for i, existingOverlaps := range overlappedBlocks {
					// Sort to be able to compare.
					sort.Slice(overlaps, func(i int, j int) bool {
						return overlaps[i].ULID.Compare(overlaps[j].ULID) < 0
					})
					if metaSliceEquals(existingOverlaps, overlaps) {
						found = true
						overlappedBlocks[i] = append(overlappedBlocks[i], *currT.meta)
						break
					}
				}

				if !found {
					overlaps := append(overlaps, *currT.meta)
					sort.Slice(overlaps, func(i int, j int) bool {
						return overlaps[i].ULID.Compare(overlaps[j].ULID) < 0
					})
					overlappedBlocks = append(overlappedBlocks, overlaps)
				}
			}
		}

		// Start block.
		rollingBlocks[currT.meta.ULID] = currT.meta
	}

	return overlappedBlocks
}

func metaSliceEquals(a []block.Meta, b []block.Meta) bool {
	if len(a) != len(b) {
		return false
	}

	for i, m := range a {
		if m.ULID != b[i].ULID {
			return false
		}
	}
	return true
}
