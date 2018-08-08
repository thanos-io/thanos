package verifier

import (
	"context"

	"fmt"

	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
)

const DuplicatedCompactionIssueID = "duplicated_compaction"

// DuplicatedCompactionIssue was a bug fixed in https://github.com/improbable-eng/thanos/commit/94e26c63e52ba45b713fd998638d0e7b2492664f.
// Bug resulted in source block not being removed immediately after compaction, so we were compacting again and again same sources
// until sync-delay passes.
// The expected print of this are same overlapped blocks with exactly the same sources, time ranges and stats.
// If repair is enabled, all but one duplicates are safely deleted.
func DuplicatedCompactionIssue(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, repair bool, idMatcher func(ulid.ULID) bool) error {
	if idMatcher != nil {
		return errors.Errorf("id matching is not supported by issue %s verifier", DuplicatedCompactionIssueID)
	}

	level.Info(logger).Log("msg", "started verifying issue", "with-repair", repair, "issue", DuplicatedCompactionIssueID)

	overlaps, err := fetchOverlaps(ctx, logger, bkt)
	if err != nil {
		return errors.Wrap(err, DuplicatedCompactionIssueID)
	}

	if len(overlaps) == 0 {
		// All good.
		return nil
	}

	// We have overlaps, let's see if they include exact duplicates. If yes, let's put them into distinct set.
	var (
		toKillLookup = map[ulid.ULID]struct{}{}
		toKill       []ulid.ULID
	)

	// Loop over label-resolution groups.
	for k, o := range overlaps {
		// Loop over overlap group.
		for r, blocks := range o {
			dups := duplicatedBlocks(blocks)

			// Loop over duplicates sets.
			for _, d := range dups {
				level.Warn(logger).Log("msg", "found duplicated blocks", "group", k, "range-min", r.Min, "range-max", r.Max,
					"kill", sprintMetas(d[1:]), "issue", DuplicatedCompactionIssueID)

				for _, m := range d[1:] {
					if _, ok := toKillLookup[m.ULID]; ok {
						continue
					}

					toKillLookup[m.ULID] = struct{}{}
					toKill = append(toKill, m.ULID)
				}
			}

			if len(dups) == 0 {
				level.Warn(logger).Log("msg", "found overlapped blocks, but all of the blocks are unique. Seems like unrelated issue. Ignoring overlap", "group", k,
					"range", fmt.Sprintf("%v", r), "overlap", sprintMetas(blocks), "issue", DuplicatedCompactionIssueID)
			}
		}
	}

	level.Warn(logger).Log("msg", "Found duplicated blocks that are ok to be removed", "ULIDs", fmt.Sprintf("%v", toKill), "num", len(toKill), "issue", DuplicatedCompactionIssueID)
	if !repair {
		return nil
	}

	for i, id := range toKill {
		if err := SafeDelete(ctx, logger, bkt, backupBkt, id); err != nil {
			return err
		}
		level.Info(logger).Log("msg", "Removed duplicated block", "id", id, "to-be-removed", len(toKill)-(i+1), "removed", i+1, "issue", DuplicatedCompactionIssueID)
	}

	level.Info(logger).Log("msg", "Removed all duplicated blocks. You might want to rerun this verify to check if there is still any unrelated overlap",
		"issue", DuplicatedCompactionIssueID)
	return nil
}

// duplicatedBlocks returns duplicated blocks that have exactly same range, sources and stats.
// If block is unique it is not included in the resulted blocs.
func duplicatedBlocks(blocks []tsdb.BlockMeta) (res [][]tsdb.BlockMeta) {
	var dups [][]tsdb.BlockMeta
	for _, b := range blocks {
		added := false
		for i, d := range dups {
			if d[0].MinTime != b.MinTime || d[0].MaxTime != b.MaxTime {
				continue
			}

			if d[0].Compaction.Level != b.Compaction.Level {
				continue
			}

			if !sameULIDSlices(d[0].Compaction.Sources, b.Compaction.Sources) {
				continue
			}

			if d[0].Stats != b.Stats {
				continue
			}

			dups[i] = append(dups[i], b)
			added = true
			break
		}

		if !added {
			dups = append(dups, []tsdb.BlockMeta{b})
		}
	}

	for _, d := range dups {
		if len(d) < 2 {
			continue
		}
		res = append(res, d)
	}
	return res
}

func sameULIDSlices(a []ulid.ULID, b []ulid.ULID) bool {
	if len(a) != len(b) {
		return false
	}

	for i, m := range a {
		if m.Compare(b[i]) != 0 {
			return false
		}
	}
	return true
}

func sprintMetas(ms []tsdb.BlockMeta) string {
	var infos []string
	for _, m := range ms {
		infos = append(infos, fmt.Sprintf("<ulid: %s, mint: %d, maxt: %d, range: %s>", m.ULID, m.MinTime, m.MaxTime, (time.Duration((m.MaxTime-m.MinTime)/1000)*time.Second).String()))
	}
	return fmt.Sprintf("blocks: %d, [%s]", len(ms), strings.Join(infos, ","))
}
