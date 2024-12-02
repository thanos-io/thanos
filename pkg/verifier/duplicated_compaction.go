// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
)

// DuplicatedCompactionBlocks is for bug fixed in https://github.com/thanos-io/thanos/commit/94e26c63e52ba45b713fd998638d0e7b2492664f.
// Bug resulted in source block not being removed immediately after compaction, so we were compacting again and again same sources
// until sync-delay passes.
// The expected print of this are same overlapped blocks with exactly the same sources, time ranges and stats.
// If repair is enabled, all but one duplicates are safely deleted.
type DuplicatedCompactionBlocks struct{}

func (DuplicatedCompactionBlocks) IssueID() string { return "duplicated_compaction" }

func (DuplicatedCompactionBlocks) VerifyRepair(ctx Context, idMatcher func(ulid.ULID) bool, repair bool) error {
	if idMatcher != nil {
		return errors.Errorf("id matching is not supported")
	}

	level.Info(ctx.Logger).Log("msg", "started verifying issue", "with-repair", repair)

	overlaps, err := fetchOverlaps(ctx, ctx.Fetcher)
	if err != nil {
		return errors.Wrap(err, "fetch overlaps")
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
				level.Warn(ctx.Logger).Log("msg", "found duplicated blocks", "group", k, "range-min", r.Min, "range-max", r.Max, "kill", sprintMetas(d[1:]))

				for _, m := range d[1:] {
					if _, ok := toKillLookup[m.ULID]; ok {
						continue
					}

					toKillLookup[m.ULID] = struct{}{}
					toKill = append(toKill, m.ULID)
				}
			}

			if len(dups) == 0 {
				level.Warn(ctx.Logger).Log("msg", "found overlapped blocks, but all of the blocks are unique. Seems like unrelated issue. Ignoring overlap", "group", k,
					"range", fmt.Sprintf("%v", r), "overlap", sprintMetas(blocks))
			}
		}
	}

	level.Warn(ctx.Logger).Log("msg", "Found duplicated blocks that are ok to be removed", "ULIDs", fmt.Sprintf("%v", toKill), "num", len(toKill))
	if !repair {
		return nil
	}

	for i, id := range toKill {
		if err := BackupAndDelete(ctx, id); err != nil {
			return err
		}
		level.Info(ctx.Logger).Log("msg", "Removed duplicated block", "id", id, "to-be-removed", len(toKill)-(i+1), "removed", i+1)
	}

	level.Info(ctx.Logger).Log("msg", "Removed all duplicated blocks. You might want to rerun this verify to check if there is still any unrelated overlap")
	return nil
}

// duplicatedBlocks returns duplicated blocks that have exactly same range, sources and stats.
// If block is unique it is not included in the resulted blocks.
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

func sameULIDSlices(a, b []ulid.ULID) bool {
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
