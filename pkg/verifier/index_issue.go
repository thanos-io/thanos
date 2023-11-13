// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
)

// IndexKnownIssues verifies any known index issue.
// It rewrites the problematic blocks while fixing repairable inconsistencies.
// If the replacement was created successfully it is uploaded to the bucket and the input
// block is deleted.
// NOTE: This also verifies all indexes against chunks mismatches and duplicates.
type IndexKnownIssues struct{}

func (IndexKnownIssues) IssueID() string { return "index_known_issues" }

func (IndexKnownIssues) VerifyRepair(ctx Context, idMatcher func(ulid.ULID) bool, repair bool) error {
	level.Info(ctx.Logger).Log("msg", "started verifying issue", "with-repair", repair)

	metas, _, err := ctx.Fetcher.Fetch(ctx)
	if err != nil {
		return err
	}

	for id, meta := range metas {
		if idMatcher != nil && !idMatcher(id) {
			continue
		}

		tmpdir, err := os.MkdirTemp("", fmt.Sprintf("index-issue-block-%s-", id))
		if err != nil {
			return err
		}
		defer func() {
			if err := os.RemoveAll(tmpdir); err != nil {
				level.Warn(ctx.Logger).Log("msg", "failed to delete dir", "tmpdir", tmpdir, "err", err)
			}
		}()

		stats, err := verifyIndex(ctx, id, tmpdir, meta)
		if err == nil {
			level.Debug(ctx.Logger).Log("msg", "no issue", "id", id)
			continue
		}

		level.Warn(ctx.Logger).Log("msg", "detected issue", "id", id, "err", err)

		if !repair {
			// Only verify.
			continue
		}

		if err = repairIndex(stats, ctx, id, meta, tmpdir); err != nil {
			level.Error(ctx.Logger).Log("msg", "could not repair index", "err", err)
			continue
		}
		level.Info(ctx.Logger).Log("msg", "all good, continuing", "id", id)
	}

	level.Info(ctx.Logger).Log("msg", "verified issue", "with-repair", repair)
	return nil
}

func repairIndex(stats block.HealthStats, ctx Context, id ulid.ULID, meta *metadata.Meta, dir string) (err error) {
	if stats.OutOfOrderChunks > stats.DuplicatedChunks {
		level.Warn(ctx.Logger).Log("msg", "detected overlaps are not entirely by duplicated chunks. We are able to repair only duplicates", "id", id)
	}

	if stats.OutsideChunks > (stats.CompleteOutsideChunks + stats.Issue347OutsideChunks) {
		level.Warn(ctx.Logger).Log("msg", "detected outsiders are not all 'complete' outsiders or outsiders from https://github.com/prometheus/tsdb/issues/347. We can safely delete only these outsiders", "id", id)
	}

	if meta.Thanos.Downsample.Resolution > 0 {
		return errors.Wrap(err, "cannot repair downsampled blocks")
	}

	level.Info(ctx.Logger).Log("msg", "downloading block for repair", "id", id)
	if err = block.Download(ctx, ctx.Logger, ctx.Bkt, id, path.Join(dir, id.String())); err != nil {
		return errors.Wrapf(err, "download block %s", id)
	}
	level.Info(ctx.Logger).Log("msg", "downloaded block to be repaired", "id", id, "issue")

	level.Info(ctx.Logger).Log("msg", "repairing block", "id", id, "issue")
	resid, err := block.Repair(
		ctx,
		ctx.Logger,
		dir,
		id,
		metadata.BucketRepairSource,
		block.IgnoreCompleteOutsideChunk,
		block.IgnoreDuplicateOutsideChunk,
		block.IgnoreIssue347OutsideChunk,
	)
	if err != nil {
		return errors.Wrapf(err, "repair failed for block %s", id)
	}
	level.Info(ctx.Logger).Log("msg", "verifying repaired block", "id", id, "newID", resid)

	if err := block.VerifyIndex(ctx, ctx.Logger, filepath.Join(dir, resid.String(), block.IndexFilename), meta.MinTime, meta.MaxTime); err != nil {
		return errors.Wrapf(err, "repaired block is invalid %s", resid)
	}

	level.Info(ctx.Logger).Log("msg", "uploading repaired block", "newID", resid)
	if err = block.Upload(ctx, ctx.Logger, ctx.Bkt, filepath.Join(dir, resid.String()), metadata.NoneFunc); err != nil {
		return errors.Wrapf(err, "upload of %s failed", resid)
	}

	level.Info(ctx.Logger).Log("msg", "safe deleting broken block", "id", id, "issue")
	if err := BackupAndDeleteDownloaded(ctx, filepath.Join(dir, id.String()), id); err != nil {
		return errors.Wrapf(err, "safe deleting old block %s failed", id)
	}

	return nil
}

func verifyIndex(ctx Context, id ulid.ULID, dir string, meta *metadata.Meta) (stats block.HealthStats, err error) {
	if err := objstore.DownloadFile(ctx, ctx.Logger, ctx.Bkt, path.Join(id.String(), block.IndexFilename), filepath.Join(dir, block.IndexFilename)); err != nil {
		return stats, errors.Wrapf(err, "download index file %s", path.Join(id.String(), block.IndexFilename))
	}

	stats, err = block.GatherIndexHealthStats(ctx, ctx.Logger, filepath.Join(dir, block.IndexFilename), meta.MinTime, meta.MaxTime)
	if err != nil {
		return stats, errors.Wrapf(err, "gather index issues %s", id)
	}

	level.Debug(ctx.Logger).Log("stats", fmt.Sprintf("%+v", stats), "id", id)

	return stats, stats.AnyErr()
}
