// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
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
			return nil
		}

		tmpdir, err := ioutil.TempDir("", fmt.Sprintf("index-issue-block-%s-", id))
		if err != nil {
			return err
		}
		defer func() {
			if err := os.RemoveAll(tmpdir); err != nil {
				level.Warn(ctx.Logger).Log("msg", "failed to delete dir", "tmpdir", tmpdir, "err", err)
			}
		}()

		if err = objstore.DownloadFile(ctx, ctx.Logger, ctx.Bkt, path.Join(id.String(), block.IndexFilename), filepath.Join(tmpdir, block.IndexFilename)); err != nil {
			return errors.Wrapf(err, "download index file %s", path.Join(id.String(), block.IndexFilename))
		}

		stats, err := block.GatherIndexHealthStats(ctx.Logger, filepath.Join(tmpdir, block.IndexFilename), meta.MinTime, meta.MaxTime)
		if err != nil {
			return errors.Wrapf(err, "gather index issues %s", id)
		}

		level.Debug(ctx.Logger).Log("stats", fmt.Sprintf("%+v", stats), "id", id)
		if err = stats.AnyErr(); err == nil {
			return nil
		}

		level.Warn(ctx.Logger).Log("msg", "detected issue", "id", id, "err", err)

		if !repair {
			// Only verify.
			return nil
		}

		if stats.OutOfOrderChunks > stats.DuplicatedChunks {
			level.Warn(ctx.Logger).Log("msg", "detected overlaps are not entirely by duplicated chunks. We are able to repair only duplicates", "id", id)
		}

		if stats.OutsideChunks > (stats.CompleteOutsideChunks + stats.Issue347OutsideChunks) {
			level.Warn(ctx.Logger).Log("msg", "detected outsiders are not all 'complete' outsiders or outsiders from https://github.com/prometheus/tsdb/issues/347. We can safely delete only these outsiders", "id", id)
		}

		if meta.Thanos.Downsample.Resolution > 0 {
			return errors.New("cannot repair downsampled blocks")
		}

		level.Info(ctx.Logger).Log("msg", "downloading block for repair", "id", id)
		if err = block.Download(ctx, ctx.Logger, ctx.Bkt, id, path.Join(tmpdir, id.String())); err != nil {
			return errors.Wrapf(err, "download block %s", id)
		}
		level.Info(ctx.Logger).Log("msg", "downloaded block to be repaired", "id", id, "issue")

		level.Info(ctx.Logger).Log("msg", "repairing block", "id", id, "issue")
		resid, err := block.Repair(
			ctx.Logger,
			tmpdir,
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

		// Verify repaired block before uploading it.
		if err := block.VerifyIndex(ctx.Logger, filepath.Join(tmpdir, resid.String(), block.IndexFilename), meta.MinTime, meta.MaxTime); err != nil {
			return errors.Wrapf(err, "repaired block is invalid %s", resid)
		}

		level.Info(ctx.Logger).Log("msg", "uploading repaired block", "newID", resid)
		if err = block.Upload(ctx, ctx.Logger, ctx.Bkt, filepath.Join(tmpdir, resid.String()), metadata.NoneFunc); err != nil {
			return errors.Wrapf(err, "upload of %s failed", resid)
		}

		level.Info(ctx.Logger).Log("msg", "safe deleting broken block", "id", id, "issue")
		if err := BackupAndDeleteDownloaded(ctx, filepath.Join(tmpdir, id.String()), id); err != nil {
			return errors.Wrapf(err, "safe deleting old block %s failed", id)
		}
		level.Info(ctx.Logger).Log("msg", "all good, continuing", "id", id)
		return nil
	}

	level.Info(ctx.Logger).Log("msg", "verified issue", "with-repair", repair)
	return nil
}
