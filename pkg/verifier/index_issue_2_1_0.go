package verifier

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

const IndexIssueID = "index-issue-prom-2_1"

// IndexIssue verifies TSDB index issue from Prometheus 2.1.
// It rewrites the problematic blocks while fixing repairable inconsistencies.
// If the replacement was created successfully it is uploaded to the bucket and the input
// block is deleted.
func IndexIssue() Issue {
	return func(ctx context.Context, logger log.Logger, bkt objstore.Bucket, repair bool) error {
		level.Info(logger).Log("msg", "started verifying issue", "with-repair", repair, "issue", IndexIssueID)

		err := compact.ForeachBlockID(ctx, bkt, func(id ulid.ULID) error {
			tmpdir, err := ioutil.TempDir("", fmt.Sprintf("index-issue-block-%s", id))
			if err != nil {
				return err
			}
			defer os.RemoveAll(tmpdir)

			// Get index file.
			indexPath := filepath.Join(tmpdir, "index")
			err = objstore.DownloadFile(ctx, bkt, path.Join(id.String(), "index"), indexPath)
			if err != nil {
				return errors.Wrapf(err, "download index file %s", path.Join(id.String(), "index"))
			}

			err = block.VerifyIndex(indexPath)
			if err == nil {
				return nil
			}

			level.Warn(logger).Log("msg", "detected issue", "id", id, "err", err, "issue", IndexIssueID)

			if !repair {
				// Only verify.
				return nil
			}

			level.Info(logger).Log("msg", "repairing block", "id", id, "issue", IndexIssueID)

			meta, err := compact.DownloadMeta(ctx, bkt, id)
			if err != nil {
				return errors.Wrap(err, "download meta file")
			}

			if meta.Thanos.Downsample.Resolution > 0 {
				return errors.New("cannot repair downsampled blocks")
			}

			resid, err := block.Repair(tmpdir, meta.ULID)
			if err != nil {
				return errors.Wrapf(err, "repair failed for block %s", id)
			}

			// Verify repaired block before uploading it.
			if err := block.VerifyIndex(filepath.Join(tmpdir, resid.String(), "index")); err != nil {
				return errors.Wrap(err, "repaired block is invalid")
			}

			level.Info(logger).Log("msg", "create repaired block", "newID", resid, "issue", IndexIssueID)

			err = objstore.UploadDir(ctx, bkt, filepath.Join(tmpdir, resid.String()), resid.String())
			if err != nil {
				return errors.Wrapf(err, "upload of %s failed", resid)
			}
			if err := objstore.DeleteDir(ctx, bkt, id.String()); err != nil {
				return errors.Wrapf(err, "deleting old block %s failed", id)
			}

			return nil
		})

		if err != nil {
			return errors.Wrap(err, IndexIssueID)
		}

		level.Info(logger).Log("msg", "verified issue", "with-repair", repair, "issue", IndexIssueID)
		return nil
	}
}
