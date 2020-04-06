// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// TSDBBlockExistsInBucket checks to see if a given TSDB block ID exists in a
// bucket. If so, true is returned. An error is returned on failure and in
// such case the boolean result has no meaning.
func TSDBBlockExistsInBucket(ctx context.Context, bkt objstore.Bucket, id ulid.ULID) (bool, error) {
	foundDir := false
	err := bkt.Iter(ctx, id.String(), func(name string) error {
		foundDir = true
		return nil
	})

	return foundDir, err
}

// BackupAndDelete moves a TSDB block to a backup bucket and on success removes
// it from the source bucket. If deleteDelay is zero, block is removed from source bucket.
// else the block is marked for deletion.
// It returns error if block dir already exists in
// the backup bucket (blocks should be immutable) or if any of the operations
// fail.
func BackupAndDelete(ctx context.Context, logger log.Logger, bkt, backupBkt objstore.Bucket, id ulid.ULID, deleteDelay time.Duration, blocksMarkedForDeletion prometheus.Counter) error {
	// Does this TSDB block exist in backupBkt already?
	found, err := TSDBBlockExistsInBucket(ctx, backupBkt, id)
	if err != nil {
		return err
	}
	if found {
		return errors.Errorf("%s dir seems to exists in backup bucket. Remove this block manually if you are sure it is safe to do", id)
	}

	// Create a tempdir to locally store TSDB block.
	tempdir, err := ioutil.TempDir("", fmt.Sprintf("safe-delete-%s", id.String()))
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tempdir); err != nil {
			level.Warn(logger).Log("msg", "failed to delete dir", "dir", tempdir, "err", err)
		}
	}()

	// Download the TSDB block.
	dir := filepath.Join(tempdir, id.String())
	if err := block.Download(ctx, logger, bkt, id, dir); err != nil {
		return errors.Wrap(err, "download from source")
	}

	// Backup the block.
	if err := backupDownloaded(ctx, logger, dir, backupBkt, id); err != nil {
		return err
	}

	// Block uploaded, so we are ok to remove from src bucket.
	if deleteDelay.Seconds() == 0 {
		level.Info(logger).Log("msg", "Deleting block", "id", id.String())
		if err := block.Delete(ctx, logger, bkt, id); err != nil {
			return errors.Wrap(err, "delete from source")
		}
	}

	level.Info(logger).Log("msg", "Marking block as deleted", "id", id.String())
	if err := block.MarkForDeletion(ctx, logger, bkt, id, blocksMarkedForDeletion); err != nil {
		return errors.Wrap(err, "marking delete from source")
	}
	return nil
}

// BackupAndDeleteDownloaded works much like BackupAndDelete in that it will
// move a TSDB block from a bucket to a backup bucket. If deleteDelay param is zero, block is removed from source bucket.
// else the block is marked for deletion. The bdir parameter
// points to the location on disk where the TSDB block was previously
// downloaded allowing this function to avoid downloading the TSDB block from
// the source bucket again. An error is returned if any operation fails.
func BackupAndDeleteDownloaded(ctx context.Context, logger log.Logger, bdir string, bkt, backupBkt objstore.Bucket, id ulid.ULID, deleteDelay time.Duration, blocksMarkedForDeletion prometheus.Counter) error {
	// Does this TSDB block exist in backupBkt already?
	found, err := TSDBBlockExistsInBucket(ctx, backupBkt, id)
	if err != nil {
		return err
	}
	if found {
		return errors.Errorf("%s dir seems to exists in backup bucket. Remove this block manually if you are sure it is safe to do", id)
	}

	// Backup the block.
	if err := backupDownloaded(ctx, logger, bdir, backupBkt, id); err != nil {
		return err
	}

	// Block uploaded, so we are ok to remove from src bucket.
	if deleteDelay.Seconds() == 0 {
		level.Info(logger).Log("msg", "Deleting block", "id", id.String())
		if err := block.Delete(ctx, logger, bkt, id); err != nil {
			return errors.Wrap(err, "delete from source")
		}
		return nil
	}

	level.Info(logger).Log("msg", "Marking block as deleted", "id", id.String())
	if err := block.MarkForDeletion(ctx, logger, bkt, id, blocksMarkedForDeletion); err != nil {
		return errors.Wrap(err, "marking delete from source")
	}
	return nil
}

// backupDownloaded is a helper function that uploads a TSDB block
// found on disk to the given bucket. An error is returned if any operation
// fails.
func backupDownloaded(ctx context.Context, logger log.Logger, bdir string, backupBkt objstore.Bucket, id ulid.ULID) error {
	// Safety checks.
	if _, err := os.Stat(filepath.Join(bdir, "meta.json")); err != nil {
		// If there is any error stat'ing meta.json inside the TSDB block
		// then declare the existing block as bad and refuse to upload it.
		// TODO: Make this check more robust.
		return errors.Wrap(err, "existing tsdb block is invalid")
	}

	// Upload the on disk TSDB block.
	level.Info(logger).Log("msg", "Uploading block to backup bucket", "id", id.String())
	if err := block.Upload(ctx, logger, backupBkt, bdir); err != nil {
		return errors.Wrap(err, "upload to backup")
	}

	return nil
}
