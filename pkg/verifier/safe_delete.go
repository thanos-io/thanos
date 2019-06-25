package verifier

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// SafeDelete moves a TSDB block to a backup bucket and on success removes
// it from the source bucket.  It returns error if block dir already exists in
// the backup bucket (blocks should be immutable) or if any of the operations
// fail. When useExistingTempdir is true do not download the block but use the
// previously downloaded block found in tempdir to avoid repeated downloads.
// The tempdir value is ignored unless useExistingTempdir is set to true.
func SafeDelete(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, id ulid.ULID, useExistingTempdir bool, tempdir string) error {
	foundDir := false
	err := backupBkt.Iter(ctx, id.String(), func(name string) error {
		foundDir = true
		return nil
	})
	if err != nil {
		return err
	}

	if foundDir {
		return errors.Errorf("%s dir seems to exists in backup bucket. Remove this block manually if you are sure it is safe to do", id)
	}

	if useExistingTempdir && tempdir == "" {
		return errors.New("instructed to use existing tempdir but no tempdir provided")
	}

	var dir string
	if useExistingTempdir {
		dir = filepath.Join(tempdir, id.String())
		if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
			// If there is any error stat'ing meta.json inside the TSDB block
			// then declare the existing block as bad and refuse to upload it.
			// TODO: Make this check more robust.
			return errors.Wrap(err, "existing tsdb block is invalid")
		}
		level.Info(logger).Log("msg", "using previously downloaded tsdb block",
			"id", id.String())
	} else {
		// tempdir unspecified, create one
		tempdir, err = ioutil.TempDir("", fmt.Sprintf("safe-delete-%s", id.String()))
		if err != nil {
			return err
		}
		// We manage this tempdir so we clean it up on exit.
		defer func() {
			if err := os.RemoveAll(tempdir); err != nil {
				level.Warn(logger).Log("msg", "failed to delete dir", "dir", tempdir, "err", err)
			}
		}()

		dir = filepath.Join(tempdir, id.String())
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			// TSDB block not already present, create it.
			err = os.Mkdir(dir, 0755)
			if err != nil {
				return errors.Wrap(err, "creating tsdb block tempdir")
			}
		} else if err != nil {
			// Error calling Stat() and something is really wrong.
			return errors.Wrap(err, "stat call on directory")
		}
		if err := block.Download(ctx, logger, bkt, id, dir); err != nil {
			return errors.Wrap(err, "download from source")
		}
	}

	if err := block.Upload(ctx, logger, backupBkt, dir); err != nil {
		return errors.Wrap(err, "upload to backup")
	}

	// Block uploaded, so we are ok to remove from src bucket.
	if err := block.Delete(ctx, bkt, id); err != nil {
		return errors.Wrap(err, "delete from source")
	}

	return nil
}
