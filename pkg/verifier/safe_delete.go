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

// SafeDelete moves block to backup bucket and if succeeded, removes it from
// source bucket.  It returns error if block dir already exists in backup
// bucket (blocks should be immutable) or any of the operation fails.  If
// the block has already been downloaded it must exist in tempdir, else it
// will be downloaded to be copied to the backup bucket.  tempdir set to the
// zero value forces download.
func SafeDelete(ctx context.Context, logger log.Logger, bkt objstore.Bucket, backupBkt objstore.Bucket, id ulid.ULID, tempdir string) error {
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

	if tempdir == "" {
		// tempdir unspecified, create one
		tempdir, err = ioutil.TempDir("", fmt.Sprintf("safe-delete-%s", id.String()))
		if err != nil {
			return err
		}
	}
	dir := filepath.Join(tempdir, id.String())

	if _, err := os.Stat(dir); err != nil {
		// TSDB block not already present, download it
		err = os.Mkdir(dir, 0755)
		if err != nil {
			return err
		}
		defer func() {
			if err := os.RemoveAll(tempdir); err != nil {
				level.Warn(logger).Log("msg", "failed to delete dir", "dir", tempdir, "err", err)
			}
		}()

		if err := block.Download(ctx, logger, bkt, id, dir); err != nil {
			return errors.Wrap(err, "download from source")
		}
	} else {
		level.Info(logger).Log("msg", "using previously downloaded tsdb block",
			"id", id.String())
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
