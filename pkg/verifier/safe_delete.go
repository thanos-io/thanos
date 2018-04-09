package verifier

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// SafeDelete moves block to backup bucket and if succeeded, removes it from source bucket.
// It returns error if block dir already exists in backup bucket (blocks should be immutable) or any
// of the operation fails.
func SafeDelete(ctx context.Context, bkt objstore.Bucket, backupBkt objstore.Bucket, id ulid.ULID) error {
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

	dir, err := ioutil.TempDir("", fmt.Sprintf("safe-delete-%s", id))
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	if err := block.Download(ctx, bkt, id, dir); err != nil {
		return errors.Wrap(err, "download from source")
	}

	if err := objstore.UploadDir(ctx, backupBkt, dir, id.String()); err != nil {
		return errors.Wrap(err, "upload to backup")
	}

	// Block uploaded, so we are ok to remove from src bucket.
	if err := objstore.DeleteDir(ctx, bkt, id.String()); err != nil {
		return errors.Wrap(err, "delete from source")
	}

	return nil
}
