package objstore

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
)

// Bucket provides read and write access to an object storage bucket.
// NOTE: We assume strong consistency for write-read flow.
type Bucket interface {
	io.Closer
	BucketReader

	// Upload the contents of the reader as an object into the bucket.
	Upload(ctx context.Context, name string, r io.Reader) error

	// Delete removes the object with the given name.
	Delete(ctx context.Context, name string) error

	// Name returns the bucket name for the provider.
	Name() string
}

// BucketReader provides read access to an object storage bucket.
type BucketReader interface {
	// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
	// object name including the prefix of the inspected directory.
	Iter(ctx context.Context, dir string, f func(string) error) error

	// GetObjectNameList gets list of all objects names in the given directory. The object list contains the full
	// object name including the prefix of the inspected directory.
	GetObjectNameList(ctx context.Context, dir string) (ObjectNameList, error)

	// Get returns a reader for the given object name.
	Get(ctx context.Context, name string) (io.ReadCloser, error)

	// GetRange returns a new range reader for the given object name and range.
	GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)

	// Exists checks if the given object exists in the bucket.
	// TODO(bplotka): Consider removing Exists in favor of helper that do Get & IsObjNotFoundErr (less code to maintain).
	Exists(ctx context.Context, name string) (bool, error)

	// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
	IsObjNotFoundErr(err error) bool
}

// ObjectNameList contains the full object name including the prefix of the inspected directory.
type ObjectNameList []string

// UploadDir uploads all files in srcdir to the bucket with into a top-level directory
// named dstdir. It is a caller responsibility to clean partial upload in case of failure.
func UploadDir(ctx context.Context, logger log.Logger, bkt Bucket, srcdir, dstdir string) error {
	df, err := os.Stat(srcdir)
	if err != nil {
		return errors.Wrap(err, "stat dir")
	}
	if !df.IsDir() {
		return errors.Errorf("%s is not a directory", srcdir)
	}
	return filepath.Walk(srcdir, func(src string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		dst := filepath.Join(dstdir, strings.TrimPrefix(src, srcdir))

		return UploadFile(ctx, logger, bkt, src, dst)
	})
}

// UploadFile uploads the file with the given name to the bucket.
// It is a caller responsibility to clean partial upload in case of failure
func UploadFile(ctx context.Context, logger log.Logger, bkt Bucket, src, dst string) error {
	r, err := os.Open(src)
	if err != nil {
		return errors.Wrapf(err, "open file %s", src)
	}
	defer runutil.CloseWithLogOnErr(logger, r, "close file %s", src)

	errorNotifier := func(err error, d time.Duration) {
		level.Warn(logger).Log("msg", "unsuccessful attempt uploading", "src", src, "dst", dst, "err", err, "next", d.String())
	}

	do := func() error {
		if err := bkt.Upload(ctx, dst, r); err != nil {
			return handleErrors(errors.Wrapf(err, "upload file %s", dst))
		}
		return nil
	}

	// TODO: should we clean an uploaded file before retry?
	if err := backoff.RetryNotify(do, getCustomBackoff(ctx), errorNotifier); err != nil {
		return errors.Wrapf(err, "upload file %s as %s", src, dst)
	}
	return nil
}

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// DeleteDir removes all objects prefixed with dir from the bucket.
func DeleteDir(ctx context.Context, bucket Bucket, dir string) error {
	// Delete directory.
	do := func() error {
		if err := deleteDir(ctx, bucket, dir); err != nil {
			return handleErrors(errors.Wrapf(err, "delete block dir %s", dir))
		}
		return nil
	}

	if err := backoff.Retry(do, getCustomBackoff(ctx)); err != nil {
		return errors.Wrapf(err, "delete block dir %s", dir)
	}
	return nil
}

// deleteDir is an internal function to delete directory recursively without retries.
func deleteDir(ctx context.Context, bucket Bucket, dir string) error {
	return bucket.Iter(ctx, dir, func(name string) error {
		// If we hit a directory, call DeleteDir recursively.
		if strings.HasSuffix(name, DirDelim) {
			return deleteDir(ctx, bucket, name)
		}
		return bucket.Delete(ctx, name)
	})
}

// GetFile downloads file from bucket and returns bytes.
func GetFile(ctx context.Context, logger log.Logger, bucket BucketReader, src string) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	errorNotifier := func(err error, d time.Duration) {
		level.Warn(logger).Log("msg", "unsuccessful attempt to download", "err", err, "next", d.String())
	}

	do := func() error {
		rc, err := bucket.Get(ctx, src)
		if err != nil {
			return handleErrors(errors.Wrapf(err, "download file %s from bucket", src))
		}
		defer runutil.CloseWithLogOnErr(logger, rc, "download file")

		if _, err := io.Copy(buf, rc); err != nil {
			return handleErrors(errors.Wrap(err, "download file"))
		}

		return nil
	}

	if err := backoff.RetryNotify(do, getCustomBackoff(ctx), errorNotifier); err != nil {
		return nil, errors.Wrapf(err, "download file %s", src)
	}

	return buf.Bytes(), nil
}

// DownloadFile downloads the src file from the bucket to dst. If dst is an existing
// directory, a file with the same name as the source is created in dst.
// If destination file is already existing, download file will overwrite it.
func DownloadFile(ctx context.Context, logger log.Logger, bkt BucketReader, src, dst string) error {
	if fi, err := os.Stat(dst); err == nil {
		if fi.IsDir() {
			dst = filepath.Join(dst, filepath.Base(src))
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer runutil.CloseWithLogOnErr(logger, f, "download block's output file")

	defer func() {
		if err != nil {
			if rerr := os.Remove(dst); rerr != nil {
				level.Warn(logger).Log("msg", "failed to remove partially downloaded file", "file", dst, "err", rerr)
			}
		}
	}()

	errorNotifier := func(err error, d time.Duration) {
		level.Warn(logger).Log("msg", "unsuccessful attempt to download", "err", err, "next", d.String())
	}

	do := func() error {
		rc, err := bkt.Get(ctx, src)
		if err != nil {
			return handleErrors(errors.Wrap(err, "get file"))
		}
		defer runutil.CloseWithLogOnErr(logger, rc, "download block's file reader")

		if _, err = io.Copy(f, rc); err != nil {
			return handleErrors(errors.Wrap(err, "copy object to file"))
		}
		return nil
	}
	err = backoff.RetryNotify(do, getCustomBackoff(ctx), errorNotifier)
	if err != nil {
		return errors.Wrapf(err, "failed to download file %s", src)
	}
	return nil
}

// DownloadDir downloads all object found in the directory into the local directory.
func DownloadDir(ctx context.Context, logger log.Logger, bucket BucketReader, src, dst string) error {
	var (
		err             error
		downloadedFiles []string
	)
	err = os.MkdirAll(dst, 0777)
	if err != nil {
		return errors.Wrap(err, "create dir")
	}

	defer func() {
		if err != nil {
			// Best-effort cleanup if the download failed.
			for _, f := range downloadedFiles {
				if rErr := os.Remove(f); rErr != nil {
					level.Warn(logger).Log("msg", "failed to remove file on partial dir download error", "file", f, "err", rErr)
				}
			}
		}
	}()

	list, err := GetObjectNameList(ctx, logger, bucket, src)
	if err != nil {
		return errors.Wrapf(err, "failed to download dir %s", src)
	}

	for _, name := range list {
		if strings.HasSuffix(name, DirDelim) {
			return DownloadDir(ctx, logger, bucket, name, filepath.Join(dst, filepath.Base(name)))
		}
		if err := DownloadFile(ctx, logger, bucket, name, dst); err != nil {
			return err
		}

		downloadedFiles = append(downloadedFiles, dst)
	}

	return nil
}

// GetObjectNameList returns list of object names, each name is a the full object name including the prefix of the
// inspected directory
func GetObjectNameList(ctx context.Context, logger log.Logger, bucket BucketReader, src string) (ObjectNameList, error) {
	var list ObjectNameList

	errorNotifier := func(err error, d time.Duration) {
		level.Warn(logger).Log("msg", "unsuccessful attempt to get object list", "err", err, "next", d.String())
	}

	do := func() error {
		var err error
		list, err = bucket.GetObjectNameList(ctx, src)
		if err != nil {
			return handleErrors(errors.Wrap(err, "get object list"))
		}
		return nil
	}

	if err := backoff.RetryNotify(do, getCustomBackoff(ctx), errorNotifier); err != nil {
		return list, errors.Wrapf(err, "failed to get object list %s", src)
	}
	return list, nil
}

// Exists checks if file exists with backoff retries.
func Exists(ctx context.Context, logger log.Logger, bucket BucketReader, src string) (bool, error) {
	var ok bool

	errorNotifier := func(err error, d time.Duration) {
		level.Warn(logger).Log("msg", "unsuccessful attempt getting the file/dir", "src", src, "err", err, "next", d.String())
	}

	do := func() error {
		var err error
		ok, err = bucket.Exists(ctx, src)
		if err != nil {
			return handleErrors(errors.Wrap(err, "check exists"))
		}
		return nil
	}

	err := backoff.RetryNotify(do, getCustomBackoff(ctx), errorNotifier)
	if err != nil {
		return ok, errors.Wrapf(err, "failed to check file %s", src)
	}

	return ok, nil
}

// handleErrors handles net error and wraps in permanent otherwise.
func handleErrors(err error) error {
	cErr := errors.Cause(err)
	if _, ok := cErr.(net.Error); !ok {
		return backoff.Permanent(errors.Wrap(err, "permanent error"))
	}
	return err
}

// getCustomBackoff returns custom backoff to be used to retry operations.
func getCustomBackoff(ctx context.Context) backoff.BackOff {
	// TODO(xjewer): customize backoff with the data from context
	return backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
}
