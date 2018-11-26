package hdfs

import (
	"context"
	"encoding/base64"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2"
	"github.com/fharding1/limit"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// FIXME: Pass on context to HDFS lib somehow (https://medium.com/@zombiezen/canceling-i-o-in-go-capn-proto-5ae8c09c5b29)
// FIXME: Metrics

const (
	partialsDir       = ".partials"
	partialsDirPrefix = partialsDir + "/"
)

var (
	errPartials = errors.Errorf("the %q directory is reserved for internal use", partialsDir)
)

type hdfsBucket struct {
	logger     log.Logger
	bucketPath hdfsPath
	client     *hdfs.Client
}

func NewBucket(logger log.Logger, config []byte) (objstore.Bucket, error) {
	var conf Config
	if err := yaml.Unmarshal(config, &conf); err != nil {
		return nil, err
	}

	return newBucket(logger, &conf)
}

func NewTestBucket(t *testing.T, config *Config) (objstore.Bucket, func(), error) {
	bucket, err := newBucket(log.NewLogfmtLogger(os.Stdout), config)
	if err != nil {
		return nil, nil, err
	}

	closeFn := func() {
		if err := bucket.client.RemoveAll(string(bucket.bucketPath)); err != nil {
			t.Errorf("failed to delete bucket: %v", err)
		}
		if err := bucket.Close(); err != nil {
			t.Errorf("failed to close HDFS client: %v", err)
		}
	}

	return bucket, closeFn, nil
}

func newBucket(logger log.Logger, config *Config) (*hdfsBucket, error) {
	bucketPath, err := config.validate()
	if err != nil {
		return nil, err
	}

	dialFunc := (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext

	ctxDialFunc := func(ctx context.Context, network, address string) (net.Conn, error) {
		level.Debug(logger).Log("msg", "Dialing HDFS", "ctx", ctx, "network", network, "address", address)
		return dialFunc(ctx, network, address)
	}

	opts := hdfs.ClientOptions{
		Addresses:           config.NamenodeAddresses,
		User:                config.Username,
		UseDatanodeHostname: config.UseDatanodeHostnames,
		NamenodeDialFunc:    ctxDialFunc,
		DatanodeDialFunc:    ctxDialFunc,
	}

	client, err := hdfs.NewClient(opts)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to %v", opts.Addresses)
	}

	partialsPath, err := bucketPath.join(partialsDir)
	if err != nil {
		return nil, err
	}

	if err := mkdirAll(client, partialsPath); err != nil {
		if closeErr := client.Close(); closeErr != nil {
			return nil, moreErrors([]error{err, closeErr})
		}
		return nil, err
	}

	return &hdfsBucket{
		logger:     logger,
		bucketPath: bucketPath,
		client:     client,
	}, nil
}

// Name returns the bucket name for the provider.
func (h *hdfsBucket) Name() string {
	return string(h.bucketPath)
}

func (h *hdfsBucket) Close() error {
	return h.client.Close()
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (h *hdfsBucket) Iter(ctx context.Context, dir string, callback func(string) error) error {
	if strings.HasSuffix(dir, "/") {
		dir = dir[:len(dir)-1]
	}

	reader, _, err := h.open(ctx, dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	prefix := dir
	if prefix != "" {
		prefix += "/"
	}

	return allErrors(
		iter(reader, prefix, callback),
		reader.Close(),
	)
}

// Get returns a reader for the given object name.
func (h *hdfsBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return h.openFile(ctx, name)
}

// GetRange returns a new range reader for the given object name and range.
func (h *hdfsBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	reader, err := h.openFile(ctx, name)
	if err != nil {
		return nil, err
	}

	if _, err := reader.Seek(off, io.SeekStart); err != nil {
		if closeErr := reader.Close(); closeErr != nil {
			return nil, moreErrors([]error{err, closeErr})
		}
		return nil, err
	}

	return limit.ReadCloser(reader, length), nil
}

// Exists checks if the given object exists in the bucket.
func (h *hdfsBucket) Exists(ctx context.Context, name string) (bool, error) {
	path, err := h.fromExternalName(name)
	if err != nil {
		return false, &os.PathError{Op: "stat", Path: name, Err: err}
	}

	info, err := h.client.Stat(string(path))
	if err != nil {
		if h.IsObjNotFoundErr(err) {
			return false, nil
		}

		return false, err
	}

	return !info.IsDir(), nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (h *hdfsBucket) IsObjNotFoundErr(err error) bool {
	return os.IsNotExist(err)
}

// Upload the contents of the reader as an object into the bucket.
func (h *hdfsBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	path, err := h.fromExternalName(name)
	if err != nil {
		return &os.PathError{Op: "create", Path: name, Err: err}
	}

	// Upload the data to a protected path inside the bucket first. HDFS behaves
	// like a real file system, so files are actually visible to other clients
	// directly after their creation. In order to not disturb other Thanos
	// clients, use the partials directory for uploads and do a rename after the
	// upload completed.

	// Encode the object's name to "flatten" deep directory structures
	partialsName := base64.URLEncoding.EncodeToString([]byte(name))
	partialsPath, err := h.bucketPath.join(partialsDir, partialsName)
	if err != nil {
		return &os.PathError{Op: "create", Path: name, Err: err}
	}

	if err := h.upload(partialsPath, r); err != nil {
		return err
	}

	if err := h.rename(partialsPath, path); err != nil {
		return allErrors(err, h.prune(partialsPath))
	}

	return nil
}

// Delete removes the object with the given name.
func (h *hdfsBucket) Delete(ctx context.Context, name string) error {
	path, err := h.fromExternalName(name)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}

	// Capture the case when trying to delete a directory. Directories should
	// never be empty, due to pruning, so calls to h.client.Remove(path) should
	// return ENOTEMPTY, but it feels better to proactively check for that case.
	stat, err := h.client.Stat(string(path))
	if h.IsObjNotFoundErr(err) {
		return &os.PathError{Op: "remove", Path: string(path), Err: os.ErrNotExist}
	}
	if err != nil {
		return err
	}
	if stat.IsDir() {
		// TODO: Return os.ErrNotExist here? Strictly speaking, this object doesn't
		// exist, but its name is obstructed by a directory.
		return &os.PathError{Op: "remove", Path: string(path), Err: syscall.EISDIR}
	}

	// Okay, path exists and is not a directory: prune it!
	if err := h.prune(path); err != nil {
		return &os.PathError{Op: "remove", Path: string(path), Err: err}
	}

	return nil
}

func isPartials(name string) bool {
	return name == partialsDir || strings.HasPrefix(name, partialsDirPrefix)
}

func (h *hdfsBucket) fromExternalName(name string) (hdfsPath, error) {
	if isPartials(name) {
		return invalidHdfsPath, errPartials
	}

	if name == "" {
		return h.bucketPath, nil
	}

	return h.bucketPath.join(name)
}

func (h *hdfsBucket) open(ctx context.Context, name string) (*hdfs.FileReader, hdfsPath, error) {
	path, err := h.fromExternalName(name)
	if err != nil {
		return nil, path, &os.PathError{Op: "open", Path: name, Err: err}
	}

	reader, err := h.client.Open(string(path))
	return reader, path, err
}

func (h *hdfsBucket) openFile(ctx context.Context, name string) (*hdfs.FileReader, error) {
	reader, path, err := h.open(ctx, name)
	if err == nil && reader.Stat().IsDir() {
		return nil, allErrors(
			&os.PathError{Op: "open", Path: string(path), Err: syscall.EISDIR},
			reader.Close(),
		)
	}

	return reader, err
}

func iter(reader *hdfs.FileReader, prefix string, callback func(string) error) error {
	// TODO: Clarify if ordering is important, or if it's just an implementation
	// detail of the acceptance tests. If ordering is not important, remove the
	// client-side sorting code here and don't fetch the whole directory
	// contents in memory, but use a fetch size > 0.
	readDirFetchSize := 0
	content, err := reader.Readdir(readDirFetchSize)
	if err != nil {
		return err
	}

	sort.Slice(content, func(i, j int) bool {
		if content[i].IsDir() != content[j].IsDir() {
			return content[j].IsDir()
		}
		return content[i].Name() < content[j].Name()
	})

	for _, info := range content {
		name := prefix + info.Name()

		// Hide the internal stuff
		if isPartials(name) {
			continue
		}

		if info.IsDir() {
			name += "/"
		}

		if err := callback(name); err != nil {
			return err
		}
	}

	return nil
}

func (h *hdfsBucket) upload(path hdfsPath, r io.Reader) error {
	writer, err := h.create(path)
	if err != nil {
		return err
	}

	level.Debug(h.logger).Log("msg", "starting upload", "path", path)

	written, err := io.Copy(writer, r)
	err = allErrors(err, writer.Close())
	if err != nil {
		return allErrors(err, h.prune(path))
	}

	level.Debug(h.logger).Log("msg", "upload complete", "path", path, "written", written)
	return nil
}

func mkdirAll(c *hdfs.Client, path hdfsPath) error {
	if err := c.MkdirAll(string(path), os.FileMode(0755)); err != nil {
		return errors.Wrapf(err, "failed to create directory %q", path)
	}

	return nil
}

func (h *hdfsBucket) create(path hdfsPath) (*hdfs.FileWriter, error) {
	writer, err := h.doOnPath(path, func() (io.Closer, error) {
		return h.client.Create(string(path))
	})

	if err != nil {
		return nil, err
	}

	if writer, ok := writer.(*hdfs.FileWriter); ok {
		return writer, nil
	}

	return nil, allErrors(
		errors.New("internal error"),
		writer.Close(),
		h.prune(path),
	)
}

func (h *hdfsBucket) rename(from, to hdfsPath) error {
	_, err := h.doOnPath(to, func() (io.Closer, error) {
		err := h.client.Rename(string(from), string(to))
		return nil, err
	})

	if err == nil {
		level.Debug(h.logger).Log("msg", "renamed file", "from", from, "to", to)
	}
	return err
}

func (h *hdfsBucket) doOnPath(path hdfsPath, fn func() (io.Closer, error)) (io.Closer, error) {
	closer, err := fn()
	if err == nil {
		return closer, nil
	}

	if !h.IsObjNotFoundErr(err) {
		return nil, err
	}

	// try to create parent dir
	parent, ok := path.parent()
	if !ok || !h.bucketPath.isParentOf(parent) {
		return nil, errors.Errorf("refusing to create parent directory for %q", path)
	}

	if err := mkdirAll(h.client, parent); err != nil {
		return nil, err
	}

	closer, err = fn()
	if err != nil {
		return nil, allErrors(err, h.prune(path))
	}

	return closer, nil
}

func (h *hdfsBucket) prune(leafPath hdfsPath) error {
	currentPath := leafPath

	for h.bucketPath.isParentOf(currentPath) {
		if err := h.client.Remove(string(currentPath)); err != nil {
			if h.IsObjNotFoundErr(err) {
				// Not found should only happen at the leaf path. Don't exit yet, but
				// try to delete it's parent, if it is empty.
				if currentPath != leafPath {
					return err
				}
			} else {
				if pathErr, ok := err.(*os.PathError); ok && pathErr.Err == syscall.ENOTEMPTY {
					return nil // ok, found a non-empty parent path, all good!
				}

				return err
			}
		}

		parentPath, ok := currentPath.parent()
		if !ok {
			return errors.Errorf("failed to determine parent directory of %q", currentPath)
		}

		currentPath = parentPath
	}

	if currentPath == h.bucketPath {
		return nil // reached the bucket's root
	}

	return errors.Errorf("refusing to prune %q", currentPath)
}

type moreErrors []error

func (errs moreErrors) Error() string {
	var errStrings []string
	for _, err := range errs {
		errStrings = append(errStrings, err.Error())
	}

	return strings.Join(errStrings, "; ")
}

func allErrors(allErrors ...error) error {
	var errs []error
	for _, err := range allErrors {
		if err != nil {
			if moreErrs, ok := err.(moreErrors); ok {
				errs = append(errs, moreErrs...)
			} else {
				errs = append(errs, err)
			}
		}
	}

	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	}

	return moreErrors(errs)
}
