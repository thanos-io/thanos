package inmem

import (
	"context"
	"io"

	"bytes"
	"io/ioutil"
	"strings"

	"path"

	"github.com/pkg/errors"
)

// Bucket implements the store.Bucket and shipper.Bucket interfaces against local memory.
type Bucket struct {
	objects map[string][]byte
	dirs    map[string]struct{}
}

// NewBucket returns a new in memory Bucket.
// NOTE: Returned bucket is just a naive in memory bucket implementation. For test use cases only.
func NewBucket() *Bucket {
	return &Bucket{
		objects: map[string][]byte{},
		dirs:    map[string]struct{}{},
	}
}

// Objects returns internally stored objects.
// NOTE: For assert purposes.
func (b *Bucket) Objects() map[string][]byte {
	return b.objects
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(_ context.Context, dir string, f func(string) error) error {
	for filename := range b.objects {
		if !strings.HasPrefix(filename, dir) {
			continue
		}

		if err := f(filename); err != nil {
			return err
		}
	}
	// Mimick GCS behaviour where dirs are also listed separately.
	for dirName := range b.dirs {
		if !strings.HasPrefix(dirName, dir) {
			continue
		}

		// Again, mimick GCS and add trailing slash.
		if err := f(dirName + "/"); err != nil {
			return err
		}
	}
	return nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(_ context.Context, name string) (io.ReadCloser, error) {
	file, ok := b.objects[name]
	if !ok {
		return nil, errors.Errorf("no such file %s", name)
	}

	return ioutil.NopCloser(bytes.NewReader(file)), nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(_ context.Context, name string, off, length int64) (io.ReadCloser, error) {
	file, ok := b.objects[name]
	if !ok {
		return nil, errors.Errorf("no such file %s", name)
	}

	if int64(len(file)) < off {
		return nil, errors.Errorf("offset larger than content length. Len %d. Offset: %v", len(file), off)
	}

	if int64(len(file)) <= off+length {
		// Just return maximum of what we have.
		length = int64(len(file)) - off
	}

	return ioutil.NopCloser(bytes.NewReader(file[off : off+length])), nil
}

// Exists checks if the given directory exists in memory.
func (b *Bucket) Exists(_ context.Context, dir string) (bool, error) {
	for filename := range b.objects {
		if !strings.HasPrefix(filename, dir) {
			continue
		}

		return true, nil
	}

	return false, nil
}

// Upload writes the file specified in src to into the memory.
func (b *Bucket) Upload(_ context.Context, src, target string) error {
	body, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}

	b.dirs[path.Dir(target)] = struct{}{}
	b.objects[target] = body
	return nil
}

// Delete removes all data prefixed with the dir.
func (b *Bucket) Delete(_ context.Context, dir string) error {
	for filename := range b.objects {
		if !strings.HasPrefix(filename, dir) {
			continue
		}

		delete(b.objects, filename)
	}
	delete(b.objects, dir)
	return nil
}
