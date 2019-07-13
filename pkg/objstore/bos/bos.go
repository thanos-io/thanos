package bos

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/baidu/baiducloud-sdk-go/bce"
	"github.com/baidu/baiducloud-sdk-go/bos"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const dirDelim = "/"

// Bucket implements the store.Bucket interface against bos-compatible(Baidu Object Storage) APIs.
type Bucket struct {
	logger log.Logger
	client *bos.Client
	name   string
}

// Config encapsulates the necessary config values to instantiate an bos client.
type Config struct {
	Bucket          string `yaml:"bucket"`
	Region          string `yaml:"region"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
}

func (conf *Config) validate() error {
	if conf.Bucket == "" ||
		conf.Region == "" ||
		conf.AccessKeyID == "" ||
		conf.SecretAccessKey == "" {
		return errors.New("insufficient bos configuration information")
	}
	return nil
}

func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var config *Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, errors.Wrap(err, "parsing bos configuration")
	}
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "validate bos configuration")
	}

	client := bos.NewClient(&bce.Config{
		Region: config.Region,
		Credentials: &bce.Credentials{
			AccessKeyID:     config.AccessKeyID,
			SecretAccessKey: config.SecretAccessKey,
		},
	})

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
	}
	return bkt, nil
}

// Name returns the bucket name for the provider.
func (b *Bucket) Name() string {
	return b.name
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	if err := b.client.DeleteObject(b.name, name, nil); err != nil {
		return err
	}
	return nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	if _, err := b.client.PutObject(b.name, name, r, nil, nil); err != nil {
		return err
	}
	return nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, dirDelim) + dirDelim
	}

	for object := range b.listObjects(ctx, dir) {
		if object.err != nil {
			return object.err
		}
		if object.key == "" {
			continue
		}
		if err := f(object.key); err != nil {
			return err
		}
	}

	return nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, b.name, name, 0, -1)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, b.name, name, off, length)
}

// Exists checks if the given object exists in the bucket.
// TODO(bplotka): Consider removing Exists in favor of helper that do Get & IsObjNotFoundErr (less code to maintain).
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	if _, err := b.client.GetObjectMetadata(b.name, name, nil); err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "get bos object metadata")
	}
	return true, nil
}

func (b *Bucket) Close() error {
	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	switch tmpErr := err.(type) {
	case *bce.Error:
		if tmpErr.StatusCode == http.StatusNotFound {
			return true
		}
	}
	return false
}

func (b *Bucket) getRange(ctx context.Context, bucketName, objectKey string, off, length int64) (io.ReadCloser, error) {
	if len(objectKey) == 0 {
		return nil, errors.Errorf("given object name should not empty")
	}
	getObjectRequest := &bos.GetObjectRequest{
		BucketName: bucketName,
		ObjectKey:  objectKey,
	}
	if length != -1 {
		if err := setRange(getObjectRequest, off, off+length-1); err != nil {
			return nil, err
		}
	}
	obj, err := b.client.GetObjectFromRequest(*getObjectRequest, nil)
	if err != nil {
		return nil, err
	}
	return obj.ObjectContent, nil
}

func setRange(getObjectRequest *bos.GetObjectRequest, start, end int64) error {
	if start >= 0 && end <= 0 {
		getObjectRequest.Range = fmt.Sprintf("%v-", start)
	} else if start >= 0 && start <= end {
		getObjectRequest.Range = fmt.Sprintf("%v-%v", start, end)
	} else {
		return errors.Errorf("Invalid range specified: start=%d end=%d", start, end)
	}
	return nil
}

type objectInfo struct {
	key string
	err error
}

func (b *Bucket) listObjects(ctx context.Context, objectPrefix string) <-chan objectInfo {
	objectsCh := make(chan objectInfo, 1)

	go func(objectsCh chan<- objectInfo) {
		defer close(objectsCh)
		var marker string
		for {
			result, err := b.client.ListObjectsFromRequest(bos.ListObjectsRequest{
				Prefix:    objectPrefix,
				Delimiter: dirDelim,
				Marker:    marker,
				MaxKeys:   1000,
			}, nil)
			if err != nil {
				select {
				case objectsCh <- objectInfo{
					err: err,
				}:
				case <-ctx.Done():
				}
				return
			}

			for _, object := range result.Contents {
				select {
				case objectsCh <- objectInfo{
					key: object.Key,
				}:
				case <-ctx.Done():
					return
				}
			}

			// The result of CommonPrefixes contains the objects
			// that have the same keys between Prefix and the key specified by delimiter.
			for _, obj := range result.GetCommonPrefixes() {
				select {
				case objectsCh <- objectInfo{
					key: obj,
				}:
				case <-ctx.Done():
					return
				}
			}

			if !result.IsTruncated {
				return
			}

			marker = result.NextMarker
		}
	}(objectsCh)
	return objectsCh
}

func configFromEnv() Config {
	c := Config{
		Bucket:          os.Getenv("BOS_BUCKET"),
		Region:          os.Getenv("BOS_REGION"),
		AccessKeyID:     os.Getenv("BOS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("BOS_SECRET_ACCESS_KEY"),
	}
	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := validateForTest(c); err != nil {
		return nil, nil, err
	}

	if c.Bucket != "" {
		if os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
			return nil, nil, errors.New("BOS_BUCKET is defined. Normally this tests will create temporary bucket " +
				"and delete it after test. Unset BOS_BUCKET env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod bucket for test) as well as BOS not being fully strong consistent.")
		}

		bc, err := yaml.Marshal(c)
		if err != nil {
			return nil, nil, err
		}

		b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
		if err != nil {
			return nil, nil, err
		}

		if err := b.Iter(context.Background(), "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "bos check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "BOS bucket for BOS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	src := rand.NewSource(time.Now().UnixNano())

	tmpBucketName := strings.Replace(fmt.Sprintf("test_%x", src.Int63()), "_", "-", -1)
	if len(tmpBucketName) >= 31 {
		tmpBucketName = tmpBucketName[:31]
	}
	c.Bucket = tmpBucketName

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}

	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	if err := b.client.CreateBucket(b.name, nil); err != nil {
		return nil, nil, err
	}
	t.Log("created temporary BOS bucket for BOS tests with name", tmpBucketName)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if err := b.client.DeleteBucket(b.name, nil); err != nil {
			t.Logf("deleting bucket %s failed: %s", tmpBucketName, err)
		}
	}, nil
}

func validateForTest(conf Config) error {
	if conf.Bucket == "" ||
		conf.Region == "" ||
		conf.AccessKeyID == "" ||
		conf.SecretAccessKey == "" {
		return errors.New("insufficient bos configuration information")
	}
	return nil
}
