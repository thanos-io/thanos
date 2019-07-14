package bos

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/objstore"
)

const partSize = 1024 * 1024 * 128

var endpoints = map[string]string{
	"bj": "bj.bcebos.com",
	"gz": "gz.bcebos.com",
	"hk": "hkg.bcebos.com",
	"su": "su.bcebos.com",
	"bd": "bd.bcebos.com",
	"wh": "fwh.bcebos.com",
}

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

	if _, exist := endpoints[conf.Region]; !exist {
		return errors.New(fmt.Sprintf("region %s not supported", conf.Region))
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

	client, err := bos.NewClient(config.AccessKeyID, config.SecretAccessKey, endpoints[config.Region])
	if err != nil {
		return nil, errors.Wrap(err, "create bos client")
	}
	client.MaxParallel = 20
	client.MultipartSize = 128 * 1024

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
	if err := b.client.DeleteObject(b.name, name); err != nil {
		return err
	}
	return nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	chunksnum, lastslice, err := calculateChunks(name, r)
	if err != nil {
		return err
	}

	switch chunksnum {
	case 0:
		body, err := bce.NewBodyFromSizedReader(r, lastslice)
		if err != nil {
			return errors.Wrap(err, "failed to NewBodyFromSizedReader")
		}
		if _, err := b.client.PutObject(b.name, name, body, nil); err != nil {
			return errors.Wrap(err, "failed to upload oss object")
		}
	default:
		{
			init, err := b.client.BasicInitiateMultipartUpload(b.name, name)
			if err != nil {
				return errors.Wrap(err, "failed to initiate multi-part upload")
			}
			chunk := 0
			uploadEveryPart := func(everypartsize int64, cnk int) (string, error) {
				body, err := bce.NewBodyFromSectionFile(r.(*os.File), (int64(cnk)-1)*everypartsize+1, everypartsize)
				if err != nil {
					return "", errors.Wrap(err, "failed to NewBodyFromSectionFile")
				}
				etag, err := b.client.BasicUploadPart(b.name, name, init.UploadId, cnk, body)
				if err != nil {
					if err := b.client.AbortMultipartUpload(b.name, name, init.UploadId); err != nil {
						return etag, errors.Wrap(err, "failed to abort multi-part upload")
					}

					return etag, errors.Wrap(err, "failed to upload multi-part chunk")
				}
				return etag, nil
			}
			var parts []api.UploadInfoType
			for ; chunk < chunksnum; chunk++ {
				etag, err := uploadEveryPart(partSize, chunk+1)
				if err != nil {
					return errors.Wrap(err, "failed to upload every part")
				}
				parts = append(parts, api.UploadInfoType{PartNumber: chunk + 1, ETag: etag})
			}
			if lastslice != 0 {
				etag, err := uploadEveryPart(lastslice, chunksnum+1)
				if err != nil {
					return errors.Wrap(err, "failed to upload the last chunk")
				}
				parts = append(parts, api.UploadInfoType{PartNumber: chunksnum + 1, ETag: etag})
			}
			if _, err := b.client.CompleteMultipartUploadFromStruct(b.name, name, init.UploadId, &api.CompleteMultipartUploadArgs{Parts: parts}); err != nil {
				return errors.Wrap(err, "failed to set multi-part upload completive")
			}
		}
	}
	return nil
}

// Iter calls f for each entry in the given directory (not recursive). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim) + objstore.DirDelim
	}

	var marker string
	for {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "context closed while iterating bucket")
		}
		objects, err := b.client.ListObjects(b.name, &api.ListObjectsArgs{
			Delimiter: objstore.DirDelim,
			Marker:    marker,
			MaxKeys:   1000,
			Prefix:    dir,
		})
		if err != nil {
			return errors.Wrap(err, "listing baidu bos bucket failed")
		}
		marker = objects.NextMarker
		for _, object := range objects.Contents {
			if err := f(object.Key); err != nil {
				return errors.Wrapf(err, "callback func invoke for object %s failed ", object.Key)
			}
		}
		for _, object := range objects.CommonPrefixes {
			if err := f(object.Prefix); err != nil {
				return errors.Wrapf(err, "callback func invoke for object %s failed ", object.Prefix)
			}
		}
		if !objects.IsTruncated {
			break
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
	_, err := b.client.GetObjectMeta(b.name, name)
	if err != nil {
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
// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	switch tmpErr := err.(type) {
	case *bce.BceServiceError:
		if tmpErr.Code == "NoSuchKey" || tmpErr.StatusCode == http.StatusNotFound {
			return true
		}
	}
	return false
}

func calculateChunks(name string, r io.Reader) (int, int64, error) {
	switch r.(type) {
	case *os.File:
		f, _ := r.(*os.File)
		if fileInfo, err := f.Stat(); err == nil {
			s := fileInfo.Size()
			return int(math.Floor(float64(s) / partSize)), s % partSize, nil
		}
	case *strings.Reader:
		f, _ := r.(*strings.Reader)
		return int(math.Floor(float64(f.Size()) / partSize)), f.Size() % partSize, nil
	}
	return -1, 0, errors.New("unsupported implement of io.Reader")
}

func (b *Bucket) getRange(ctx context.Context, bucketName, objectKey string, off, length int64) (io.ReadCloser, error) {
	if len(objectKey) == 0 {
		return nil, errors.Errorf("given object name should not empty")
	}

	ranges := make([]int64, 0)
	ranges = append(ranges, off)
	if length != -1 {
		ranges = append(ranges, off+length-1)
	}

	responseHeaders := make(map[string]string)

	if obj, err := b.client.GetObject(bucketName, objectKey, responseHeaders, ranges...); err != nil {
		return nil, err
	} else {
		return obj.Body, nil
	}
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

	if _, err := b.client.PutBucket(b.name); err != nil {
		return nil, nil, err
	}
	t.Log("created temporary BOS bucket for BOS tests with name", tmpBucketName)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if err := b.client.DeleteBucket(b.name); err != nil {
			t.Logf("deleting bucket %s failed: %s", tmpBucketName, err)
		}
	}, nil
}

func validateForTest(conf Config) error {
	if conf.Region == "" ||
		conf.AccessKeyID == "" ||
		conf.SecretAccessKey == "" {
		return errors.New("insufficient bos configuration information")
	}
	return nil
}
