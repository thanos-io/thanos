package oss

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

	alioss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	Endpoint        string `yaml:"endpoint"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret"`
}

type objectInfo struct {
	key string
	err error
}

type Bucket struct {
	name   string
	logger log.Logger
	client *alioss.Client
	config Config
	bucket *alioss.Bucket
}

func NewTestBucket(t testing.TB, location string) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := validate(c); err != nil {
		return nil, nil, err
	}
	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
		return nil, nil, errors.New("OSS_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset OSS_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true.")
	}

	return NewTestBucketFromConfig(t, location, c, true)
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	if err := b.bucket.PutObject(name, r); err != nil {
		return errors.Wrap(err, "upload oss object")
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	if err := b.bucket.DeleteObject(name); err != nil {
		return errors.Wrap(err, "delete oss object")
	}
	return nil
}

func configFromEnv() Config {
	c := Config{
		Endpoint:        os.Getenv("ALIYUNOSS_ENDPOINT"),
		Bucket:          os.Getenv("ALIYUNOSS_BUCKET"),
		AccessKeyID:     os.Getenv("ALIYUNOSS_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("ALIYUNOSS_ACCESS_KEY_SECRET"),
	}
	return c
}

func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	config, err := func(conf []byte) (Config, error) {
		var config Config
		if err := yaml.Unmarshal(conf, &config); err != nil {
			return Config{}, err
		}

		return config, nil
	} (conf)

	if err != nil {
		return nil, errors.Wrap(err, "parse aliyun oss config file failed")
	}
	if err := validate(config); err != nil {
		return nil, errors.Wrap(err, "validate aliyun oss config file failed")
	}

	client, err := alioss.New(config.Endpoint, config.AccessKeyID, config.AccessKeySecret)
	if err != nil {
		return nil, errors.Wrap(err, "create aliyun oss client failed")
	}
	bk, err := client.Bucket(config.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "use oss bucket "+ config.Bucket+" failed")
	}

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
		config: config,
		bucket: bk,
	}
	return bkt, nil
}

// Iter calls f for each entry in the given directory (not recursive). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim) + objstore.DirDelim
	}

	marker := alioss.Marker("")
	for {
		if err := ctx.Err() ; err != nil {
			return  errors.Wrap(err, "a done channel closed with errors")
		}
		objects, err := b.bucket.ListObjects(alioss.Prefix(dir), alioss.Delimiter(objstore.DirDelim), marker)
		if err != nil {
			return errors.Wrap(err, "oss bucket list")
		}
		marker = alioss.Marker(objects.NextMarker)

		for _, object := range objects.Objects {
			if err := f(object.Key); err != nil {
				return errors.Wrap(err, "callback func invoke with filename "+object.Key+" failed")
			}
		}

		for _, object := range objects.CommonPrefixes {
			if err := f(object); err != nil {
				return errors.Wrap(err, "callback func invoke with dirname "+object+" failed")
			}
		}
		if !objects.IsTruncated {
			break
		}
	}

	return nil
}

func (b *Bucket) Name() string {
	return b.name
}

// validate checks to see the config options are set.
func validate(conf Config) error {
	if conf.Endpoint == "" {
		return errors.New("no oss endpoint in config file")
	}
	if conf.Bucket == "" {
		return errors.New("no oss bucket in config file")
	}

	if conf.AccessKeyID == "" || conf.AccessKeySecret == "" {
		return errors.New("either oss access_key_id or oss access_key_secret is not present in config file.")
	}

	return nil
}

func NewTestBucketFromConfig(t testing.TB, location string, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}
	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	bktToCreate := c.Bucket
	if c.Bucket != "" && reuseBucket {
		if err := b.Iter(context.Background(), "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "oss check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "Aliyun OSS bucket for OSS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	if c.Bucket == "" {
		src := rand.NewSource(time.Now().UnixNano())

		bktToCreate = strings.Replace(fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63()), "_", "-", -1)
		if len(bktToCreate) >= 63 {
			bktToCreate = bktToCreate[:63]
		}
	}

	if err := b.client.CreateBucket(bktToCreate, alioss.ACL(alioss.ACLPrivate), alioss.StorageClass(alioss.StorageArchive)); err != nil {
		return nil, nil, errors.Wrap(err, "create test bucket failed")
	}
	b.name = bktToCreate

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if err := b.client.DeleteBucket(bktToCreate); err != nil {
			t.Logf("deleting bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}

func (b *Bucket) Close() error { return nil }

func setRange(start, end int64) (alioss.Option, error) {
	var opt alioss.Option
	if 0 <= start && start <= end {
		opt = alioss.Range(start, end)
	} else {
		return nil, errors.Errorf("Invalid range specified: start=%d end=%d", start, end)
	}
	return opt, nil
}

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.Errorf("given object name should not empty")
	}

	var opts []alioss.Option
	if length != -1 {
		opt, err := setRange(off, off+length-1)
		if err != nil {
			return nil, err
		}
		opts = append(opts, opt)
	}

	resp, err := b.bucket.GetObject(name, opts...)
	if err != nil {
		return nil, err
	}

	if _, err := resp.Read(nil); err != nil {
		runutil.CloseWithLogOnErr(b.logger, resp, "oss get range obj close")
		return nil, err
	}

	return resp, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	exists, err := b.bucket.IsObjectExist(name)
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "object exists already")
	}

	return exists, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	switch aliErr := err.(type) {
	case alioss.ServiceError:
		if aliErr.StatusCode == http.StatusNotFound {
			return true
		}
	}
	return false
}
