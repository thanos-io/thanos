package oss

import (
	"context"
	"fmt"
	"io"
	"math/rand"
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
	EndPoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	SecretKey string `yaml:"secret_key"`
	SecretId  string `yaml:"secret_id"`
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
}

func NewTestBucket(t testing.TB, location string) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := ValidateForTests(c); err != nil {
		return nil, nil, err
	}
	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
		return nil, nil, errors.New("OSS_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset OSS_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
			"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
			"to safety (accidentally pointing prod bucket for test) as well as aliyun oss not being fully strong consistent.")
	}

	return NewTestBucketFromConfig(t, location, c, true)
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	bucket, err := b.client.Bucket(b.config.Bucket)
	if err != nil {
		return errors.Wrap(err, "oss bucket")
	}
	if err2 := bucket.PutObject(name, r); err != nil {
		return errors.Wrap(err2, "upload oss object")
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	bucket, err := b.client.Bucket(b.config.Bucket)
	if err != nil {
		return errors.Wrap(err, "oss bucket")
	}
	if err2 := bucket.DeleteObject(name); err != nil {
		return errors.Wrap(err2, "delete oss object")
	}
	return nil
}

func configFromEnv() Config {
	c := Config{
		EndPoint:  os.Getenv("OSS_ENDPOINT"),
		Bucket:    os.Getenv("OSS_BUCKET"),
		SecretKey: os.Getenv("OSS_SECRET_KEY"),
		SecretId:  os.Getenv("OSS_SECRET_ID"),
	}

	return c
}

// ValidateForTests checks to see the config options for tests are set.
func ValidateForTests(conf Config) error {
	if conf.EndPoint == "" ||
		conf.SecretId == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient oss test configuration information")
	}
	return nil
}

func parseConfig(conf []byte) (Config, error) {
	var config Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return Config{}, err
	}

	return config, nil
}

func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	config, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}
	client, err := alioss.New(config.EndPoint, config.SecretId, config.SecretKey)
	if err != nil {
		return nil, err
	}
	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
		config: config,
	}
	return bkt, nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim) + objstore.DirDelim
	}

	bucket, err := b.client.Bucket(b.config.Bucket)
	if err != nil {
		return errors.Wrap(err, "oss bucket")
	}
	objects, err2 := bucket.ListObjects(alioss.Prefix(dir), alioss.Delimiter(objstore.DirDelim))
	if err2 != nil {
		return errors.Wrap(err2, "oss bucket list")
	}
	for _, object := range objects.Objects {

		if object.Key == "" {
			continue
		}
		if err := f(object.Key); err != nil {
			return err
		}
	}
	for _, object := range objects.CommonPrefixes {
		if err := f(object); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) Name() string {
	return b.name
}

// validate checks to see the config options are set.
func validate(conf Config) error {
	if conf.EndPoint == "" {
		return errors.New("no oss endpoint in config file")
	}

	if conf.SecretId == "" && conf.SecretKey != "" {
		return errors.New("no oss acccess_key specified while secret_key is present in config file; both of them should be present in config.")
	}

	if conf.SecretId != "" && conf.SecretKey == "" {
		return errors.New("no oss secret_key specified while access_key is present in config file; both of them should be present in config.")
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
		return nil, nil, err
	}
	b.name = bktToCreate
	t.Log("created temporary Aliyun bucket for Aliyun tests with name: %s @ %s ", bktToCreate, location)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if err := b.client.DeleteBucket(bktToCreate); err != nil {
			t.Logf("deleting bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}

func (b *Bucket) Close() error { return nil }

func setRange(opts *[]alioss.Option, start, end int64) error {
	if 0 <= start && start <= end {
		*opts = []alioss.Option{alioss.Range(start, end)}
	} else {
		return errors.Errorf("Invalid range specified: start=%d end=%d", start, end)
	}
	return nil
}

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.Errorf("given object name should not empty")
	}

	var opts []alioss.Option
	if length != -1 {
		if err := setRange(&opts, off, off+length-1); err != nil {
			return nil, err
		}
	}

	bucket, err := b.client.Bucket(b.config.Bucket)
	if err != nil {
		return nil, err
	}

	resp, err2 := bucket.GetObject(name, opts...)
	if err2 != nil {
		return nil, err2
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
	bucket, err := b.client.Bucket(b.config.Bucket)
	if err != nil {
		return false, errors.Wrap(err, "oss bucket")
	}
	exists, err := bucket.IsObjectExist(name)
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "head oss object")
	}

	return exists, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	switch err.(type) {
	case alioss.ServiceError:
		if err.(alioss.ServiceError).StatusCode == 404 {
			return true
		}
	}
	return false
}
