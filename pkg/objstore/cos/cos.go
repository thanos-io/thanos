// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cos

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/tencentyun/cos-go-sdk-v5"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/exthttp"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/clientutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const dirDelim = "/"

// Bucket implements the store.Bucket interface against cos-compatible(Tencent Object Storage) APIs.
type Bucket struct {
	logger log.Logger
	client *cos.Client
	name   string
}

// DefaultConfig is the default config for an cos client. default tune the `MaxIdleConnsPerHost`.
var DefaultConfig = Config{
	HTTPConfig: exthttp.HTTPConfig{
		IdleConnTimeout:       model.Duration(90 * time.Second),
		ResponseHeaderTimeout: model.Duration(2 * time.Minute),
		TLSHandshakeTimeout:   model.Duration(10 * time.Second),
		ExpectContinueTimeout: model.Duration(1 * time.Second),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
	},
}

// Config encapsulates the necessary config values to instantiate an cos client.
type Config struct {
	Bucket     string             `yaml:"bucket"`
	Region     string             `yaml:"region"`
	AppId      string             `yaml:"app_id"`
	Endpoint   string             `yaml:"endpoint"`
	SecretKey  string             `yaml:"secret_key"`
	SecretId   string             `yaml:"secret_id"`
	HTTPConfig exthttp.HTTPConfig `yaml:"http_config"`
}

// Validate checks to see if mandatory cos config options are set.
func (conf *Config) validate() error {
	if conf.Endpoint != "" {
		if _, err := url.Parse(conf.Endpoint); err != nil {
			return errors.Wrap(err, "parse endpoint")
		}
		if conf.SecretId == "" ||
			conf.SecretKey == "" {
			return errors.New("secret_id or secret_key is empty")
		}
		return nil
	}
	if conf.Bucket == "" ||
		conf.AppId == "" ||
		conf.Region == "" ||
		conf.SecretId == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient cos configuration information")
	}
	return nil
}

// parseConfig unmarshal a buffer into a Config with default HTTPConfig values.
func parseConfig(conf []byte) (Config, error) {
	config := DefaultConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return Config{}, err
	}

	return config, nil
}

// NewBucket returns a new Bucket using the provided cos configuration.
func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	config, err := parseConfig(conf)
	if err != nil {
		return nil, errors.Wrap(err, "parsing cos configuration")
	}

	return NewBucketWithConfig(logger, config, component)
}

// NewBucketWithConfig returns a new Bucket using the provided cos config values.
func NewBucketWithConfig(logger log.Logger, config Config, component string) (*Bucket, error) {
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "validate cos configuration")
	}

	var bucketURL *url.URL
	var err error
	if config.Endpoint != "" {
		bucketURL, err = url.Parse(config.Endpoint)
		if err != nil {
			return nil, errors.Wrap(err, "parse endpoint")
		}
	} else {
		bucketURL = cos.NewBucketURL(fmt.Sprintf("%s-%s", config.Bucket, config.AppId), config.Region, true)
	}
	b := &cos.BaseURL{BucketURL: bucketURL}
	tpt, _ := exthttp.DefaultTransport(config.HTTPConfig)
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  config.SecretId,
			SecretKey: config.SecretKey,
			Transport: tpt,
		},
	})

	bkt := &Bucket{
		logger: logger,
		client: client,
		name:   config.Bucket,
	}
	return bkt, nil
}

// Name returns the bucket name for COS.
func (b *Bucket) Name() string {
	return b.name
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	resp, err := b.client.Object.Head(ctx, name, nil)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	size, err := clientutil.ParseContentLength(resp.Header)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	// tencent cos return Last-Modified header in RFC1123 format.
	// see api doc for details: https://intl.cloud.tencent.com/document/product/436/7729
	mod, err := clientutil.ParseLastModified(resp.Header, time.RFC1123)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}

	return objstore.ObjectAttributes{
		Size:         size,
		LastModified: mod,
	}, nil
}

var (
	_ cos.FixedLengthReader = (*fixedLengthReader)(nil)
)

type fixedLengthReader struct {
	io.Reader
	size int64
}

func newFixedLengthReader(r io.Reader, size int64) io.Reader {
	return fixedLengthReader{
		Reader: io.LimitReader(r, size),
		size:   size,
	}
}

// Size implement cos.FixedLengthReader interface.
func (r fixedLengthReader) Size() int64 {
	return r.size
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	size, err := objstore.TryToGetSize(r)
	if err != nil {
		return errors.Wrapf(err, "getting size of %s", name)
	}
	// partSize 128MB.
	const partSize = 1024 * 1024 * 128
	partNums, lastSlice := int(math.Floor(float64(size)/partSize)), size%partSize
	if partNums == 0 {
		if _, err := b.client.Object.Put(ctx, name, r, nil); err != nil {
			return errors.Wrapf(err, "Put object: %s", name)
		}
		return nil
	}
	// 1. init.
	result, _, err := b.client.Object.InitiateMultipartUpload(ctx, name, nil)
	if err != nil {
		return errors.Wrapf(err, "InitiateMultipartUpload %s", name)
	}
	uploadEveryPart := func(partSize int64, part int, uploadID string) (string, error) {
		r := newFixedLengthReader(r, partSize)
		resp, err := b.client.Object.UploadPart(ctx, name, uploadID, part, r, &cos.ObjectUploadPartOptions{
			ContentLength: partSize,
		})
		if err != nil {
			if _, err := b.client.Object.AbortMultipartUpload(ctx, name, uploadID); err != nil {
				return "", err
			}
			return "", err
		}
		etag := resp.Header.Get("ETag")
		return etag, nil
	}
	optcom := &cos.CompleteMultipartUploadOptions{}
	// 2. upload parts.
	for part := 1; part <= partNums; part++ {
		etag, err := uploadEveryPart(partSize, part, result.UploadID)
		if err != nil {
			return errors.Wrapf(err, "uploadPart %d, %s", part, name)
		}
		optcom.Parts = append(optcom.Parts, cos.Object{
			PartNumber: part, ETag: etag},
		)
	}
	// 3. upload last part.
	if lastSlice != 0 {
		part := partNums + 1
		etag, err := uploadEveryPart(lastSlice, part, result.UploadID)
		if err != nil {
			return errors.Wrapf(err, "uploadPart %d, %s", part, name)
		}
		optcom.Parts = append(optcom.Parts, cos.Object{
			PartNumber: part, ETag: etag},
		)
	}
	// 4. complete.
	if _, _, err := b.client.Object.CompleteMultipartUpload(ctx, name, result.UploadID, optcom); err != nil {
		return errors.Wrapf(err, "CompleteMultipartUpload %s", name)
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	if _, err := b.client.Object.Delete(ctx, name); err != nil {
		return errors.Wrap(err, "delete cos object")
	}
	return nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, dirDelim) + dirDelim
	}

	for object := range b.listObjects(ctx, dir, options...) {
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

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.New("given object name should not empty")
	}

	opts := &cos.ObjectGetOptions{}
	if length != -1 {
		if err := setRange(opts, off, off+length-1); err != nil {
			return nil, err
		}
	} else if off > 0 {
		if err := setRange(opts, off, 0); err != nil {
			return nil, err
		}
	}

	resp, err := b.client.Object.Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	if _, err := resp.Body.Read(nil); err != nil {
		runutil.ExhaustCloseWithLogOnErr(b.logger, resp.Body, "cos get range obj close")
		return nil, err
	}
	// Add size info into reader to pass it to Upload function.
	r := objectSizerReadCloser{ReadCloser: resp.Body, size: resp.ContentLength}
	return r, nil
}

type objectSizerReadCloser struct {
	io.ReadCloser
	size int64
}

// ObjectSize implement objstore.ObjectSizer.
func (o objectSizerReadCloser) ObjectSize() (int64, error) {
	return o.size, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	if _, err := b.client.Object.Head(ctx, name, nil); err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "head cos object")
	}

	return true, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	switch tmpErr := errors.Cause(err).(type) {
	case *cos.ErrorResponse:
		if tmpErr.Code == "NoSuchKey" ||
			(tmpErr.Response != nil && tmpErr.Response.StatusCode == http.StatusNotFound) {
			return true
		}
		return false
	default:
		return false
	}
}

func (b *Bucket) Close() error { return nil }

type objectInfo struct {
	key string
	err error
}

func (b *Bucket) listObjects(ctx context.Context, objectPrefix string, options ...objstore.IterOption) <-chan objectInfo {
	objectsCh := make(chan objectInfo, 1)

	// If recursive iteration is enabled we should pass an empty delimiter.
	delimiter := dirDelim
	if objstore.ApplyIterOptions(options...).Recursive {
		delimiter = ""
	}

	go func(objectsCh chan<- objectInfo) {
		defer close(objectsCh)
		var marker string
		for {
			result, _, err := b.client.Bucket.Get(ctx, &cos.BucketGetOptions{
				Prefix:    objectPrefix,
				MaxKeys:   1000,
				Marker:    marker,
				Delimiter: delimiter,
			})
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
			for _, obj := range result.CommonPrefixes {
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

func setRange(opts *cos.ObjectGetOptions, start, end int64) error {
	if start == 0 && end < 0 {
		opts.Range = fmt.Sprintf("bytes=%d", end)
	} else if 0 < start && end == 0 {
		opts.Range = fmt.Sprintf("bytes=%d-", start)
	} else if 0 <= start && start <= end {
		opts.Range = fmt.Sprintf("bytes=%d-%d", start, end)
	} else {
		return errors.Errorf("Invalid range specified: start=%d end=%d", start, end)
	}
	return nil
}

func configFromEnv() Config {
	c := Config{
		Bucket:    os.Getenv("COS_BUCKET"),
		AppId:     os.Getenv("COS_APP_ID"),
		Region:    os.Getenv("COS_REGION"),
		Endpoint:  os.Getenv("COS_ENDPOINT"),
		SecretId:  os.Getenv("COS_SECRET_ID"),
		SecretKey: os.Getenv("COS_SECRET_KEY"),
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
			return nil, nil, errors.New("COS_BUCKET is defined. Normally this tests will create temporary bucket " +
				"and delete it after test. Unset COS_BUCKET env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod bucket for test) as well as COS not being fully strong consistent.")
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
			return nil, nil, errors.Wrapf(err, "cos check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "COS bucket for COS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}
	c.Bucket = createTemporaryTestBucketName(t)

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}

	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	if _, err := b.client.Bucket.Put(context.Background(), nil); err != nil {
		return nil, nil, err
	}
	t.Log("created temporary COS bucket for COS tests with name", c.Bucket)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if _, err := b.client.Bucket.Delete(context.Background()); err != nil {
			t.Logf("deleting bucket %s failed: %s", c.Bucket, err)
		}
	}, nil
}

func validateForTest(conf Config) error {
	if conf.Endpoint != "" {
		if _, err := url.Parse(conf.Endpoint); err != nil {
			return errors.Wrap(err, "parse endpoint")
		}
		if conf.SecretId == "" ||
			conf.SecretKey == "" {
			return errors.New("secret_id or secret_key is empty")
		}
		return nil
	}
	if conf.AppId == "" ||
		conf.Region == "" ||
		conf.SecretId == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient cos configuration information")
	}
	return nil
}

// createTemporaryTestBucketName create a temp cos bucket for test.
// Bucket Naming Conventions: https://intl.cloud.tencent.com/document/product/436/13312#overview
func createTemporaryTestBucketName(t testing.TB) string {
	src := rand.New(rand.NewSource(time.Now().UnixNano()))
	name := fmt.Sprintf("test_%x_%s", src.Int31(), strings.ToLower(t.Name()))
	name = strings.NewReplacer("_", "-", "/", "-").Replace(name)
	const maxLength = 50
	if len(name) >= maxLength {
		name = name[:maxLength]
	}
	return strings.TrimSuffix(name, "-")
}
