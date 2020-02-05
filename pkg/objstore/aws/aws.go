// Package aws implements common object storage abstractions against s3-compatible APIs.
package aws

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"gopkg.in/yaml.v2"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// Config stores the configuration for s3 bucket.
type Config struct {
	Bucket        string             `yaml:"bucket"`
	Region        string             `yaml:"region"`
	AccessKey     string             `yaml:"accessKey"`
	ACL           string             `yaml:"acl"`
	SSEEncryption bool               `yaml:"encryptSSE"`
	SecretKey     string             `yaml:"secretKey"`
	UserMetadata  map[string]*string `yaml:"userMetadata"`
	HTTPConfig    HTTPConfig         `yaml:"httpConfig"`
	TraceConfig   TraceConfig        `yaml:"trace"`
	PartSize      int64              `yaml:"maxPartSize"`
	Retries       int                `yaml:"maxRetries"`
}

// TraceConfig enables or disables tracing.
type TraceConfig struct {
	Enable bool `yaml:"enable"`
}

// HTTPConfig stores the http.Transport configuration for the s3 minio client.
type HTTPConfig struct {
	IdleConnTimeout       model.Duration `yaml:"idleConnectTimeout"`
	ResponseHeaderTimeout model.Duration `yaml:"responseHeaderTimeout"`
	InsecureSkipVerify    bool           `yaml:"insecureSkipVerify"`
}

// DefaultConfig for object storage.
var DefaultConfig = Config{
	UserMetadata: map[string]*string{},
	HTTPConfig: HTTPConfig{
		IdleConnTimeout:       model.Duration(90 * time.Second),
		ResponseHeaderTimeout: model.Duration(2 * time.Minute),
	},
	// Minimum file size after which an HTTP multipart request should be used to upload objects to storage.
	PartSize: 5 * 1024 * 1024,
	Retries:  3,
}

// Bucket implements the store.Bucket interface against s3-compatible APIs.
type Bucket struct {
	acl          string
	logger       log.Logger
	name         string
	client       *s3.S3
	userMetadata map[string]*string
	maxPartSize  int64
	maxRetries   int
}

// parseConfig unmarshals a buffer into a Config with default HTTPConfig values.
func parseConfig(conf []byte) (Config, error) {
	config := DefaultConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return Config{}, err
	}

	return config, nil
}

// NewBucket returns a new Bucket using the provided s3 config values.
func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	config, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewBucketWithConfig(logger, config, component)
}

// NewBucketWithConfig returns a new Bucket using the provided s3 config values.
func NewBucketWithConfig(logger log.Logger, config Config, component string) (*Bucket, error) {
	if err := validate(config); err != nil {
		return nil, err
	}

	httpClient := &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       time.Duration(config.HTTPConfig.IdleConnTimeout),
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer: https://golang.org/src/net/http/transport.go?h=roundTrip#L1843.
		DisableCompression: true,
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: config.HTTPConfig.InsecureSkipVerify},
	}}
	session, err := session.NewSession(&aws.Config{
		Region:     aws.String(config.Region),
		HTTPClient: httpClient,
	})

	// If static credentials specified override default provider chain.
	if config.AccessKey != "" {
		session.Config.Credentials = credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, "")
	}

	client := s3.New(session)
	if err != nil {
		return nil, errors.Wrap(err, "initialize s3 client")
	}

	if config.TraceConfig.Enable {
		client.Config.Logger = aws.NewDefaultLogger()
	}

	bkt := &Bucket{
		logger:       logger,
		name:         config.Bucket,
		client:       client,
		userMetadata: config.UserMetadata,
		maxPartSize:  config.PartSize,
		maxRetries:   config.Retries,
		acl:          config.ACL,
	}
	return bkt, nil
}

// Name returns the bucket name for s3.
func (b *Bucket) Name() string {
	return b.name
}

// validate checks to see the config options are set.
func validate(conf Config) error {
	if conf.Region == "" {
		return errors.New("Region not specified in config file")
	}
	if conf.AccessKey == "" && conf.SecretKey != "" {
		return errors.New("no s3 acccess_key specified while secret_key is present in config file; either both should be present in config or envvars/IAM should be used")
	}

	if conf.AccessKey != "" && conf.SecretKey == "" {
		return errors.New("no s3 secret_key specified while access_key is present in config file; either both should be present in config or envvars/IAM should be used")
	}
	return nil
}

// ValidateForTests checks to see the config options for tests are set.
func ValidateForTests(conf Config) error {
	if conf.AccessKey == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient s3 test configuration information")
	}
	return nil
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	object, err := b.client.ListObjects(&s3.ListObjectsInput{Bucket: &b.name,
		Prefix:    aws.String(dir),
		Delimiter: aws.String(DirDelim)})

	if err != nil {
		return err
	}

	for _, obj := range object.Contents {
		if *obj.Key == "" {
			continue
		}

		if *obj.Key == dir {
			continue
		}

		if err := f(*obj.Key); err != nil {
			return err
		}
	}

	for _, obj := range object.CommonPrefixes {
		if *obj.Prefix == "" {
			continue
		}

		if *obj.Prefix == dir {
			continue
		}

		if err := f(*obj.Prefix); err != nil {
			return err
		}
	}
	return nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.get(ctx, name, "")
}

func (b *Bucket) get(ctx context.Context, name string, byteRange string) (io.ReadCloser, error) {
	getObjectInput := &s3.GetObjectInput{Bucket: aws.String(b.name), Key: aws.String(name)}
	if byteRange != "" {
		getObjectInput.Range = aws.String(byteRange)
	}
	object, err := b.client.GetObjectWithContext(ctx, getObjectInput)
	if err != nil {
		return nil, err
	}

	// NotFoundObject error is revealed only after first Read. This does the initial GetRequest. Prefetch this here
	// for convenience.
	if _, err := object.Body.Read(nil); err != nil {
		runutil.CloseWithLogOnErr(b.logger, object.Body, "s3 get range obj close")

		// First GET Object request error.
		return nil, err
	}

	return object.Body, nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	byteRange := ""
	if length < 0 && off < 0 {
		byteRange = fmt.Sprintf("bytes=%d", off)
	} else if length < 0 && off > 0 {
		byteRange = fmt.Sprintf("bytes=%d-", off)
	} else if length > 0 {
		byteRange = fmt.Sprintf("bytes=%d-%d", off, off+length-1)
	} else {
		return nil, errors.New(fmt.Sprintf("Invalid range specified: start=%d end=%d", off, length))
	}
	return b.get(ctx, name, byteRange)
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	_, err := b.client.HeadObject(&s3.HeadObjectInput{Bucket: &b.name, Key: &name})

	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.WithMessage(err, "stat s3 object")
	}
	return true, nil
}

func (b *Bucket) guessFileSize(name string, r io.Reader) int64 {
	if f, ok := r.(*os.File); ok {
		fileInfo, err := f.Stat()
		if err == nil {
			return fileInfo.Size()
		}
		level.Warn(b.logger).Log("msg", "could not stat file for multipart upload", "name", name, "err", err)
		return -1
	}

	level.Warn(b.logger).Log("msg", "could not guess file size for multipart upload", "name", name)
	return -1
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	// TODO(https://github.com/thanos-io/thanos/issues/678): Remove guessing length when minio provider will support multipart upload without this.
	size := b.guessFileSize(name, r)

	buffer := make([]byte, size)
	_, err := r.Read(buffer)
	if err != nil {
		return errors.Wrap(err, "Upload: failed to read file into buffer")
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(b.name),
		Key:         aws.String(name),
		ContentType: aws.String(http.DetectContentType(buffer)),
		ACL:         aws.String(b.acl),
		Metadata:    b.userMetadata,
	}

	resp, err := b.client.CreateMultipartUpload(input)
	if err != nil {
		level.Error(b.logger).Log("err", err.Error())
		return err
	}
	level.Debug(b.logger).Log("msg", "Created multipart upload request", "name", name)

	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	partNumber := 1
	for curr = 0; remaining != 0; curr += partLength {
		if remaining < b.maxPartSize {
			partLength = remaining
		} else {
			partLength = b.maxPartSize
		}
		completedPart, err := b.uploadPart(b.client, resp, buffer[curr:curr+partLength], partNumber, b.maxRetries)
		if err != nil {
			level.Error(b.logger).Log("err", err.Error())
			err := b.abortMultipartUpload(b.client, resp)
			if err != nil {
				level.Error(b.logger).Log("err", err.Error())
			}
			return err
		}
		remaining -= partLength
		partNumber++
		completedParts = append(completedParts, completedPart)
	}

	completeResponse, err := b.completeMultipartUpload(b.client, resp, completedParts)
	if err != nil {
		level.Error(b.logger).Log("err", err.Error())
		return err
	}
	level.Debug(b.logger).Log("msg", "Successfully uploaded file", "name", completeResponse.String())

	return nil
}

func (b *Bucket) completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func (b *Bucket) uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int, maxRetries int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			level.Debug(b.logger).Log("msg", "Retrying to upload part", "part", partNumber)
			tryNum++
		} else {
			level.Debug(b.logger).Log("msg", "Uploaded part", "part", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func (b *Bucket) abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	level.Debug(b.logger).Log("msg", "Aborting multipart upload for UploadId #", "uploadid", *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}

// ObjectSize returns the size of the specified object.
func (b *Bucket) ObjectSize(ctx context.Context, name string) (uint64, error) {
	objInfo, err := b.client.HeadObject(&s3.HeadObjectInput{Bucket: &b.name, Key: &name})

	if err != nil {
		return 0, err
	}
	return uint64(*objInfo.ContentLength), nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	_, err := b.client.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(b.name), Key: aws.String(name)})
	if err != nil {
		return err
	}

	err = b.client.WaitUntilObjectNotExists(&s3.HeadObjectInput{Bucket: aws.String(b.name), Key: aws.String(name)})
	if err != nil {
		return err
	}

	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	level.Debug(b.logger).Log("msg", "IsObjNotFoundError", "error", err)
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		// https://github.com/aws/aws-sdk-go/issues/2095.
		// API for HeadObject returns 404 (NotFound) vs NoSuchKey.
		case "NotFound":
			return true
		case "NoSuchKey":
			return true
		}
	}
	return false
}

func (b *Bucket) Close() error { return nil }

func configFromEnv() Config {
	c := Config{
		Bucket:    os.Getenv("S3_BUCKET"),
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}

	c.HTTPConfig.InsecureSkipVerify, _ = strconv.ParseBool(os.Getenv("S3_INSECURE_SKIP_VERIFY"))
	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, location string) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := ValidateForTests(c); err != nil {
		return nil, nil, err
	}

	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
		return nil, nil, errors.New("S3_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset S3_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
			"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
			"to safety (accidentally pointing prod bucket for test) as well as aws s3 not being fully strong consistent.")
	}

	return NewTestBucketFromConfig(t, location, c, true)
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
			return nil, nil, errors.Wrapf(err, "s3 check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "AWS bucket for AWS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	if c.Bucket == "" {
		bktToCreate = objstore.CreateTemporaryTestBucketName(t)
	}

	if _, err := b.client.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bktToCreate)}); err != nil {
		return nil, nil, err
	}
	b.name = bktToCreate
	t.Log("created temporary AWS bucket for AWS tests with name", bktToCreate, "in", location)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if _, err := b.client.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bktToCreate)}); err != nil {
			t.Logf("deleting bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}
