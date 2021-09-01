package ks3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/ks3sdklib/aws-sdk-go/aws"
	"github.com/ks3sdklib/aws-sdk-go/aws/credentials"
	"github.com/ks3sdklib/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"gopkg.in/yaml.v2"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

var DefaultConfig = Config{
	DisableSSL:       true,
	LogLevel:         1,
	PutMetadata:      map[string]string{},
	ContentType:      "content-type",
	ACL:              "public-read",
	PartSize:         1024 * 1024 * 100,
	S3ForcePathStyle: true,
	LogHTTPBody:      true,
}

type Config struct {
	Bucket           string            `yaml:"bucket"`
	AccessKeyID      string            `yaml:"access_key_id"`
	AccessKeySecret  string            `yaml:"access_key_secret"`
	Endpoint         string            `yaml:"endpoint"`
	Region           string            `yaml:"region"`
	DisableSSL       bool              `yaml:"disable_ssl"`
	LogHTTPBody      bool              `yaml:"log_http_body"`
	LogLevel         uint              `yaml:"log_level"`
	S3ForcePathStyle bool              `yaml:"s3_force_path_style"`
	PartSize         uint64            `yaml:"part_size"`
	PutMetadata      map[string]string `yaml:"put_metadata"`
	ContentType      string            `yaml:"content_type"`
	ACL              string            `yaml:"acl"`
}

type Bucket struct {
	name        string
	logger      log.Logger
	ks3client   *s3.S3
	partSize    uint64
	putMetadata map[string]*string
	contentType string
	acl         string
}

var _ objstore.Bucket = new(Bucket)

// parseConfig unmarshals a buffer into a Config with default HTTPConfig values.
func parseConfig(conf []byte) (Config, error) {
	config := DefaultConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return Config{}, err
	}
	return config, nil
}

// NewBucket returns a new Bucket using the provided ks3 config values.
func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	config, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}
	return NewBucketWithConfig(logger, config, component)
}

func NewBucketWithConfig(logger log.Logger, config Config, component string) (*Bucket, error) {
	credentials := credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKeySecret, "")
	ks3client := s3.New(&aws.Config{
		Region:           config.Region,
		Credentials:      credentials,
		Endpoint:         config.Endpoint,
		DisableSSL:       config.DisableSSL,
		LogLevel:         config.LogLevel,
		S3ForcePathStyle: config.S3ForcePathStyle,
		LogHTTPBody:      config.LogHTTPBody,
		Logger:           os.Stdout,
	})

	PutMetadataPoint := make(map[string]*string, len(config.PutMetadata))
	for i, v := range config.PutMetadata {
		PutMetadataPoint[i] = aws.String(v)
	}

	bkt := &Bucket{
		logger:      logger,
		name:        config.Bucket,
		ks3client:   ks3client,
		partSize:    config.PartSize,
		putMetadata: PutMetadataPoint,
		contentType: config.ContentType,
		acl:         config.ACL,
	}
	return bkt, nil
}

// Name returns the bucket name for ks3.
func (b *Bucket) Name() string {
	return b.name
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	input := &s3.DeleteObjectInput{
		Bucket: &b.name,
		Key:    &name,
	}
	if _, err := b.ks3client.DeleteObject(input); err != nil {
		return errors.Wrap(err, "delete ks3 object")
	}
	return nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	size, err := objstore.TryToGetSize(r)
	if err != nil {
		level.Warn(b.logger).Log("msg", "could not guess file size for multipart upload; upload might be not optimized", "name", name, "err", err)

		err := b.putFile(name, r)
		if err != nil {
			return err
		}
	}
	// partSize cannot be larger than object size.
	partSize := b.partSize
	if size < int64(partSize) {
		err := b.putFile(name, r)
		if err != nil {
			return err
		}
	} else {
		// size less than 100M, perform block upload
		// CreateMultipartUpload
		createMultipartParams := &s3.CreateMultipartUploadInput{
			Bucket:      aws.String(b.name),
			Key:         aws.String(name),
			ACL:         aws.String(b.acl),
			ContentType: aws.String("application/octet-stream"),
			Metadata:    b.putMetadata,
		}
		resp, err := b.ks3client.CreateMultipartUpload(createMultipartParams)
		if err != nil {
			return fmt.Errorf("bucket:%s,objectKey:%s,init MultipartUpload failed,err:%v", b.name, b.name, err)
		}
		uploadId := *resp.UploadID

		var i int64 = 1
		compParts := []*s3.CompletedPart{}
		partsNum := []int64{0}
		s := make([]byte, 52428800)

		for {
			nr, err := io.ReadFull(r, s)
			if nr < 0 {
				return fmt.Errorf("bucket:%s,objectKey:%s,do createMultipartUpload read r data failed,err:%v", b.name, b.name, err)
			} else if nr == 0 {
				break
			} else {
				//  UploadPart
				params := &s3.UploadPartInput{
					Bucket:        aws.String(b.name),
					Key:           aws.String(name),
					PartNumber:    aws.Long(i),
					UploadID:      aws.String(uploadId),
					Body:          bytes.NewReader(s[0:nr]),
					ContentLength: aws.Long(int64(len(s[0:nr]))),
				}
				upRet, upErr := b.ks3client.UploadPart(params)
				if upErr != nil {
					return fmt.Errorf("bucket:%s,objectKey:%s,uploadPart failed,err:%v", b.name, b.name, upErr)
				}
				partsNum = append(partsNum, i)
				compParts = append(compParts, &s3.CompletedPart{PartNumber: &partsNum[i], ETag: upRet.ETag})
				i++
			}
		}

		// CompleteMultipartUpload
		compRet, compErr := b.ks3client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(b.name),
			Key:      aws.String(name),
			UploadID: aws.String(uploadId),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: compParts,
			},
		})
		if compErr != nil {
			return fmt.Errorf("bucket:%s,objectKey:%s,CompleteMultipartUpload failed,out:%v,err:%v", b.name, b.name, compRet, compErr)
		}
	}
	return nil
}

// Upload file
func (b *Bucket) putFile(name string, r io.Reader) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("bucket:%s,objectKey:%s,do putFile read r data failed,err:%v", b.name, b.name, err)
	}
	body := bytes.NewReader(data)

	input := &s3.PutObjectInput{
		Bucket:      &b.name,
		ACL:         &b.acl,
		Key:         &name,
		Body:        body,
		ContentType: &b.contentType,
		Metadata:    b.putMetadata,
	}
	_, err = b.ks3client.PutObject(input)
	if err != nil {
		return fmt.Errorf("bucket:%s,objectKey:%s,put object ks3 failed,err:%v", b.name, b.name, err)
	}
	return nil
}

// Iter calls f for each entry in the given directory (not recursive). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {

	if dir == "" {
		marker := ""
		delimiter := "/"
		for {
			objects, _ := b.ks3client.ListObjects(&s3.ListObjectsInput{
				Bucket:    &b.name,
				Marker:    &marker,
				Delimiter: &delimiter,
			})
			for _, object := range objects.Contents {
				if err := f(*object.Key); err != nil {
					return errors.Wrapf(err, "callback func invoke for object %s failed ", *object.Key)
				}
			}
			for _, object := range objects.CommonPrefixes {
				if err := f(*object.Prefix); err != nil {
					return errors.Wrapf(err, "callback func invoke for directory %s failed", *object.Prefix)
				}
			}
			if !*objects.IsTruncated {
				break
			}
			marker = *objects.NextMarker
		}

	} else {
		// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
		//// object itself as one prefix item.
		if dir != "" {
			dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
		}

		for {
			marker := ""
			objects, _ := b.ks3client.ListObjects(&s3.ListObjectsInput{
				Bucket: &b.name,
				Prefix: &dir,
				Marker: &marker,
			})
			for _, object := range objects.Contents {
				if err := f(*object.Key); err != nil {
					return errors.Wrapf(err, "callback func invoke for object %s failed ", *object.Key)
				}
			}
			if !*objects.IsTruncated {
				break
			}
			marker = *objects.Contents[len(objects.Contents)-1].Key
		}
	}
	return nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == "" {
		return nil, fmt.Errorf("getObject key don't allow not empty")
	}
	resp, err := b.ks3client.GetObject(&s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &name,
	})
	if err != nil {
		return nil, fmt.Errorf("bucket:%v,object:%v,get object failed, err: %v", b.name, name, err)
	}
	return resp.Body, nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.New("given object name should not empty")
	}

	if length != -1 {
		ranges, err := b.SetRange(off, off+length-1)
		if err != nil {
			return nil, err
		}
		resp, err := b.ks3client.GetObject(&s3.GetObjectInput{
			Bucket: &b.name,
			Key:    &name,
			Range:  &ranges,
		})
		if err != nil {
			return nil, fmt.Errorf("bucket:%v,object:%v,get range object failed,err:%v", b.name, name, err)
		}
		return resp.Body, nil

	} else if off > 0 {
		ranges, err := b.SetRange(off, 0)
		if err != nil {
			return nil, err
		}
		resp, err := b.ks3client.GetObject(&s3.GetObjectInput{
			Bucket: &b.name,
			Key:    &name,
			Range:  &ranges,
		})
		if err != nil {
			return nil, fmt.Errorf("bucket:%v,object:%v,get range object failed,err:%v", b.name, name, err)
		}
		return resp.Body, nil
	}
	return nil, fmt.Errorf("a unknown error")
}

func (b *Bucket) SetRange(start, end int64) (string, error) {
	switch {
	case start == 0 && end < 0:
		// Read last '-end' bytes. `bytes=-N`.
		return fmt.Sprintf("bytes=%d", end), nil
	case 0 < start && end == 0:
		// Read everything starting from offset
		// 'start'. `bytes=N-`.
		return fmt.Sprintf("bytes=%d-", start), nil
	case 0 <= start && start <= end:
		// Read everything starting at 'start' till the
		// 'end'. `bytes=N-M`
		return fmt.Sprintf("bytes=%d-%d", start, end), nil
	default:
		// All other cases such as
		// bytes=-3-
		// bytes=5-3
		// bytes=-2-4
		// bytes=-3-0
		// bytes=-3--2
		// are invalid.
		return "", fmt.Errorf("set range failed")
	}
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	_, err := b.ks3client.GetObject(&s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &name,
	})
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "ks3 getObject failed")
	}
	return true, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "NoSuchKey") {
		return true
	}
	return false
}

// Attributes returns information about the specified object.
func (b *Bucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	resp, err := b.ks3client.GetObject(&s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &name,
	})
	if err != nil {
		return objstore.ObjectAttributes{}, fmt.Errorf("get object failed,err:%v", err.Error())
	}
	return objstore.ObjectAttributes{
		Size:         *resp.ContentLength,
		LastModified: *resp.LastModified,
	}, nil
}

func (b *Bucket) Close() error { return nil }

func configFromEnv() Config {
	c := Config{
		Bucket:          os.Getenv("KS3_BUCKET"),
		Endpoint:        os.Getenv("KS3_ENDPOINT"),
		AccessKeyID:     os.Getenv("KS3_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("KS3_ACCESS_KEY_SECRET"),
		Region:          os.Getenv("KS3_REGION"),
	}
	return c
}

// ValidateForTests checks to see the config options for tests are set.
func ValidateForTests(conf Config) error {
	if conf.Endpoint == "" ||
		conf.AccessKeyID == "" ||
		conf.AccessKeySecret == "" ||
		conf.Region == "" {
		return errors.New("insufficient ks3 test configuration information")
	}
	return nil
}

func NewTestBucket(t testing.TB, location string) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := ValidateForTests(c); err != nil {
		return nil, nil, err
	}

	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
		return nil, nil, errors.New("KS3_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset S3_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
			"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
			"to safety (accidentally pointing prod bucket for test) as well as aws s3 not being fully strong consistent.")
	}
	return NewTestBucketFromConfig(t, location, c, true)
}

func NewTestBucketFromConfig(t testing.TB, location string, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
	ctx := context.Background()
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
		if err := b.Iter(ctx, "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "ks3 check bucket %s", c.Bucket)
		}
		t.Log("WARNING. Reusing", c.Bucket, "KS3 bucket for KS3 tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	if c.Bucket == "" {
		bktToCreate = objstore.CreateTemporaryTestBucketName(t)

	}

	AccessKeyID := os.Getenv("AccessKeyID")
	AccessKeySecret := os.Getenv("AccessKeySecret")
	credentials := credentials.NewStaticCredentials(AccessKeyID, AccessKeySecret, "")
	client := s3.New(&aws.Config{
		Region:           location,
		Credentials:      credentials,
		Endpoint:         "ks3-cn-beijing.ksyun.com",
		DisableSSL:       true,
		LogLevel:         1,
		S3ForcePathStyle: true,
		LogHTTPBody:      true,
		Logger:           os.Stdout,
	})

	_, err = client.CreateBucket(&s3.CreateBucketInput{Bucket: &bktToCreate})
	if err != nil {
		return nil, nil, fmt.Errorf("create bucket failed,err:%v", err)
	}
	b.name = bktToCreate
	t.Log("created temporary KS3 bucket for KS3 tests with name", bktToCreate, "in", location)

	return b, func() {
		objstore.EmptyBucket(t, ctx, b)
		if _, err := client.DeleteBucket(&s3.DeleteBucketInput{Bucket: &bktToCreate}); err != nil {
			t.Logf("deleting ks3 bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}
