// Package s3 implements common object storage abstractions against s3-compatible APIs.
package s3

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/encrypt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	opObjectsList  = "ListBucket"
	opObjectInsert = "PutObject"
	opObjectGet    = "GetObject"
	opObjectHead   = "HEADObject"
	opObjectDelete = "DeleteObject"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// Bucket implements the store.Bucket interface against s3-compatible APIs.
type Bucket struct {
	logger   log.Logger
	bucket   string
	client   *minio.Client
	sse      encrypt.ServerSide
	opsTotal *prometheus.CounterVec
}

// Config encapsulates the necessary config values to instantiate an s3 client.
type Config struct {
	Bucket       string
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Insecure     bool
	SignatureV2  bool
	SSEEnprytion bool
}

// RegisterS3Params registers the s3 flags and returns an initialized Config struct.
func RegisterS3Params(cmd *kingpin.CmdClause) *Config {
	var s3config Config

	cmd.Flag("s3.bucket", "S3-Compatible API bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("S3_BUCKET").StringVar(&s3config.Bucket)

	cmd.Flag("s3.endpoint", "S3-Compatible API endpoint for stored blocks.").
		PlaceHolder("<api-url>").Envar("S3_ENDPOINT").StringVar(&s3config.Endpoint)

	cmd.Flag("s3.access-key", "Access key for an S3-Compatible API.").
		PlaceHolder("<key>").Envar("S3_ACCESS_KEY").StringVar(&s3config.AccessKey)

	s3config.SecretKey = os.Getenv("S3_SECRET_KEY")

	cmd.Flag("s3.insecure", "Whether to use an insecure connection with an S3-Compatible API.").
		Default("false").Envar("S3_INSECURE").BoolVar(&s3config.Insecure)

	cmd.Flag("s3.signature-version2", "Whether to use S3 Signature Version 2; otherwise Signature Version 4 will be used.").
		Default("false").Envar("S3_SIGNATURE_VERSION2").BoolVar(&s3config.SignatureV2)

	cmd.Flag("s3.encrypt-sse", "Whether to use Server Side Encryption").
		Default("false").Envar("S3_SSE_ENCRYPTION").BoolVar(&s3config.SSEEnprytion)

	return &s3config
}

// Validate checks to see if mandatory s3 config options are set.
func (conf *Config) Validate() error {
	if conf.Bucket == "" ||
		conf.Endpoint == "" ||
		conf.AccessKey == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient s3 configuration information")
	}
	return nil
}

// ValidateForTests checks to see if mandatory s3 config options for tests are set.
func (conf *Config) ValidateForTests() error {
	if conf.Endpoint == "" ||
		conf.AccessKey == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient s3 test configuration information")
	}
	return nil
}

// NewBucket returns a new Bucket using the provided s3 config values.
func NewBucket(logger log.Logger, conf *Config, reg prometheus.Registerer, component string) (*Bucket, error) {
	var f func(string, string, string, bool) (*minio.Client, error)
	if conf.SignatureV2 {
		f = minio.NewV2
	} else {
		f = minio.NewV4
	}

	client, err := f(conf.Endpoint, conf.AccessKey, conf.SecretKey, !conf.Insecure)
	if err != nil {
		return nil, errors.Wrap(err, "initialize s3 client")
	}
	client.SetAppInfo(fmt.Sprintf("thanos-%s", component), fmt.Sprintf("%s (%s)", version.Version, runtime.Version()))
	client.SetCustomTransport(&http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// The ResponseHeaderTimeout here is the only change from the
		// default minio transport, it was introduced to cover cases
		// where the tcp connection works but the server never answers
		ResponseHeaderTimeout: 15 * time.Second,
		// Set this value so that the underlying transport round-tripper
		// doesn't try to auto decode the body of objects with
		// content-encoding set to `gzip`.
		//
		// Refer:
		//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
		DisableCompression: true,
	})

	var sse encrypt.ServerSide
	if conf.SSEEnprytion {
		sse = encrypt.NewSSE()
	}

	bkt := &Bucket{
		logger: logger,
		bucket: conf.Bucket,
		client: client,
		sse:    sse,
		opsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_s3_bucket_operations_total",
			Help:        "Total number of operations that were executed against an s3 bucket.",
			ConstLabels: prometheus.Labels{"bucket": conf.Bucket},
		}, []string{"operation"}),
	}
	if reg != nil {
		reg.MustRegister(bkt.opsTotal)
	}
	return bkt, nil
}

// Iter calls f for each entry in the given directory. The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	b.opsTotal.WithLabelValues(opObjectsList).Inc()
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	for object := range b.client.ListObjects(b.bucket, dir, false, ctx.Done()) {
		// this sometimes happens with empty buckets
		if object.Key == "" {
			continue
		}
		if err := f(object.Key); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.opsTotal.WithLabelValues(opObjectGet).Inc()
	opts := &minio.GetObjectOptions{ServerSideEncryption: b.sse}
	if length != -1 {
		if err := opts.SetRange(off, off+length-1); err != nil {
			return nil, err
		}
	}
	r, err := b.client.GetObjectWithContext(ctx, b.bucket, name, *opts)
	if err != nil {
		return nil, err
	}

	// NotFoundObject error is revealed only after first Read. This does the initial GetRequest. Prefetch this here
	// for convenience.
	if _, err := r.Read(nil); err != nil {
		runutil.CloseWithLogOnErr(b.logger, r, "s3 get range obj close")

		// First GET Object request error.
		return nil, err
	}

	return r, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	b.opsTotal.WithLabelValues(opObjectHead).Inc()
	_, err := b.client.StatObject(b.bucket, name, minio.StatObjectOptions{})
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "stat s3 object")
	}

	return true, nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	b.opsTotal.WithLabelValues(opObjectInsert).Inc()

	_, err := b.client.PutObjectWithContext(ctx, b.bucket, name, r, -1,
		minio.PutObjectOptions{ServerSideEncryption: b.sse},
	)

	return errors.Wrap(err, "upload s3 object")
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	b.opsTotal.WithLabelValues(opObjectDelete).Inc()
	return b.client.RemoveObject(b.bucket, name)
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

func (b *Bucket) Close() error { return nil }

func configFromEnv() *Config {
	c := &Config{
		Bucket:    os.Getenv("S3_BUCKET"),
		Endpoint:  os.Getenv("S3_ENDPOINT"),
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}

	insecure, err := strconv.ParseBool(os.Getenv("S3_INSECURE"))
	if err != nil {
		c.Insecure = insecure
	}
	signV2, err := strconv.ParseBool(os.Getenv("S3_SIGNATURE_VERSION2"))
	if err != nil {
		c.SignatureV2 = signV2
	}
	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, location string) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := c.ValidateForTests(); err != nil {
		return nil, nil, err
	}

	b, err := NewBucket(log.NewNopLogger(), c, nil, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	if c.Bucket != "" {
		if os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
			return nil, nil, errors.New("S3_BUCKET is defined. Normally this tests will create temporary bucket " +
				"and delete it after test. Unset S3_BUCKET env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod bucket for test) as well as aws s3 not being fully strong consistent.")
		}

		if err := b.Iter(context.Background(), "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "s3 check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "AWS bucket for AWS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	src := rand.NewSource(time.Now().UnixNano())

	// Bucket name need to conform: https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
	tmpBucketName := strings.Replace(fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63()), "_", "-", -1)
	if len(tmpBucketName) >= 63 {
		tmpBucketName = tmpBucketName[:63]
	}
	if err := b.client.MakeBucket(tmpBucketName, location); err != nil {
		return nil, nil, err
	}
	b.bucket = tmpBucketName
	t.Log("created temporary AWS bucket for AWS tests with name", tmpBucketName, "in", location)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if err := b.client.RemoveBucket(tmpBucketName); err != nil {
			t.Logf("deleting bucket %s failed: %s", tmpBucketName, err)
		}
	}, nil
}
