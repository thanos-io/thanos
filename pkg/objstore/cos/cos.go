package cos

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

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/mozillazg/go-cos"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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

// Bucket implements the store.Bucket interface against cos-compatible(Tencent Object Storage) APIs.
type Bucket struct {
	logger   log.Logger
	client   *cos.Client
	opsTotal *prometheus.CounterVec
}

// Config encapsulates the necessary config values to instantiate an cos client.
type Config struct {
	Bucket    string
	AppId     string
	Region    string
	SecretId  string
	SecretKey string
}

// RegisterCosParams registers the cos flags and returns an initialized Config struct.
func RegisterCosParams(cmd *kingpin.CmdClause) *Config {
	var cosConfig Config

	cmd.Flag("cos.bucket", "Cos-Compatible API bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("COS_BUCKET").StringVar(&cosConfig.Bucket)

	cmd.Flag("cos.appid", "Cos-Compatible API endpoint for stored blocks.").
		PlaceHolder("<appid>").Envar("COS_APPID").StringVar(&cosConfig.AppId)

	cmd.Flag("cos.region", "Cos-Compatible API region for stored blocks.").
		PlaceHolder("<region>").Envar("COS_REGION").StringVar(&cosConfig.Region)

	cosConfig.SecretKey = os.Getenv("COS_SECRET_KEY")
	cosConfig.SecretId = os.Getenv("COS_SECRET_ID")

	return &cosConfig
}

// Validate checks to see if mandatory cos config options are set.
func (conf *Config) Validate() error {
	if conf.Bucket == "" ||
		conf.AppId == "" ||
		conf.Region == "" ||
		conf.SecretId == "" ||
		conf.SecretKey == "" {
		return errors.New("insufficient cos configuration information")
	}
	return nil
}

func NewBucket(logger log.Logger, conf *Config, reg prometheus.Registerer, component string) (*Bucket, error) {
	if nil == logger {
		logger = log.NewNopLogger()
	}

	bucketUrl := cos.NewBucketURL(conf.Bucket, conf.AppId, conf.Region, true)

	b, err := cos.NewBaseURL(bucketUrl.String())
	if nil != err {
		return nil, errors.Wrap(err, "initialize cos base url")
	}

	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  conf.SecretId,
			SecretKey: conf.SecretKey,
		},
	})

	bkt := &Bucket{
		logger: logger,
		client: client,
		opsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_cos_bucket_operations_total",
			Help:        "Total number of operations that were executed against an cos bucket.",
			ConstLabels: prometheus.Labels{"bucket": conf.Bucket},
		}, []string{"operation"}),
	}
	if reg != nil {
		reg.MustRegister(bkt.opsTotal)
	}
	return bkt, nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	b.opsTotal.WithLabelValues(opObjectInsert).Inc()

	_, err := b.client.Object.Put(ctx, name, r, nil)
	if nil != err {
		return errors.Wrap(err, "upload cos object")
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	b.opsTotal.WithLabelValues(opObjectDelete).Inc()
	_, err := b.client.Object.Delete(ctx, name)
	if nil != err {
		return errors.Wrap(err, "delete cos object")
	}
	return nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	b.opsTotal.WithLabelValues(opObjectsList).Inc()

	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	for object := range b.ListObjects(ctx, dir, false) {
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
	if len(name) == 0 {
		return nil, errors.Errorf("given object name should not empty")
	}

	opts := &cos.ObjectGetOptions{}
	if length != -1 {
		if err := setRange(opts, off, off+length-1); err != nil {
			return nil, err
		}
	}

	resp, err := b.client.Object.Get(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	if _, err := resp.Body.Read(nil); err != nil {
		runutil.CloseWithLogOnErr(b.logger, resp.Body, "cos get range obj close")
		return nil, err
	}

	return resp.Body, nil
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
	b.opsTotal.WithLabelValues(opObjectHead).Inc()
	_, err := b.client.Object.Head(ctx, name, nil)
	if err != nil {
		if b.IsObjNotFoundErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "head cos object")
	}

	return true, nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	tmpErr := toErrorResponse(err)
	if tmpErr.Code == "NoSuchKey" {
		return true
	}
	if nil != tmpErr.Response && tmpErr.Response.StatusCode == http.StatusNotFound {
		return true
	}
	return false
}

func toErrorResponse(err error) *cos.ErrorResponse {
	switch err := err.(type) {
	case *cos.ErrorResponse:
		return err
	default:
		return &cos.ErrorResponse{}
	}
}

func (b *Bucket) Close() error { return nil }

type ObjectInfo struct {
	cos.Object
	Err error
}

func (b *Bucket) ListObjects(ctx context.Context, objectPrefix string, recursive bool) <-chan ObjectInfo {
	objectsCh := make(chan ObjectInfo, 1)
	delimiter := "/"
	if recursive {
		delimiter = ""
	}

	go func(objectsCh chan<- ObjectInfo) {
		defer close(objectsCh)
		var marker string
		for {
			opt := &cos.BucketGetOptions{
				Prefix:    objectPrefix,
				MaxKeys:   1000,
				Marker:    marker,
				Delimiter: delimiter,
			}
			result, _, err := b.client.Bucket.Get(ctx, opt)
			if err != nil {
				objectsCh <- ObjectInfo{
					Err: err,
				}
				return
			}

			for _, object := range result.Contents {
				marker = object.Key
				select {
				case objectsCh <- ObjectInfo{
					Object: object,
					Err:    nil,
				}:
				case <-ctx.Done():
					return
				}
			}

			for _, obj := range result.CommonPrefixes {
				object := cos.Object{}
				object.Key = obj
				object.Size = 0
				select {
				// Send object prefixes.
				case objectsCh <- ObjectInfo{
					Object: object,
					Err:    nil,
				}:
				case <-ctx.Done():
					return
				}
			}

			if result.NextMarker != "" {
				marker = result.NextMarker
			}

			if !result.IsTruncated {
				return
			}
		}
	}(objectsCh)
	return objectsCh
}

func setRange(opts *cos.ObjectGetOptions, start, end int64) error {
	switch {
	case start == 0 && end < 0:
		opts.Range = fmt.Sprintf("bytes=%d", end)
	case 0 < start && end == 0:
		opts.Range = fmt.Sprintf("bytes=%d-", start)
	case 0 <= start && start <= end:
		opts.Range = fmt.Sprintf("bytes=%d-%d", start, end)
	default:
		return errors.Errorf("Invalid range specified: start=%d end=%d", start, end)
	}
	return nil
}

func configFromEnv() *Config {
	c := &Config{
		Bucket:    os.Getenv("COS_BUCKET"),
		AppId:     os.Getenv("COS_APPID"),
		Region:    os.Getenv("COS_REGION"),
		SecretId:  os.Getenv("COS_SECRET_ID"),
		SecretKey: os.Getenv("COS_SECRET_KEY"),
	}

	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := configFromEnv()

	if c.Bucket != "" {
		if os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
			return nil, nil, errors.New("COS_BUCKET is defined. Normally this tests will create temporary bucket " +
				"and delete it after test. Unset COS_BUCKET env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod bucket for test) as well as aws s3 not being fully strong consistent.")
		}

		b, err := NewBucket(log.NewNopLogger(), c, nil, "thanos-e2e-test")
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

	src := rand.NewSource(time.Now().UnixNano())

	tmpBucketName := strings.Replace(fmt.Sprintf("test_%x", src.Int63()), "_", "-", -1)
	if len(tmpBucketName) >= 31 {
		tmpBucketName = tmpBucketName[:31]
	}
	c.Bucket = tmpBucketName
	b, err := NewBucket(log.NewNopLogger(), c, nil, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	if _, err := b.client.Bucket.Put(context.Background(), nil); err != nil {
		return nil, nil, err
	}
	t.Log("created temporary COS bucket for COS tests with name", tmpBucketName)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if _, err := b.client.Bucket.Delete(context.Background()); err != nil {
			t.Logf("deleting bucket %s failed: %s", tmpBucketName, err)
		}
	}, nil
}
