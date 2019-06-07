// Package OSS implements common object storage abstractions against Alibaba object storage service(OSS).
package oss

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	slog "log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/runutil"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

// Config stores the configuration for OSS bucket.
type Config struct {
	Bucket     string     `yaml:"bucket"`
	Endpoint   string     `yaml:"endpoint"`
	AccessKey  string     `yaml:"access_key"`
	SecretKey  string     `yaml:"secret_key"`
	HTTPConfig HTTPConfig `yaml:"http_config"`
	LogLevel   int        `yaml:"log_level"`
}

type Bucket struct {
	logger log.Logger
	name   string
	client *oss.Client
}

type HTTPConfig struct {
	EnableCRC                 bool  `yaml:"enable_crc"`
	ConnectionTimeout         int64 `yaml:"connection_timeout"`
	ReadWriteTimeout          int64 `yaml:"read_write_timeout"`
	MaxIdleConnections        int   `yaml:"max_idle_connections"`
	MaxIdleConnectionsPerHost int   `yaml:"max_idle_connections_per_host"`
}

func parseConfig(conf []byte) (Config, error) {
	defaultHTTPConfig := getDefaultHTTPConfig()

	config := Config{HTTPConfig: defaultHTTPConfig, LogLevel: oss.Info}
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return Config{}, err
	}

	return config, nil
}

func getDefaultHTTPConfig() HTTPConfig {
	defaultHTTPConfig := HTTPConfig{
		EnableCRC:                 true,
		ConnectionTimeout:         30,
		ReadWriteTimeout:          60,
		MaxIdleConnections:        100,
		MaxIdleConnectionsPerHost: 100,
	}
	return defaultHTTPConfig
}

func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	config, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewBucketWithConfig(logger, config, component)
}

func HTTPMaxConnections(httpMaxConnections oss.HTTPMaxConns) oss.ClientOption {
	return func(client *oss.Client) {
		client.Config.HTTPMaxConns = httpMaxConnections
	}
}

func NewBucketWithConfig(logger log.Logger, config Config, component string) (*Bucket, error) {
	if err := validate(config); err != nil {
		return nil, err
	}

	userAgent := fmt.Sprintf("thanos-%s", component)
	httpConnections := oss.HTTPMaxConns{}
	httpConnections.MaxIdleConns = config.HTTPConfig.MaxIdleConnections
	httpConnections.MaxIdleConnsPerHost = config.HTTPConfig.MaxIdleConnectionsPerHost
	var ossLogger log.Logger
	switch config.LogLevel {
	case oss.LogOff, oss.Info:
		ossLogger = level.Info(logger)
	case oss.Debug:
		ossLogger = level.Debug(logger)
	case oss.Error:
		ossLogger = level.Error(logger)
	case oss.Warn:
		ossLogger = level.Warn(logger)
	}

	client, err := oss.New(
		config.Endpoint,
		config.AccessKey,
		config.SecretKey,
		oss.UserAgent(userAgent),
		oss.EnableCRC(config.HTTPConfig.EnableCRC),
		oss.SetLogLevel(config.LogLevel),
		oss.Timeout(config.HTTPConfig.ConnectionTimeout, config.HTTPConfig.ReadWriteTimeout),
		HTTPMaxConnections(httpConnections),
		oss.SetLogger(slog.New(log.NewStdlibAdapter(ossLogger, log.MessageKey("ossLogger")), "", slog.LstdFlags)))
	if err != nil {
		return nil, errors.Wrap(err, "initialize OSS client")
	}

	bucket := &Bucket{
		logger: logger,
		name:   config.Bucket,
		client: client,
	}
	return bucket, nil
}

func (b *Bucket) Name() string {
	return b.name
}

func validate(conf Config) error {
	if conf.Endpoint == "" {
		return errors.New("OSS endpoint is empty in config file, please check!")
	}

	if conf.AccessKey == "" || conf.SecretKey == "" {
		return errors.New("Access key or secret key is empty in config file, please check!")
	}

	return nil
}

func (b *Bucket) Delete(ctx context.Context, name string) error {
	bucket, err := b.client.Bucket(b.name)
	if err != nil {
		return errors.Wrap(err, "delete oss object: "+name)
	}

	return bucket.DeleteObject(name)
}

func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	bucket, err := b.client.Bucket(b.name)
	if err != nil {
		return false, errors.Wrap(err, "exists oss object: "+name)
	}

	res, err := bucket.IsObjectExist(name)
	if err != nil {
		return false, errors.Wrap(err, "exists oss object")
	}

	return res, nil
}

func (b *Bucket) Close() error {
	return nil
}

func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}

	bucket, err := b.client.Bucket(b.name)
	if err != nil {
		return errors.Wrap(err, "iterate oss objects")
	}

	var marker = oss.Marker("")
	for {
		lsRes, err := bucket.ListObjects(oss.Prefix(dir), oss.Delimiter(DirDelim), marker)
		if err != nil {
			return errors.Wrap(err, "iterate oss objects")
		}

		for _, object := range lsRes.Objects {
			if object.Key == "" {
				continue
			}
			if err := f(object.Key); err != nil {
				return errors.Wrap(err, "iterate oss objects")
			}
		}

		for _, commonPrefixes := range lsRes.CommonPrefixes {
			if err := f(commonPrefixes); err != nil {
				return errors.Wrap(err, "iterate oss objects")
			}
		}

		marker = oss.Marker(lsRes.NextMarker)
		if !lsRes.IsTruncated {
			break
		}
	}

	return nil
}

func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.GetRange(ctx, name, 0, -1)
}

func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if name == "" {
		return nil, errors.Errorf("object name should not empty")
	}

	bucket, err := b.client.Bucket(b.name)
	if err != nil {
		return nil, errors.Wrap(err, "get oss object: "+name)
	}

	var r io.ReadCloser
	if length != -1 {
		r, err = bucket.GetObject(name, oss.Range(off, off+length-1))
	} else {
		r, err = bucket.GetObject(name)
	}

	if err != nil {
		return nil, err
	}

	if _, err := r.Read(nil); err != nil {
		runutil.CloseWithLogOnErr(b.logger, r, "oss get range obj close")
		return nil, err
	}

	return r, nil
}

func (b *Bucket) IsObjNotFoundErr(err error) bool {
	switch err.(type) {
	case oss.ServiceError:
		if err.(oss.ServiceError).StatusCode == 404 {
			return true
		}
		return false
	default:
		return false
	}
}

func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	bucket, err := b.client.Bucket(b.name)
	if err != nil {
		return errors.Wrap(err, "upload oss object")
	}

	rc := ioutil.NopCloser(r)
	err = bucket.PutObject(name, rc)
	if err != nil {
		return errors.Wrap(err, "upload oss object")
	}
	return err
}

func configFromEnv() Config {
	c := Config{
		Bucket:     os.Getenv("OSS_BUCKET"),
		Endpoint:   os.Getenv("OSS_ENDPOINT"),
		AccessKey:  os.Getenv("OSS_ACCESS_KEY"),
		SecretKey:  os.Getenv("OSS_SECRET_KEY"),
		HTTPConfig: getDefaultHTTPConfig(),
		LogLevel:   oss.Debug,
	}
	return c
}

func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := configFromEnv()

	if c.Endpoint == "" || c.AccessKey == "" || c.SecretKey == "" {
		return nil, nil, errors.New("insufficient OSS test configuration information")
	}

	if c.Bucket != "" && os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
		return nil, nil, errors.New("OSS_BUCKET is defined. Normally this tests will create temporary bucket " +
			"and delete it after test. Unset OSS_BUCKET env variable to use default logic. If you really want to run " +
			"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
			"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
			"to safety.")
	}

	return NewTestBucketFromConfig(t, c, true)
}

func NewTestBucketFromConfig(t testing.TB, c Config, reuseBucket bool) (objstore.Bucket, func(), error) {
	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}
	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-with-oss-test")
	if err != nil {
		return nil, nil, err
	}

	bktToCreate := c.Bucket
	if c.Bucket != "" && reuseBucket {
		if err := b.Iter(context.Background(), "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "OSS check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "OSS bucket for OSS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	if c.Bucket == "" {
		src := rand.NewSource(time.Now().UnixNano())

		bktToCreate = strings.Replace(fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63()), "_", "-", -1)
		if len(bktToCreate) >= 63 {
			bktToCreate = bktToCreate[:63]
		}
	}

	if err := b.client.CreateBucket(bktToCreate); err != nil {
		return nil, nil, err
	}
	b.name = bktToCreate
	t.Log("created temporary OSS bucket for OSS tests with name", bktToCreate)

	return b, func() {
		objstore.EmptyBucket(t, context.Background(), b)
		if err := b.client.DeleteBucket(bktToCreate); err != nil {
			t.Logf("deleting bucket %s failed: %s", bktToCreate, err)
		}
	}, nil
}
