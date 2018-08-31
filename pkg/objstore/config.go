package objstore

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

type ObjProvider string

const (
	GCS         ObjProvider = "GCS"
	S3          ObjProvider = "S3"
	Unsupported ObjProvider = "UNSUPPORTED"
)

var ErrUnsupported = errors.New("unsupported provider")
var ErrMissingGCSBucket = errors.New("missing Google Cloud Storage bucket name for stored blocks")
var ErrInsufficientS3Info = errors.New("insufficient s3 configuration information")

// BucketConfig encapsulates the necessary config values to instantiate an object store client.
// Use Provider to represent the Object Stores
type BucketConfig struct {
	Provider      ObjProvider
	Bucket        string
	Endpoint      string
	AccessKey     string
	secretKey     string
	Insecure      bool
	SignatureV2   bool
	SSEEncryption bool
}

// NewBucketConfig return the configuration of object store
// TODO(jojohappy) should it support multiple bucket?
func NewBucketConfig(cmd *kingpin.CmdClause) *BucketConfig {
	var bucketConfig BucketConfig
	provider := cmd.Flag("provider.type", "Specify the provider for object store. If empty or unsupport provider, Thanos won't read and store any block to the object store. Now supported GCS / S3.").
		PlaceHolder("<provider>").String()

	bucketConfig.Provider = ObjProvider(strings.ToUpper(strings.Trim(*provider, " ")))
	cmd.Flag("provider.bucket", "The bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("PROVIDER_BUCKET").StringVar(&bucketConfig.Bucket)

	cmd.Flag("provider.endpoint", "The object store API endpoint for stored blocks. Support S3-Compatible API").
		PlaceHolder("<api-url>").Envar("PROVIDER_ENDPOINT").StringVar(&bucketConfig.Endpoint)

	cmd.Flag("provider.access-key", "Access key for an object store API. Support S3-Compatible API").
		PlaceHolder("<key>").Envar("PROVIDER_ACCESS_KEY").StringVar(&bucketConfig.AccessKey)

	// TODO(jojohappy): should it be encrypted?
	bucketConfig.secretKey = os.Getenv("PROVIDER_SECRET_KEY")

	cmd.Flag("provider.insecure", "Whether to use an insecure connection with an object store API. Support S3-Compatible API").
		Default("false").Envar("PROVIDER_INSECURE").BoolVar(&bucketConfig.Insecure)

	cmd.Flag("provider.signature-version2", "Whether to use S3 Signature Version 2; otherwise Signature Version 4 will be used").
		Default("false").Envar("PROVIDER_SIGNATURE_VERSION2").BoolVar(&bucketConfig.SignatureV2)

	cmd.Flag("provider.encrypt-sse", "Whether to use Server Side Encryption").
		Default("false").Envar("PROVIDER_SSE_ENCRYPTION").BoolVar(&bucketConfig.SSEEncryption)

	return &bucketConfig
}

// NewBackupBucketConfig return the configuration of backup object store
func NewBackupBucketConfig(cmd *kingpin.CmdClause) *BucketConfig {
	var bucketConfig BucketConfig
	provider := cmd.Flag("provider-backup.type", "Specify the provider for backup object store. If empty or unsupport provider, Thanos won't backup any block to the object store. Now supported GCS / S3.").
		PlaceHolder("<provider>").String()

	bucketConfig.Provider = ObjProvider(strings.ToUpper(strings.Trim(*provider, " ")))
	cmd.Flag("provider-backup.bucket", "The bucket name for backup stored blocks.").
		PlaceHolder("<bucket>").StringVar(&bucketConfig.Bucket)

	return &bucketConfig
}

// Provider return the provider of Object Store
func (conf *BucketConfig) BucketProvider() string {
	return string(conf.Provider)
}

// String returns the Provider information
func (conf *BucketConfig) String() string {
	return fmt.Sprintf("Provider: %s, Bucket: %s, Endpoint: %s", conf.BucketProvider(), conf.Bucket, conf.Endpoint)
}

// BucketSecretKey returns the Provider Secret Key
// TODO(jojohappy): it should return the key decrypted.
func (conf *BucketConfig) BucketSecretKey() string {
	return conf.secretKey
}

// SetSecretKey to setup the Secret Key for the Provider
func (conf *BucketConfig) SetSecretKey(secretKey string) {
	conf.secretKey = secretKey
}

// Validate checks to see the config options are set.
func (conf *BucketConfig) Validate() error {
	switch conf.Provider {
	case GCS:
		if conf.Bucket == "" {
			return ErrMissingGCSBucket
		}
	case S3:
		if conf.Endpoint == "" ||
			(conf.AccessKey == "" && conf.secretKey != "") ||
			(conf.AccessKey != "" && conf.secretKey == "") {
			return ErrInsufficientS3Info
		}
	default:
		return ErrUnsupported
	}
	return nil
}

// ValidateForTests checks to see the config options for tests are set.
func (conf *BucketConfig) ValidateForTests() error {
	switch conf.Provider {
	case GCS:
		if conf.Bucket == "" {
			return ErrMissingGCSBucket
		}
	case S3:
		if conf.Endpoint == "" ||
			conf.AccessKey == "" ||
			conf.secretKey == "" {
			return ErrInsufficientS3Info
		}
	default:
		return ErrUnsupported
	}
	return nil
}
