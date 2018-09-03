package objstore

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

type objProvider string

const (
	GCS objProvider = "GCS"
	S3  objProvider = "S3"
)

func newObjProvider(s kingpin.Settings, op *objProvider) {
	s.SetValue(op)
	return
}

func (p *objProvider) Set(value string) error {
	*p = objProvider(value)
	return nil
}

func (p *objProvider) String() string {
	return string(*p)
}

// BucketConfig encapsulates the necessary config values to instantiate an object store client.
// Use Provider to represent the Object Stores
type BucketConfig struct {
	Provider      objProvider
	Bucket        string
	Endpoint      string
	AccessKey     string
	secretKey     string
	Insecure      bool
	SignatureV2   bool
	SSEEncryption bool
}

// NewBucketConfig return the configuration of object store
func NewBucketConfig(cmd *kingpin.CmdClause, suffix string) *BucketConfig {
	var bucketConfig BucketConfig
	flagSuffix := ""
	envSuffix := ""
	if strings.Trim(suffix, " ") != "" {
		flagSuffix = "-" + suffix
		envSuffix = "_" + strings.ToUpper(suffix)
	}

	newObjProvider(cmd.Flag(fmt.Sprintf("objstore%s.type", flagSuffix), "Specify the provider for object store. If empty or unsupported provider, Thanos won't read and store any block to the object store. Now supported GCS / S3.").
		PlaceHolder("<provider>"), &bucketConfig.Provider)

	cmd.Flag(fmt.Sprintf("objstore%s.bucket", flagSuffix), "The bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar(fmt.Sprintf("OBJSTORE%s_BUCKET", envSuffix)).StringVar(&bucketConfig.Bucket)

	cmd.Flag(fmt.Sprintf("objstore%s.endpoint", flagSuffix), "The object store API endpoint for stored blocks. Supported S3-Compatible API").
		PlaceHolder("<api-url>").Envar(fmt.Sprintf("OBJSTORE%s_ENDPOINT", envSuffix)).StringVar(&bucketConfig.Endpoint)

	cmd.Flag(fmt.Sprintf("objstore%s.access-key", flagSuffix), "Access key for an object store API. Supported S3-Compatible API").
		PlaceHolder("<key>").Envar(fmt.Sprintf("OBJSTORE%s_ACCESS_KEY", envSuffix)).StringVar(&bucketConfig.AccessKey)

	bucketConfig.secretKey = os.Getenv(fmt.Sprintf("OBJSTORE%s_SECRET_KEY", envSuffix))

	cmd.Flag(fmt.Sprintf("objstore%s.insecure", flagSuffix), "Whether to use an insecure connection with an object store API. Supported S3-Compatible API").
		Default("false").Envar(fmt.Sprintf("OBJSTORE%s_INSECURE", envSuffix)).BoolVar(&bucketConfig.Insecure)

	cmd.Flag(fmt.Sprintf("objstore%s.signature-version2", flagSuffix), "Whether to use S3 Signature Version 2; otherwise Signature Version 4 will be used").
		Default("false").Envar(fmt.Sprintf("OBJSTORE%s_SIGNATURE_VERSION2", envSuffix)).BoolVar(&bucketConfig.SignatureV2)

	cmd.Flag(fmt.Sprintf("objstore%s.encrypt-sse", flagSuffix), "Whether to use Server Side Encryption").
		Default("false").Envar(fmt.Sprintf("OBJSTORE%s_SSE_ENCRYPTION", envSuffix)).BoolVar(&bucketConfig.SSEEncryption)

	return &bucketConfig
}

// String returns the Provider information
func (conf *BucketConfig) String() string {
	return fmt.Sprintf("Provider: %s, Bucket: %s, Endpoint: %s", string(conf.Provider), conf.Bucket, conf.Endpoint)
}

// GetSecretKey returns the Provider Secret Key
func (conf *BucketConfig) GetSecretKey() string {
	return conf.secretKey
}

// SetSecretKey to setup the Secret Key for the Provider
func (conf *BucketConfig) SetSecretKey(secretKey string) {
	conf.secretKey = secretKey
}
