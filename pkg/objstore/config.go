package objstore

import (
	"fmt"
	"os"

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
// TODO(jojohappy) should it support multiple bucket?
func NewBucketConfig(cmd *kingpin.CmdClause) *BucketConfig {
	var bucketConfig BucketConfig

	newObjProvider(cmd.Flag("objstore.type", "Specify the provider for object store. If empty or unsupported provider, Thanos won't read and store any block to the object store. Now supported GCS / S3.").
		PlaceHolder("<provider>"), &bucketConfig.Provider)

	cmd.Flag("objstore.bucket", "The bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("OBJSTORE_BUCKET").StringVar(&bucketConfig.Bucket)

	cmd.Flag("objstore.endpoint", "The object store API endpoint for stored blocks. Supported S3-Compatible API").
		PlaceHolder("<api-url>").Envar("OBJSTORE_ENDPOINT").StringVar(&bucketConfig.Endpoint)

	cmd.Flag("objstore.access-key", "Access key for an object store API. Supported S3-Compatible API").
		PlaceHolder("<key>").Envar("OBJSTORE_ACCESS_KEY").StringVar(&bucketConfig.AccessKey)

	bucketConfig.secretKey = os.Getenv("PROVIDER_SECRET_KEY")

	cmd.Flag("objstore.insecure", "Whether to use an insecure connection with an object store API. Supported S3-Compatible API").
		Default("false").Envar("OBJSTORE_INSECURE").BoolVar(&bucketConfig.Insecure)

	cmd.Flag("objstore.signature-version2", "Whether to use S3 Signature Version 2; otherwise Signature Version 4 will be used").
		Default("false").Envar("OBJSTORE_SIGNATURE_VERSION2").BoolVar(&bucketConfig.SignatureV2)

	cmd.Flag("objstore.encrypt-sse", "Whether to use Server Side Encryption").
		Default("false").Envar("OBJSTORE_SSE_ENCRYPTION").BoolVar(&bucketConfig.SSEEncryption)

	return &bucketConfig
}

// NewBackupBucketConfig return the configuration of backup object store
func NewBackupBucketConfig(cmd *kingpin.CmdClause) *BucketConfig {
	var bucketConfig BucketConfig

	newObjProvider(cmd.Flag("objstore-backup.type", "Specify the provider for backup object store. If empty or unsupport provider, Thanos won't backup any block to the object store. Now supported GCS / S3.").
		PlaceHolder("<provider>"), &bucketConfig.Provider)

	cmd.Flag("objstore-backup.bucket", "The bucket name for backup stored blocks.").
		PlaceHolder("<bucket>").StringVar(&bucketConfig.Bucket)

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
