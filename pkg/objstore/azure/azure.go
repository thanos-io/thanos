package azure

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// Config Azure storage configuration
type Config struct {
	StorageAccountName string
	StorageAccountKey  string
}

// Bucket implements the store.Bucket interface against s3-compatible APIs.
type Bucket struct {
	storageAccount string
	opsTotal       *prometheus.CounterVec
}

// RegisterAzureParams registers the Azure flags and returns an initialized Config struct.
func RegisterAzureParams(cmd *kingpin.CmdClause) *Config {
	var azureConfig Config

	cmd.Flag("azure.storage-account-name", "Azure storage account name.").
		PlaceHolder("<storage-account>").Envar("AZURE_STORAGE_ACCOUNT").StringVar(&azureConfig.StorageAccountName)

	cmd.Flag("azure.storage-account-key", "Azure storage account access key.").
		PlaceHolder("<api-url>").Envar("AZURE_STORAGE_ACCESS_KEY").StringVar(&azureConfig.StorageAccountKey)

	return &azureConfig
}

// Validate checks to see if any of the s3 config options are set.
func (conf *Config) Validate() error {
	if conf.StorageAccountName == "" ||
		conf.StorageAccountKey == "" {
		return errors.New("invalid Azure storage configuration")
	}
	return nil
}

// NewBucket returns a new Bucket using the provided Azure config.
func NewBucket(conf *Config, reg prometheus.Registerer, component string) (*Bucket, error) {
	bkt := &Bucket{
		storageAccount: conf.StorageAccountName,
		opsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_azure_storage_account_operations_total",
			Help:        "Total number of operations that were executed against an Azure storage account.",
			ConstLabels: prometheus.Labels{"storage-account": conf.StorageAccountName},
		}, []string{"operation"}),
	}
	if reg != nil {
		reg.MustRegister(bkt.opsTotal)
	}
	return bkt, nil
}
