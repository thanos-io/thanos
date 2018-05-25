package azure

import (
	"bytes"
	"context"
	"fmt"
	"io"

	blob "github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// Config Azure storage configuration
type Config struct {
	StorageAccountName string
	StorageAccountKey  string
	ContainerName      string
}

// Bucket implements the store.Bucket interface against s3-compatible APIs.
type Bucket struct {
	containerURL blob.ContainerURL
	config       *Config
	opsTotal     *prometheus.CounterVec
}

// RegisterAzureParams registers the Azure flags and returns an initialized Config struct.
func RegisterAzureParams(cmd *kingpin.CmdClause) *Config {
	var azureConfig Config

	cmd.Flag("azure.storage", "Azure storage account name.").
		PlaceHolder("<sa>").Envar("AZURE_STORAGE_ACCOUNT").StringVar(&azureConfig.StorageAccountName)

	cmd.Flag("azure.access-key", "Azure storage account access key.").
		PlaceHolder("<key>").Envar("AZURE_STORAGE_ACCESS_KEY").StringVar(&azureConfig.StorageAccountKey)

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

	containerName := fmt.Sprintf("thanos-%s", component)

	ctx := context.Background()

	container, err := getContainer(ctx, conf.StorageAccountName, conf.StorageAccountKey, containerName)

	if err != nil {
		serviceError := err.(blob.StorageError)
		if serviceError.ServiceCode() == "ContainerNotFound" {
			container, err = createContainer(ctx, conf.StorageAccountName, conf.StorageAccountKey, containerName)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	bkt := &Bucket{
		containerURL: container,
		config:       conf,
		opsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "thanos_objstore_azure_storage_operations_total",
			Help:        "Total number of operations that were executed against an Azure storage account.",
			ConstLabels: prometheus.Labels{"storage_account": conf.StorageAccountName},
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
	return nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	pageblob := getPageBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	properties, err := pageblob.GetProperties(ctx, blob.BlobAccessConditions{})
	if err != nil {
		return nil, err
	}
	resp, err := pageblob.Download(ctx, 0, properties.ContentLength(), blob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}

	defer resp.Body(blob.RetryReaderOptions{MaxRetryRequests: 3}).Close()

	return resp.Body(blob.RetryReaderOptions{MaxRetryRequests: 3}), nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	pageblob := getPageBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	resp, err := pageblob.Download(ctx, off, length, blob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}

	defer resp.Body(blob.RetryReaderOptions{MaxRetryRequests: 3}).Close()

	return resp.Body(blob.RetryReaderOptions{MaxRetryRequests: 3}), nil
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	pageblob := getPageBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)

	resp, err := pageblob.GetProperties(ctx, blob.BlobAccessConditions{})
	if err != nil {
		if resp.StatusCode() == 404 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	exists, err := b.Exists(ctx, name)
	if err != nil {
		return err
	}
	var pageblob blob.PageBlobURL

	if !exists {
		pageblob, _ = createPageBlob(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name, 0)
	}

	buff := bytes.NewBuffer(nil)

	_, err = pageblob.UploadPages(
		ctx,
		0,
		bytes.NewReader(buff.Bytes()),
		blob.BlobAccessConditions{},
	)
	if err != nil {
		return err
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	pageblob := getPageBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := pageblob.Delete(ctx, blob.DeleteSnapshotsOptionInclude, blob.BlobAccessConditions{})
	if err != nil {
		return err
	}
	return nil
}
