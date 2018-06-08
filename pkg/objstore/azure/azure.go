package azure

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	blob "github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vglafirov/thanos/pkg/objstore"
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

	conf.ContainerName = containerName

	ctx := context.Background()

	container, err := getContainer(ctx, conf.StorageAccountName, conf.StorageAccountKey, containerName)

	if err != nil {
		serviceError := err.(blob.StorageError)
		if serviceError.Response().StatusCode == 404 {
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
	fmt.Printf("Iter called DIR: %s\n", dir)
	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	fmt.Print("IsObjNotFoundErr called\n")
	if err != nil {
		serviceError := err.(blob.StorageError)
		if serviceError.Response().StatusCode == 404 {
			fmt.Print("IsObjNotFoundErr: true\n")
			return true
		}
	}
	fmt.Print("IsObjNotFoundErr: false\n")
	return false
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	exists, err := b.Exists(ctx, name)
	fmt.Printf("EXISTS: %v ERR: %s\n", exists, err)
	if exists {
		blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
		get, err := blobURL.Download(ctx, 0, 0, blob.BlobAccessConditions{}, false)

		reader := get.Body(blob.RetryReaderOptions{})
		defer reader.Close()

		return reader, err
	}
	return nil, err
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	fmt.Print("GetRange called\n")
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	get, err := blobURL.Download(ctx, off, length, blob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}
	reader := get.Body(blob.RetryReaderOptions{})
	defer reader.Close()

	return reader, nil
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	fmt.Printf("Exists: %s\n", name)
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := blobURL.GetProperties(ctx, blob.BlobAccessConditions{})
	fmt.Printf("ERR: %s\n", err)
	if !b.IsObjNotFoundErr(err) {
		fmt.Printf("Exists false: %s\n", err)
		return false, err
	}
	fmt.Printf("Exists true: %s\n", err)
	return true, err
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	fmt.Print("Upload called\n")
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := blob.UploadStreamToBlockBlob(ctx, r, blobURL,
		blob.UploadStreamToBlockBlobOptions{
			BufferSize:       128,
			MaxBuffers:       1024,
			BlobHTTPHeaders:  blob.BlobHTTPHeaders{ContentType: "text/plain"},
			Metadata:         blob.Metadata{},
			AccessConditions: blob.BlobAccessConditions{},
		})
	if err != nil {
		return err
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	fmt.Print("Delete called\n")
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := blobURL.Delete(ctx, blob.DeleteSnapshotsOptionInclude, blob.BlobAccessConditions{})
	if err != nil {
		return err
	}
	return nil
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, component string) (objstore.Bucket, func(), error) {
	fmt.Print("NewTestBucket called\n")

	t.Log("Using test Azure bucket.")

	conf := &Config{
		StorageAccountName: os.Getenv("AZURE_STORAGE_ACCOUNT"),
		StorageAccountKey:  os.Getenv("AZURE_STORAGE_ACCESS_KEY"),
	}

	bkt, err := NewBucket(conf, nil, component)
	if err != nil {
		t.Errorf("Cannot create Azure storage container:")
		return nil, nil, err
	}

	//	src := rand.NewSource(time.Now().UnixNano())
	//	name := fmt.Sprintf("test_%s_%x", strings.ToLower(t.Name()), src.Int63())

	return bkt, func() {
		objstore.EmptyBucket(t, context.Background(), bkt)
		bkt.Delete(context.Background(), bkt.config.ContainerName)
		if err != nil {
			t.Logf("deleting bucket failed: %s", err)
		}
	}, nil
}
