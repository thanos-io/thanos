package azure

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

	blob "github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vglafirov/thanos/pkg/objstore"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	opObjectsList  = "ListBucket"
	opObjectInsert = "PutObject"
	opObjectGet    = "GetObject"
	opObjectHead   = "HEADObject"
	opObjectDelete = "DeleteObject"
)

// Config Azure storage configuration
type Config struct {
	StorageAccountName string
	StorageAccountKey  string
	ContainerName      string
}

// Bucket implements the store.Bucket interface against s3-compatible APIs.
type Bucket struct {
	logger       log.Logger
	containerURL blob.ContainerURL
	config       *Config
	opsTotal     *prometheus.CounterVec
	closer       io.Closer
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
func NewBucket(logger log.Logger, conf *Config, reg prometheus.Registerer, component string) (*Bucket, error) {

	containerName := fmt.Sprintf("thanos-%s", component)

	conf.ContainerName = containerName

	ctx := context.Background()

	level.Info(logger).Log("msg", "Starting Azure storage provider")

	container, err := createContainer(ctx, conf.StorageAccountName, conf.StorageAccountKey, containerName)
	if err != nil {
		if err.(blob.StorageError).ServiceCode() == "ContainerAlreadyExists" {
			level.Info(logger).Log("msg", "Using existent container", "address", container)
			container, err = getContainer(ctx, conf.StorageAccountName, conf.StorageAccountKey, conf.ContainerName)
			if err != nil {
				level.Error(logger).Log("msg", "Cannot get existing container", "address", container)
				return nil, err
			}
		} else {
			level.Error(logger).Log("msg", "Error creating container", "address", container)
			return nil, err
		}
	} else {
		level.Info(logger).Log("msg", "Container successfully created", "address", container)
	}

	bkt := &Bucket{
		logger:       logger,
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
	b.opsTotal.WithLabelValues(opObjectsList).Inc()
	var prefix string
	if dir == "" {
		prefix = ""
	} else if !strings.HasSuffix(dir, DirDelim) {
		prefix = dir + DirDelim
	} else {
		prefix = dir
	}

	list, err := b.containerURL.ListBlobsHierarchySegment(ctx, blob.Marker{}, DirDelim, blob.ListBlobsSegmentOptions{
		Prefix: prefix,
	})
	if err != nil {
		level.Error(b.logger).Log("msg", "Cannot list blogs in directory %s", dir)
		return err
	}
	var listNames []string

	for _, blob := range list.Segment.BlobItems {
		listNames = append(listNames, blob.Name)
	}

	for _, blobPrefix := range list.Segment.BlobPrefixes {
		listNames = append(listNames, blobPrefix.Name)
	}

	for _, name := range listNames {
		if err := f(name); err != nil {
			return err
		}
	}
	return nil
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	switch errorCode := parseError(err.Error()); errorCode {
	case "InvalidUri":
		return true
	case "BlobNotFound":
		return true
	default:
		return false
	}
}

func (b *Bucket) getBlobReader(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.New("X-Ms-Error-Code: [EmptyContainerName]")
	}
	exists, err := b.Exists(ctx, name)

	if !exists {
		return nil, errors.New("X-Ms-Error-Code: [BlobNotFound]")
	}

	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)

	props, err := blobURL.GetProperties(ctx, blob.BlobAccessConditions{})
	if err != nil {
		level.Error(b.logger).Log("msg", "Cannot get properties for container", "address", name)
		return nil, err
	}

	var size int64
	if length > 0 {
		size = length
	} else {
		size = props.ContentLength() - offset
	}

	var destBuffer []byte

	destBuffer = make([]byte, size)

	err = blob.DownloadBlobToBuffer(context.Background(), blobURL.BlobURL, offset, length,
		blob.BlobAccessConditions{}, destBuffer, blob.DownloadFromBlobOptions{
			BlockSize:   blob.BlobDefaultDownloadBlockSize,
			Parallelism: uint16(3),
			Progress:    nil,
		})

	reader := ioutil.NopCloser(bytes.NewReader(destBuffer))

	defer reader.Close()

	return reader, err
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getBlobReader(ctx, name, 0, blob.CountToEnd)
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getBlobReader(ctx, name, off, length)
}

// Exists checks if the given object exists.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	b.opsTotal.WithLabelValues(opObjectHead).Inc()
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := blobURL.GetProperties(ctx, blob.BlobAccessConditions{})
	if b.IsObjNotFoundErr(err) {
		return false, nil
	}
	return true, nil
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	b.opsTotal.WithLabelValues(opObjectHead).Inc()
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
		level.Error(b.logger).Log("msg", "Cannot upload blob", "address", name)
		return err
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	b.opsTotal.WithLabelValues(opObjectHead).Inc()
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := blobURL.Delete(ctx, blob.DeleteSnapshotsOptionInclude, blob.BlobAccessConditions{})
	if err != nil {
		level.Error(b.logger).Log("msg", "Error deleting blob", "address", name)
		return err
	}
	return nil
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, component string) (objstore.Bucket, func(), error) {

	t.Log("Using test Azure bucket.")

	conf := &Config{
		StorageAccountName: os.Getenv("AZURE_STORAGE_ACCOUNT"),
		StorageAccountKey:  os.Getenv("AZURE_STORAGE_ACCESS_KEY"),
	}

	bkt, err := NewBucket(log.NewNopLogger(), conf, nil, component)
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

// Close bucket
func (b *Bucket) Close() error {
	return b.closer.Close()
}
