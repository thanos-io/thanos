package azure

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	blob "github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	yaml "gopkg.in/yaml.v2"
)

const (
	opObjectsList  = "ListBucket"
	opObjectInsert = "PutObject"
	opObjectGet    = "GetObject"
	opObjectHead   = "HEADObject"
	opObjectDelete = "DeleteObject"
)

// Config Azure storage configuration.
type Config struct {
	StorageAccountName string `yaml:"storage-account"`
	StorageAccountKey  string `yaml:"storage-account-key"`
	ContainerName      string `yaml:"container"`
}

// Bucket implements the store.Bucket interface against s3-compatible APIs.
type Bucket struct {
	logger       log.Logger
	containerURL blob.ContainerURL
	config       *Config
	opsTotal     *prometheus.CounterVec
	closer       blobReadCloser
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
func NewBucket(logger log.Logger, azureConfig []byte, reg prometheus.Registerer, component string) (*Bucket, error) {

	level.Debug(logger).Log("msg", "Creating new Azure bucket connection", "component", component)

	var conf Config
	if err := yaml.Unmarshal(azureConfig, &conf); err != nil {
		return nil, err
	}

	err := conf.Validate()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	container, err := createContainer(ctx, conf.StorageAccountName, conf.StorageAccountKey, conf.ContainerName)
	if err != nil {
		if err.(blob.StorageError).ServiceCode() == "ContainerAlreadyExists" {
			level.Debug(logger).Log("msg", "Using existing Azure blob container", "address", container)
			level.Debug(logger).Log("msg", "Getting connection to existing Azure blob container", "container", conf.ContainerName)
			container, err = getContainer(ctx, conf.StorageAccountName, conf.StorageAccountKey, conf.ContainerName)
			if err != nil {
				level.Error(logger).Log("msg", "Cannot get existing Azure blob container", "address", container)
				return nil, err
			}
		} else {
			level.Error(logger).Log("msg", "Error creating Azure blob container", "address", container)
			return nil, err
		}
	} else {
		level.Info(logger).Log("msg", "Azure blob container successfully created", "address", container)
	}

	bkt := &Bucket{
		logger:       logger,
		containerURL: container,
		config:       &conf,
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
	level.Debug(b.logger).Log("msg", "Iterating through the dir", "dir", dir)
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
	level.Debug(b.logger).Log("msg", "Getting blob", "blob", name, "offset", offset, "length", length)
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

	reader := bytes.NewReader(destBuffer)

	readerCloser := &blobReadCloser{
		Reader: reader,
	}

	return readerCloser, err
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
	level.Debug(b.logger).Log("msg", "Check if blob exists", "blob", name)
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
	level.Debug(b.logger).Log("msg", "Uploading blob", "blob", name)
	b.opsTotal.WithLabelValues(opObjectHead).Inc()
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := blob.UploadStreamToBlockBlob(ctx, r, blobURL,
		blob.UploadStreamToBlockBlobOptions{
			BufferSize: 3 * 1024 * 1024,
			MaxBuffers: 4,
		})
	if err != nil {
		level.Error(b.logger).Log("msg", "Cannot upload Azure blob", "address", name, "error", err)
		return err
	}
	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	level.Debug(b.logger).Log("msg", "Deleting blob", "blob", name)
	b.opsTotal.WithLabelValues(opObjectHead).Inc()
	blobURL := getBlobURL(ctx, b.config.StorageAccountName, b.config.StorageAccountKey, b.config.ContainerName, name)
	_, err := blobURL.Delete(ctx, blob.DeleteSnapshotsOptionInclude, blob.BlobAccessConditions{})
	if err != nil {
		level.Error(b.logger).Log("msg", "Error deleting blob", "address", name)
		return err
	}
	return nil
}

// Name returns the bucket name for gcs.
func (b *Bucket) Name() string {
	return b.config.ContainerName
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB, component string) (objstore.Bucket, func(), error) {

	t.Log("Using test Azure bucket.")

	conf := &Config{
		StorageAccountName: os.Getenv("AZURE_STORAGE_ACCOUNT"),
		StorageAccountKey:  os.Getenv("AZURE_STORAGE_ACCESS_KEY"),
		ContainerName:      "thanos-e2e-test",
	}

	bc, err := yaml.Marshal(conf)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()

	bkt, err := NewBucket(log.NewNopLogger(), bc, nil, component)
	if err != nil {
		t.Errorf("Cannot create Azure storage container:")
		return nil, nil, err
	}

	return bkt, func() {
		objstore.EmptyBucket(t, ctx, bkt)
		err = bkt.Delete(ctx, conf.ContainerName)
		if err != nil {
			t.Logf("deleting bucket failed: %s", err)
		}
	}, nil
}

// Close bucket
func (b *Bucket) Close() error {
	return b.closer.Close()
}
