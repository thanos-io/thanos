package azure

import (
	"context"
	"fmt"
	"net/url"

	blob "github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
)

var (
	blobFormatString = `https://%s.blob.core.windows.net`
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

func getContainerURL(ctx context.Context, accountName, accountKey, containerName string) blob.ContainerURL {
	c := blob.NewSharedKeyCredential(accountName, accountKey)
	p := blob.NewPipeline(c, blob.PipelineOptions{
		Telemetry: blob.TelemetryOptions{Value: "Thanos"},
	})
	u, _ := url.Parse(fmt.Sprintf(blobFormatString, accountName))
	service := blob.NewServiceURL(*u, p)
	container := service.NewContainerURL(containerName)
	return container
}

func getContainer(ctx context.Context, accountName, accountKey, containerName string) (blob.ContainerURL, error) {
	c := getContainerURL(ctx, accountName, accountKey, containerName)

	_, err := c.GetProperties(ctx, blob.LeaseAccessConditions{})
	return c, err
}

func createContainer(ctx context.Context, accountName, accountKey, containerName string) (blob.ContainerURL, error) {
	c := getContainerURL(ctx, accountName, accountKey, containerName)

	_, err := c.Create(
		context.Background(),
		blob.Metadata{},
		blob.PublicAccessNone)
	return c, err
}

func getPageBlobURL(ctx context.Context, accountName, accountKey, containerName, blobName string) blob.PageBlobURL {
	container := getContainerURL(ctx, accountName, accountKey, containerName)
	return container.NewPageBlobURL(blobName)
}

// createPageBlob creates a new test blob in the container specified by env var
func createPageBlob(ctx context.Context, accountName, accountKey, containerName, blobName string, pages int) (blob.PageBlobURL, error) {
	b := getPageBlobURL(ctx, accountName, accountKey, containerName, blobName)

	_, err := b.Create(
		ctx,
		int64(blob.PageBlobPageBytes*pages),
		0,
		blob.BlobHTTPHeaders{
			ContentType: "text/plain",
		},
		blob.Metadata{},
		blob.BlobAccessConditions{},
	)
	return b, err
}
