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
	u, err := url.Parse(fmt.Sprintf(blobFormatString, accountName))
	if err != nil {
		return blob.ContainerURL{}
	}
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

func getBlobURL(ctx context.Context, accountName, accountKey, containerName, blobName string) blob.BlockBlobURL {
	container := getContainerURL(ctx, accountName, accountKey, containerName)
	return container.NewBlockBlobURL(blobName)
}
