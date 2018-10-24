package azure

import (
	"context"
	"fmt"
	"net/url"
	"regexp"

	blob "github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
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

	return service.NewContainerURL(containerName)
}

func getContainer(ctx context.Context, accountName, accountKey, containerName string) (blob.ContainerURL, error) {
	c := getContainerURL(ctx, accountName, accountKey, containerName)

	// Getting container properties to check if it exists or not. Returns error which will be parsed further
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
	return getContainerURL(ctx, accountName, accountKey, containerName).NewBlockBlobURL(blobName)
}

func parseError(errorCode string) string {
	re, _ := regexp.Compile(`X-Ms-Error-Code:\D*\[(\w+)\]`)
	return re.FindStringSubmatch(errorCode)[1]
}
