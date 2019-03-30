package azure

import (
	"context"
	"fmt"
	"net/url"
	"regexp"

	blob "github.com/Azure/azure-storage-blob-go/azblob"
)

var (
	blobFormatString = `https://%s.blob.core.windows.net`
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const DirDelim = "/"

func getContainerURL(ctx context.Context, accountName, accountKey, containerName string) (blob.ContainerURL, error) {
	c, err := blob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return blob.ContainerURL{}, err
	}
	p := blob.NewPipeline(c, blob.PipelineOptions{
		Telemetry: blob.TelemetryOptions{Value: "Thanos"},
	})
	u, err := url.Parse(fmt.Sprintf(blobFormatString, accountName))
	if err != nil {
		return blob.ContainerURL{}, err
	}
	service := blob.NewServiceURL(*u, p)

	return service.NewContainerURL(containerName), nil
}

func getContainer(ctx context.Context, accountName, accountKey, containerName string) (blob.ContainerURL, error) {
	c, err := getContainerURL(ctx, accountName, accountKey, containerName)
	if err != nil {
		return blob.ContainerURL{}, err
	}
	// Getting container properties to check if it exists or not. Returns error which will be parsed further
	_, err = c.GetProperties(ctx, blob.LeaseAccessConditions{})
	return c, err
}

func createContainer(ctx context.Context, accountName, accountKey, containerName string) (blob.ContainerURL, error) {
	c, err := getContainerURL(ctx, accountName, accountKey, containerName)
	if err != nil {
		return blob.ContainerURL{}, err
	}
	_, err = c.Create(
		context.Background(),
		blob.Metadata{},
		blob.PublicAccessNone)
	return c, err
}

func getBlobURL(ctx context.Context, accountName, accountKey, containerName, blobName string) (blob.BlockBlobURL, error) {
	c, err := getContainerURL(ctx, accountName, accountKey, containerName)
	if err != nil {
		return blob.BlockBlobURL{}, err
	}
	return c.NewBlockBlobURL(blobName), nil
}

func parseError(errorCode string) string {
	re, _ := regexp.Compile(`X-Ms-Error-Code:\D*\[(\w+)\]`)
	match := re.FindStringSubmatch(errorCode)
	if match != nil && len(match) == 2 {
		return match[1]
	}
	return errorCode
}
