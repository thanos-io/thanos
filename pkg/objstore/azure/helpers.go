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

// putPage adds a page to the page blob
// func putPage(ctx context.Context, accountName, containerName, blobName, message string, page int) error {
// 	b := getPageBlobURL(ctx, accountName, containerName, blobName)

// 	fullMessage := make([]byte, blob.PageBlobPageBytes)
// 	for i, e := range []byte(message) {
// 		fullMessage[i] = e
// 	}

// 	_, err := b.PutPages(ctx,
// 		blob.PageRange{
// 			Start: int32(page * blob.PageBlobPageBytes),
// 			End:   int32((page+1)*blob.PageBlobPageBytes - 1),
// 		},
// 		bytes.NewReader(fullMessage),
// 		blob.BlobAccessConditions{},
// 	)
// 	return err
// }

// // clearPage clears the specified page in the page blob
// func clearPage(ctx context.Context, accountName, containerName, blobName string, page int) error {
// 	b := getPageBlobURL(ctx, accountName, containerName, blobName)

// 	_, err := b.ClearPages(ctx,
// 		blob.PageRange{
// 			Start: int32(page * blob.PageBlobPageBytes),
// 			End:   int32((page+1)*blob.PageBlobPageBytes - 1),
// 		},
// 		blob.BlobAccessConditions{},
// 	)
// 	return err
// }

// // getPageRanges gets a list of valid page ranges in the page blob
// func getPageRanges(ctx context.Context, accountName, containerName, blobName string, pages int) (*blob.PageList, error) {
// 	b := getPageBlobURL(ctx, accountName, containerName, blobName)
// 	return b.GetPageRanges(
// 		ctx,
// 		blob.BlobRange{
// 			Offset: 0 * blob.PageBlobPageBytes,
// 			Count:  int64(pages*blob.PageBlobPageBytes - 1),
// 		},
// 		blob.BlobAccessConditions{},
// 	)
// }
