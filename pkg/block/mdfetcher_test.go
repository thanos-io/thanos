package block

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const (
	testConcurrencyLimit = 5
)

type metadataFetcherTestHarness struct {
	fetcher *MetadataFetcher

	bkt *inmem.Bucket
}

func newMetadataFetcherTestHarness() *metadataFetcherTestHarness {
	bkt := inmem.NewBucket()

	return &metadataFetcherTestHarness{
		bkt:     bkt,
		fetcher: NewMetadataFetcher(log.NewNopLogger(), testConcurrencyLimit, bkt),
	}
}

func TestRemoteBlockMetadataFetcher_Fetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newMetadataFetcherTestHarness()

	blockIDWithoutMD, err := ulid.New(uint64(time.Now().Unix()*1000), nil)
	testutil.Ok(t, err)
	blockIDWithMD, err := ulid.New(uint64(time.Now().Add(time.Minute).Unix()*1000), nil)
	testutil.Ok(t, err)

	var fakeChunk1 bytes.Buffer
	fakeChunk1.Write([]byte{0, 1, 2, 3})
	testutil.Ok(t, h.bkt.Upload(ctx, blockIDWithoutMD.String(), &fakeChunk1))
	var fakeChunk2 bytes.Buffer
	fakeChunk2.Write([]byte{0, 1, 2, 3, 4})
	testutil.Ok(t, h.bkt.Upload(ctx, blockIDWithMD.String(), &fakeChunk2))
	fakeChunk2.Write([]byte{0, 1, 2, 3, 4})

	md := metadata.Meta{}
	mdBytes, err := json.Marshal(md)
	testutil.Ok(t, err)
	var fakeChunk3 bytes.Buffer
	fakeChunk3.Write(mdBytes)
	testutil.Ok(t, h.bkt.Upload(ctx, path.Join(blockIDWithMD.String(), MetaFilename), &fakeChunk3))

	res, err := h.fetcher.Fetch(ctx)

	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(res))
	md1, exist := res[blockIDWithoutMD]
	testutil.Equals(t, true, md1 == nil)
	testutil.Equals(t, true, exist)
	md2, exist := res[blockIDWithMD]
	testutil.Equals(t, true, md2 != nil)
	testutil.Equals(t, true, exist)
}
