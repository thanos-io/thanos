// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"math/big"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/objstore"
)

func TestParallelBucket_InMemoryBuffering(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	parallelBucket := &parallelBucketReader{
		BucketReader:  bkt,
		tmpDir:        "",
		partitionSize: 100,
	}
	testParallelBucket(t, bkt, parallelBucket)
}

func TestParallelBucket_TmpFileBuffering(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	parallelBucket := &parallelBucketReader{
		BucketReader:  bkt,
		tmpDir:        t.TempDir(),
		partitionSize: 100,
	}
	testParallelBucket(t, bkt, parallelBucket)
}

func testParallelBucket(t *testing.T, bkt objstore.Bucket, parallelBucket *parallelBucketReader) {
	name := "test/data"
	ctx := context.Background()

	var size int64 = 10 * 1024
	o, err := rand.Int(rand.Reader, big.NewInt(size/2))
	testutil.Ok(t, err)
	offset := o.Int64()

	l, err := rand.Int(rand.Reader, big.NewInt(size/2))
	testutil.Ok(t, err)
	length := l.Int64()

	randBytes := uploadRandom(t, ctx, bkt, name, size)

	r1, err := parallelBucket.GetRange(ctx, name, offset, length)
	testutil.Ok(t, err)

	parallelBytes, err := io.ReadAll(r1)
	testutil.Ok(t, err)
	testutil.Assert(t, length == int64(len(parallelBytes)))

	expectedBytes := randBytes[offset : offset+length]
	testutil.Assert(t, length == int64(len(expectedBytes)))
	testutil.Equals(t, expectedBytes, parallelBytes)

	r2, err := bkt.GetRange(ctx, name, offset, length)
	testutil.Ok(t, err)
	memoryBytes, err := io.ReadAll(r2)
	testutil.Ok(t, err)
	testutil.Assert(t, length == int64(len(memoryBytes)))
	testutil.Equals(t, memoryBytes, parallelBytes)

	err = r1.Close()
	testutil.Ok(t, err)

	err = r2.Close()
	testutil.Ok(t, err)
}

func uploadRandom(t *testing.T, ctx context.Context, bkt objstore.Bucket, name string, size int64) []byte {
	b := make([]byte, size)
	_, err := rand.Read(b)
	testutil.Ok(t, err)
	r := bytes.NewReader(b)
	err = bkt.Upload(ctx, name, r)
	testutil.Ok(t, err)

	return b
}
