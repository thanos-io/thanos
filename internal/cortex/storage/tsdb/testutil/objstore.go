// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package testutil

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/internal/cortex/storage/bucket/filesystem"
)

func PrepareFilesystemBucket(t testing.TB) (objstore.Bucket, string) {
	storageDir, err := ioutil.TempDir(os.TempDir(), "bucket")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(storageDir))
	})

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	return objstore.BucketWithMetrics("test", bkt, nil), storageDir
}
