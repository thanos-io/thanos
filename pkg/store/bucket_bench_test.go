package store

import (
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkJSON1(b *testing.B){
	if ok := t.Run("filesystem", func(t *testing.T) {
		t.Parallel()

		dir, err := ioutil.TempDir("", "filesystem-foreach-store-test")
		testutil.Ok(t, err)
		defer testutil.Ok(t, os.RemoveAll(dir))

		b, err := filesystem.NewBucket(dir)
		testutil.Ok(t, err)
		testFn(t, b)
	}); !ok {
		return
	}


	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "test_bucketstore_e2e")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	s := prepareStoreWithTestBlocks(t, dir, bkt, false, 0, emptyRelabelConfig)
	defer s.Close()

	t.Log("Test with no index cache")
	s.cache.SwapWith(noopCache{})
	testBucketStore_e2e(t, ctx, s)

	t.Log("Test with large, sufficient index cache")
	indexCache, err := storecache.NewIndexCache(s.logger, nil, storecache.Opts{
		MaxItemSizeBytes: 1e5,
		MaxSizeBytes:     2e5,
	})
	testutil.Ok(t, err)
	s.cache.SwapWith(indexCache)
	testBucketStore_e2e(t, ctx, s)

	t.Log("Test with small index cache")
	indexCache2, err := storecache.NewIndexCache(s.logger, nil, storecache.Opts{
		MaxItemSizeBytes: 50,
		MaxSizeBytes:     100,
	})
	testutil.Ok(t, err)
	s.cache.SwapWith(indexCache2)
	testBucketStore_e2e(t, ctx, s)




}