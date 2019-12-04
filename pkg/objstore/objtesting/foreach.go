package objtesting

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/thanos-io/thanos/pkg/objstore/filesystem"

	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/azure"
	"github.com/thanos-io/thanos/pkg/objstore/cos"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/objstore/oss"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/objstore/swift"
	"github.com/thanos-io/thanos/pkg/testutil"
)

// ForeachStore runs given test using all available objstore implementations.
// For each it creates a new bucket with a random name and a cleanup function
// that deletes it after test was run.
// Use THANOS_SKIP_<objstorename>_TESTS to skip explicitly certain tests.
func ForeachStore(t *testing.T, testFn func(t testing.TB, bkt objstore.Bucket)) {
	t.Parallel()

	// Mandatory Inmem.
	if ok := t.Run("inmem", func(t *testing.T) {
		t.Parallel()
		testFn(t, inmem.NewBucket())
	}); !ok {
		return
	}

	// Mandatory Filesystem.
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

	// Optional GCS.
	if _, ok := os.LookupEnv("THANOS_SKIP_GCS_TESTS"); !ok {
		t.Run("gcs", func(t *testing.T) {
			bkt, closeFn, err := gcs.NewTestBucket(t, os.Getenv("GCP_PROJECT"))
			testutil.Ok(t, err)

			t.Parallel()
			defer closeFn()

			// TODO(bwplotka): Add leaktest when https://github.com/GoogleCloudPlatform/google-cloud-go/issues/1025 is resolved.
			testFn(t, bkt)
		})

	} else {
		t.Log("THANOS_SKIP_GCS_TESTS envvar present. Skipping test against GCS.")
	}

	// Optional S3.
	if _, ok := os.LookupEnv("THANOS_SKIP_S3_AWS_TESTS"); !ok {
		t.Run("aws s3", func(t *testing.T) {
			// TODO(bwplotka): Allow taking location from envvar.
			bkt, closeFn, err := s3.NewTestBucket(t, "us-west-2")
			testutil.Ok(t, err)

			t.Parallel()
			defer closeFn()

			// TODO(bwplotka): Add leaktest when we fix potential leak in minio library.
			// We cannot use leaktest for detecting our own potential leaks, when leaktest detects leaks in minio itself.
			// This needs to be investigated more.

			testFn(t, bkt)
		})

	} else {
		t.Log("THANOS_SKIP_S3_AWS_TESTS envvar present. Skipping test against S3 AWS.")
	}

	// Optional Azure.
	if _, ok := os.LookupEnv("THANOS_SKIP_AZURE_TESTS"); !ok {
		t.Run("azure", func(t *testing.T) {
			bkt, closeFn, err := azure.NewTestBucket(t, "e2e-tests")
			testutil.Ok(t, err)

			t.Parallel()
			defer closeFn()

			testFn(t, bkt)
		})

	} else {
		t.Log("THANOS_SKIP_AZURE_TESTS envvar present. Skipping test against Azure.")
	}

	// Optional SWIFT.
	if _, ok := os.LookupEnv("THANOS_SKIP_SWIFT_TESTS"); !ok {
		t.Run("swift", func(t *testing.T) {
			container, closeFn, err := swift.NewTestContainer(t)
			testutil.Ok(t, err)

			t.Parallel()
			defer closeFn()

			testFn(t, container)
		})

	} else {
		t.Log("THANOS_SKIP_SWIFT_TESTS envvar present. Skipping test against swift.")
	}

	// Optional COS.
	if _, ok := os.LookupEnv("THANOS_SKIP_TENCENT_COS_TESTS"); !ok {
		t.Run("Tencent cos", func(t *testing.T) {
			bkt, closeFn, err := cos.NewTestBucket(t)
			testutil.Ok(t, err)

			t.Parallel()
			defer closeFn()

			testFn(t, bkt)
		})

	} else {
		t.Log("THANOS_SKIP_TENCENT_COS_TESTS envvar present. Skipping test against Tencent COS.")
	}

	// Optional OSS.
	if _, ok := os.LookupEnv("THANOS_SKIP_ALIYUN_OSS_TESTS"); !ok {
		bkt, closeFn, err := oss.NewTestBucket(t)
		testutil.Ok(t, err)

		ok := t.Run("AliYun oss", func(t *testing.T) {
			testFn(t, bkt)
		})

		closeFn()
		if !ok {
			return
		}
	} else {
		t.Log("THANOS_SKIP_ALIYUN_OSS_TESTS envvar present. Skipping test against AliYun OSS.")
	}
}
