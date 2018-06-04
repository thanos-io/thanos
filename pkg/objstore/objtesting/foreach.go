package objtesting

import (
	"os"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/inmem"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/improbable-eng/thanos/pkg/testutil"
)

// ForeachStore runs given test using all available objstore implementations.
// For each it creates a new bucket with a random name and a cleanup function
// that deletes it after test was run.
// Use THANOS_SKIP_<objstorename>_TESTS to skip explicitly certain tests.
func ForeachStore(t *testing.T, testFn func(t testing.TB, bkt objstore.Bucket)) {
	// Mandatory Inmem.
	if ok := t.Run("inmem", func(t *testing.T) {
		defer leaktest.CheckTimeout(t, 10*time.Second)()

		testFn(t, inmem.NewBucket())

	}); !ok {
		return
	}

	// Optional GCS.
	if _, ok := os.LookupEnv("THANOS_SKIP_GCS_TESTS"); !ok {
		bkt, closeFn, err := gcs.NewTestBucket(t, os.Getenv("GCP_PROJECT"))
		testutil.Ok(t, err)

		ok := t.Run("gcs", func(t *testing.T) {
			// TODO(bplotka): Add leaktest when https://github.com/GoogleCloudPlatform/google-cloud-go/issues/1025 is resolved.
			testFn(t, bkt)
		})
		closeFn()
		if !ok {
			return
		}
	} else {
		t.Log("THANOS_SKIP_GCS_TESTS envvar present. Skipping test against GCS.")
	}

	// Optional S3 AWS.
	// TODO(bplotka): Prepare environment & CI to run it automatically.
	// TODO(bplotka): Find a user with S3 AWS project ready to run this test.
	if _, ok := os.LookupEnv("THANOS_SKIP_S3_AWS_TESTS"); !ok {
		// TODO(bplotka): Allow taking location from envvar.
		bkt, closeFn, err := s3.NewTestBucket(t, "eu-west-1")
		testutil.Ok(t, err)

		ok := t.Run("aws s3", func(t *testing.T) {
			// TODO(bplotka): Add leaktest when we fix potential leak in minio library.
			// We cannot use leaktest for detecting our own potential leaks, when leaktest detects leaks in minio itself.
			// This needs to be investigated more.

			testFn(t, bkt)
		})
		closeFn()
		if !ok {
			return
		}
	} else {
		t.Log("THANOS_SKIP_S3_AWS_TESTS envvar present. Skipping test against S3 AWS.")
	}
}
