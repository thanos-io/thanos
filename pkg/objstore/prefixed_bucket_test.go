package objstore

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestPrefixedBucket_Acceptance(t *testing.T) {
	prefix := "/someprefix/anotherprefix/"
	bkt := NewPrefixedBucket(NewInMemBucket(), prefix)
	AcceptanceTest(t, bkt)
	UsesPrefixTest(t, bkt, prefix)

	prefix = "someprefix/anotherprefix/"
	bkt = NewPrefixedBucket(NewInMemBucket(), prefix)
	AcceptanceTest(t, bkt)
	UsesPrefixTest(t, bkt, prefix)

	prefix = "someprefix/anotherprefix"
	bkt = NewPrefixedBucket(NewInMemBucket(), prefix)
	AcceptanceTest(t, bkt)
	UsesPrefixTest(t, bkt, prefix)

	prefix = "someprefix/"
	bkt = NewPrefixedBucket(NewInMemBucket(), prefix)
	AcceptanceTest(t, bkt)
	UsesPrefixTest(t, bkt, prefix)

	prefix = "someprefix"
	bkt = NewPrefixedBucket(NewInMemBucket(), prefix)
	AcceptanceTest(t, bkt)
	UsesPrefixTest(t, bkt, prefix)
}

func UsesPrefixTest(t *testing.T, bkt Bucket, prefix string) {
	bkt.Upload(context.Background(), strings.Trim(prefix, "/")+"/file1.jpg", strings.NewReader("@test-data1"))

	pBkt := NewPrefixedBucket(bkt, prefix)
	rc1, err := pBkt.Get(context.Background(), "file1.jpg")

	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc1.Close()) }()
	content, err := ioutil.ReadAll(rc1)
	testutil.Ok(t, err)
	testutil.Equals(t, "@test-data1", string(content))

	// Upload
	// Delete
	// GetRange
	// Exists
	// IsObjNotFoundErr
}
