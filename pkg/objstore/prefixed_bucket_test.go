package objstore

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestPrefixedBucket_Acceptance(t *testing.T) {
	bkt := NewPrefixedBucket(NewInMemBucket(), "/someprefix/anotherprefix/")
	AcceptanceTest(t, bkt)

	bkt = NewPrefixedBucket(NewInMemBucket(), "someprefix/anotherprefix/")
	AcceptanceTest(t, bkt)

	bkt = NewPrefixedBucket(NewInMemBucket(), "someprefix/anotherprefix")
	AcceptanceTest(t, bkt)

	bkt = NewPrefixedBucket(NewInMemBucket(), "someprefix/")
	AcceptanceTest(t, bkt)

	bkt = NewPrefixedBucket(NewInMemBucket(), "someprefix")
	AcceptanceTest(t, bkt)
}

func TestPrefixedBucket_UsesPrefix(t *testing.T) {
	bkt := NewInMemBucket()
	bkt.Upload(context.Background(), "our_prefix/file1.jpg", strings.NewReader("@test-data1"))

	pBkt := NewPrefixedBucket(bkt, "our_prefix")
	rc1, err := pBkt.Get(context.Background(), "file1.jpg")

	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc1.Close()) }()
	content, err := ioutil.ReadAll(rc1)
	testutil.Ok(t, err)
	testutil.Equals(t, "@test-data1", string(content))
}
