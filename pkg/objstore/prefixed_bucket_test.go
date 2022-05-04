package objstore

import "testing"

func TestPrefixedBucket_AcceptanceTest(t *testing.T) {
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
