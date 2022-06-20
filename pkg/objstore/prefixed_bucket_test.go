// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objstore

import (
	"context"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestPrefixedBucket_Acceptance(t *testing.T) {

	prefixes := []string{
		"/someprefix/anotherprefix/",
		"someprefix/anotherprefix/",
		"someprefix/anotherprefix",
		"someprefix/",
		"someprefix"}

	for _, prefix := range prefixes {
		AcceptanceTest(t, NewPrefixedBucket(NewInMemBucket(), prefix))
		UsesPrefixTest(t, NewInMemBucket(), prefix)
	}
}

func UsesPrefixTest(t *testing.T, bkt Bucket, prefix string) {
	testutil.Ok(t, bkt.Upload(context.Background(), strings.Trim(prefix, "/")+"/file1.jpg", strings.NewReader("test-data1")))

	pBkt := NewPrefixedBucket(bkt, prefix)
	rc1, err := pBkt.Get(context.Background(), "file1.jpg")
	testutil.Ok(t, err)

	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc1.Close()) }()
	content, err := ioutil.ReadAll(rc1)
	testutil.Ok(t, err)
	testutil.Equals(t, "test-data1", string(content))

	testutil.Ok(t, pBkt.Upload(context.Background(), "file2.jpg", strings.NewReader("test-data2")))
	rc2, err := bkt.Get(context.Background(), strings.Trim(prefix, "/")+"/file2.jpg")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc2.Close()) }()
	contentUpload, err := ioutil.ReadAll(rc2)
	testutil.Ok(t, err)
	testutil.Equals(t, "test-data2", string(contentUpload))

	testutil.Ok(t, pBkt.Delete(context.Background(), "file2.jpg"))
	_, err = bkt.Get(context.Background(), strings.Trim(prefix, "/")+"/file2.jpg")
	testutil.NotOk(t, err)
	testutil.Assert(t, pBkt.IsObjNotFoundErr(err), "expected not found error got %s", err)

	rc3, err := pBkt.GetRange(context.Background(), "file1.jpg", 1, 3)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rc3.Close()) }()
	content, err = ioutil.ReadAll(rc3)
	testutil.Ok(t, err)
	testutil.Equals(t, "est", string(content))

	ok, err := pBkt.Exists(context.Background(), "file1.jpg")
	testutil.Ok(t, err)
	testutil.Assert(t, ok, "expected exits")

	attrs, err := pBkt.Attributes(context.Background(), "file1.jpg")
	testutil.Ok(t, err)
	testutil.Assert(t, attrs.Size == 10, "expected size to be equal to 10")

	testutil.Ok(t, bkt.Upload(context.Background(), strings.Trim(prefix, "/")+"/dir/file1.jpg", strings.NewReader("test-data1")))
	seen := []string{}
	testutil.Ok(t, pBkt.Iter(context.Background(), "", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}, WithRecursiveIter))
	expected := []string{"dir/file1.jpg", "file1.jpg"}
	sort.Strings(expected)
	sort.Strings(seen)
	testutil.Equals(t, expected, seen)

	seen = []string{}
	testutil.Ok(t, pBkt.Iter(context.Background(), "", func(fn string) error {
		seen = append(seen, fn)
		return nil
	}))
	expected = []string{"dir/", "file1.jpg"}
	sort.Strings(expected)
	sort.Strings(seen)
	testutil.Equals(t, expected, seen)
}
