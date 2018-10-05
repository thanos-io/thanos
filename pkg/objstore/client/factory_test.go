package client

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"

	"github.com/go-kit/kit/log"
)

const unknownTypeConfig = `type: UNKNOWN
config:
  bucket: test-bucket`

func TestNewBucketUnknownType(t *testing.T) {
	_, err := NewBucket(log.NewNopLogger(), []byte(unknownTypeConfig), nil, "bkt-client-test")
	testutil.NotOk(t, err)
	testutil.Assert(t, err != ErrNotFound, "it should not error with not found")
}

const blankGCSConfig = `type: GCS`

func TestNewBucketBlankConfig(t *testing.T) {
	_, err := NewBucket(log.NewNopLogger(), []byte(blankGCSConfig), nil, "bkt-client-test")
	testutil.NotOk(t, err)
	testutil.Assert(t, err != ErrNotFound, "it should not error with not found")
}

func TestNewBucketNoConfig(t *testing.T) {
	_, err := NewBucket(log.NewNopLogger(), []byte{}, nil, "bkt-client-test")
	testutil.NotOk(t, err)
	testutil.Assert(t, err == ErrNotFound, "it should error with not found")
}
