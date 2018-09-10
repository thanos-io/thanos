package client

import (
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"

	"github.com/go-kit/kit/log"
)

func TestErrorBucketConfig(t *testing.T) {
	conf := `
    type: FAKE-GCS
    config:
        bucket: test-bucket
    `
	_, err := NewBucket(log.NewNopLogger(), conf, nil, "bkt-client-test")
	testutil.NotOk(t, err)
	testutil.Assert(t, err != ErrNotFound, "it should not error with not found")
}

func TestBlankBucketConfig(t *testing.T) {
	conf := ""
	_, err := NewBucket(log.NewNopLogger(), conf, nil, "bkt-client-test")
	testutil.NotOk(t, err)
	testutil.Assert(t, err == ErrNotFound, "it should error with not found")
}

func TestBlankBucketConfigContent(t *testing.T) {
	conf := `
    type: GCS
    `
	_, err := NewBucket(log.NewNopLogger(), conf, nil, "bkt-client-test")
	testutil.NotOk(t, err)
	testutil.Assert(t, err != ErrNotFound, "it should not error with not found")
}
