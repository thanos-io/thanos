// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"testing"

	"github.com/go-kit/kit/log"

	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestUploadTombstone(t *testing.T) {
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())

	{
		SampleTombstone := NewTombstone("up{a=\"b\"}", 00, 9999999)
		err := UploadTombstone(SampleTombstone, bkt, log.NewNopLogger())
		testutil.Ok(t, err)
	}
}
