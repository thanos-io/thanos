// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package verifier

import (
	"context"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestRepairIndexRejectsDownsampledBlocks(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	ctx := Context{Context: context.Background(), Logger: log.NewNopLogger(), Bkt: bkt}
	meta := &metadata.Meta{Thanos: metadata.Thanos{Downsample: metadata.ThanosDownsample{Resolution: 300000}}}

	err := repairIndex(block.HealthStats{}, ctx, ulid.MustNew(1, nil), meta, t.TempDir())
	testutil.NotOk(t, err)
	testutil.Equals(t, "cannot repair downsampled blocks", err.Error())
	testutil.Equals(t, 0, len(bkt.Objects()))
}
