// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestGatherMarkedBlocks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	bkt := objstore.NewInMemBucket()

	ids := []ulid.ULID{
		ulid.MustNew(1, nil),
		ulid.MustNew(2, nil),
		ulid.MustNew(3, nil),
	}
	metas := make(map[ulid.ULID]*metadata.Meta, len(ids))
	for _, id := range ids {
		metas[id] = &metadata.Meta{}
	}

	testutil.Ok(t, MarkForNoCompact(ctx, logger, bkt, ids[0], metadata.ManualNoCompactReason, "details", prometheus.NewCounter(prometheus.CounterOpts{})))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(ids[1].String(), metadata.NoCompactMarkFilename), bytes.NewBufferString("{")))

	markedBlocks, err := GatherMarkedBlocks(
		ctx,
		logger,
		objstore.WithNoopInstr(bkt),
		metas,
		2,
		func() *metadata.NoCompactMark { return &metadata.NoCompactMark{} },
		metadata.NoCompactMarkFilename,
		nil,
		MarkedForNoCompactionMeta,
	)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(markedBlocks))
	testutil.Equals(t, metadata.ManualNoCompactReason, markedBlocks[ids[0]].Reason)
}
