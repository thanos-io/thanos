// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
)

func TestGatherNoDownsampleMarkFilter_WithListedMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.NewInMemBucket()
	id1, id2, id3 := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil)
	for _, id := range []ulid.ULID{id1, id2} {
		m := metadata.NoDownsampleMark{ID: id, Version: metadata.NoDownsampleMarkVersion1, NoDownsampleTime: time.Now().Unix(), Reason: metadata.ManualNoDownsampleReason}
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.NoDownsampleMarkFilename), &buf))
	}
	metas := func() map[ulid.ULID]*metadata.Meta {
		return map[ulid.ULID]*metadata.Meta{id1: {}, id2: {}, id3: {}}
	}
	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})

	// Only the marker of block 2 was seen by the listing, so block 1 is not probed
	// even though its marker exists in the bucket.
	listed := block.NewListedMarkers()
	listed.Set(map[ulid.ULID]map[string]struct{}{id2: {metadata.NoDownsampleMarkFilename: {}}})
	f := NewGatherNoDownsampleMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 4).WithListedMarkers(listed)
	testutil.Ok(t, f.Filter(ctx, metas(), synced, nil))
	got := f.NoDownsampleMarkedBlocks()
	testutil.Equals(t, 1, len(got))
	_, ok := got[id2]
	testutil.Equals(t, true, ok)

	// Without a complete listing every block is probed.
	f = NewGatherNoDownsampleMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 4).WithListedMarkers(block.NewListedMarkers())
	testutil.Ok(t, f.Filter(ctx, metas(), synced, nil))
	testutil.Equals(t, 2, len(f.NoDownsampleMarkedBlocks()))

	// Without listed markers at all (default) every block is probed.
	f = NewGatherNoDownsampleMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 4)
	testutil.Ok(t, f.Filter(ctx, metas(), synced, nil))
	testutil.Equals(t, 2, len(f.NoDownsampleMarkedBlocks()))
}
