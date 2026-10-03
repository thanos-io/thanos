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
	upload := func(id ulid.ULID) {
		m := metadata.NoDownsampleMark{ID: id, Version: metadata.NoDownsampleMarkVersion1, NoDownsampleTime: time.Now().Unix(), Reason: metadata.ManualNoDownsampleReason}
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.NoDownsampleMarkFilename), &buf))
	}
	list := func(lister *block.RecursiveLister) {
		ch := make(chan block.ActiveBlockFetchData, 16)
		_, err := lister.GetActiveAndPartialBlockIDs(ctx, ch)
		testutil.Ok(t, err)
		close(ch)
	}
	metas := func() map[ulid.ULID]*metadata.Meta {
		return map[ulid.ULID]*metadata.Meta{id1: {}, id2: {}, id3: {}}
	}
	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
	insBkt := objstore.WithNoopInstr(bkt)

	// Blocks 1 and 2 are marked when the listing runs; block 3 is marked afterwards,
	// so its marker exists in the bucket but was not listed and must not be probed.
	upload(id1)
	upload(id2)
	lister := block.NewRecursiveLister(log.NewNopLogger(), insBkt)
	list(lister)
	upload(id3)

	f := NewGatherNoDownsampleMarkFilter(log.NewNopLogger(), insBkt, 4).WithListedMarkers(lister)
	testutil.Ok(t, f.Filter(ctx, metas(), synced, nil))
	got := f.NoDownsampleMarkedBlocks()
	testutil.Equals(t, 2, len(got))
	_, ok := got[id3]
	testutil.Equals(t, false, ok)

	// The next listing sees the new marker.
	list(lister)
	testutil.Ok(t, f.Filter(ctx, metas(), synced, nil))
	testutil.Equals(t, 3, len(f.NoDownsampleMarkedBlocks()))

	// A lister that has not listed yet, or no source at all, probes every block.
	for _, src := range []block.ListedMarkersSource{block.NewRecursiveLister(log.NewNopLogger(), insBkt), nil} {
		f = NewGatherNoDownsampleMarkFilter(log.NewNopLogger(), insBkt, 4).WithListedMarkers(src)
		testutil.Ok(t, f.Filter(ctx, metas(), synced, nil))
		testutil.Equals(t, 3, len(f.NoDownsampleMarkedBlocks()))
	}
}
