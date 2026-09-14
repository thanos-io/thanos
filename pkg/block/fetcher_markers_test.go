// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func uploadTestMeta(t *testing.T, ctx context.Context, bkt objstore.Bucket, id ulid.ULID) {
	var m metadata.Meta
	m.Version = 1
	m.ULID = id
	var buf bytes.Buffer
	testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), MetaFilename), &buf))
}

func TestRecursiveLister_RecordsListedMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.NewInMemBucket()
	for _, id := range []ulid.ULID{ULID(1), ULID(2), ULID(3)} {
		uploadTestMeta(t, ctx, bkt, id)
	}
	testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(1).String(), metadata.DeletionMarkFilename), bytes.NewBufferString("{}")))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(3).String(), metadata.NoCompactMarkFilename), bytes.NewBufferString("{}")))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(3).String(), metadata.NoDownsampleMarkFilename), bytes.NewBufferString("{}")))
	// A marker file name below the block directory is not a marker.
	testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(2).String(), "chunks", metadata.DeletionMarkFilename), bytes.NewBufferString("{}")))

	markers := NewListedMarkers()
	testutil.Equals(t, false, markers.Complete())
	// Nothing listed yet: every marker has to be probed.
	testutil.Equals(t, true, markers.ShouldProbe(ULID(2), metadata.DeletionMarkFilename))

	lister := NewRecursiveListerWithMarkers(log.NewNopLogger(), objstore.WithNoopInstr(bkt), markers)
	activeBlocksCh := make(chan ActiveBlockFetchData, 10)
	partial, err := lister.GetActiveAndPartialBlockIDs(ctx, activeBlocksCh)
	testutil.Ok(t, err)
	close(activeBlocksCh)

	var active []ulid.ULID
	for b := range activeBlocksCh {
		active = append(active, b.ULID)
		testutil.Equals(t, false, partial[b.ULID])
	}
	testutil.Equals(t, 3, len(active))

	testutil.Equals(t, true, markers.Complete())
	testutil.Equals(t, true, markers.ShouldProbe(ULID(1), metadata.DeletionMarkFilename))
	testutil.Equals(t, false, markers.ShouldProbe(ULID(1), metadata.NoCompactMarkFilename))
	testutil.Equals(t, false, markers.ShouldProbe(ULID(1), metadata.NoDownsampleMarkFilename))
	testutil.Equals(t, false, markers.ShouldProbe(ULID(2), metadata.DeletionMarkFilename))
	testutil.Equals(t, false, markers.ShouldProbe(ULID(3), metadata.DeletionMarkFilename))
	testutil.Equals(t, true, markers.ShouldProbe(ULID(3), metadata.NoCompactMarkFilename))
	testutil.Equals(t, true, markers.ShouldProbe(ULID(3), metadata.NoDownsampleMarkFilename))
	// A block the listing never saw has no markers to probe either.
	testutil.Equals(t, false, markers.ShouldProbe(ULID(4), metadata.DeletionMarkFilename))

	// A nil ListedMarkers keeps the probing behaviour.
	var none *ListedMarkers
	testutil.Equals(t, false, none.Complete())
	testutil.Equals(t, true, none.ShouldProbe(ULID(2), metadata.DeletionMarkFilename))

	// The lister without markers does not touch any index.
	untouched := NewListedMarkers()
	plainCh := make(chan ActiveBlockFetchData, 10)
	_, err = NewRecursiveLister(log.NewNopLogger(), objstore.WithNoopInstr(bkt)).GetActiveAndPartialBlockIDs(ctx, plainCh)
	testutil.Ok(t, err)
	close(plainCh)
	testutil.Equals(t, false, untouched.Complete())
}

func TestIgnoreDeletionMarkFilter_Filter_WithListedMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.NewInMemBucket()
	now := time.Now()
	for _, m := range []*metadata.DeletionMark{
		{ID: ULID(1), DeletionTime: now.Add(-15 * time.Hour).Unix(), Version: metadata.DeletionMarkVersion1},
		{ID: ULID(2), DeletionTime: now.Add(-60 * time.Hour).Unix(), Version: metadata.DeletionMarkVersion1},
	} {
		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(m))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ID.String(), metadata.DeletionMarkFilename), &buf))
	}
	input := func() map[ulid.ULID]*metadata.Meta {
		return map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(2): {}, ULID(3): {}}
	}
	newFilter := func(l *ListedMarkers) *IgnoreDeletionMarkFilter {
		return NewIgnoreDeletionMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 48*time.Hour, 4).WithListedMarkers(l)
	}

	// The listing only saw the marker of block 2: block 1 is not probed, so its
	// marker is neither reported nor applied; block 2 is filtered as usual.
	listed := NewListedMarkers()
	listed.Set(map[ulid.ULID]map[string]struct{}{ULID(2): {metadata.DeletionMarkFilename: {}}})
	f := newFilter(listed)
	metas := input()
	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, metas, m.Synced, nil))
	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(3): {}}, metas)
	testutil.Equals(t, 1, len(f.DeletionMarkBlocks()))
	_, ok := f.DeletionMarkBlocks()[ULID(2)]
	testutil.Equals(t, true, ok)

	// A listing that saw both markers probes both: block 1 is reported (within the
	// delay, so kept), block 2 is filtered.
	listed.Set(map[ulid.ULID]map[string]struct{}{
		ULID(1): {metadata.DeletionMarkFilename: {}},
		ULID(2): {metadata.DeletionMarkFilename: {}},
	})
	f = newFilter(listed)
	metas = input()
	testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(3): {}}, metas)
	testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))

	// Without a complete listing every block is probed, as without ListedMarkers.
	f = newFilter(NewListedMarkers())
	metas = input()
	testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(3): {}}, metas)
	testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))
}
