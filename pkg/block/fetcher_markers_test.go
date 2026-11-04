// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

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

func uploadTestDeletionMark(t *testing.T, ctx context.Context, bkt objstore.Bucket, id ulid.ULID, age time.Duration) {
	m := metadata.DeletionMark{ID: id, DeletionTime: time.Now().Add(-age).Unix(), Version: metadata.DeletionMarkVersion1}
	var buf bytes.Buffer
	testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(id.String(), metadata.DeletionMarkFilename), &buf))
}

// staticMarkers is a ListedMarkersSource with a fixed answer.
type staticMarkers struct {
	listed *ListedMarkers
}

func (s staticMarkers) ListedMarkers() *ListedMarkers { return s.listed }

func fetchedULIDs(metas map[ulid.ULID]*metadata.Meta) map[ulid.ULID]struct{} {
	out := make(map[ulid.ULID]struct{}, len(metas))
	for id := range metas {
		out[id] = struct{}{}
	}
	return out
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

	lister := NewRecursiveLister(log.NewNopLogger(), objstore.WithNoopInstr(bkt))

	// No listing yet: no information, so every marker has to be probed.
	testutil.Equals(t, (*ListedMarkers)(nil), lister.ListedMarkers())
	testutil.Equals(t, true, lister.ListedMarkers().ShouldProbe(ULID(2), metadata.DeletionMarkFilename))

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

	listed := lister.ListedMarkers()
	testutil.Assert(t, listed != nil, "a complete listing must record markers")
	testutil.Equals(t, true, listed.ShouldProbe(ULID(1), metadata.DeletionMarkFilename))
	testutil.Equals(t, false, listed.ShouldProbe(ULID(1), metadata.NoCompactMarkFilename))
	testutil.Equals(t, false, listed.ShouldProbe(ULID(1), metadata.NoDownsampleMarkFilename))
	testutil.Equals(t, false, listed.ShouldProbe(ULID(2), metadata.DeletionMarkFilename))
	testutil.Equals(t, false, listed.ShouldProbe(ULID(3), metadata.DeletionMarkFilename))
	testutil.Equals(t, true, listed.ShouldProbe(ULID(3), metadata.NoCompactMarkFilename))
	testutil.Equals(t, true, listed.ShouldProbe(ULID(3), metadata.NoDownsampleMarkFilename))
	// A block the listing never saw has no markers to probe either.
	testutil.Equals(t, false, listed.ShouldProbe(ULID(4), metadata.DeletionMarkFilename))

	// Nil lister and nil markers keep the probing behavior.
	var none *RecursiveLister
	testutil.Equals(t, (*ListedMarkers)(nil), none.ListedMarkers())
	testutil.Equals(t, true, none.ListedMarkers().ShouldProbe(ULID(1), metadata.DeletionMarkFilename))
}

func TestIgnoreDeletionMarkFilter_Filter_WithListedMarkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.NewInMemBucket()
	uploadTestDeletionMark(t, ctx, bkt, ULID(1), 15*time.Hour)
	uploadTestDeletionMark(t, ctx, bkt, ULID(2), 60*time.Hour)
	input := func() map[ulid.ULID]*metadata.Meta {
		return map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(2): {}, ULID(3): {}}
	}
	newFilter := func(src ListedMarkersSource) *IgnoreDeletionMarkFilter {
		return NewIgnoreDeletionMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 48*time.Hour, 4).WithListedMarkers(src)
	}
	deletion := map[string]struct{}{metadata.DeletionMarkFilename: {}}

	// The listing only saw the marker of block 2: block 1 is not probed, so its
	// marker is neither reported nor applied; block 2 is filtered as usual.
	f := newFilter(staticMarkers{&ListedMarkers{markers: map[ulid.ULID]map[string]struct{}{ULID(2): deletion}}})
	metas := input()
	testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(3): {}}, metas)
	testutil.Equals(t, 1, len(f.DeletionMarkBlocks()))
	_, ok := f.DeletionMarkBlocks()[ULID(2)]
	testutil.Equals(t, true, ok)

	// A listing that saw both markers probes both: block 1 is reported (within
	// the delay, so kept), block 2 is filtered.
	f = newFilter(staticMarkers{&ListedMarkers{markers: map[ulid.ULID]map[string]struct{}{ULID(1): deletion, ULID(2): deletion}}})
	metas = input()
	testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
	testutil.Equals(t, map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(3): {}}, metas)
	testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))

	// A source without a completed listing, or no source at all, probes every block.
	for _, src := range []ListedMarkersSource{staticMarkers{nil}, nil} {
		f = newFilter(src)
		metas = input()
		testutil.Ok(t, f.Filter(ctx, metas, newTestFetcherMetrics().Synced, nil))
		testutil.Equals(t, map[ulid.ULID]*metadata.Meta{ULID(1): {}, ULID(3): {}}, metas)
		testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))
	}
}

// markerProbeBucket counts GETs of marker files and can make listings fail.
type markerProbeBucket struct {
	objstore.InstrumentedBucketReader
	markerGets atomic.Int64
	failIter   atomic.Bool
}

func (b *markerProbeBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if isMarkerFile(path.Base(name)) {
		b.markerGets.Inc()
	}
	return b.InstrumentedBucketReader.Get(ctx, name)
}

func (b *markerProbeBucket) ReaderWithExpectedErrs(objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b
}

func (b *markerProbeBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if b.failIter.Load() {
		return errors.New("listing failed")
	}
	return b.InstrumentedBucketReader.IterWithAttributes(ctx, dir, f, options...)
}

// TestMetaFetcher_RecursiveListerSkipsMarkerProbes exercises the whole path
// through BaseFetcher: recursive listing -> ListedMarkers -> filter.
func TestMetaFetcher_RecursiveListerSkipsMarkerProbes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	inmem := objstore.NewInMemBucket()
	bkt := &markerProbeBucket{InstrumentedBucketReader: objstore.WithNoopInstr(inmem)}
	for _, id := range []ulid.ULID{ULID(1), ULID(2), ULID(3)} {
		uploadTestMeta(t, ctx, inmem, id)
	}
	uploadTestDeletionMark(t, ctx, inmem, ULID(2), time.Hour)

	logger := log.NewNopLogger()
	lister := NewRecursiveLister(logger, bkt)
	base, err := NewBaseFetcher(logger, 4, bkt, lister, t.TempDir(), nil)
	testutil.Ok(t, err)
	newFetcher := func() *MetaFetcher {
		return base.NewMetaFetcher(nil, []MetadataFilter{NewIgnoreDeletionMarkFilter(logger, bkt, 0, 4).WithListedMarkers(lister)})
	}
	fetcher := newFetcher()

	// Only the one listed marker is fetched; the two blocks without a marker are not probed.
	metas, partial, err := fetcher.Fetch(ctx)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(partial))
	testutil.Equals(t, map[ulid.ULID]struct{}{ULID(1): {}, ULID(3): {}}, fetchedULIDs(metas))
	testutil.Equals(t, int64(1), bkt.markerGets.Load())

	// A marker uploaded between two syncs is listed, fetched and applied by the next sync.
	uploadTestDeletionMark(t, ctx, inmem, ULID(1), time.Hour)
	metas, _, err = fetcher.Fetch(ctx)
	testutil.Ok(t, err)
	testutil.Equals(t, map[ulid.ULID]struct{}{ULID(3): {}}, fetchedULIDs(metas))
	testutil.Equals(t, int64(3), bkt.markerGets.Load())

	// A failed listing fails the sync and leaves the last complete listing in place.
	bkt.failIter.Store(true)
	_, _, err = fetcher.Fetch(ctx)
	testutil.NotOk(t, err)
	listed := lister.ListedMarkers()
	testutil.Equals(t, true, listed.ShouldProbe(ULID(1), metadata.DeletionMarkFilename))
	testutil.Equals(t, true, listed.ShouldProbe(ULID(2), metadata.DeletionMarkFilename))
	testutil.Equals(t, false, listed.ShouldProbe(ULID(3), metadata.DeletionMarkFilename))
	bkt.failIter.Store(false)

	// Several fetchers sharing one BaseFetcher, and therefore one lister, may sync concurrently.
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for _, f := range []*MetaFetcher{fetcher, newFetcher(), newFetcher(), newFetcher()} {
		wg.Add(1)
		go func(f *MetaFetcher) {
			defer wg.Done()
			_, _, err := f.Fetch(ctx)
			errs <- err
		}(f)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		testutil.Ok(t, err)
	}
}
