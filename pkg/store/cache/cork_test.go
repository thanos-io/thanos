// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/testutil"
	"golang.org/x/sync/errgroup"
)

type indexCacheCorkMock struct {
	calls int
}

func (i *indexCacheCorkMock) StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte) {
}

func (i *indexCacheCorkMock) FetchMultiPostings(ctx context.Context, toFetch map[ulid.ULID][]labels.Label) (hits map[ulid.ULID]map[labels.Label][]byte, misses map[ulid.ULID][]labels.Label) {
	i.calls++
	return nil, nil
}

func (i *indexCacheCorkMock) StoreSeries(ctx context.Context, blockID ulid.ULID, id storage.SeriesRef, v []byte) {

}

func (i *indexCacheCorkMock) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	return nil, nil
}

func TestCorkedIndexCache(t *testing.T) {
	indexCache := &indexCacheCorkMock{}

	c := NewCorkedIndexCache(indexCache)
	const workers = 3
	c.CorkAt(workers)

	eg := &errgroup.Group{}

	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			c.FetchMultiPostings(context.Background(), map[ulid.ULID][]labels.Label{ulid.MustNew(0, nil): {}})
			return nil
		})
	}
	testutil.Ok(t, eg.Wait())

	testutil.Equals(t, 1, indexCache.calls)

	// Try again to see if it works.
	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			c.FetchMultiPostings(context.Background(), map[ulid.ULID][]labels.Label{ulid.MustNew(0, nil): {}})
			return nil
		})
	}
	testutil.Ok(t, eg.Wait())
	testutil.Equals(t, 2, indexCache.calls)

	// Call DonePostings one time too much to see whether the counter works well.
	for i := 0; i < workers+1; i++ {
		c.DonePostings()
	}

	// Now we should only need two more calls for an actual call to happen.
	for i := 0; i < workers-1; i++ {
		eg.Go(func() error {
			c.FetchMultiPostings(context.Background(), map[ulid.ULID][]labels.Label{ulid.MustNew(0, nil): {}})
			return nil
		})
	}

	testutil.Ok(t, eg.Wait())
	testutil.Equals(t, 3, indexCache.calls)

	// Let's see if it works with just one caller.
	c.CorkAt(1)
	c.FetchMultiPostings(context.Background(), map[ulid.ULID][]labels.Label{ulid.MustNew(0, nil): {}})
	testutil.Equals(t, 4, indexCache.calls)

	// Edge case.
	c.CorkAt(0)
	c.FetchMultiPostings(context.Background(), map[ulid.ULID][]labels.Label{ulid.MustNew(0, nil): {}})
	testutil.Equals(t, 5, indexCache.calls)
}
