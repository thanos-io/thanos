// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"context"
	"sync"

	"go.uber.org/atomic"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// ValueOnce is similar to Once, except it returns the retrieved hits and misses each time.
type valueOnce struct {
	m            sync.Mutex
	done         atomic.Uint32
	hits, misses interface{}
}

// Do runs the specified function only once, but all callers gets the same
// result from that single execution.
func (o *valueOnce) Do(f func() (interface{}, interface{})) (interface{}, interface{}) {
	if o.done.Load() == 1 {
		return o.hits, o.misses
	}

	o.m.Lock()
	defer o.m.Unlock()
	if o.done.Load() == 0 {
		defer o.done.Store(1)
		o.hits, o.misses = f()
	}
	return o.hits, o.misses
}

// corkedIndexCache waits for a bit until at least the given
// number of workers have executed a Fetch operation, only then
// it executes the query once and fans out the results.
// This greatly improves performance because with some
// cache providers even a simple Get() operation might result in a new
// TCP connection being established, and requests for only one key seriously
// hamper the performance because the roundtrip takes a much longer time in comparison
// with how long it takes to retrieve the cache item.
// WARNING: Only use it where the number of callers is guaranteed.
// The corking word comes from corking TCP sockets:
// https://baus.net/on-tcp_cork/.
type corkedIndexCache struct {
	originalIndexCache IndexCache

	cork                uint
	postingsWG          *sync.WaitGroup
	postingsGet         *valueOnce
	postingsToBeFetched map[ulid.ULID][]labels.Label
	postingsLock        sync.Mutex
	postingsCounter     int
}

// CorkedIndexCache is like a IndexCache but it is corked.
// If the user is not calling bucketIndexReader.fetchPostings() then it must call
// DonePostings() at the end.
type CorkedIndexCache interface {
	IndexCache

	// CorkAt ensures that at least the given number of calls will happen before
	// the actual fetch.
	CorkAt(uint)

	// DonePostings reduces the counter by one of how many calls we are waiting for.
	// It is necessary to call it at least the number at which the index cache
	// was corked.
	DonePostings()
}

// CorkedDoneTracker is for tracking whether DonePostings() has been called.
// It calls DonePostings() if no fetching has happened.
type CorkedDoneTracker struct {
	indexCache CorkedIndexCache

	fetchedPostings bool
}

// NewCorkedTracker constructs a new helper for tracking whether
// DonePostings() has been called before Close(). If not, it calls
// DonePostings().
func NewCorkedTracker(ic CorkedIndexCache) *CorkedDoneTracker {
	return &CorkedDoneTracker{indexCache: ic}
}

func (c *CorkedDoneTracker) Close() {
	if !c.fetchedPostings {
		c.indexCache.DonePostings()
	}
}

func (c *CorkedDoneTracker) StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte) {
	c.indexCache.StorePostings(ctx, blockID, l, v)
}

func (c *CorkedDoneTracker) FetchMultiPostings(ctx context.Context, toFetch map[ulid.ULID][]labels.Label) (hits map[ulid.ULID]map[labels.Label][]byte, misses map[ulid.ULID][]labels.Label) {
	c.fetchedPostings = true
	return c.indexCache.FetchMultiPostings(ctx, toFetch)
}

func (c *CorkedDoneTracker) StoreSeries(ctx context.Context, blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	c.indexCache.StoreSeries(ctx, blockID, id, v)
}

func (c *CorkedDoneTracker) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	return c.indexCache.FetchMultiSeries(ctx, blockID, ids)
}

func (c *CorkedDoneTracker) DonePostings() {
	c.indexCache.DonePostings()
}

func (c *CorkedDoneTracker) CorkAt(n uint) {
	c.indexCache.CorkAt(n)
}

// NewCorkedIndexCache makes a new index cache that batches postings requests
// into a smaller number of operations to reduce load on the caching layer.
func NewCorkedIndexCache(original IndexCache) CorkedIndexCache {
	return &corkedIndexCache{originalIndexCache: original,
		postingsGet:         new(valueOnce),
		postingsToBeFetched: map[ulid.ULID][]labels.Label{},
		postingsLock:        sync.Mutex{},
		postingsWG:          &sync.WaitGroup{},
	}
}

func (c *corkedIndexCache) reducePostingsCounter() {
	c.postingsCounter--
	if c.postingsCounter == 0 {
		c.postingsWG = &sync.WaitGroup{}
		c.postingsWG.Add(int(c.cork))
		c.postingsToBeFetched = map[ulid.ULID][]labels.Label{}
		c.postingsGet = new(valueOnce)
		c.postingsCounter = int(c.cork)
	}
}

func (c *corkedIndexCache) DonePostings() {
	if c.cork <= 1 {
		return
	}

	c.postingsLock.Lock()
	currentWG := c.postingsWG
	c.reducePostingsCounter()
	c.postingsLock.Unlock()

	currentWG.Done()
}

func (c *corkedIndexCache) CorkAt(n uint) {
	c.cork = n
	c.postingsWG.Add(int(n))
	c.postingsCounter = int(n)
}

// StorePostings stores postings for a single series.
func (c *corkedIndexCache) StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte) {
	c.originalIndexCache.StorePostings(ctx, blockID, l, v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (c *corkedIndexCache) FetchMultiPostings(ctx context.Context, toFetch map[ulid.ULID][]labels.Label) (map[ulid.ULID]map[labels.Label][]byte, map[ulid.ULID][]labels.Label) {
	// Call directly if we are the only one.
	if c.cork <= 1 {
		return c.originalIndexCache.FetchMultiPostings(ctx, toFetch)
	}

	c.postingsLock.Lock()
	// Add new keys.
	for blID, keys := range toFetch {
		c.postingsToBeFetched[blID] = append(c.postingsToBeFetched[blID], keys...)
	}

	currentWG := c.postingsWG
	currentValueOnce := c.postingsGet
	currentToBeFetched := c.postingsToBeFetched

	// If everyone has arrived at this point then refresh the whole data.
	c.reducePostingsCounter()
	c.postingsLock.Unlock()

	// Block until everyone has arrived.
	currentWG.Done()
	currentWG.Wait()

	// Do the work with the old data (pre-refresh).
	hits, misses := currentValueOnce.Do(func() (interface{}, interface{}) {
		return c.originalIndexCache.FetchMultiPostings(ctx, currentToBeFetched)
	})

	return hits.(map[ulid.ULID]map[labels.Label][]byte), misses.(map[ulid.ULID][]labels.Label)
}

// StoreSeries stores a single series.
func (c *corkedIndexCache) StoreSeries(ctx context.Context, blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	c.originalIndexCache.StoreSeries(ctx, blockID, id, v)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *corkedIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	return c.originalIndexCache.FetchMultiSeries(ctx, blockID, ids)
}
