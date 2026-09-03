// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

// gatedExistsBucket lists a fixed set of block prefixes and drives two things
// out of Exists: one block fails, which is how an object store hiccup cancels
// the lister's errgroup, and one block blocks until released, which keeps a
// worker demonstrably in flight while the failure propagates.
type gatedExistsBucket struct {
	objstore.Bucket

	ids     []ulid.ULID
	failFor ulid.ULID
	holdFor ulid.ULID

	existsErr error

	hold     chan struct{} // closed by the test to release the held worker
	held     chan struct{} // closed once a worker is inside the held Exists
	failed   chan struct{} // closed once the failure has been returned
	heldOnce sync.Once
	failOnce sync.Once
}

func (b *gatedExistsBucket) Exists(_ context.Context, name string) (bool, error) {
	switch {
	case strings.HasPrefix(name, b.failFor.String()+"/"):
		b.failOnce.Do(func() { close(b.failed) })
		return false, b.existsErr
	case strings.HasPrefix(name, b.holdFor.String()+"/"):
		b.heldOnce.Do(func() { close(b.held) })
		<-b.hold
		return true, nil
	}
	return true, nil
}

func (b *gatedExistsBucket) Iter(ctx context.Context, _ string, f func(string) error, _ ...objstore.IterOption) error {
	for _, id := range b.ids {
		if err := f(id.String() + "/"); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (b *gatedExistsBucket) ReaderWithExpectedErrs(objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b
}

func (b *gatedExistsBucket) WithExpectedErrs(objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	return b
}

// TestConcurrentLister_WaitsForWorkersOnIterError pins the invariant behind
// #8996. BaseFetcher.fetchMetadata closes the activeBlocks channel as soon as
// GetActiveAndPartialBlockIDs returns, so a worker that outlives the call can
// send on a closed channel and take the process down with "send on closed
// channel". The lister therefore has to have waited for every worker before it
// returns, including on the path where the bucket listing itself fails.
//
// Asserting on the panic directly would be racy: once the group context is
// canceled a parked worker usually takes its ctx.Done branch, and only
// sometimes loses the select to the closed channel. Holding one worker inside
// Exists makes the same invariant deterministic.
func TestConcurrentLister_WaitsForWorkersOnIterError(t *testing.T) {
	// More prefixes than the lister's 64 workers, so the listing is still in
	// flight when the failure cancels the group.
	const numBlocks = 512

	ids := make([]ulid.ULID, 0, numBlocks)
	for i := range numBlocks {
		ids = append(ids, ulid.MustNew(uint64(i+1), nil))
	}

	bkt := &gatedExistsBucket{
		ids:       ids,
		holdFor:   ids[0],
		failFor:   ids[3],
		existsErr: errors.New("simulated object store failure"),
		hold:      make(chan struct{}),
		held:      make(chan struct{}),
		failed:    make(chan struct{}),
	}

	lister := NewConcurrentLister(log.NewNopLogger(), bkt)

	activeBlocks := make(chan ActiveBlockFetchData)
	var drained sync.WaitGroup
	drained.Add(1)
	go func() {
		defer drained.Done()
		for range activeBlocks {
		}
	}()

	var (
		err      error
		returned = make(chan struct{})
	)
	go func() {
		defer close(returned)
		_, err = lister.GetActiveAndPartialBlockIDs(context.Background(), activeBlocks)
	}()

	// One worker is parked inside Exists, and the failure that cancels the
	// group context has been returned, so the listing is on its way out.
	<-bkt.held
	<-bkt.failed

	select {
	case <-returned:
		t.Fatal("GetActiveAndPartialBlockIDs returned while a worker was still running; " +
			"the caller closes the activeBlocks channel at this point (#8996)")
	case <-time.After(500 * time.Millisecond):
	}

	close(bkt.hold)

	select {
	case <-returned:
	case <-time.After(30 * time.Second):
		t.Fatal("GetActiveAndPartialBlockIDs did not return after the worker was released")
	}

	testutil.NotOk(t, err)

	close(activeBlocks)
	drained.Wait()
}
