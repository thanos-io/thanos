package compact

import (
	"bytes"
	"context"
	"path"
	"testing"
	"time"

	"github.com/improbable-eng/thanos/pkg/objstore/inmem"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

func TestHaltError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsHaltError(err), "halt error")

	err = halt(errors.New("test"))
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(halt(errors.New("test")), "something")
	testutil.Assert(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(errors.Wrap(halt(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error")
}

func TestRetryError(t *testing.T) {
	err := errors.New("test")
	testutil.Assert(t, !IsRetryError(err), "retry error")

	err = retry(errors.New("test"))
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.New("test")), "something")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(errors.Wrap(retry(errors.New("test")), "something"), "something2")
	testutil.Assert(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.Wrap(halt(errors.New("test")), "something")), "something2")
	testutil.Assert(t, IsHaltError(err), "not a halt error. Retry should not hide halt error")
}

func TestSyncer_SyncMetas_HandlesMalformedBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bkt := inmem.NewBucket()
	sy, err := NewSyncer(nil, nil, bkt, 10*time.Second, 1, false)
	testutil.Ok(t, err)

	// Generate 1 block which is older than MinimumAgeForRemoval which has chunk data but no meta.  Compactor should delete it.
	shouldDeleteId, err := ulid.New(uint64(time.Now().Add(-time.Hour).Unix()*1000), nil)
	testutil.Ok(t, err)

	var fakeChunk bytes.Buffer
	fakeChunk.Write([]byte{0, 1, 2, 3})
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldDeleteId.String(), "chunks", "000001"), &fakeChunk))

	// Generate 1 block which is older than consistencyDelay but younger than MinimumAgeForRemoval, and which has chunk
	// data but no meta.  Compactor should ignore it.
	shouldIgnoreId, err := ulid.New(uint64(time.Now().Unix()*1000), nil)
	testutil.Ok(t, err)

	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnoreId.String(), "chunks", "000001"), &fakeChunk))

	testutil.Ok(t, sy.SyncMetas(ctx))

	exists, err := bkt.Exists(ctx, path.Join(shouldDeleteId.String(), "chunks", "000001"))
	testutil.Ok(t, err)
	testutil.Equals(t, false, exists)

	exists, err = bkt.Exists(ctx, path.Join(shouldIgnoreId.String(), "chunks", "000001"))
	testutil.Ok(t, err)
	testutil.Equals(t, true, exists)
}
