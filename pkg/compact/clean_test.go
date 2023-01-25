// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestBestEffortCleanAbortedPartialUploads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	logger := log.NewNopLogger()

	metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, "", nil, nil)
	testutil.Ok(t, err)

	// 1. No meta, old block, should be removed.
	shouldDeleteID, err := ulid.New(uint64(time.Now().Add(-PartialUploadThresholdAge-1*time.Hour).Unix()*1000), nil)
	testutil.Ok(t, err)

	var fakeChunk bytes.Buffer
	fakeChunk.Write([]byte{0, 1, 2, 3})
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldDeleteID.String(), "chunks", "000001"), &fakeChunk))

	// 2.  Old block with meta, so should be kept.
	shouldIgnoreID1, err := ulid.New(uint64(time.Now().Add(-PartialUploadThresholdAge-2*time.Hour).Unix()*1000), nil)
	testutil.Ok(t, err)
	var meta metadata.Meta
	meta.Version = 1
	meta.ULID = shouldIgnoreID1

	var buf bytes.Buffer
	testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnoreID1.String(), metadata.MetaFilename), &buf))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnoreID1.String(), "chunks", "000001"), &fakeChunk))

	// 3. No meta, newer block that should be kept.
	shouldIgnoreID2, err := ulid.New(uint64(time.Now().Add(-2*time.Hour).Unix()*1000), nil)
	testutil.Ok(t, err)

	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnoreID2.String(), "chunks", "000001"), &fakeChunk))

	deleteAttempts := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	blockCleanups := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	blockCleanupFailures := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
	_, partial, err := metaFetcher.Fetch(ctx)
	testutil.Ok(t, err)

	BestEffortCleanAbortedPartialUploads(ctx, logger, partial, bkt, deleteAttempts, blockCleanups, blockCleanupFailures)
	testutil.Equals(t, 1.0, promtest.ToFloat64(deleteAttempts))
	testutil.Equals(t, 1.0, promtest.ToFloat64(blockCleanups))
	testutil.Equals(t, 0.0, promtest.ToFloat64(blockCleanupFailures))

	exists, err := bkt.Exists(ctx, path.Join(shouldDeleteID.String(), "chunks", "000001"))
	testutil.Ok(t, err)
	testutil.Equals(t, false, exists)

	exists, err = bkt.Exists(ctx, path.Join(shouldIgnoreID1.String(), "chunks", "000001"))
	testutil.Ok(t, err)
	testutil.Equals(t, true, exists)

	exists, err = bkt.Exists(ctx, path.Join(shouldIgnoreID2.String(), "chunks", "000001"))
	testutil.Ok(t, err)
	testutil.Equals(t, true, exists)
}
