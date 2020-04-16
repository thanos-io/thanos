// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestGarbage_ApplyRetention(t *testing.T) {
	type testBlock struct {
		id         string
		minTime    time.Time
		maxTime    time.Time
		resolution ResolutionLevel
	}

	logger := log.NewNopLogger()
	ctx := context.TODO()

	for _, tt := range []struct {
		name                  string
		blocks                []testBlock
		retentionByResolution map[ResolutionLevel]time.Duration
		want                  []string
		wantErr               bool
	}{
		{
			"empty bucket",
			[]testBlock{},
			map[ResolutionLevel]time.Duration{
				ResolutionLevelRaw: 24 * time.Hour,
				ResolutionLevel5m:  7 * 24 * time.Hour,
				ResolutionLevel1h:  14 * 24 * time.Hour,
			},
			[]string{},
			false,
		},
		{
			"only raw retention",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					ResolutionLevel1h,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					time.Now().Add(-23 * time.Hour),
					time.Now().Add(-6 * time.Hour),
					ResolutionLevelRaw,
				},
			},
			map[ResolutionLevel]time.Duration{
				ResolutionLevelRaw: 24 * time.Hour,
				ResolutionLevel5m:  0,
				ResolutionLevel1h:  0,
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW49/",
				"01CPHBEX20729MJQZXE3W0BW50/",
				"01CPHBEX20729MJQZXE3W0BW51/",
			},
			false,
		},
		{
			"no retention",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					ResolutionLevel1h,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					time.Now().Add(-23 * time.Hour),
					time.Now().Add(-6 * time.Hour),
					ResolutionLevelRaw,
				},
			},
			map[ResolutionLevel]time.Duration{
				ResolutionLevelRaw: 0,
				ResolutionLevel5m:  0,
				ResolutionLevel1h:  0,
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/",
				"01CPHBEX20729MJQZXE3W0BW49/",
				"01CPHBEX20729MJQZXE3W0BW50/",
				"01CPHBEX20729MJQZXE3W0BW51/",
			},
			false,
		},
		{
			"no retention 1900",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					time.Date(1900, 1, 1, 1, 0, 0, 0, time.Local),
					time.Date(1900, 1, 1, 2, 0, 0, 0, time.Local),
					ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					time.Date(1900, 1, 1, 1, 0, 0, 0, time.Local),
					time.Date(1900, 1, 1, 2, 0, 0, 0, time.Local),
					ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					time.Date(1900, 1, 1, 1, 0, 0, 0, time.Local),
					time.Date(1900, 1, 1, 2, 0, 0, 0, time.Local),
					ResolutionLevel1h,
				},
			},
			map[ResolutionLevel]time.Duration{
				ResolutionLevelRaw: 0,
				ResolutionLevel5m:  0,
				ResolutionLevel1h:  0,
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/",
				"01CPHBEX20729MJQZXE3W0BW49/",
				"01CPHBEX20729MJQZXE3W0BW50/",
			},
			false,
		},
		{
			"unknown resolution",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					ResolutionLevel(1),
				},
			},
			map[ResolutionLevel]time.Duration{},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/",
			},
			false,
		},
		{
			"every retention deletes",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW40",
					time.Now().Add(-1 * 24 * time.Hour),
					time.Now().Add(-0 * 24 * time.Hour),
					ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW41",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-1 * 24 * time.Hour),
					ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW42",
					time.Now().Add(-7 * 24 * time.Hour),
					time.Now().Add(-6 * 24 * time.Hour),
					ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW43",
					time.Now().Add(-8 * 24 * time.Hour),
					time.Now().Add(-7 * 24 * time.Hour),
					ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW44",
					time.Now().Add(-14 * 24 * time.Hour),
					time.Now().Add(-13 * 24 * time.Hour),
					ResolutionLevel1h,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW45",
					time.Now().Add(-15 * 24 * time.Hour),
					time.Now().Add(-14 * 24 * time.Hour),
					ResolutionLevel1h,
				},
			},
			map[ResolutionLevel]time.Duration{
				ResolutionLevelRaw: 24 * time.Hour,
				ResolutionLevel5m:  7 * 24 * time.Hour,
				ResolutionLevel1h:  14 * 24 * time.Hour,
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW40/",
				"01CPHBEX20729MJQZXE3W0BW42/",
				"01CPHBEX20729MJQZXE3W0BW44/",
			},
			false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
			for _, b := range tt.blocks {
				uploadMockBlock(t, bkt, b.id, b.minTime, b.maxTime, int64(b.resolution))
			}

			metaFetcher, err := block.NewMetaFetcher(logger, 32, bkt, "", nil, nil, nil)
			testutil.Ok(t, err)
			metas, _, err := metaFetcher.Fetch(ctx)
			testutil.Ok(t, err)

			reg := extprom.NewMockedRegisterer()
			g := NewGarbage(logger, nil, metadata.NewDeletionMarker(reg, logger, bkt))
			markedForDeletion := reg.Collectors[0].(*prometheus.CounterVec)

			testutil.Ok(t, g.ApplyRetention(ctx, tt.retentionByResolution, metas))

			var got []string
			gotMarkedBlocksCount := 0.0
			testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
				exists, err := bkt.Exists(ctx, filepath.Join(name, metadata.DeletionMarkFilename))
				if err != nil {
					return err
				}
				if !exists {
					got = append(got, name)
					return nil
				}
				gotMarkedBlocksCount += 1.0
				return nil
			}))

			testutil.Equals(t, got, tt.want)
			testutil.Equals(t, 4, promtest.CollectAndCount(markedForDeletion))
			testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.BetweenCompactDuplicateReason))))
			testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PostCompactDuplicateDeletion))))
			testutil.Equals(t, gotMarkedBlocksCount, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.RetentionDeletion))))
			testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PartialForTooLongDeletion))))
		})
	}
}

func uploadMockBlock(t *testing.T, bkt objstore.Bucket, id string, minTime, maxTime time.Time, resolutionLevel int64) {
	t.Helper()
	meta1 := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustParse(id),
			MinTime: minTime.Unix() * 1000,
			MaxTime: maxTime.Unix() * 1000,
			Version: 1,
		},
		Thanos: metadata.Thanos{
			Downsample: metadata.ThanosDownsample{
				Resolution: resolutionLevel,
			},
		},
	}

	b, err := json.Marshal(meta1)
	testutil.Ok(t, err)

	testutil.Ok(t, bkt.Upload(context.Background(), id+"/meta.json", bytes.NewReader(b)))
	testutil.Ok(t, bkt.Upload(context.Background(), id+"/chunks/000001", strings.NewReader("@test-data@")))
	testutil.Ok(t, bkt.Upload(context.Background(), id+"/chunks/000002", strings.NewReader("@test-data@")))
	testutil.Ok(t, bkt.Upload(context.Background(), id+"/chunks/000003", strings.NewReader("@test-data@")))
}

func TestGarbage_CleanTooLongPartialBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	logger := log.NewNopLogger()

	metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, "", nil, nil, nil)
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

	_, partial, err := metaFetcher.Fetch(ctx)
	testutil.Ok(t, err)

	reg := extprom.NewMockedRegisterer()
	g := NewGarbage(logger, nil, metadata.NewDeletionMarker(reg, logger, bkt))
	markedForDeletion := reg.Collectors[0].(*prometheus.CounterVec)

	g.CleanTooLongPartialBlocks(ctx, partial)
	testutil.Equals(t, 1.0, promtest.ToFloat64(g.metrics.partialUploadDeleteAttempts))
	testutil.Equals(t, 4, promtest.CollectAndCount(markedForDeletion))
	testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.BetweenCompactDuplicateReason))))
	testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PostCompactDuplicateDeletion))))
	testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.RetentionDeletion))))
	testutil.Equals(t, 0.0, promtest.ToFloat64(markedForDeletion.WithLabelValues(string(metadata.PartialForTooLongDeletion))))
}
