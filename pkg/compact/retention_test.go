// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
)

func TestApplyRetentionPolicyByResolution(t *testing.T) {
	t.Parallel()

	type testBlock struct {
		id         string
		minTime    time.Time
		maxTime    time.Time
		resolution compact.ResolutionLevel
	}

	logger := log.NewNopLogger()
	ctx := context.TODO()

	for _, tt := range []struct {
		name                  string
		blocks                []testBlock
		retentionByResolution map[compact.ResolutionLevel]time.Duration
		want                  []string
		wantErr               bool
	}{
		{
			"empty bucket",
			[]testBlock{},
			map[compact.ResolutionLevel]time.Duration{
				compact.ResolutionLevelRaw: 24 * time.Hour,
				compact.ResolutionLevel5m:  7 * 24 * time.Hour,
				compact.ResolutionLevel1h:  14 * 24 * time.Hour,
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
					compact.ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					compact.ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					compact.ResolutionLevel1h,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					time.Now().Add(-23 * time.Hour),
					time.Now().Add(-6 * time.Hour),
					compact.ResolutionLevelRaw,
				},
			},
			map[compact.ResolutionLevel]time.Duration{
				compact.ResolutionLevelRaw: 24 * time.Hour,
				compact.ResolutionLevel5m:  0,
				compact.ResolutionLevel1h:  0,
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
					compact.ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					compact.ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					compact.ResolutionLevel1h,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					time.Now().Add(-23 * time.Hour),
					time.Now().Add(-6 * time.Hour),
					compact.ResolutionLevelRaw,
				},
			},
			map[compact.ResolutionLevel]time.Duration{
				compact.ResolutionLevelRaw: 0,
				compact.ResolutionLevel5m:  0,
				compact.ResolutionLevel1h:  0,
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
					compact.ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					time.Date(1900, 1, 1, 1, 0, 0, 0, time.Local),
					time.Date(1900, 1, 1, 2, 0, 0, 0, time.Local),
					compact.ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					time.Date(1900, 1, 1, 1, 0, 0, 0, time.Local),
					time.Date(1900, 1, 1, 2, 0, 0, 0, time.Local),
					compact.ResolutionLevel1h,
				},
			},
			map[compact.ResolutionLevel]time.Duration{
				compact.ResolutionLevelRaw: 0,
				compact.ResolutionLevel5m:  0,
				compact.ResolutionLevel1h:  0,
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
					compact.ResolutionLevel(1),
				},
			},
			map[compact.ResolutionLevel]time.Duration{},
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
					compact.ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW41",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-1 * 24 * time.Hour),
					compact.ResolutionLevelRaw,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW42",
					time.Now().Add(-7 * 24 * time.Hour),
					time.Now().Add(-6 * 24 * time.Hour),
					compact.ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW43",
					time.Now().Add(-8 * 24 * time.Hour),
					time.Now().Add(-7 * 24 * time.Hour),
					compact.ResolutionLevel5m,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW44",
					time.Now().Add(-14 * 24 * time.Hour),
					time.Now().Add(-13 * 24 * time.Hour),
					compact.ResolutionLevel1h,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW45",
					time.Now().Add(-15 * 24 * time.Hour),
					time.Now().Add(-14 * 24 * time.Hour),
					compact.ResolutionLevel1h,
				},
			},
			map[compact.ResolutionLevel]time.Duration{
				compact.ResolutionLevelRaw: 24 * time.Hour,
				compact.ResolutionLevel5m:  7 * 24 * time.Hour,
				compact.ResolutionLevel1h:  14 * 24 * time.Hour,
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

			baseBlockIDsFetcher := block.NewConcurrentLister(logger, bkt)
			metaFetcher, err := block.NewMetaFetcher(logger, 32, bkt, baseBlockIDsFetcher, "", nil, nil)
			testutil.Ok(t, err)

			blocksMarkedForDeletion := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

			metas, _, err := metaFetcher.Fetch(ctx)
			testutil.Ok(t, err)

			if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, bkt, metas, tt.retentionByResolution, blocksMarkedForDeletion); (err != nil) != tt.wantErr {
				t.Errorf("ApplyRetentionPolicyByResolution() error = %v, wantErr %v", err, tt.wantErr)
			}

			got := []string{}
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
			testutil.Equals(t, gotMarkedBlocksCount, promtest.ToFloat64(blocksMarkedForDeletion))
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

func TestParseRetentionPolicyByTenant(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name             string
		retentionTenants []string
		expected         map[string]compact.RetentionPolicy
		expectedErr      bool
	}{
		{
			"empty",
			[]string{},
			map[string]compact.RetentionPolicy{},
			false,
		},
		{
			"valid",
			[]string{"tenant-1:2021-01-01", "tenant-2:11d"},
			map[string]compact.RetentionPolicy{
				"tenant-1": {
					CutoffDate:        time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					RetentionDuration: time.Duration(0),
				},
				"tenant-2": {
					CutoffDate:        time.Time{},
					RetentionDuration: 11 * 24 * time.Hour,
				},
			},
			false,
		},
		{
			"invalid tenant",
			[]string{"tenant1:2021-01-01", "tenant#2:1d"},
			nil,
			true,
		},
		{
			"invalid date",
			[]string{"tenant1:2021-010-01", "tenant2:1d"},
			nil,
			true,
		},
		{
			"invalid duration",
			[]string{"tenant1:2021-01-01", "tenant2:1w"},
			nil,
			true,
		},
		{
			"invalid duration which is 0",
			[]string{"tenant1:2021-01-01", "tenant2:0d"},
			nil,
			true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compact.ParesRetentionPolicyByTenant(log.NewNopLogger(), tt.retentionTenants)
			if (err != nil) != tt.expectedErr {
				t.Errorf("ParseRetentionPolicyByTenant() error = %v, wantErr %v", err, tt.expectedErr)
				return
			}
			testutil.Equals(t, got, tt.expected)
		})
	}
}

func TestApplyRetentionPolicyByTenant(t *testing.T) {
	t.Parallel()

	type testBlock struct {
		id, tenant string
		minTime    time.Time
		maxTime    time.Time
	}

	logger := log.NewNopLogger()
	ctx := context.TODO()

	for _, tt := range []struct {
		name              string
		blocks            []testBlock
		retentionByTenant map[string]compact.RetentionPolicy
		want              []string
		wantErr           bool
	}{
		{
			"empty bucket",
			[]testBlock{},
			map[string]compact.RetentionPolicy{},
			[]string{},
			false,
		},
		{
			"tenant retention disabled",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-1",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-2",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
				},
			},
			map[string]compact.RetentionPolicy{},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/",
				"01CPHBEX20729MJQZXE3W0BW49/",
			},
			false,
		},
		{
			"tenant retention with duration",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-1",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-1",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant-2",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					"tenant-2",
					time.Now().Add(-23 * time.Hour),
					time.Now().Add(-6 * time.Hour),
				},
			},
			map[string]compact.RetentionPolicy{
				"tenant-2": {
					CutoffDate:        time.Time{},
					RetentionDuration: 10 * time.Hour,
				},
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/",
				"01CPHBEX20729MJQZXE3W0BW49/",
				"01CPHBEX20729MJQZXE3W0BW51/",
			},
			false,
		},
		{
			"tenant retention with cutoff date",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-1",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-1",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant-2",
					time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					"tenant-2",
					time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			map[string]compact.RetentionPolicy{
				"tenant-2": {
					CutoffDate:        time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
					RetentionDuration: 0,
				},
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/",
				"01CPHBEX20729MJQZXE3W0BW49/",
				"01CPHBEX20729MJQZXE3W0BW50/",
			},
			false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
			for _, b := range tt.blocks {
				uploadTenantBlock(t, bkt, b.id, b.tenant, b.minTime, b.maxTime)
			}

			baseBlockIDsFetcher := block.NewConcurrentLister(logger, bkt)
			metaFetcher, err := block.NewMetaFetcher(logger, 32, bkt, baseBlockIDsFetcher, "", nil, nil)
			testutil.Ok(t, err)

			blocksMarkedForDeletion := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

			metas, _, err := metaFetcher.Fetch(ctx)
			testutil.Ok(t, err)

			if err := compact.ApplyRetentionPolicyByTenant(ctx, logger, bkt, metas, tt.retentionByTenant, blocksMarkedForDeletion); (err != nil) != tt.wantErr {
				t.Errorf("ApplyRetentionPolicyByResolution() error = %v, wantErr %v", err, tt.wantErr)
			}

			got := []string{}
			testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
				exists, err := bkt.Exists(ctx, filepath.Join(name, metadata.DeletionMarkFilename))
				if err != nil {
					return err
				}
				if !exists {
					got = append(got, name)
					return nil
				}
				return nil
			}))
			deleted := float64(len(tt.blocks) - len(got))

			testutil.Equals(t, got, tt.want)
			testutil.Equals(t, deleted, promtest.ToFloat64(blocksMarkedForDeletion))
		})
	}
}

func uploadTenantBlock(t *testing.T, bkt objstore.Bucket, id, tenant string, minTime, maxTime time.Time) {
	t.Helper()
	meta1 := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustParse(id),
			MinTime: minTime.Unix() * 1000,
			MaxTime: maxTime.Unix() * 1000,
			Version: 1,
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				metadata.TenantLabel: tenant,
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
