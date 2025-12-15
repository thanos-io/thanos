// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compact_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
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
			[]string{"tenant-1:2021-01-01", "tenant-2:11d", "tenant-3:2024-10-17:all"},
			map[string]compact.RetentionPolicy{
				"tenant-1": {
					CutoffDate:        time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					RetentionDuration: time.Duration(0),
					IsAll:             false,
				},
				"tenant-2": {
					CutoffDate:        time.Time{},
					RetentionDuration: 11 * 24 * time.Hour,
					IsAll:             false,
				},
				"tenant-3": {
					CutoffDate:        time.Date(2024, 10, 17, 0, 0, 0, 0, time.UTC),
					RetentionDuration: time.Duration(0),
					IsAll:             true,
				},
			},
			false,
		},
		{
			"invalid string",
			[]string{"ewrwerwerw:werqj:Werw", "tenant#2:1:all"},
			nil,
			true,
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
		{
			"wildcard tenant with duration",
			[]string{"*:30d"},
			map[string]compact.RetentionPolicy{
				"*": {
					CutoffDate:        time.Time{},
					RetentionDuration: 30 * 24 * time.Hour,
					IsAll:             false,
				},
			},
			false,
		},
		{
			"wildcard tenant with cutoff date and all flag",
			[]string{"*:2024-01-01:all"},
			map[string]compact.RetentionPolicy{
				"*": {
					CutoffDate:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					RetentionDuration: time.Duration(0),
					IsAll:             true,
				},
			},
			false,
		},
		{
			"wildcard with specific tenant override",
			[]string{"*:90d", "tenant-special:30d:all"},
			map[string]compact.RetentionPolicy{
				"*": {
					CutoffDate:        time.Time{},
					RetentionDuration: 90 * 24 * time.Hour,
					IsAll:             false,
				},
				"tenant-special": {
					CutoffDate:        time.Time{},
					RetentionDuration: 30 * 24 * time.Hour,
					IsAll:             true,
				},
			},
			false,
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
		lvl        int
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
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-2",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					compact.Level1,
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
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-1",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant-2",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					"tenant-2",
					time.Now().Add(-23 * time.Hour),
					time.Now().Add(-6 * time.Hour),
					compact.Level1,
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
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-1",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant-2",
					time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					"tenant-2",
					time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					compact.Level1,
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
		{
			"tenant retention with duration and level 1 only (default)",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-other",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					"tenant",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					compact.Level2,
				},
			},
			map[string]compact.RetentionPolicy{
				"tenant": {
					CutoffDate:        time.Time{},
					RetentionDuration: 10 * time.Hour,
					IsAll:             false, // Default behavior: only level 1 blocks
				},
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/",
				"01CPHBEX20729MJQZXE3W0BW51/",
			},
			false,
		},
		{
			"wildcard tenant applies to all tenants",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-b",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant-c",
					time.Now().Add(-24 * time.Hour),
					time.Now().Add(-23 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					"tenant-d",
					time.Now().Add(-5 * time.Hour),
					time.Now().Add(-4 * time.Hour),
					compact.Level1,
				},
			},
			map[string]compact.RetentionPolicy{
				"*": {
					CutoffDate:        time.Time{},
					RetentionDuration: 10 * time.Hour,
					IsAll:             false,
				},
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW51/",
			},
			false,
		},
		{
			"wildcard tenant with all flag applies to all levels",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-b",
					time.Now().Add(-2 * 24 * time.Hour),
					time.Now().Add(-24 * time.Hour),
					compact.Level2,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant-c",
					time.Now().Add(-5 * time.Hour),
					time.Now().Add(-4 * time.Hour),
					compact.Level1,
				},
			},
			map[string]compact.RetentionPolicy{
				"*": {
					CutoffDate:        time.Time{},
					RetentionDuration: 10 * time.Hour,
					IsAll:             true,
				},
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW50/",
			},
			false,
		},
		{
			"wildcard with specific tenant override - wildcard longer retention, specific shorter",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-50 * 24 * time.Hour),
					time.Now().Add(-49 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-cleanup",
					time.Now().Add(-15 * 24 * time.Hour),
					time.Now().Add(-14 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW50",
					"tenant-b",
					time.Now().Add(-20 * 24 * time.Hour),
					time.Now().Add(-19 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW51",
					"tenant-cleanup",
					time.Now().Add(-5 * time.Hour),
					time.Now().Add(-4 * time.Hour),
					compact.Level1,
				},
			},
			map[string]compact.RetentionPolicy{
				"*": {
					CutoffDate:        time.Time{},
					RetentionDuration: 30 * 24 * time.Hour, // 30 days for most tenants
					IsAll:             false,
				},
				"tenant-cleanup": {
					CutoffDate:        time.Time{},
					RetentionDuration: 10 * 24 * time.Hour, // 10 days for cleanup tenant
					IsAll:             false,
				},
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW50/",
				"01CPHBEX20729MJQZXE3W0BW51/",
			},
			false,
		},
		{
			"wildcard precedence - specific policy takes priority over wildcard",
			[]testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-override",
					time.Now().Add(-15 * 24 * time.Hour),
					time.Now().Add(-14 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-normal",
					time.Now().Add(-15 * 24 * time.Hour),
					time.Now().Add(-14 * 24 * time.Hour),
					compact.Level1,
				},
			},
			map[string]compact.RetentionPolicy{
				"*": {
					CutoffDate:        time.Time{},
					RetentionDuration: 10 * 24 * time.Hour, // 10 days wildcard
					IsAll:             false,
				},
				"tenant-override": {
					CutoffDate:        time.Time{},
					RetentionDuration: 20 * 24 * time.Hour, // 20 days specific override
					IsAll:             false,
				},
			},
			[]string{
				"01CPHBEX20729MJQZXE3W0BW48/", // kept due to 20-day specific policy
			},
			false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
			for _, b := range tt.blocks {
				uploadTenantBlock(t, bkt, b.id, b.tenant, b.minTime, b.maxTime, b.lvl)
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

func uploadTenantBlock(t *testing.T, bkt objstore.Bucket, id, tenant string, minTime, maxTime time.Time, lvl int) {
	t.Helper()
	meta1 := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustParse(id),
			MinTime: minTime.Unix() * 1000,
			MaxTime: maxTime.Unix() * 1000,
			Version: 1,
			Compaction: tsdb.BlockMetaCompaction{
				Level: lvl,
			},
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

func TestApplyFastRetentionByTenant(t *testing.T) {
	t.Parallel()

	type testBlock struct {
		id, tenant string
		minTime    time.Time
		maxTime    time.Time
		lvl        int
	}

	logger := log.NewNopLogger()
	ctx := context.TODO()

	for _, tt := range []struct {
		name              string
		blocks            []testBlock
		retentionByTenant map[string]compact.RetentionPolicy
		want              []string
		wantDeleted       int
		wantCorrupt       int
		wantErr           bool
	}{
		{
			name:   "empty bucket",
			blocks: []testBlock{},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"*": {RetentionDuration: 24 * time.Hour},
			},
			want:        []string{},
			wantDeleted: 0,
			wantCorrupt: 0,
			wantErr:     false,
		},
		{
			name: "delete expired blocks with wildcard policy",
			blocks: []testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-3 * 24 * time.Hour),
					time.Now().Add(-2 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-b",
					time.Now().Add(-5 * time.Hour),
					time.Now().Add(-4 * time.Hour),
					compact.Level1,
				},
			},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"*": {RetentionDuration: 10 * time.Hour},
			},
			want:        []string{"01CPHBEX20729MJQZXE3W0BW49/"},
			wantDeleted: 1,
			wantCorrupt: 0,
			wantErr:     false,
		},
		{
			name: "delete corrupt blocks (no tenant label)",
			blocks: []testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-5 * time.Hour),
					time.Now().Add(-4 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"", // no tenant - corrupt
					time.Now().Add(-5 * time.Hour),
					time.Now().Add(-4 * time.Hour),
					compact.Level1,
				},
			},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"*": {RetentionDuration: 10 * time.Hour},
			},
			want:        []string{"01CPHBEX20729MJQZXE3W0BW48/"},
			wantDeleted: 0,
			wantCorrupt: 1,
			wantErr:     false,
		},
		{
			name: "specific tenant policy takes precedence over wildcard",
			blocks: []testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-special",
					time.Now().Add(-15 * 24 * time.Hour),
					time.Now().Add(-14 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-normal",
					time.Now().Add(-15 * 24 * time.Hour),
					time.Now().Add(-14 * 24 * time.Hour),
					compact.Level1,
				},
			},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"*":              {RetentionDuration: 10 * 24 * time.Hour}, // 10 days wildcard
				"tenant-special": {RetentionDuration: 20 * 24 * time.Hour}, // 20 days specific
			},
			want:        []string{"01CPHBEX20729MJQZXE3W0BW48/"},
			wantDeleted: 1,
			wantCorrupt: 0,
			wantErr:     false,
		},
		{
			name: "skip blocks without matching policy",
			blocks: []testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-30 * 24 * time.Hour),
					time.Now().Add(-29 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-b",
					time.Now().Add(-30 * 24 * time.Hour),
					time.Now().Add(-29 * 24 * time.Hour),
					compact.Level1,
				},
			},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"tenant-a": {RetentionDuration: 10 * 24 * time.Hour}, // only tenant-a has policy
			},
			want:        []string{"01CPHBEX20729MJQZXE3W0BW49/"},
			wantDeleted: 1,
			wantCorrupt: 0,
			wantErr:     false,
		},
		{
			name: "default only deletes level 1 blocks",
			blocks: []testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-30 * 24 * time.Hour),
					time.Now().Add(-29 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-a",
					time.Now().Add(-30 * 24 * time.Hour),
					time.Now().Add(-29 * 24 * time.Hour),
					compact.Level2, // level 2 should NOT be deleted without IsAll
				},
			},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"*": {RetentionDuration: 10 * 24 * time.Hour, IsAll: false},
			},
			want:        []string{"01CPHBEX20729MJQZXE3W0BW49/"}, // level 2 block kept
			wantDeleted: 1,
			wantCorrupt: 0,
			wantErr:     false,
		},
		{
			name: "IsAll flag deletes all levels",
			blocks: []testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Now().Add(-30 * 24 * time.Hour),
					time.Now().Add(-29 * 24 * time.Hour),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-a",
					time.Now().Add(-30 * 24 * time.Hour),
					time.Now().Add(-29 * 24 * time.Hour),
					compact.Level2, // level 2 should be deleted with IsAll
				},
			},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"*": {RetentionDuration: 10 * 24 * time.Hour, IsAll: true},
			},
			want:        []string{},
			wantDeleted: 2,
			wantCorrupt: 0,
			wantErr:     false,
		},
		{
			name: "cutoff date based retention",
			blocks: []testBlock{
				{
					"01CPHBEX20729MJQZXE3W0BW48",
					"tenant-a",
					time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					compact.Level1,
				},
				{
					"01CPHBEX20729MJQZXE3W0BW49",
					"tenant-a",
					time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2024, 11, 2, 0, 0, 0, 0, time.UTC),
					compact.Level1,
				},
			},
			retentionByTenant: map[string]compact.RetentionPolicy{
				"*": {CutoffDate: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)},
			},
			want:        []string{"01CPHBEX20729MJQZXE3W0BW49/"},
			wantDeleted: 1,
			wantCorrupt: 0,
			wantErr:     false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
			for _, b := range tt.blocks {
				if b.tenant == "" {
					uploadBlockWithoutTenant(t, bkt, b.id, b.minTime, b.maxTime, b.lvl)
				} else {
					uploadTenantBlock(t, bkt, b.id, b.tenant, b.minTime, b.maxTime, b.lvl)
				}
			}

			blocksDeleted := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

			err := compact.ApplyFastRetentionByTenant(ctx, logger, bkt, tt.retentionByTenant, "", 4, false, blocksDeleted)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyFastRetentionByTenant() error = %v, wantErr %v", err, tt.wantErr)
			}

			got := []string{}
			testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
				if !strings.Contains(name, "/") || strings.HasSuffix(name, "/") {
					if strings.HasSuffix(name, "/") {
						got = append(got, name)
					}
				}
				return nil
			}))

			testutil.Equals(t, tt.want, got)
		})
	}
}

func TestApplyFastRetentionByTenantWithCache(t *testing.T) {
	t.Parallel()

	logger := log.NewNopLogger()
	ctx := context.TODO()

	// Create temp cache directory
	cacheDir := t.TempDir()

	// Create test blocks in bucket
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())

	// Block 1: expired, should be deleted
	uploadTenantBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW48", "tenant-a",
		time.Now().Add(-30*24*time.Hour), time.Now().Add(-29*24*time.Hour), compact.Level1)

	// Block 2: not expired, should be kept
	uploadTenantBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW49", "tenant-a",
		time.Now().Add(-5*time.Hour), time.Now().Add(-4*time.Hour), compact.Level1)

	// Create cache entries for both blocks
	for _, id := range []string{"01CPHBEX20729MJQZXE3W0BW48", "01CPHBEX20729MJQZXE3W0BW49"} {
		blockDir := filepath.Join(cacheDir, id)
		testutil.Ok(t, os.MkdirAll(blockDir, 0755))

		// Download and save meta to cache
		m, err := block.DownloadMeta(ctx, logger, bkt, ulid.MustParse(id))
		testutil.Ok(t, err)
		testutil.Ok(t, m.WriteToDir(logger, blockDir))
	}

	retentionByTenant := map[string]compact.RetentionPolicy{
		"*": {RetentionDuration: 10 * 24 * time.Hour},
	}

	blocksDeleted := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

	// Run with cache directory
	err := compact.ApplyFastRetentionByTenant(ctx, logger, bkt, retentionByTenant, cacheDir, 4, false, blocksDeleted)
	testutil.Ok(t, err)

	// Verify bucket state - only block 49 should remain
	got := []string{}
	testutil.Ok(t, bkt.Iter(ctx, "", func(name string) error {
		if strings.HasSuffix(name, "/") {
			got = append(got, name)
		}
		return nil
	}))
	testutil.Equals(t, []string{"01CPHBEX20729MJQZXE3W0BW49/"}, got)

	// Verify block 48 is completely deleted from bucket (meta.json and chunks)
	exists, err := bkt.Exists(ctx, "01CPHBEX20729MJQZXE3W0BW48/meta.json")
	testutil.Ok(t, err)
	testutil.Assert(t, !exists, "block 48 meta.json should be deleted from bucket")

	exists, err = bkt.Exists(ctx, "01CPHBEX20729MJQZXE3W0BW48/chunks/000001")
	testutil.Ok(t, err)
	testutil.Assert(t, !exists, "block 48 chunks should be deleted from bucket")

	// Verify block 49 still exists in bucket with all files
	exists, err = bkt.Exists(ctx, "01CPHBEX20729MJQZXE3W0BW49/meta.json")
	testutil.Ok(t, err)
	testutil.Assert(t, exists, "block 49 meta.json should exist in bucket")

	exists, err = bkt.Exists(ctx, "01CPHBEX20729MJQZXE3W0BW49/chunks/000001")
	testutil.Ok(t, err)
	testutil.Assert(t, exists, "block 49 chunks should exist in bucket")

	// Verify cache state - block 48 cache should be deleted
	_, err = os.Stat(filepath.Join(cacheDir, "01CPHBEX20729MJQZXE3W0BW48"))
	testutil.Assert(t, os.IsNotExist(err), "cache for deleted block should be removed")

	// Block 49 cache should still exist
	_, err = os.Stat(filepath.Join(cacheDir, "01CPHBEX20729MJQZXE3W0BW49"))
	testutil.Ok(t, err)

	// Verify counter
	testutil.Equals(t, 1.0, promtest.ToFloat64(blocksDeleted))
}

func TestApplyFastRetentionByTenantDryRun(t *testing.T) {
	t.Parallel()

	logger := log.NewNopLogger()
	ctx := context.TODO()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())

	// Create expired block
	uploadTenantBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW48", "tenant-a",
		time.Now().Add(-30*24*time.Hour), time.Now().Add(-29*24*time.Hour), compact.Level1)

	retentionByTenant := map[string]compact.RetentionPolicy{
		"*": {RetentionDuration: 10 * 24 * time.Hour},
	}

	blocksDeleted := promauto.With(nil).NewCounter(prometheus.CounterOpts{})

	// Run with dry-run enabled
	err := compact.ApplyFastRetentionByTenant(ctx, logger, bkt, retentionByTenant, "", 4, true, blocksDeleted)
	testutil.Ok(t, err)

	// Verify block is NOT deleted in dry-run mode
	got := []string{}
	testutil.Ok(t, bkt.Iter(ctx, "", func(name string) error {
		if strings.HasSuffix(name, "/") {
			got = append(got, name)
		}
		return nil
	}))
	testutil.Equals(t, []string{"01CPHBEX20729MJQZXE3W0BW48/"}, got)

	// But counter should still be incremented
	testutil.Equals(t, 1.0, promtest.ToFloat64(blocksDeleted))
}

func uploadBlockWithoutTenant(t *testing.T, bkt objstore.Bucket, id string, minTime, maxTime time.Time, lvl int) {
	t.Helper()
	meta1 := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustParse(id),
			MinTime: minTime.Unix() * 1000,
			MaxTime: maxTime.Unix() * 1000,
			Version: 1,
			Compaction: tsdb.BlockMetaCompaction{
				Level: lvl,
			},
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{}, // no tenant label
		},
	}

	b, err := json.Marshal(meta1)
	testutil.Ok(t, err)

	testutil.Ok(t, bkt.Upload(context.Background(), id+"/meta.json", bytes.NewReader(b)))
	testutil.Ok(t, bkt.Upload(context.Background(), id+"/chunks/000001", strings.NewReader("@test-data@")))
}
