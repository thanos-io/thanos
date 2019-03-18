package compact_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/inmem"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb"
)

func TestApplyRetentionPolicyByResolution(t *testing.T) {
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
			bkt := inmem.NewBucket()
			for _, b := range tt.blocks {
				uploadMockBlock(t, bkt, b.id, b.minTime, b.maxTime, int64(b.resolution))
			}
			if err := compact.ApplyRetentionPolicyByResolution(ctx, logger, bkt, tt.retentionByResolution); (err != nil) != tt.wantErr {
				t.Errorf("ApplyRetentionPolicyByResolution() error = %v, wantErr %v", err, tt.wantErr)
			}

			got := []string{}
			testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
				got = append(got, name)
				return nil
			}))

			testutil.Equals(t, got, tt.want)
		})
	}
}

func uploadMockBlock(t *testing.T, bkt objstore.Bucket, id string, minTime, maxTime time.Time, resolutionLevel int64) {
	t.Helper()
	meta1 := metadata.Meta{
		Version: 1,
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustParse(id),
			MinTime: minTime.Unix() * 1000,
			MaxTime: maxTime.Unix() * 1000,
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
