package compact_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/compact"
	"github.com/improbable-eng/thanos/pkg/compact/downsample"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/inmem"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb"
)

func TestTimeBasedRetentionPolicyOnEmptyBucket(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := inmem.NewBucket()

	testutil.Ok(t, compact.ApplyDefaultRetentionPolicy(context.TODO(), logger, bkt, 24*time.Hour))

	var (
		want []string
		got  []string
	)
	testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
		got = append(got, name)
		return nil
	}))

	testutil.Equals(t, got, want)
}

func TestTimeBasedRetentionPolicyKeepsBucketsBeforeDuration(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := inmem.NewBucket()

	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW48", time.Now().Add(-3*24*time.Hour), time.Now().Add(-2*24*time.Hour), downsample.ResLevel0)
	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW49", time.Now().Add(-2*24*time.Hour), time.Now().Add(-24*time.Hour), downsample.ResLevel1)
	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW50", time.Now().Add(-24*time.Hour), time.Now().Add(-23*time.Hour), downsample.ResLevel2)
	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW51", time.Now().Add(-23*time.Hour), time.Now().Add(-6*time.Hour), downsample.ResLevel0)
	testutil.Ok(t, compact.ApplyDefaultRetentionPolicy(context.TODO(), logger, bkt, 24*time.Hour))

	want := []string{"01CPHBEX20729MJQZXE3W0BW50/", "01CPHBEX20729MJQZXE3W0BW51/"}

	var got []string
	testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
		got = append(got, name)
		return nil
	}))

	testutil.Equals(t, got, want)
}

func TestTimeBasedRetentionPolicyByResolutionOnEmptyBucket(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := inmem.NewBucket()

	testutil.Ok(t, compact.ApplyRetentionPolicyByResolution(context.TODO(), logger, bkt, 24*time.Hour, downsample.ResLevel0))

	var (
		want []string
		got  []string
	)
	testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
		got = append(got, name)
		return nil
	}))

	testutil.Equals(t, got, want)
}

func TestTimeBasedRetentionPolicyByResolutionKeepsBucketsBeforeDuration(t *testing.T) {
	logger := log.NewNopLogger()
	bkt := inmem.NewBucket()

	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW48", time.Now().Add(-3*24*time.Hour), time.Now().Add(-2*24*time.Hour), downsample.ResLevel0)
	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW49", time.Now().Add(-2*24*time.Hour), time.Now().Add(-24*time.Hour), downsample.ResLevel1)
	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW50", time.Now().Add(-24*time.Hour), time.Now().Add(-23*time.Hour), downsample.ResLevel2)
	uploadMockBlock(t, bkt, "01CPHBEX20729MJQZXE3W0BW51", time.Now().Add(-23*time.Hour), time.Now().Add(-6*time.Hour), downsample.ResLevel0)
	testutil.Ok(t, compact.ApplyRetentionPolicyByResolution(context.TODO(), logger, bkt, 24*time.Hour, downsample.ResLevel0))

	want := []string{"01CPHBEX20729MJQZXE3W0BW49/", "01CPHBEX20729MJQZXE3W0BW50/", "01CPHBEX20729MJQZXE3W0BW51/"}

	var got []string
	testutil.Ok(t, bkt.Iter(context.TODO(), "", func(name string) error {
		got = append(got, name)
		return nil
	}))

	testutil.Equals(t, got, want)
}

func uploadMockBlock(t *testing.T, bkt objstore.Bucket, id string, minTime, maxTime time.Time, resolutionLevel int64) {
	t.Helper()
	meta1 := block.Meta{
		Version: 1,
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustParse(id),
			MinTime: minTime.Unix() * 1000,
			MaxTime: maxTime.Unix() * 1000,
		},
		Thanos: block.ThanosMeta{
			Downsample: block.ThanosDownsampleMeta{
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
