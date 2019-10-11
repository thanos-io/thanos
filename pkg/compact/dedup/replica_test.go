package dedup

import (
	"context"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestNewReplicas(t *testing.T) {
	blocks := make([]*metadata.Meta, 0)
	blocks = append(blocks, mockMeta(t, "s0", "r0", 0, 100, 200))
	blocks = append(blocks, mockMeta(t, "s0", "r0", 0, 200, 300))
	blocks = append(blocks, mockMeta(t, "s1", "r0", 0, 100, 200))
	blocks = append(blocks, mockMeta(t, "s0", "r0", 1, 100, 200))
	replicas, err := NewReplicas("replica", blocks)
	testutil.Ok(t, err)
	testutil.Assert(t, len(replicas) == 3, "new replicas failed")
	for _, r := range replicas {
		switch group := r.Group(); group {
		case "[resolution=0,shard=s0]":
			testutil.Assert(t, len(r.Blocks) == 2, "new replicas failed")
			testutil.Assert(t, r.Name == "r0", "new replicas failed")
			testutil.Assert(t, r.MinTime() == 100, "new replicas failed")
			testutil.Assert(t, r.MaxTime() == 300, "new replicas failed")
		case "[resolution=0,shard=s1]":
			testutil.Assert(t, len(r.Blocks) == 1, "new replicas failed")
			testutil.Assert(t, r.Name == "r0", "new replicas failed")
			testutil.Assert(t, r.MinTime() == 100, "new replicas failed")
			testutil.Assert(t, r.MaxTime() == 200, "new replicas failed")
		default:
			testutil.Assert(t, len(r.Blocks) == 1, "new replicas failed")
			testutil.Assert(t, r.Name == "r0", "new replicas failed")
			testutil.Assert(t, r.MinTime() == 100, "new replicas failed")
			testutil.Assert(t, r.MaxTime() == 200, "new replicas failed")
		}
		for i := range r.Blocks {
			if i == 0 {
				continue
			}
			testutil.Assert(t, r.Blocks[i].MinTime > r.Blocks[i-1].MinTime, "new replicas failed")
		}
	}
}

func TestReplicaSyncer_Sync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	logger := log.NewNopLogger()
	reg := prometheus.NewRegistry()
	bkt := mockObjectStoreBucket(t, ctx, logger)

	syncer := NewReplicaSyncer(logger, NewDedupMetrics(reg), bkt, "replica", 0, 1)

	replicas, err := syncer.Sync(ctx)
	testutil.Ok(t, err)
	testutil.Assert(t, len(replicas) == 2, "replica syncer failed")
	testutil.Assert(t, len(replicas[0].Blocks) == 1, "replica syncer failed")
	testutil.Assert(t, len(replicas[1].Blocks) == 1, "replica syncer failed")
}

func mockObjectStoreBucket(t *testing.T, ctx context.Context, logger log.Logger) objstore.Bucket {
	dataDir, err := ioutil.TempDir("", "thanos-dedup-bucket")
	testutil.Ok(t, err)
	bkt := inmem.NewBucket()

	id0 := createBlock(t, ctx, dataDir, "r0")
	err = objstore.UploadDir(ctx, logger, bkt, filepath.Join(dataDir, id0.String()), id0.String())
	testutil.Ok(t, err)

	id1 := createBlock(t, ctx, dataDir, "r1")
	err = objstore.UploadDir(ctx, logger, bkt, filepath.Join(dataDir, id1.String()), id1.String())
	testutil.Ok(t, err)

	return bkt
}

func createBlock(t *testing.T, ctx context.Context, dataDir string, replica string) ulid.ULID {
	globalConfigs := make(map[string]string)
	globalConfigs["shard"] = "s0"
	globalConfigs["replica"] = replica

	var lset []labels.Labels
	if replica == "r0" {
		lset = []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}
	} else {
		lset = []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "b", Value: "1"}},
			{{Name: "b", Value: "2"}},
		}
	}
	id, err := testutil.CreateBlock(ctx, dataDir, lset, 100, 0, 1000, labels.FromMap(globalConfigs), 0)
	testutil.Ok(t, err)
	return id
}

func mockMeta(t *testing.T, shard, replica string, resolution, minTime, maxTime int64) *metadata.Meta {
	meta := metadata.Meta{}
	meta.Version = metadata.MetaVersion1
	meta.BlockMeta = tsdb.BlockMeta{
		ULID:    ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano()))),
		MinTime: minTime,
		MaxTime: maxTime,
	}
	labels := make(map[string]string)
	labels["shard"] = shard
	labels["replica"] = replica
	meta.Thanos = metadata.Thanos{
		Labels: labels,
		Downsample: metadata.ThanosDownsample{
			Resolution: resolution,
		},
		Source: metadata.TestSource,
	}
	return &meta
}
