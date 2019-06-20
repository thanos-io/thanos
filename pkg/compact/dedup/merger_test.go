package dedup

import (
	"context"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestNewBlockGroups(t *testing.T) {
	input := []struct {
		blocks []*metadata.Meta
	}{
		{
			blocks: []*metadata.Meta{
				mockMeta(t, "s0", "r0", 0, 100, 200),
				mockMeta(t, "s0", "r1", 0, 100, 200),
			},
		},
		{
			blocks: []*metadata.Meta{
				mockMeta(t, "s0", "r0", 0, 200, 300),
				mockMeta(t, "s0", "r1", 0, 100, 200),
				mockMeta(t, "s0", "r1", 0, 200, 300),
			},
		},
		{
			blocks: []*metadata.Meta{
				mockMeta(t, "s0", "r0", 0, 100, 200),
				mockMeta(t, "s0", "r0", 0, 200, 300),
				mockMeta(t, "s0", "r0", 0, 300, 400),
				mockMeta(t, "s0", "r1", 0, 200, 400),
			},
		},
		{
			blocks: []*metadata.Meta{
				mockMeta(t, "s0", "r0", 0, 100, 300),
				mockMeta(t, "s0", "r0", 0, 300, 400),
				mockMeta(t, "s0", "r1", 0, 200, 400),
			},
		},
	}

	expected := []struct {
		length    int
		windows   []*TimeWindow
		blockNums []int
	}{
		{
			length: 1,
			windows: []*TimeWindow{
				NewTimeWindow(100, 200),
			},
			blockNums: []int{2},
		},
		{
			length: 2,
			windows: []*TimeWindow{
				NewTimeWindow(100, 200),
				NewTimeWindow(200, 300),
			},
			blockNums: []int{1, 2},
		},
		{
			length: 2,
			windows: []*TimeWindow{
				NewTimeWindow(100, 200),
				NewTimeWindow(200, 400),
			},
			blockNums: []int{1, 3},
		},
		{
			length: 2,
			windows: []*TimeWindow{
				NewTimeWindow(100, 300),
				NewTimeWindow(300, 400),
			},
			blockNums: []int{2, 2},
		},
	}

	for i, v := range input {
		replicas, err := NewReplicas("replica", v.blocks)
		testutil.Ok(t, err)
		groups := NewBlockGroups(replicas)
		testutil.Assert(t, len(groups) == expected[i].length, "new block groups failed")
		for j, g := range groups {
			testutil.Assert(t, reflect.DeepEqual(g.window, expected[i].windows[j]), "new block groups failed")
			testutil.Assert(t, len(g.blocks) == expected[i].blockNums[j], "new block groups failed")

		}
	}
}

func TestReplicaMerger_Merge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	logger := log.NewNopLogger()
	dataDir, err := ioutil.TempDir("", "thanos-dedup-merger")
	testutil.Ok(t, err)
	metrics := NewDedupMetrics(prometheus.NewRegistry())
	bkt := mockObjectStoreBucket(t, ctx, logger)

	replicas := getBucketReplicas(t, ctx, logger, bkt)
	testutil.Assert(t, len(replicas) == 2, "merge failed")
	testutil.Assert(t, len(replicas[0].Blocks) > 0, "merge failed")
	testutil.Assert(t, len(replicas[1].Blocks) > 0, "merge failed")

	merger := NewReplicaMerger(logger, metrics, bkt, dataDir, "replica")

	err = merger.Merge(ctx, replicas)
	testutil.Ok(t, err)
}
