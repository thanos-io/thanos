package dedup

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestBucketDeduper_Dedup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	logger := log.NewNopLogger()
	dataDir, err := ioutil.TempDir("", "thanos-dedup")
	testutil.Ok(t, err)
	reg := prometheus.NewRegistry()
	bkt := mockObjectStoreBucket(t, ctx, logger)

	rawLabelSeries := getBucketTimeSeries(t, ctx, logger, bkt)

	deduper := NewBucketDeduper(logger, reg, bkt, dataDir, "replica", 0, 1)

	err = deduper.Dedup(ctx)
	testutil.Ok(t, err)

	newLabelSeries := getBucketTimeSeries(t, ctx, logger, bkt)

	testutil.Assert(t, len(newLabelSeries) == len(rawLabelSeries), "dedup failed")
	for k := range newLabelSeries {
		rawDataPoints := rawLabelSeries[k]
		newDataPoints := newLabelSeries[k]
		testutil.Assert(t, len(rawDataPoints) == len(newDataPoints), "dedup failed")
		for timestamp, values := range newDataPoints {
			testutil.Assert(t, len(values) == 1, "dedup failed")
			rawValues := rawDataPoints[timestamp]
			found := false
			for _, rv := range rawValues {
				if rv == values[0] {
					found = true
					break
				}
			}
			testutil.Assert(t, found, "dedup failed")
		}
	}

	replicas := getBucketReplicas(t, ctx, logger, bkt)
	testutil.Assert(t, len(replicas) == 1, "dedup failed")
	for _, replica := range replicas {
		testutil.Assert(t, replica.isAggReplica(), "dedup failed")
	}
}

func getBucketReplicas(t *testing.T, ctx context.Context, logger log.Logger, bkt objstore.Bucket) Replicas {
	syncer := NewReplicaSyncer(logger, NewDedupMetrics(prometheus.NewRegistry()), bkt, "replica", 0, 1)
	replicas, err := syncer.Sync(ctx)
	testutil.Ok(t, err)
	return replicas
}

func getBucketTimeSeries(t *testing.T, ctx context.Context, logger log.Logger, bkt objstore.Bucket) map[string]map[int64][]float64 {
	dataDir, err := ioutil.TempDir("", "thanos-dedup-"+string(rand.Int()))
	testutil.Ok(t, err)

	replicas := getBucketReplicas(t, ctx, logger, bkt)
	result := make(map[string]map[int64][]float64)
	for _, replica := range replicas {
		for _, b := range replica.Blocks {
			blockDir := filepath.Join(dataDir, b.ULID.String())
			err = os.RemoveAll(blockDir)
			testutil.Ok(t, err)
			err = block.Download(ctx, logger, bkt, b.ULID, blockDir)
			testutil.Ok(t, err)
			labelSamples := getBlockSampleSeries(t, logger, blockDir)
			for k, samples := range labelSamples {
				if _, ok := result[k]; !ok {
					result[k] = make(map[int64][]float64)
				}
				for _, v := range samples {
					if _, ok := result[k][v.timestamp]; !ok {
						result[k][v.timestamp] = make([]float64, 0)
					}
					result[k][v.timestamp] = append(result[k][v.timestamp], v.value)
				}
			}
		}
	}
	return result
}

func getBlockSampleSeries(t *testing.T, logger log.Logger, blockDir string) map[string][]*Sample {
	b, err := tsdb.OpenBlock(logger, blockDir, chunkenc.NewPool())
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, b.Close())
	}()
	ir, err := b.Index()
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, ir.Close())
	}()
	cr, err := b.Chunks()
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, cr.Close())
	}()
	postings, err := ir.Postings(index.AllPostingsKey())
	testutil.Ok(t, err)
	postings = ir.SortedPostings(postings)

	result := make(map[string][]*Sample)
	var lset labels.Labels
	var chks []chunks.Meta
	for postings.Next() {
		lset = lset[:0]
		chks = chks[:0]
		err := ir.Series(postings.At(), &lset, &chks)
		testutil.Ok(t, err)
		key := fmt.Sprint(lset)
		var samples []*Sample
		for _, c := range chks {
			chk, _ := cr.Chunk(c.Ref)
			iterator := chk.Iterator()
			for iterator.Next() {
				timestamp, value := iterator.At()
				samples = append(samples, &Sample{
					timestamp: timestamp,
					value:     value,
				})
			}
		}
		if _, ok := result[key]; !ok {
			result[key] = make([]*Sample, 0)
		}
		result[key] = append(result[key], samples...)
	}
	return result
}
