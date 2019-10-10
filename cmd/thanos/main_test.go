package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"

	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestCleanupCompactCacheFolder(t *testing.T) {
	ctx, logger, dir, _, bkt, actReg := bootstrap(t)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	sy, err := compact.NewSyncer(logger, actReg, bkt, 0*time.Second, 1, false, nil)
	testutil.Ok(t, err)

	expReg := prometheus.NewRegistry()
	syncExp := prometheus.NewCounter(prometheus.CounterOpts{
		Name: compact.MetricSyncMetaName,
		Help: compact.MetricSyncMetaHelp,
	})
	expReg.MustRegister(syncExp)

	testutil.GatherAndCompare(t, expReg, actReg, compact.MetricSyncMetaName)

	comp, err := tsdb.NewLeveledCompactor(ctx, nil, logger, []int64{1}, nil)
	testutil.Ok(t, err)

	bComp, err := compact.NewBucketCompactor(logger, sy, comp, dir, bkt, 1)
	testutil.Ok(t, err)

	// Even with with a single uploaded block the bucker compactor needs to
	// downloads the meta file to plan the compaction groups.
	testutil.Ok(t, bComp.Compact(ctx))

	syncExp.Inc()

	testutil.GatherAndCompare(t, expReg, actReg, compact.MetricSyncMetaName)

	_, err = os.Stat(dir)
	testutil.Assert(t, os.IsNotExist(err), "index cache dir shouldn't not exist at the end of execution")

}

func TestCleanupIndexCacheFolder(t *testing.T) {
	ctx, logger, dir, _, bkt, actReg := bootstrap(t)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	expReg := prometheus.NewRegistry()
	genIndexExp := prometheus.NewCounter(prometheus.CounterOpts{
		Name: MetricIndexGenerateName,
		Help: MetricIndexGenerateHelp,
	})
	expReg.MustRegister(genIndexExp)

	testutil.GatherAndCompare(t, expReg, actReg, compact.MetricSyncMetaName)

	testutil.Ok(t, genMissingIndexCacheFiles(ctx, logger, actReg, bkt, dir))

	genIndexExp.Inc()
	testutil.GatherAndCompare(t, expReg, actReg, compact.MetricSyncMetaName)

	_, err := os.Stat(dir)
	testutil.Assert(t, os.IsNotExist(err), "index cache dir shouldn't not exist at the end of execution")
}

func TestCleanupDownsampleCacheFolder(t *testing.T) {
	ctx, logger, dir, blckID, bkt, reg := bootstrap(t)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	meta, err := block.DownloadMeta(ctx, logger, bkt, blckID)
	testutil.Ok(t, err)

	metrics := newDownsampleMetrics(reg)
	testutil.Equals(t, 0.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(compact.GroupKey(meta))))
	testutil.Ok(t, downsampleBucket(ctx, logger, metrics, bkt, dir))
	testutil.Equals(t, 1.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(compact.GroupKey(meta))))

	_, err = os.Stat(dir)
	testutil.Assert(t, os.IsNotExist(err), "index cache dir shouldn't not exist at the end of execution")
}

func bootstrap(t *testing.T) (context.Context, log.Logger, string, ulid.ULID, objstore.Bucket, *prometheus.Registry) {
	logger := log.NewLogfmtLogger(os.Stderr)
	dir, err := ioutil.TempDir("", "test-compact-cleanup")
	testutil.Ok(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bkt := inmem.NewBucket()
	var blckID ulid.ULID

	// Create and upload a single block to the bucker.
	// The compaction will download the meta block of
	// this block to plan the compaction groups.
	{
		blckID, err = testutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{
				{{Name: "a", Value: "1"}},
			},
			1, 0, downsample.DownsampleRange0+1, // Pass the minimum DownsampleRange0 check.
			labels.Labels{{Name: "e1", Value: "1"}},
			downsample.ResLevel0)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, blckID.String())))
	}

	return ctx, logger, dir, blckID, bkt, prometheus.NewRegistry()
}
