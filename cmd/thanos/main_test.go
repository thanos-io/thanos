// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestCleanupIndexCacheFolder(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	dir, err := ioutil.TempDir("", "test-compact-cleanup")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())

	// Upload one compaction lvl = 2 block, one compaction lvl = 1.
	// We generate index cache files only for lvl > 1 blocks.
	{
		id, err := e2eutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{{{Name: "a", Value: "1"}}},
			1, 0, downsample.DownsampleRange0+1, // Pass the minimum DownsampleRange0 check.
			labels.Labels{{Name: "e1", Value: "1"}},
			downsample.ResLevel0)
		testutil.Ok(t, err)

		meta, err := metadata.Read(filepath.Join(dir, id.String()))
		testutil.Ok(t, err)

		meta.Compaction.Level = 2

		testutil.Ok(t, metadata.Write(logger, filepath.Join(dir, id.String()), meta))
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id.String())))
	}
	{
		id, err := e2eutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{{{Name: "a", Value: "1"}}},
			1, 0, downsample.DownsampleRange0+1, // Pass the minimum DownsampleRange0 check.
			labels.Labels{{Name: "e1", Value: "1"}},
			downsample.ResLevel0)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id.String())))
	}

	reg := prometheus.NewRegistry()
	expReg := prometheus.NewRegistry()
	genIndexExp := promauto.With(expReg).NewCounter(prometheus.CounterOpts{
		Name: metricIndexGenerateName,
		Help: metricIndexGenerateHelp,
	})
	metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, "", nil, nil, nil)
	testutil.Ok(t, err)

	metas, _, err := metaFetcher.Fetch(ctx)
	testutil.Ok(t, err)
	testutil.Ok(t, genMissingIndexCacheFiles(ctx, logger, reg, bkt, metas, dir))

	genIndexExp.Inc()
	testutil.GatherAndCompare(t, expReg, reg, metricIndexGenerateName)

	_, err = os.Stat(dir)
	testutil.Assert(t, os.IsNotExist(err), "index cache dir should not exist at the end of execution")
}

func TestCleanupDownsampleCacheFolder(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stderr)
	dir, err := ioutil.TempDir("", "test-compact-cleanup")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	var id ulid.ULID
	{
		id, err = e2eutil.CreateBlock(
			ctx,
			dir,
			[]labels.Labels{{{Name: "a", Value: "1"}}},
			1, 0, downsample.DownsampleRange0+1, // Pass the minimum DownsampleRange0 check.
			labels.Labels{{Name: "e1", Value: "1"}},
			downsample.ResLevel0)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id.String())))
	}

	meta, err := block.DownloadMeta(ctx, logger, bkt, id)
	testutil.Ok(t, err)

	metrics := newDownsampleMetrics(prometheus.NewRegistry())
	testutil.Equals(t, 0.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(compact.GroupKey(meta.Thanos))))
	metaFetcher, err := block.NewMetaFetcher(nil, 32, bkt, "", nil, nil, nil)
	testutil.Ok(t, err)

	metas, _, err := metaFetcher.Fetch(ctx)
	testutil.Ok(t, err)
	testutil.Ok(t, downsampleBucket(ctx, logger, metrics, bkt, metas, dir))
	testutil.Equals(t, 1.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(compact.GroupKey(meta.Thanos))))

	_, err = os.Stat(dir)
	testutil.Assert(t, os.IsNotExist(err), "index cache dir should not exist at the end of execution")
}
