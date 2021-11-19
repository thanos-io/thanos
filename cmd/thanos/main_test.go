// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

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
			downsample.ResLevel0, metadata.NoneFunc)
		testutil.Ok(t, err)
		testutil.Ok(t, block.Upload(ctx, logger, bkt, path.Join(dir, id.String()), metadata.NoneFunc))
	}

	meta, err := block.DownloadMeta(ctx, logger, bkt, id)
	testutil.Ok(t, err)

	metrics := newDownsampleMetrics(prometheus.NewRegistry())
	testutil.Equals(t, 0.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(compact.DefaultGroupKey(meta.Thanos))))
	metaFetcher, err := block.NewMetaFetcher(nil, block.FetcherConcurrency, bkt, "", nil, nil, nil)
	testutil.Ok(t, err)

	metas, _, err := metaFetcher.Fetch(ctx)
	testutil.Ok(t, err)
	testutil.Ok(t, downsampleBucket(ctx, logger, metrics, bkt, metas, dir, 1, metadata.NoneFunc))
	testutil.Equals(t, 1.0, promtest.ToFloat64(metrics.downsamples.WithLabelValues(compact.DefaultGroupKey(meta.Thanos))))

	_, err = os.Stat(dir)
	testutil.Assert(t, os.IsNotExist(err), "index cache dir should not exist at the end of execution")
}
