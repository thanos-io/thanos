// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestRewrite(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	b, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.New(labels.Label{Name: "a", Value: "1"}),
		labels.New(labels.Label{Name: "a", Value: "2"}),
		labels.New(labels.Label{Name: "a", Value: "3"}),
		labels.New(labels.Label{Name: "a", Value: "4"}),
		labels.New(labels.Label{Name: "a", Value: "1"}),
		labels.New(labels.Label{Name: "b", Value: "1"}),
	}, 150, 0, 1000, labels.EmptyLabels(), 124, metadata.NoneFunc)
	testutil.Ok(t, err)

	ir, err := index.NewFileReader(filepath.Join(tmpDir, b.String(), IndexFilename))
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, ir.Close()) }()

	cr, err := chunks.NewDirReader(filepath.Join(tmpDir, b.String(), "chunks"), nil)
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, cr.Close()) }()

	m := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: ULID(1)},
		Thanos:    metadata.Thanos{},
	}

	testutil.Ok(t, os.MkdirAll(filepath.Join(tmpDir, m.ULID.String()), os.ModePerm))
	iw, err := index.NewWriter(ctx, filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	testutil.Ok(t, err)
	defer iw.Close()

	cw, err := chunks.NewWriter(filepath.Join(tmpDir, m.ULID.String()))
	testutil.Ok(t, err)

	defer cw.Close()

	testutil.Ok(t, rewrite(ctx, log.NewNopLogger(), ir, cr, iw, cw, m, []ignoreFnType{func(mint, maxt int64, prev *chunks.Meta, curr *chunks.Meta) (bool, error) {
		return curr.MaxTime == 696, nil
	}}))

	testutil.Ok(t, iw.Close())
	testutil.Ok(t, cw.Close())

	ir2, err := index.NewFileReader(filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, ir2.Close()) }()

	key, value := index.AllPostingsKey()
	all, err := ir2.Postings(ctx, key, value)
	testutil.Ok(t, err)

	for p := ir2.SortedPostings(all); p.Next(); {
		var builder labels.ScratchBuilder
		var chks []chunks.Meta

		testutil.Ok(t, ir2.Series(p.At(), &builder, &chks))
		testutil.Equals(t, 1, len(chks))
	}
}

func TestGatherIndexHealthStatsReturnsOutOfOrderChunksErr(t *testing.T) {
	ctx := context.Background()

	stats, err := GatherIndexHealthStats(ctx, log.NewLogfmtLogger(os.Stderr), "testdata/out_of_order_chunks/index", 0, math.MaxInt64)

	testutil.Ok(t, err)
	testutil.Equals(t, 1, stats.OutOfOrderChunks)
	testutil.NotOk(t, stats.OutOfOrderChunksErr())
}
