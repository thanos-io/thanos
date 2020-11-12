// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package indexheader

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestNewLazyBinaryReader_ShouldFailIfUnableToBuildIndexHeader(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	_, err = NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, ulid.MustNew(0, nil), 3, NewLazyBinaryReaderMetrics(nil), nil)
	testutil.NotOk(t, err)
}

func TestNewLazyBinaryReader_ShouldBuildIndexHeaderFromBucket(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String())))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))

	// Should lazy load the index upon first usage.
	v, err := r.IndexVersion()
	testutil.Ok(t, err)
	testutil.Equals(t, 2, v)
	testutil.Assert(t, r.reader != nil)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))

	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))
}

func TestNewLazyBinaryReader_ShouldRebuildCorruptedIndexHeader(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String())))

	// Write a corrupted index-header for the block.
	headerFilename := filepath.Join(tmpDir, blockID.String(), block.IndexHeaderFilename)
	testutil.Ok(t, ioutil.WriteFile(headerFilename, []byte("xxx"), os.ModePerm))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))

	// Ensure it can read data.
	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadCount))
}

func TestLazyBinaryReader_ShouldReopenOnUsageAfterClose(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	// Create block.
	blockID, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String())))

	m := NewLazyBinaryReaderMetrics(nil)
	r, err := NewLazyBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, m, nil)
	testutil.Ok(t, err)
	testutil.Assert(t, r.reader == nil)

	// Should lazy load the index upon first usage.
	labelNames, err := r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))

	// Close it.
	testutil.Ok(t, r.Close())
	testutil.Assert(t, r.reader == nil)
	testutil.Equals(t, float64(1), promtestutil.ToFloat64(m.unloadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadFailedCount))

	// Should lazy load again upon next usage.
	labelNames, err = r.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, []string{"a"}, labelNames)
	testutil.Equals(t, float64(2), promtestutil.ToFloat64(m.loadCount))
	testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.loadFailedCount))

	// Closing an already closed lazy reader should be a no-op.
	for i := 0; i < 2; i++ {
		testutil.Ok(t, r.Close())
		testutil.Equals(t, float64(2), promtestutil.ToFloat64(m.unloadCount))
		testutil.Equals(t, float64(0), promtestutil.ToFloat64(m.unloadFailedCount))
	}
}
