// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestBucketUploadBlocksKeepsResourcesOpenDuringRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	blockDir := t.TempDir()
	bucketDir := t.TempDir()

	id, err := e2eutil.CreateBlock(
		ctx,
		blockDir,
		[]labels.Labels{labels.FromStrings("__name__", "up", "job", "test")},
		1,
		0,
		1000,
		labels.EmptyLabels(),
		downsample.ResLevel0,
		metadata.NoneFunc,
		nil,
	)
	testutil.Ok(t, err)

	objstoreConfigPath := filepath.Join(t.TempDir(), "objstore.yml")
	testutil.Ok(t, os.WriteFile(objstoreConfigPath, []byte(fmt.Sprintf(`type: FILESYSTEM
config:
  directory: %s
`, bucketDir)), 0600))

	app := extkingpin.NewApp(kingpin.New("test", "test"))
	registerTools(app)

	oldArgs := os.Args
	os.Args = []string{
		"test",
		"tools",
		"bucket",
		"upload-blocks",
		"--objstore.config-file=" + objstoreConfigPath,
		"--path=" + blockDir,
		`--label=tenant_id="example_tenant"`,
	}
	t.Cleanup(func() { os.Args = oldArgs })

	cmd, setup := app.Parse()
	testutil.Equals(t, "tools bucket upload-blocks", cmd)

	var g run.Group
	testutil.Ok(t, setup(&g, log.NewNopLogger(), prometheus.NewRegistry(), opentracing.NoopTracer{}, make(chan struct{}), false))
	testutil.Ok(t, g.Run())

	meta, err := metadata.ReadFromDir(filepath.Join(bucketDir, id.String()))
	testutil.Ok(t, err)
	testutil.Equals(t, "example_tenant", meta.Thanos.Labels["tenant_id"])
}

func TestBucketRewriteDownsampledBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logger := log.NewNopLogger()

	bucketDir := t.TempDir()

	// The raw block lives in the FILESYSTEM bucket too, so it can be rewritten together with the downsampled one.
	rawID, err := e2eutil.CreateBlock(
		ctx,
		bucketDir,
		[]labels.Labels{labels.FromStrings("a", "1"), labels.FromStrings("a", "2")},
		720,
		0,
		2*time.Hour.Milliseconds(),
		labels.FromStrings("ext", "1"),
		downsample.ResLevel0,
		metadata.NoneFunc,
		nil,
	)
	testutil.Ok(t, err)
	rawMeta, err := metadata.ReadFromDir(filepath.Join(bucketDir, rawID.String()))
	testutil.Ok(t, err)
	rawBlock, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), filepath.Join(bucketDir, rawID.String()), nil, nil)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, rawBlock.Close()) }()

	id, err := downsample.Downsample(ctx, logger, rawMeta, rawBlock, bucketDir, downsample.ResLevel1)
	testutil.Ok(t, err)

	objstoreConfigPath := filepath.Join(t.TempDir(), "objstore.yml")
	testutil.Ok(t, os.WriteFile(objstoreConfigPath, []byte(fmt.Sprintf(`type: FILESYSTEM
config:
  directory: %s
`, bucketDir)), 0600))

	newBlocks := func(t *testing.T) []string {
		t.Helper()
		entries, err := os.ReadDir(bucketDir)
		testutil.Ok(t, err)
		var ids []string
		for _, e := range entries {
			if e.IsDir() && e.Name() != id.String() && e.Name() != rawID.String() {
				ids = append(ids, e.Name())
			}
		}
		return ids
	}

	rewrite := func(t *testing.T, blockIDs []string, extraArgs ...string) error {
		t.Helper()
		app := extkingpin.NewApp(kingpin.New("test", "test"))
		registerTools(app)

		oldArgs := os.Args
		os.Args = []string{
			"test",
			"tools",
			"bucket",
			"rewrite",
			"--objstore.config-file=" + objstoreConfigPath,
			"--tmp.dir=" + t.TempDir(),
			"--no-dry-run",
			"--no-rewrite.add-change-log",
		}
		for _, blockID := range blockIDs {
			os.Args = append(os.Args, "--id="+blockID)
		}
		os.Args = append(os.Args, extraArgs...)
		defer func() { os.Args = oldArgs }()

		cmd, setup := app.Parse()
		testutil.Equals(t, "tools bucket rewrite", cmd)

		var g run.Group
		testutil.Ok(t, setup(&g, logger, prometheus.NewRegistry(), opentracing.NoopTracer{}, make(chan struct{}), false))
		return g.Run()
	}

	deleteWithIntervals := `--rewrite.to-delete-config=- matchers: "{a=\"1\"}"
  intervals:
  - mint: 0
    maxt: 600000
`

	t.Run("deletion with intervals is rejected", func(t *testing.T) {
		err := rewrite(t, []string{id.String()}, deleteWithIntervals)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), "only whole-series deletions are supported"), "unexpected error: %v", err)
	})

	t.Run("relabeling is rejected", func(t *testing.T) {
		err := rewrite(t, []string{id.String()}, `--rewrite.to-relabel-config=- action: labeldrop
  regex: a
`)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), "only whole-series deletions are supported"), "unexpected error: %v", err)
	})

	t.Run("no block is rewritten if any block is rejected", func(t *testing.T) {
		err := rewrite(t, []string{rawID.String(), id.String()}, deleteWithIntervals)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), "only whole-series deletions are supported"), "unexpected error: %v", err)
		testutil.Equals(t, 0, len(newBlocks(t)))
	})

	t.Run("whole series deletion", func(t *testing.T) {
		testutil.Ok(t, rewrite(t, []string{id.String()}, `--rewrite.to-delete-config=- matchers: "{a=\"1\"}"`))

		newIDs := newBlocks(t)
		testutil.Equals(t, 1, len(newIDs))

		meta, err := metadata.ReadFromDir(filepath.Join(bucketDir, newIDs[0]))
		testutil.Ok(t, err)
		testutil.Equals(t, downsample.ResLevel1, meta.Thanos.Downsample.Resolution)
		testutil.Equals(t, uint64(1), meta.Stats.NumSeries)
	})
}
