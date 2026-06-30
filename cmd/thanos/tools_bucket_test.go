// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/extkingpin"
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
