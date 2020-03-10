// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestCompact(t *testing.T) {
	t.Parallel()
	l := log.NewLogfmtLogger(os.Stdout)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	s, err := e2e.NewScenario("e2e_test_compact")
	testutil.Ok(t, err)
	defer s.Close()

	const bucket = "thanos"

	m := e2edb.NewMinio(80, bucket)
	testutil.Ok(t, s.StartAndWaitReady(m))

	s3Config := s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.NetworkHTTPEndpoint(),
		Insecure:  true,
	}

	series := []labels.Labels{labels.FromStrings("a", "1", "b", "2")}
	extLset := labels.FromStrings("ext1", "value1", "replica", "1")

	bkt, err := s3.NewBucketWithConfig(l, s3Config, "test-feed")
	testutil.Ok(t, err)

	dir := filepath.Join(s.SharedDir(), "tmp")

	now := time.Now()
	id1, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, series, 10, timestamp.FromTime(now), timestamp.FromTime(now.Add(2*time.Hour)), 30*time.Minute, extLset, 0)
	testutil.Ok(t, err)

	testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id1.String()), id1.String()))

	compact, err := e2ethanos.NewCompact(s.SharedDir(), "compact", client.BucketConfig{
		Type:   client.S3,
		Config: s3Config,
	})

	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(compact))
}
