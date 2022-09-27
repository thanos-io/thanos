// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/extkingpin"

	"github.com/efficientgo/tools/core/pkg/testutil"
	"github.com/go-kit/log"
)

func TestLimiter_StartConfigReloader(t *testing.T) {
	origLimitsFile, err := os.ReadFile(path.Join("testdata", "limits_config", "good_limits.yaml"))
	testutil.Ok(t, err)
	copyLimitsFile := path.Join(t.TempDir(), "limits.yaml")
	testutil.Ok(t, os.WriteFile(copyLimitsFile, origLimitsFile, 0666))

	goodLimits, err := extkingpin.NewStaticPathContent(copyLimitsFile)
	if err != nil {
		t.Fatalf("error trying to save static limit config: %s", err)
	}
	invalidLimitsPath := path.Join("./testdata", "limits_config", "invalid_limits.yaml")
	invalidLimits, err := os.ReadFile(invalidLimitsPath)
	if err != nil {
		t.Fatalf("could not load test content at %s: %s", invalidLimitsPath, err)
	}

	limiter, err := NewLimiter(goodLimits, nil, RouterIngestor, log.NewLogfmtLogger(os.Stdout))
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error)
	err = limiter.StartConfigReloader(ctx, errChan)
	testutil.Ok(t, err)

	time.Sleep(1 * time.Second)
	testutil.Ok(t, goodLimits.Rewrite(invalidLimits))
	testutil.NotOk(t, <-errChan)
}
