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

	"github.com/efficientgo/core/testutil"
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

	limiter, err := NewLimiter(goodLimits, nil, RouterIngestor, log.NewLogfmtLogger(os.Stdout), 1*time.Second)
	testutil.Ok(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = limiter.StartConfigReloader(ctx)
	testutil.Ok(t, err)

	time.Sleep(1 * time.Second)
	testutil.Ok(t, goodLimits.Rewrite(invalidLimits))
}

type emptyPathFile struct{}

func (e emptyPathFile) Content() ([]byte, error) {
	return []byte{}, nil
}

func (e emptyPathFile) Path() string {
	return ""
}

func TestLimiter_CanReload(t *testing.T) {
	validLimitsPath, err := extkingpin.NewStaticPathContent(
		path.Join("testdata", "limits_config", "good_limits.yaml"),
	)
	testutil.Ok(t, err)
	emptyLimitsPath := emptyPathFile{}

	type args struct {
		configFilePath fileContent
	}
	tests := []struct {
		name       string
		args       args
		wantReload bool
	}{
		{
			name:       "Nil config file path cannot be reloaded",
			args:       args{configFilePath: nil},
			wantReload: false,
		},
		{
			name:       "Empty config file path cannot be reloaded",
			args:       args{configFilePath: emptyLimitsPath},
			wantReload: false,
		},
		{
			name:       "Valid config file path can be reloaded",
			args:       args{configFilePath: validLimitsPath},
			wantReload: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configFile := tt.args.configFilePath
			limiter, err := NewLimiter(configFile, nil, RouterIngestor, log.NewLogfmtLogger(os.Stdout), 1*time.Second)
			testutil.Ok(t, err)
			if tt.wantReload {
				testutil.Assert(t, limiter.CanReload())
			} else {
				testutil.Assert(t, !limiter.CanReload())
			}
		})
	}
}
