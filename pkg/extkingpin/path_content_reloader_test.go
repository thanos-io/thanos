// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"context"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
)

func TestPathContentReloader(t *testing.T) {
	t.Parallel()
	type args struct {
		runSteps func(t *testing.T, testFile string, pathContent *staticPathContent, reloadTime time.Duration)
	}
	tests := []struct {
		name        string
		args        args
		wantReloads int
	}{
		{
			name: "Many operations, only rewrite triggers one reload + plus the initial reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, reloadTime time.Duration) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
					testutil.Ok(t, os.Remove(testFile))
					testutil.Ok(t, pathContent.Rewrite([]byte("test modified")))
				},
			},
			wantReloads: 2,
		},
		{
			name: "Many operations, two rewrites trigger two reloads + the initial reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, reloadTime time.Duration) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
					testutil.Ok(t, os.Remove(testFile))
					testutil.Ok(t, pathContent.Rewrite([]byte("test modified")))
					time.Sleep(2 * reloadTime)
					testutil.Ok(t, pathContent.Rewrite([]byte("test modified again")))
				},
			},
			wantReloads: 3,
		},
		{
			name: "Chmod doesn't trigger reload, we have only the initial reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, reloadTime time.Duration) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
					testutil.Ok(t, os.Chmod(testFile, 0666))
					testutil.Ok(t, os.Chmod(testFile, 0777))
				},
			},
			wantReloads: 1,
		},
		{
			name: "Remove doesn't trigger reload, we have only the initial reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, reloadTime time.Duration) {
					testutil.Ok(t, os.Remove(testFile))
				},
			},
			wantReloads: 1,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testFile := path.Join(t.TempDir(), "test")
			testutil.Ok(t, os.WriteFile(testFile, []byte("test"), 0666))

			pathContent, err := NewStaticPathContent(testFile)
			testutil.Ok(t, err)

			wg := &sync.WaitGroup{}
			wg.Add(tt.wantReloads)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reloadCount := &atomic.Int64{}
			configReloadTime := 500 * time.Millisecond
			err = PathContentReloader(ctx, pathContent, log.NewLogfmtLogger(os.Stdout), func() {
				reloadCount.Inc()
				wg.Done()
			}, configReloadTime)
			testutil.Ok(t, err)
			// wait for the initial reload
			testutil.NotOk(t, runutil.Repeat(configReloadTime, ctx.Done(), func() error {
				if reloadCount.Load() != 1 {
					return nil
				}
				return errors.New("reload count matched")
			}))

			tt.args.runSteps(t, testFile, pathContent, configReloadTime)
			wg.Wait()

			// wait for final reload
			testutil.NotOk(t, runutil.Repeat(2*configReloadTime, ctx.Done(), func() error {
				if reloadCount.Load() != int64(tt.wantReloads) {
					return nil
				}
				return errors.New("reload count matched")
			}))
		})
	}
}
