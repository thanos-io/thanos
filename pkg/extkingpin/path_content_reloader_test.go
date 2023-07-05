// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"context"
	"errors"
	"github.com/thanos-io/thanos/pkg/runutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
)

func TestPathContentReloader(t *testing.T) {
	t.Parallel()
	type args struct {
		runSteps func(t *testing.T, testFile string, pathContent *staticPathContent, debounceTime time.Duration)
	}
	tests := []struct {
		name        string
		args        args
		wantReloads int
	}{
		{
			name: "Many operations, only rewrite triggers one reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, debounceTime time.Duration) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
					testutil.Ok(t, os.Remove(testFile))
					testutil.Ok(t, pathContent.Rewrite([]byte("test modified")))
				},
			},
			wantReloads: 1,
		},
		{
			name: "Many operations, only rename triggers one reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, debounceTime time.Duration) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
					testutil.Ok(t, os.Rename(testFile, testFile+".tmp"))
					testutil.Ok(t, os.Rename(testFile+".tmp", testFile))
				},
			},
			wantReloads: 1,
		},
		{
			name: "Many operations, two rewrites trigger two reloads",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, debounceTime time.Duration) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
					testutil.Ok(t, os.Remove(testFile))
					testutil.Ok(t, pathContent.Rewrite([]byte("test modified")))
					time.Sleep(2 * debounceTime)
					testutil.Ok(t, pathContent.Rewrite([]byte("test modified again")))
				},
			},
			wantReloads: 2,
		},
		{
			name: "Chmod doesn't trigger reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, debounceTime time.Duration) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
					testutil.Ok(t, os.Chmod(testFile, 0666))
					testutil.Ok(t, os.Chmod(testFile, 0777))
				},
			},
			wantReloads: 0,
		},
		{
			name: "Remove doesn't trigger reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent, debounceTime time.Duration) {
					testutil.Ok(t, os.Remove(testFile))
				},
			},
			wantReloads: 0,
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
			reloadCount := 0
			debounceTime := 1 * time.Second
			err = PathContentReloader(ctx, pathContent, log.NewLogfmtLogger(os.Stdout), func() {
				reloadCount++
				wg.Done()
			}, debounceTime)
			testutil.Ok(t, err)

			tt.args.runSteps(t, testFile, pathContent, debounceTime)
			if tt.wantReloads == 0 {
				// Give things a little time to sync. The fs watcher events are heavily async and could be delayed.
				time.Sleep(2 * debounceTime)
			}
			wg.Wait()

			testutil.NotOk(t, runutil.Repeat(2*debounceTime, ctx.Done(), func() error {
				if reloadCount != tt.wantReloads {
					return nil
				}
				return errors.New("reload count matched")
			}))
		})
	}
}

func TestPathContentReloader_Symlink(t *testing.T) {
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
			testutil.Ok(t, os.Symlink(testFile, testFile+"-symlink"))

			pathContent, err := NewStaticPathContent(testFile + "-symlink")
			testutil.Ok(t, err)

			wg := &sync.WaitGroup{}
			wg.Add(tt.wantReloads)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reloadCount := 0
			configReloadTime := 500 * time.Millisecond
			err = PathContentReloader(ctx, pathContent, log.NewLogfmtLogger(os.Stdout), func() {
				reloadCount++
				wg.Done()
			}, configReloadTime)
			testutil.Ok(t, err)
			// wait for the initial reload
			testutil.NotOk(t, runutil.Repeat(configReloadTime, ctx.Done(), func() error {
				if reloadCount != 1 {
					return nil
				}
				return errors.New("reload count matched")
			}))

			tt.args.runSteps(t, testFile, pathContent, configReloadTime)
			wg.Wait()

			// wait for final reload
			testutil.NotOk(t, runutil.Repeat(2*configReloadTime, ctx.Done(), func() error {
				if reloadCount != tt.wantReloads {
					return nil
				}
				return errors.New("reload count matched")
			}))
		})
	}
}
