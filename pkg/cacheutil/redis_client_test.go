// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestRedisClient(t *testing.T) {
	// Init some data to conveniently define test cases later one.
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	type args struct {
		data      map[string][]byte
		fetchKeys []string
	}
	type want struct {
		hits map[string][]byte
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "all hit",
			args: args{
				data: map[string][]byte{
					key1: value1,
					key2: value2,
					key3: value3,
				},
				fetchKeys: []string{key1, key2, key3},
			},
			want: want{
				hits: map[string][]byte{
					key1: value1,
					key2: value2,
					key3: value3,
				},
			},
		},
		{
			name: "partial hit",
			args: args{
				data: map[string][]byte{
					key1: value1,
					key2: value2,
				},
				fetchKeys: []string{key1, key2, key3},
			},
			want: want{
				hits: map[string][]byte{
					key1: value1,
					key2: value2,
				},
			},
		},
		{
			name: "not hit",
			args: args{
				data:      map[string][]byte{},
				fetchKeys: []string{key1, key2, key3},
			},
			want: want{
				hits: map[string][]byte{},
			},
		},
	}
	s, err := miniredis.Run()
	if err != nil {
		testutil.Ok(t, err)
	}
	defer s.Close()
	redisConfigs := []struct {
		name        string
		redisConfig func() RedisClientConfig
	}{
		{
			name: "MaxConcurrency>0",
			redisConfig: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = s.Addr()
				cfg.MaxGetMultiConcurrency = 2
				cfg.GetMultiBatchSize = 2
				cfg.MaxSetMultiConcurrency = 2
				cfg.SetMultiBatchSize = 2
				return cfg
			},
		},
		{
			name: "MaxConcurrency=0",
			redisConfig: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = s.Addr()
				cfg.MaxGetMultiConcurrency = 0
				cfg.GetMultiBatchSize = 0
				cfg.MaxSetMultiConcurrency = 0
				cfg.SetMultiBatchSize = 0
				return cfg
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, redisConfig := range redisConfigs {
				t.Run(tt.name+redisConfig.name, func(t *testing.T) {
					logger := log.NewLogfmtLogger(os.Stderr)
					reg := prometheus.NewRegistry()
					c, err := NewRedisClientWithConfig(logger, t.Name(), redisConfig.redisConfig(), reg)
					if err != nil {
						testutil.Ok(t, err)
					}
					defer c.Stop()
					defer s.FlushAll()
					ctx := context.Background()
					c.SetMulti(ctx, tt.args.data, time.Hour)
					hits := c.GetMulti(ctx, tt.args.fetchKeys)
					testutil.Equals(t, tt.want.hits, hits)
				})
			}
		})
	}
}
