// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/thanos-io/thanos/pkg/cacheutil"
)

func TestRedisCache(t *testing.T) {
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
	logger := log.NewLogfmtLogger(os.Stderr)
	reg := prometheus.NewRegistry()
	cfg := cacheutil.DefaultRedisClientConfig
	cfg.Addr = s.Addr()
	c, err := cacheutil.NewRedisClientWithConfig(logger, t.Name(), cfg, reg)
	if err != nil {
		testutil.Ok(t, err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer s.FlushAll()
			c := NewRedisCache(tt.name, logger, c, reg)
			// Store the cache expected before running the test.
			ctx := context.Background()
			c.Store(tt.args.data, time.Hour)

			// Fetch postings from cached and assert on it.
			hits := c.Fetch(ctx, tt.args.fetchKeys)
			testutil.Equals(t, tt.want.hits, hits)

			// Assert on metrics.
			testutil.Equals(t, float64(len(tt.args.fetchKeys)), prom_testutil.ToFloat64(c.requests))
			testutil.Equals(t, float64(len(tt.want.hits)), prom_testutil.ToFloat64(c.hits))
		})
	}
}
