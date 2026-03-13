// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
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
		{
			name: "WithPrefix",
			redisConfig: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = s.Addr()
				cfg.Prefix = "test-prefix:"
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
					c.SetMulti(tt.args.data, time.Hour)
					hits := c.GetMulti(ctx, tt.args.fetchKeys)
					testutil.Equals(t, tt.want.hits, hits)
				})
			}
		})
	}
}

func TestValidateRedisConfig(t *testing.T) {
	addr := "127.0.0.1:6789"
	tests := []struct {
		name       string
		config     func() RedisClientConfig
		expect_err bool // func(*testing.T, interface{}, error)
	}{
		{
			name: "simpleConfig",
			config: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = addr
				cfg.Username = "user"
				cfg.Password = "1234"
				return cfg
			},
			expect_err: false,
		},
		{
			name: "tlsConfigDefaults",
			config: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = addr
				cfg.Username = "user"
				cfg.Password = "1234"
				cfg.TLSEnabled = true
				return cfg
			},
			expect_err: false,
		},
		{
			name: "tlsClientCertConfig",
			config: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = addr
				cfg.Username = "user"
				cfg.Password = "1234"
				cfg.TLSEnabled = true
				cfg.TLSConfig = TLSConfig{
					CertFile: "cert/client.pem",
					KeyFile:  "cert/client.key",
				}
				return cfg
			},
			expect_err: false,
		},
		{
			name: "tlsInvalidClientCertConfig",
			config: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = addr
				cfg.Username = "user"
				cfg.Password = "1234"
				cfg.TLSEnabled = true
				cfg.TLSConfig = TLSConfig{
					CertFile: "cert/client.pem",
				}
				return cfg
			},
			expect_err: true,
		},
		{
			name: "SetAsyncCircuitBreakerDisabled",
			config: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = addr
				cfg.SetAsyncCircuitBreaker.Enabled = false
				cfg.SetAsyncCircuitBreaker.ConsecutiveFailures = 0
				return cfg
			},
			expect_err: false,
		},
		{
			name: "invalidCircuitBreakerFailurePercent",
			config: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = addr
				cfg.SetAsyncCircuitBreaker.Enabled = true
				cfg.SetAsyncCircuitBreaker.ConsecutiveFailures = 0
				return cfg
			},
			expect_err: true,
		},
		{
			name: "invalidCircuitBreakerFailurePercent",
			config: func() RedisClientConfig {
				cfg := DefaultRedisClientConfig
				cfg.Addr = addr
				cfg.SetAsyncCircuitBreaker.Enabled = true
				cfg.SetAsyncCircuitBreaker.FailurePercent = 0
				return cfg
			},
			expect_err: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.config()

			if tt.expect_err {
				testutil.NotOk(t, cfg.validate())
			} else {
				testutil.Ok(t, cfg.validate())
			}
		})
	}

}

func TestMultipleRedisClient(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		testutil.Ok(t, err)
	}
	defer s.Close()
	cfg := DefaultRedisClientConfig
	cfg.Addr = s.Addr()
	logger := log.NewLogfmtLogger(os.Stderr)
	reg := prometheus.NewRegistry()
	cl, err := NewRedisClientWithConfig(logger, "test1", cfg, reg)
	testutil.Ok(t, err)
	t.Cleanup(cl.Stop)
	cl, err = NewRedisClientWithConfig(logger, "test2", cfg, reg)
	testutil.Ok(t, err)
	t.Cleanup(cl.Stop)
}
