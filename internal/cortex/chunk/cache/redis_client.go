// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"crypto/tls"
	"flag"
	"net"
	"strings"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/redis/rueidis"

	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
)

// RedisConfig defines how a RedisCache should be constructed.
type RedisConfig struct {
	Endpoint           string         `yaml:"endpoint"`
	MasterName         string         `yaml:"master_name"`
	Timeout            time.Duration  `yaml:"timeout"`
	Expiration         time.Duration  `yaml:"expiration"`
	DB                 int            `yaml:"db"`
	Password           flagext.Secret `yaml:"password"`
	EnableTLS          bool           `yaml:"tls_enabled"`
	InsecureSkipVerify bool           `yaml:"tls_insecure_skip_verify"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *RedisConfig) RegisterFlagsWithPrefix(prefix, description string, f *flag.FlagSet) {
	f.StringVar(&cfg.Endpoint, prefix+"redis.endpoint", "", description+"Redis Server endpoint to use for caching. A comma-separated list of endpoints for Redis Cluster or Redis Sentinel. If empty, no redis will be used.")
	f.StringVar(&cfg.MasterName, prefix+"redis.master-name", "", description+"Redis Sentinel master name. An empty string for Redis Server or Redis Cluster.")
	f.DurationVar(&cfg.Timeout, prefix+"redis.timeout", 500*time.Millisecond, description+"Maximum time to wait before giving up on redis requests.")
	f.DurationVar(&cfg.Expiration, prefix+"redis.expiration", 0, description+"How long keys stay in the redis.")
	f.IntVar(&cfg.DB, prefix+"redis.db", 0, description+"Database index.")
	f.Var(&cfg.Password, prefix+"redis.password", description+"Password to use when connecting to redis.")
	f.BoolVar(&cfg.EnableTLS, prefix+"redis.tls-enabled", false, description+"Enable connecting to redis with TLS.")
	f.BoolVar(&cfg.InsecureSkipVerify, prefix+"redis.tls-insecure-skip-verify", false, description+"Skip validating server certificate.")
}

type RedisClient struct {
	expiration time.Duration
	timeout    time.Duration
	rdb        rueidis.Client
}

// NewRedisClient creates Redis client
func NewRedisClient(cfg *RedisConfig) (*RedisClient, error) {
	clientOpts := rueidis.ClientOption{
		InitAddress:      strings.Split(cfg.Endpoint, ","),
		ShuffleInit:      true,
		Password:         cfg.Password.Value,
		SelectDB:         cfg.DB,
		Dialer:           net.Dialer{Timeout: cfg.Timeout},
		ConnWriteTimeout: cfg.Timeout,
		DisableCache:     true,
	}
	if cfg.EnableTLS {
		clientOpts.TLSConfig = &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	}
	if cfg.MasterName != "" {
		clientOpts.Sentinel = rueidis.SentinelOption{
			MasterSet: cfg.MasterName,
		}
	}

	client, err := rueidis.NewClient(clientOpts)
	if err != nil {
		return nil, err
	}

	return &RedisClient{
		expiration: cfg.Expiration,
		timeout:    cfg.Timeout,
		rdb:        client,
	}, nil
}

func (c *RedisClient) Ping(ctx context.Context) error {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	resp := c.rdb.Do(ctx, c.rdb.B().Ping().Build())
	pingResp, err := resp.ToString()
	if err != nil {
		return errors.New("converting PING response to string")
	}
	if pingResp != "PONG" {
		return errors.Errorf("redis: Unexpected PING response %q", pingResp)
	}
	return nil
}

func (c *RedisClient) MSet(ctx context.Context, keys []string, values [][]byte) error {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	if len(keys) != len(values) {
		return errors.Errorf("MSet the length of keys and values not equal, len(keys)=%d, len(values)=%d", len(keys), len(values))
	}

	cmds := make(rueidis.Commands, 0, len(keys))
	for i := range keys {
		cmds = append(cmds, c.rdb.B().Set().Key(keys[i]).Value(rueidis.BinaryString(values[i])).Ex(c.expiration).Build())
	}
	for _, resp := range c.rdb.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (c *RedisClient) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	ret := make([][]byte, 0, len(keys))

	mgetRet, err := rueidis.MGet(c.rdb, ctx, keys)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		m, ok := mgetRet[k]
		if !ok {
			return nil, errors.Errorf("not found key %s in results", k)
		}
		if m.IsNil() {
			ret = append(ret, nil)
			continue
		}
		r, err := m.ToString()
		if err != nil {
			return nil, errors.Errorf("failed to convert %s resp to string", k)
		}
		ret = append(ret, stringToBytes(r))
	}

	return ret, nil
}

func (c *RedisClient) Close() error {
	c.rdb.Close()
	return nil
}

func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
