// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func TestRedisClient(t *testing.T) {
	single, err := mockRedisClientSingle()
	require.Nil(t, err)
	defer single.Close()

	cluster, err := mockRedisClientCluster()
	require.Nil(t, err)
	defer cluster.Close()

	require.NoError(t, single.Ping(context.Background()))
	require.NoError(t, cluster.Ping(context.Background()))

	ctx := context.Background()

	tests := []struct {
		name   string
		client *RedisClient
	}{
		{
			name:   "single redis client",
			client: single,
		},
		{
			name:   "cluster redis client",
			client: cluster,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := []string{"key1", "key2", "key3"}
			bufs := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
			miss := []string{"miss1", "miss2"}

			// set values
			err := tt.client.MSet(ctx, keys, bufs)
			require.Nil(t, err)

			// get keys
			values, err := tt.client.MGet(ctx, keys)
			require.Nil(t, err)
			require.Len(t, values, len(keys))
			for i, value := range values {
				require.Equal(t, values[i], value)
			}

			// get missing keys
			values, err = tt.client.MGet(ctx, miss)
			require.NoError(t, err)
			require.Len(t, values, len(miss))
			for _, value := range values {
				require.Nil(t, value)
			}
		})
	}
}

func mockRedisClientSingle() (*RedisClient, error) {
	redisServer, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	cl, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{redisServer.Addr()},
		DisableCache: true,
	})
	if err != nil {
		return nil, err
	}
	return &RedisClient{
		expiration: time.Minute,
		timeout:    100 * time.Millisecond,
		rdb:        cl,
	}, nil
}

func mockRedisClientCluster() (*RedisClient, error) {
	redisServer1, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	redisServer2, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	cl, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{redisServer1.Addr(), redisServer2.Addr()},
		DisableCache: true,
	})
	if err != nil {
		return nil, err
	}
	return &RedisClient{
		expiration: time.Minute,
		timeout:    100 * time.Millisecond,
		rdb:        cl,
	}, nil
}
