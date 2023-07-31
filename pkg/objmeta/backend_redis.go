// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objmeta

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
)

type redisBackend struct {
	redisClient rueidis.Client
	logger      log.Logger
}

// NewRedisBackend create a backend which use redis as a database.
func NewRedisBackend(
	logger log.Logger,
	reg *prometheus.Registry,
	confContentYaml []byte,
) (Backend, error) {
	config, err := cacheutil.ParseRedisClientConfig(confContentYaml)
	if err != nil {
		return nil, err
	}
	// disable client caching because we use underly client.
	config.CacheSize = 0
	redisClient, err := cacheutil.NewRedisClientWithConfig(logger, "objmeta", config, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create redis client")
	}
	return &redisBackend{
		redisClient: redisClient.RawClient(),
		logger:      logger,
	}, nil
}

// SetBlockMeta set block meta.
func (m *redisBackend) SetBlockMeta(ctx context.Context, blockMeta *objmetapb.BlockMeta) error {
	key := genBlockKey(blockMeta.BlockId, blockMeta.Type)
	value := genBlockValue(blockMeta)
	cmd := m.redisClient.B().Set().Key(key).Value(rueidis.BinaryString(value)).Build()
	err := m.redisClient.Do(ctx, cmd).Error()
	if err != nil {
		return errors.Wrapf(err, "redisClient.Set")
	}
	zAddCmd := m.redisClient.B().Zadd().Key(genBlocksKey()).ScoreMember().
		ScoreMember(genBlocksScore(), genBlocksMember(blockMeta.BlockId)).Build()
	err = m.redisClient.Do(ctx, zAddCmd).Error()
	if err != nil {
		return errors.Wrapf(err, "redisClient.ZAdd")
	}
	return nil
}

// GetBlockMeta get block meta.
func (m *redisBackend) GetBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (*objmetapb.BlockMeta, error) {
	cmd := m.redisClient.B().Get().Key(genBlockKey(blockID, metaType)).Build()
	data, err := m.redisClient.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "redisClient.Get")
	}
	blockMeta, err := parseBlockValue([]byte(data))
	if err != nil {
		return nil, err
	}
	return blockMeta, nil
}

// ExistsBlockMeta return true if block meta exist.
func (m *redisBackend) ExistsBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error) {
	cmd := m.redisClient.B().Exists().Key(genBlockKey(blockID, metaType)).Build()
	ret, err := m.redisClient.Do(ctx, cmd).ToInt64()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "redisClient.Exists")
	}
	return ret == 1, nil
}

// DelBlockMeta delete block meta.
func (m *redisBackend) DelBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error) {
	cmd := m.redisClient.B().Zrem().Key(genBlocksKey()).Member(genBlocksMember(blockID)).Build()
	err := m.redisClient.Do(ctx, cmd).Error()
	if err != nil {
		return false, errors.Wrapf(err, "redisClient.ZRem")
	}
	delCmd := m.redisClient.B().Del().Key(genBlockKey(blockID, metaType)).Build()
	ret, err := m.redisClient.Do(ctx, delCmd).ToInt64()
	if err != nil {
		return false, errors.Wrapf(err, "redisClient.Del")
	}
	return ret == 1, nil
}

// DelBlockAllMeta delete block all type meta.
func (m *redisBackend) DelBlockAllMeta(ctx context.Context, blockID string) error {
	cmd := m.redisClient.B().Zrem().Key(genBlocksKey()).Member(genBlocksMember(blockID)).Build()
	err := m.redisClient.Do(ctx, cmd).Error()
	if err != nil {
		return errors.Wrapf(err, "redisClient.ZRem")
	}
	for _, v := range objMetaTypes {
		delCmd := m.redisClient.B().Del().Key(genBlockKey(blockID, v)).Build()
		if err := m.redisClient.Do(ctx, delCmd).Error(); err != nil {
			return errors.Wrapf(err, "redisClient.Del")
		}
	}
	return nil
}

// ListBlocks list block id list and invoke function f.
func (m *redisBackend) ListBlocks(ctx context.Context, f func(s []string) error) error {
	key := genBlocksKey()
	const pageSize = 128
	var (
		cursor uint64
		keys   []string
	)
	for {
		cmd := m.redisClient.B().Zscan().Key(key).Cursor(cursor).Count(pageSize).Build()
		results, err := m.redisClient.Do(ctx, cmd).ToArray()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				return nil
			}
			return errors.Wrapf(err, "redisClient.Zscan")
		}
		if len(results) != 2 {
			return errors.Wrapf(err, "parse zscan results length")
		}
		cursor, err = results[0].AsUint64()
		if err != nil {
			return errors.Wrapf(err, "parse zscan results[0]")
		}
		memberScores, err := results[1].AsZScores()
		if err != nil {
			return errors.Wrapf(err, "parse zscan results[1]")
		}
		keys = keys[:0]
		for _, v := range memberScores {
			keys = append(keys, v.Member)
		}
		if err := f(keys); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if cursor == 0 {
			return nil
		}
	}
}

// AcquireSyncLock Accrue a lock for sync. if return true, it accrued.
func (m *redisBackend) AcquireSyncLock(ctx context.Context, maxTTL time.Duration) (bool, func() error, error) {
	lockKey := genSyncLockKey()
	lockValue := strconv.Itoa(rand.Int())
	cmd := m.redisClient.B().Set().Key(lockKey).Value(lockValue).Nx().Ex(maxTTL).Build()
	if err := m.redisClient.Do(ctx, cmd).Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil, nil
		}
		return false, nil, errors.Wrapf(err, "lock SetNX")
	}
	releaseFunc := func() error {
		gotValue, err := m.redisClient.Do(ctx, m.redisClient.B().Get().Key(lockKey).Build()).ToString()
		if err != nil {
			return errors.Wrapf(err, "lock Get")
		}
		if lockValue != gotValue {
			level.Warn(m.logger).Log("msg", "lock gotValue not equal lockValue",
				"lockValue", lockValue, "gotValue", gotValue)
			return nil
		}
		if err := m.redisClient.Do(ctx, m.redisClient.B().Del().Key(lockKey).Build()).Error(); err != nil {
			return errors.Wrapf(err, "lock Del")
		}
		return nil
	}
	return true, releaseFunc, nil
}

// genBlockKey gen redis key.
func genBlockKey(blockID string, metaType objmetapb.Type) string {
	return fmt.Sprintf("objmeta:%d:%s", metaType, blockID)
}

// genBlockValue gen redis value.
func genBlockValue(blockMeta *objmetapb.BlockMeta) []byte {
	data, _ := proto.Marshal(blockMeta)
	return data
}

// parseBlockValue parse redis value.
func parseBlockValue(data []byte) (*objmetapb.BlockMeta, error) {
	var b objmetapb.BlockMeta
	err := proto.Unmarshal(data, &b)
	if err != nil {
		return nil, errors.Wrapf(err, "parseBlockValue")
	}
	return &b, nil
}

// genBlocksKey gen redis zset key.
func genBlocksKey() string {
	return "objmeta:blocks"
}

// genBlocksKey gen redis zset member.
func genBlocksMember(blockID string) string {
	return blockID
}

// genBlocksKey gen timestamp as redis zset score.
func genBlocksScore() float64 {
	return float64(time.Now().Unix())
}

// genSyncLockKey gen redis key for set nx lock.
func genSyncLockKey() string {
	return "objmeta:sync"
}
