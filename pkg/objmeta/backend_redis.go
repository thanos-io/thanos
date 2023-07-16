package objmeta

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/go-redis/redis/v8"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
)

type redisBackend struct {
	redisClient *cacheutil.RedisClient
	logger      log.Logger
}

// NewRedisBackend create a backend which use redis as a database.
func NewRedisBackend(
	logger log.Logger,
	reg *prometheus.Registry,
	confContentYaml []byte,
) (Backend, error) {
	redisClient, err := cacheutil.NewRedisClient(logger, "meta-store", confContentYaml, reg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create redis client")
	}
	return &redisBackend{
		redisClient: redisClient,
		logger:      logger,
	}, nil
}

// SetBlockMeta set block meta.
func (m *redisBackend) SetBlockMeta(ctx context.Context, blockMeta *objmetapb.BlockMeta) error {
	key := genBlockKey(blockMeta.BlockId, blockMeta.Type)
	value := genBlockValue(blockMeta)
	err := m.redisClient.Set(ctx, key, value, 0).Err()
	if err != nil {
		return errors.Wrapf(err, "redisClient.Set")
	}
	member := &redis.Z{Score: genBlocksScore(), Member: genBlocksMember(blockMeta.BlockId)}
	err = m.redisClient.ZAdd(ctx, genBlocksKey(), member).Err()
	if err != nil {
		return errors.Wrapf(err, "redisClient.ZAdd")
	}
	return nil
}

// GetBlockMeta get block meta.
func (m *redisBackend) GetBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (*objmetapb.BlockMeta, error) {
	data, err := m.redisClient.Get(ctx, genBlockKey(blockID, metaType)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "redisClient.Get")
	}
	blockMeta, err := parseBlockValue(data)
	if err != nil {
		return nil, err
	}
	return blockMeta, nil
}

// ExistsBlockMeta return true if block meta exist.
func (m *redisBackend) ExistsBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error) {
	ret, err := m.redisClient.Exists(ctx, genBlockKey(blockID, metaType)).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, errors.Wrapf(err, "redisClient.Exists")
	}
	return ret == 1, nil
}

// DelBlockMeta delete block meta.
func (m *redisBackend) DelBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error) {
	err := m.redisClient.ZRem(ctx, genBlocksKey(), genBlocksMember(blockID)).Err()
	if err != nil {
		return false, errors.Wrapf(err, "redisClient.ZRem")
	}
	ret, err := m.redisClient.Del(ctx, genBlockKey(blockID, metaType)).Result()
	if err != nil {
		return false, errors.Wrapf(err, "redisClient.Del")
	}
	return ret == 1, nil
}

// DelBlockAllMeta delete block all type meta.
func (m *redisBackend) DelBlockAllMeta(ctx context.Context, blockID string) error {
	err := m.redisClient.ZRem(ctx, genBlocksKey(), genBlocksMember(blockID)).Err()
	if err != nil {
		return errors.Wrapf(err, "redisClient.ZRem")
	}
	for _, v := range objMetaTypes {
		_, err := m.redisClient.Del(ctx, genBlockKey(blockID, v)).Result()
		if err != nil {
			return errors.Wrapf(err, "redisClient.Del")
		}
	}
	return nil
}

// ListBlocks list block id list and invoke function f.
func (m *redisBackend) ListBlocks(ctx context.Context, f func(s []string) error) error {
	key := genBlocksKey()
	const pageSize = 512
	opt := &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
		Count:  pageSize,
	}
	for {
		results, err := m.redisClient.ZRangeByScore(ctx, key, opt).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return errors.Wrapf(err, "redisClient.ZRangeByScore")
		}
		if len(results) == 0 {
			return nil
		}
		opt.Offset += int64(len(results))
		if err := f(results); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// AcquireSyncLock Accrue a lock for sync. if return true, it accrued.
func (m *redisBackend) AcquireSyncLock(ctx context.Context, maxTTL time.Duration) (bool, func() error, error) {
	lockKey := genSyncLockKey()
	lockValue := rand.Int()
	ok, err := m.redisClient.SetNX(ctx, lockKey, lockValue, maxTTL).Result()
	if err != nil {
		return false, nil, errors.Wrapf(err, "lock SetNX")
	}
	releaseFunc := func() error {
		gotValue, err := m.redisClient.Get(ctx, lockKey).Int()
		if err != nil {
			return errors.Wrapf(err, "lock Get")
		}
		if lockValue != gotValue {
			level.Warn(m.logger).Log("msg", "lock gotValue not equal lockValue",
				"lockValue", lockValue, "gotValue", gotValue)
			return nil
		}
		if err := m.redisClient.Del(ctx, lockKey).Err(); err != nil {
			return errors.Wrapf(err, "lock Del")
		}
		return nil
	}
	return ok, releaseFunc, nil
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

// parseBlocksScore parse redis zset score to ts.
func parseBlocksScore(score float64) time.Time {
	return time.Unix(int64(score), 0)
}

// genSyncLockKey gen redis key for set nx lock.
func genSyncLockKey() string {
	return "objmeta:sync"
}
