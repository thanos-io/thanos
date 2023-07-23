// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package objmeta

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
)

func TestNewServer(t *testing.T) {
	ctx := context.Background()
	objStoreConfContentYaml := []byte(fmt.Sprintf(`
type: FILESYSTEM
config:
  directory: %s
`, t.TempDir()))
	miniRedis := miniredis.NewMiniRedis()
	assert.NoError(t, miniRedis.Start())
	defer miniRedis.Close()
	objMetaConfContentYaml := []byte(fmt.Sprintf(`
type: Redis
config:
  addr: %s
`, miniRedis.Addr()))
	s, err := NewServer(log.NewNopLogger(), prometheus.NewRegistry(), objStoreConfContentYaml, objMetaConfContentYaml, 32)
	assert.NoError(t, err)
	m1 := buildTestBlockMeta(t)
	t.Run("set", func(t *testing.T) {
		_, err = s.SetBlockMeta(ctx, &objmetapb.SetBlockMetaRequest{BlockMeta: m1})
		assert.NoError(t, err)
	})
	t.Run("get", func(t *testing.T) {
		rsp, err := s.GetBlockMeta(ctx, &objmetapb.GetBlockMetaRequest{BlockId: m1.BlockId, Type: objmetapb.Type_TYPE_META})
		assert.NoError(t, err)
		assert.Equal(t, m1, rsp.BlockMeta)
	})
	t.Run("exists-true", func(t *testing.T) {
		rsp, err := s.ExistsBlockMeta(ctx, &objmetapb.ExistsBlockMetaRequest{BlockId: m1.BlockId, Type: objmetapb.Type_TYPE_META})
		assert.NoError(t, err)
		assert.Equal(t, true, rsp.Exist)
	})
	t.Run("list", func(t *testing.T) {
		var result []string
		err := s.backend.ListBlocks(ctx, func(s []string) error {
			result = append(result, s...)
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{m1.BlockId}, result)
	})
	t.Run("del", func(t *testing.T) {
		rsp, err := s.DelBlockMeta(ctx, &objmetapb.DelBlockMetaRequest{BlockId: m1.BlockId, Type: objmetapb.Type_TYPE_META})
		assert.NoError(t, err)
		assert.Equal(t, true, rsp.Exist)
	})
	t.Run("exists-false", func(t *testing.T) {
		rsp, err := s.ExistsBlockMeta(ctx, &objmetapb.ExistsBlockMetaRequest{BlockId: m1.BlockId, Type: objmetapb.Type_TYPE_META})
		assert.NoError(t, err)
		assert.Equal(t, false, rsp.Exist)
	})
}
