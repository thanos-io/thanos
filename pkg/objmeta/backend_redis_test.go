package objmeta

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
)

func Benchmark_redisBackend_GetBlockMeta(b *testing.B) {
	ctx := context.Background()
	redisConf := []byte(`addr: 127.0.0.1:6379`)
	backend, err := NewRedisBackend(log.NewNopLogger(), prometheus.NewRegistry(), redisConf)
	assert.NoError(b, err)
	blockMeta := buildTestBlockMeta(b)
	err = backend.SetBlockMeta(ctx, blockMeta)
	assert.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = backend.GetBlockMeta(ctx, blockMeta.BlockId, objmetapb.Type_TYPE_META)
			assert.NoError(b, err)
		}
	})
}

func Benchmark_redisBackend_ExistsBlockMeta(b *testing.B) {
	ctx := context.Background()
	redisConf := []byte(`addr: 127.0.0.1:6379`)
	backend, err := NewRedisBackend(log.NewNopLogger(), prometheus.NewRegistry(), redisConf)
	assert.NoError(b, err)
	blockMeta := buildTestBlockMeta(b)
	err = backend.SetBlockMeta(ctx, blockMeta)
	assert.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = backend.ExistsBlockMeta(ctx, blockMeta.BlockId, objmetapb.Type_TYPE_META)
			assert.NoError(b, err)
		}
	})
}

func buildTestBlockMeta(t testing.TB) *objmetapb.BlockMeta {
	metaJSON := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustNew(5, nil),
			MinTime: 2424,
			MaxTime: 134,
			Version: 1,
			Stats:   tsdb.BlockStats{NumChunks: 14, NumSamples: 245, NumSeries: 4},
		},
		Thanos: metadata.Thanos{
			Version: 1,
			Labels:  map[string]string{"ext": "lset1"},
			Source:  metadata.SidecarSource,
			Files: []metadata.File{
				{RelPath: "chunks/000001", SizeBytes: 3751},
				{RelPath: "index", SizeBytes: 401},
				{RelPath: "meta.json"},
			},
			IndexStats: metadata.IndexStats{
				SeriesMaxSize: 2000,
				ChunkMaxSize:  1000,
			},
		},
	}
	var buf bytes.Buffer
	err := metaJSON.Write(&buf)
	assert.NoError(t, err)
	meta := &objmetapb.BlockMeta{
		BlockId: metaJSON.ULID.String(),
		Type:    objmetapb.Type_TYPE_META,
		Data:    buf.Bytes(),
	}
	return meta
}
