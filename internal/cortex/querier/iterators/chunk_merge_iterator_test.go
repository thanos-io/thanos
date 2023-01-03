// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package iterators

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/chunk"
	promchunk "github.com/thanos-io/thanos/internal/cortex/chunk/encoding"
)

const (
	userID = "0"
	fp     = 0
)

func TestChunkMergeIterator(t *testing.T) {
	for i, tc := range []struct {
		chunks     []chunk.Chunk
		mint, maxt int64
	}{
		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 100,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 100,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 50, 150, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 100, 200, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 200,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
				mkChunk(t, 100, 200, 1*time.Millisecond, promchunk.Varbit),
			},
			maxt: 200,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			iter := NewChunkMergeIterator(tc.chunks, 0, 0)
			for i := tc.mint; i < tc.maxt; i++ {
				require.NotEqual(t, chunkenc.ValNone, iter.Next())
				ts, s := iter.At()
				assert.Equal(t, i, ts)
				assert.Equal(t, float64(i), s)
				assert.NoError(t, iter.Err())
			}
			assert.Equal(t, chunkenc.ValNone, iter.Next())
		})
	}
}

func TestChunkMergeIteratorSeek(t *testing.T) {
	iter := NewChunkMergeIterator([]chunk.Chunk{
		mkChunk(t, 0, 100, 1*time.Millisecond, promchunk.Varbit),
		mkChunk(t, 50, 150, 1*time.Millisecond, promchunk.Varbit),
		mkChunk(t, 100, 200, 1*time.Millisecond, promchunk.Varbit),
	}, 0, 0)

	for i := int64(0); i < 10; i += 20 {
		require.NotEqual(t, chunkenc.ValNone, iter.Seek(i))
		ts, s := iter.At()
		assert.Equal(t, i, ts)
		assert.Equal(t, float64(i), s)
		assert.NoError(t, iter.Err())

		for j := i + 1; j < 200; j++ {
			require.NotEqual(t, chunkenc.ValNone, iter.Next())
			ts, s := iter.At()
			assert.Equal(t, j, ts)
			assert.Equal(t, float64(j), s)
			assert.NoError(t, iter.Err())
		}
		require.Equal(t, chunkenc.ValNone, iter.Next())
	}
}

func mkChunk(t require.TestingT, mint, maxt model.Time, step time.Duration, encoding promchunk.Encoding) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pc, err := promchunk.NewForEncoding(encoding)
	require.NoError(t, err)
	for i := mint; i.Before(maxt); i = i.Add(step) {
		npc, err := pc.Add(model.SamplePair{
			Timestamp: i,
			Value:     model.SampleValue(float64(i)),
		})
		require.NoError(t, err)
		require.Nil(t, npc)
	}
	return chunk.NewChunk(userID, fp, metric, pc, mint, maxt)
}
