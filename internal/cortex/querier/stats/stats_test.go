package stats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStats_WallTime(t *testing.T) {
	t.Run("add and load wall time", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddWallTime(time.Second)
		stats.AddWallTime(time.Second)

		assert.Equal(t, 2*time.Second, stats.LoadWallTime())
	})

	t.Run("add and load wall time nil receiver", func(t *testing.T) {
		var stats *Stats
		stats.AddWallTime(time.Second)

		assert.Equal(t, time.Duration(0), stats.LoadWallTime())
	})
}

func TestStats_AddFetchedSeries(t *testing.T) {
	t.Run("add and load series", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedSeries(100)
		stats.AddFetchedSeries(50)

		assert.Equal(t, uint64(150), stats.LoadFetchedSeries())
	})

	t.Run("add and load series nil receiver", func(t *testing.T) {
		var stats *Stats
		stats.AddFetchedSeries(50)

		assert.Equal(t, uint64(0), stats.LoadFetchedSeries())
	})
}

func TestStats_AddFetchedChunkBytes(t *testing.T) {
	t.Run("add and load bytes", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedChunkBytes(4096)
		stats.AddFetchedChunkBytes(4096)

		assert.Equal(t, uint64(8192), stats.LoadFetchedChunkBytes())
	})

	t.Run("add and load bytes nil receiver", func(t *testing.T) {
		var stats *Stats
		stats.AddFetchedChunkBytes(1024)

		assert.Equal(t, uint64(0), stats.LoadFetchedChunkBytes())
	})
}

func TestStats_Merge(t *testing.T) {
	t.Run("merge two stats objects", func(t *testing.T) {
		stats1 := &Stats{}
		stats1.AddWallTime(time.Millisecond)
		stats1.AddFetchedSeries(50)
		stats1.AddFetchedChunkBytes(42)

		stats2 := &Stats{}
		stats2.AddWallTime(time.Second)
		stats2.AddFetchedSeries(60)
		stats2.AddFetchedChunkBytes(100)

		stats1.Merge(stats2)

		assert.Equal(t, 1001*time.Millisecond, stats1.LoadWallTime())
		assert.Equal(t, uint64(110), stats1.LoadFetchedSeries())
		assert.Equal(t, uint64(142), stats1.LoadFetchedChunkBytes())
	})

	t.Run("merge two nil stats objects", func(t *testing.T) {
		var stats1 *Stats
		var stats2 *Stats

		stats1.Merge(stats2)

		assert.Equal(t, time.Duration(0), stats1.LoadWallTime())
		assert.Equal(t, uint64(0), stats1.LoadFetchedSeries())
		assert.Equal(t, uint64(0), stats1.LoadFetchedChunkBytes())
	})
}
