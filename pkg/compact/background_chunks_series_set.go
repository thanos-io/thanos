package compact

import (
	"context"
	"github.com/prometheus/prometheus/storage"
)

type backgrounChunkSeriesSet struct {
	nextSet chan storage.ChunkSeries
	actual  storage.ChunkSeries
	cs      storage.ChunkSeriesSet
}

func (b *backgrounChunkSeriesSet) Next() bool {
	select {
	case s, ok := <-b.nextSet:
		b.actual = s
		return ok
	}
}

func (b *backgrounChunkSeriesSet) At() storage.ChunkSeries {
	return b.actual
}

func (b *backgrounChunkSeriesSet) Err() error {
	return b.cs.Err()
}

func (b *backgrounChunkSeriesSet) Warnings() storage.Warnings {
	return b.cs.Warnings()
}

func (b *backgrounChunkSeriesSet) run(ctx context.Context) {
	for {
		if !b.cs.Next() {
			close(b.nextSet)
			return
		}

		select {
		case b.nextSet <- b.cs.At():
		case <-ctx.Done():
			return
		}
	}
}

func NewBackgroundChunkSeriesSet(ctx context.Context, cs storage.ChunkSeriesSet) storage.ChunkSeriesSet {
	r := &backgrounChunkSeriesSet{
		cs:      cs,
		nextSet: make(chan storage.ChunkSeries, 1000),
	}

	go func() {
		r.run(ctx)
	}()

	return r
}
