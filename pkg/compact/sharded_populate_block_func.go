package compact

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
)

type ShardedPopulateBlockFunc struct {
	partitionCount int
	partitionId    int
}

// PopulateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
// It expects sorted blocks input by mint.
func (c ShardedPopulateBlockFunc) PopulateBlock(metrics *tsdb.CompactorMetrics, logger log.Logger, chunkPool chunkenc.Pool, ctx context.Context, mergeFunc storage.VerticalChunkSeriesMergeFunc, blocks []tsdb.BlockReader, meta *tsdb.BlockMeta, indexw tsdb.IndexWriter, chunkw tsdb.ChunkWriter) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}

	var (
		sets        []storage.ChunkSeriesSet
		setsMtx     sync.Mutex
		symbols     index.StringIter
		closers     []io.Closer
		overlapping bool
	)
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(errors.Wrap(cerr, "close"))
		}
		err = errs.Err()
		metrics.PopulatingBlocks.Set(0)
	}()
	metrics.PopulatingBlocks.Set(1)

	globalMaxt := blocks[0].Meta().MaxTime
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, b := range blocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !overlapping {
			if i > 0 && b.Meta().MinTime < globalMaxt {
				metrics.OverlappingBlocks.Inc()
				overlapping = true
				level.Info(logger).Log("msg", "Found overlapping blocks during compaction", "ulid", meta.ULID)
			}
			if b.Meta().MaxTime > globalMaxt {
				globalMaxt = b.Meta().MaxTime
			}
		}

		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %+v", b.Meta())
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %+v", b.Meta())
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %+v", b.Meta())
		}
		closers = append(closers, tombsr)

		k, v := index.AllPostingsKey()
		all, err := indexr.Postings(k, v)
		if err != nil {
			return err
		}
		all = indexr.SortedPostings(all)
		g.Go(func() error {
			shardStart := time.Now()
			shardedPosting := index.NewShardedPosting(all, uint64(c.partitionCount), uint64(c.partitionId), indexr.Series)
			fmt.Printf("finished sharding, duration: %v\n", time.Since(shardStart))
			// Blocks meta is half open: [min, max), so subtract 1 to ensure we don't hold samples with exact meta.MaxTime timestamp.
			setsMtx.Lock()
			sets = append(sets, tsdb.NewBlockChunkSeriesSet(meta.ULID, indexr, chunkr, tombsr, shardedPosting, meta.MinTime, meta.MaxTime-1, false))
			setsMtx.Unlock()
			return nil
		})
		syms := indexr.Symbols()
		if i == 0 {
			symbols = syms
			continue
		}
		symbols = tsdb.NewMergedStringIter(symbols, syms)
	}
	if err := g.Wait(); err != nil {
		return err
	}

	for symbols.Next() {
		if err := indexw.AddSymbol(symbols.At()); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}
	if symbols.Err() != nil {
		return errors.Wrap(symbols.Err(), "next symbol")
	}

	var (
		ref = storage.SeriesRef(0)
		ch  = make(chan func() error, 1000)
	)

	set := sets[0]
	if len(sets) > 1 {
		iCtx, cancel := context.WithCancel(ctx)
		// Merge series using specified chunk series merger.
		// The default one is the compacting series merger.
		set = NewBackgroundChunkSeriesSet(iCtx, storage.NewMergeChunkSeriesSet(sets, mergeFunc))
		defer cancel()
	}

	go func() {
		// Iterate over all sorted chunk series.
		for set.Next() {
			select {
			case <-ctx.Done():
				ch <- func() error { return ctx.Err() }
			default:
			}
			s := set.At()
			chksIter := s.Iterator()

			var chks []chunks.Meta
			var wg sync.WaitGroup
			r := ref
			wg.Add(1)
			go func() {
				for chksIter.Next() {
					// We are not iterating in streaming way over chunk as
					// it's more efficient to do bulk write for index and
					// chunk file purposes.
					chks = append(chks, chksIter.At())
				}
				wg.Done()
			}()

			ch <- func() error {
				wg.Wait()
				if chksIter.Err() != nil {
					return errors.Wrap(chksIter.Err(), "chunk iter")
				}

				// Skip the series with all deleted chunks.
				if len(chks) == 0 {
					return nil
				}

				if err := chunkw.WriteChunks(chks...); err != nil {
					return errors.Wrap(err, "write chunks")
				}
				if err := indexw.AddSeries(r, s.Labels(), chks...); err != nil {
					return errors.Wrap(err, "add series")
				}

				meta.Stats.NumChunks += uint64(len(chks))
				meta.Stats.NumSeries++
				for _, chk := range chks {
					meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
				}

				for _, chk := range chks {
					if err := chunkPool.Put(chk.Chunk); err != nil {
						return errors.Wrap(err, "put chunk")
					}
				}

				return nil
			}

			ref++
		}
		close(ch)
	}()

	for callback := range ch {
		err := callback()
		if err != nil {
			return err
		}
	}

	if set.Err() != nil {
		return errors.Wrap(set.Err(), "iterate compaction set")
	}

	return nil
}
