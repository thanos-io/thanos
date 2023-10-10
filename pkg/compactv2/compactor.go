// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compactv2

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/thanos/pkg/block"
)

type ProgressLogger interface {
	SeriesProcessed()
}

type progressLogger struct {
	logger log.Logger

	series    int
	processed int
}

func NewProgressLogger(logger log.Logger, series int) *progressLogger {
	return &progressLogger{logger: logger, series: series}
}

func (p *progressLogger) SeriesProcessed() {
	p.processed++
	if (p.series/10) == 0 || p.processed%(p.series/10) == 0 {
		level.Info(p.logger).Log("msg", fmt.Sprintf("processed %0.2f%s of %v series", 100*(float64(p.processed)/float64(p.series)), "%", p.series))
	}
}

type Compactor struct {
	tmpDir string
	logger log.Logger

	chunkPool    chunkenc.Pool
	changeLogger ChangeLogger

	dryRun bool
}

type seriesReader struct {
	ir tsdb.IndexReader
	cr tsdb.ChunkReader
}

func New(tmpDir string, logger log.Logger, changeLogger ChangeLogger, pool chunkenc.Pool) *Compactor {
	return &Compactor{
		tmpDir:       tmpDir,
		logger:       logger,
		changeLogger: changeLogger,
		chunkPool:    pool,
	}
}

func NewDryRun(tmpDir string, logger log.Logger, changeLogger ChangeLogger, pool chunkenc.Pool) *Compactor {
	s := New(tmpDir, logger, changeLogger, pool)
	s.dryRun = true
	return s
}

// TODO(bwplotka): Upstream this.
func (w *Compactor) WriteSeries(ctx context.Context, readers []block.Reader, sWriter block.Writer, p ProgressLogger, modifiers ...Modifier) (err error) {
	if len(readers) == 0 {
		return errors.New("cannot write from no readers")
	}

	var (
		sReaders []seriesReader
		closers  []io.Closer
	)
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(errors.Wrap(cerr, "close"))
		}
		err = errs.Err()
	}()

	for _, b := range readers {
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
		sReaders = append(sReaders, seriesReader{ir: indexr, cr: chunkr})
	}

	symbols, set, err := compactSeries(ctx, sReaders...)
	if err != nil {
		return errors.Wrapf(err, "compact series from %v", func() string {
			var metas []string
			for _, m := range readers {
				metas = append(metas, fmt.Sprintf("%v", m.Meta()))
			}
			return strings.Join(metas, ",")
		}())
	}

	for _, m := range modifiers {
		symbols, set = m.Modify(symbols, set, w.changeLogger, p)
	}

	if w.dryRun {
		// Even for dry run, we need to exhaust iterators to see potential changes.
		for set.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			s := set.At()
			iter := s.Iterator(nil)
			for iter.Next() {
			}
			if err := iter.Err(); err != nil {
				level.Error(w.logger).Log("msg", "error while iterating over chunks", "series", s.Labels(), "err", err)
			}
			p.SeriesProcessed()
		}
		if err := set.Err(); err != nil {
			level.Error(w.logger).Log("msg", "error while iterating over set", "err", err)
		}
		return nil
	}

	if err := w.write(ctx, symbols, set, sWriter, p); err != nil {
		return errors.Wrap(err, "write")
	}
	return nil
}

// compactSeries compacts blocks' series into symbols and one ChunkSeriesSet with lazy populating chunks.
func compactSeries(ctx context.Context, sReaders ...seriesReader) (symbols index.StringIter, set storage.ChunkSeriesSet, _ error) {
	if len(sReaders) == 0 {
		return nil, nil, errors.New("cannot populate block from no readers")
	}

	var sets []storage.ChunkSeriesSet
	for i, r := range sReaders {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		k, v := index.AllPostingsKey()
		all, err := r.ir.Postings(ctx, k, v)
		if err != nil {
			return nil, nil, err
		}
		all = r.ir.SortedPostings(all)
		syms := r.ir.Symbols()
		sets = append(sets, newLazyPopulateChunkSeriesSet(r, all))
		if i == 0 {
			symbols = syms
			set = sets[0]
			continue
		}
		symbols = tsdb.NewMergedStringIter(symbols, syms)
	}

	if len(sets) <= 1 {
		return symbols, set, nil
	}
	// Merge series using compacting chunk series merger.
	return symbols, storage.NewMergeChunkSeriesSet(sets, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)), nil
}
