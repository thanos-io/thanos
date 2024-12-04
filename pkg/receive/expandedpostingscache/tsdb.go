// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Original code by Alan Protasio (https://github.com/alanprot) in the Cortex project.

package expandedpostingscache

import (
	"context"
	"errors"
	"fmt"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/util/annotations"
)

/*
	This file is basically a copy from https://github.com/prometheus/prometheus/blob/e2e01c1cffbfc4f26f5e9fe6138af87d7ff16122/tsdb/querier.go
	with the difference that the PostingsForMatchers function is called from the Postings Cache
*/

type blockBaseQuerier struct {
	blockID    ulid.ULID
	index      prom_tsdb.IndexReader
	chunks     prom_tsdb.ChunkReader
	tombstones tombstones.Reader

	closed bool

	mint, maxt int64
}

func newBlockBaseQuerier(b prom_tsdb.BlockReader, mint, maxt int64) (*blockBaseQuerier, error) {
	indexr, err := b.Index()
	if err != nil {
		return nil, fmt.Errorf("open index reader: %w", err)
	}
	chunkr, err := b.Chunks()
	if err != nil {
		indexr.Close()
		return nil, fmt.Errorf("open chunk reader: %w", err)
	}
	tombsr, err := b.Tombstones()
	if err != nil {
		indexr.Close()
		chunkr.Close()
		return nil, fmt.Errorf("open tombstone reader: %w", err)
	}

	if tombsr == nil {
		tombsr = tombstones.NewMemTombstones()
	}
	return &blockBaseQuerier{
		blockID:    b.Meta().ULID,
		mint:       mint,
		maxt:       maxt,
		index:      indexr,
		chunks:     chunkr,
		tombstones: tombsr,
	}, nil
}

func (q *blockBaseQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	res, err := q.index.SortedLabelValues(ctx, name, matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	res, err := q.index.LabelNames(ctx, matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) Close() error {
	if q.closed {
		return errors.New("block querier already closed")
	}

	errs := tsdb_errors.NewMulti(
		q.index.Close(),
		q.chunks.Close(),
		q.tombstones.Close(),
	)
	q.closed = true
	return errs.Err()
}

type cachedBlockChunkQuerier struct {
	*blockBaseQuerier

	cache ExpandedPostingsCache
}

func NewCachedBlockChunkQuerier(cache ExpandedPostingsCache, b prom_tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := newBlockBaseQuerier(b, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &cachedBlockChunkQuerier{blockBaseQuerier: q, cache: cache}, nil
}

func (q *cachedBlockChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	return selectChunkSeriesSet(ctx, sortSeries, hints, ms, q.blockID, q.index, q.chunks, q.tombstones, q.mint, q.maxt, q.cache)
}

func selectChunkSeriesSet(ctx context.Context, sortSeries bool, hints *storage.SelectHints, ms []*labels.Matcher,
	blockID ulid.ULID, ir prom_tsdb.IndexReader, chunks prom_tsdb.ChunkReader, tombstones tombstones.Reader, mint, maxt int64,
	cache ExpandedPostingsCache,
) storage.ChunkSeriesSet {
	disableTrimming := false
	sharded := hints != nil && hints.ShardCount > 0

	if hints != nil {
		mint = hints.Start
		maxt = hints.End
		disableTrimming = hints.DisableTrimming
	}

	var postings index.Postings
	if cache != nil {
		p, err := cache.PostingsForMatchers(ctx, blockID, ir, ms...)
		if err != nil {
			return storage.ErrChunkSeriesSet(err)
		}
		postings = p
	} else {
		p, err := prom_tsdb.PostingsForMatchers(ctx, ir, ms...)
		if err != nil {
			return storage.ErrChunkSeriesSet(err)
		}
		postings = p
	}

	if sharded {
		postings = ir.ShardedPostings(postings, hints.ShardIndex, hints.ShardCount)
	}
	if sortSeries {
		postings = ir.SortedPostings(postings)
	}
	return prom_tsdb.NewBlockChunkSeriesSet(blockID, ir, chunks, tombstones, postings, mint, maxt, disableTrimming)
}
