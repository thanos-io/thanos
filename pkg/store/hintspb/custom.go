// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hintspb

import "github.com/oklog/ulid"

func (m *SeriesResponseHints) AddQueriedBlock(id ulid.ULID) {
	m.QueriedBlocks = append(m.QueriedBlocks, Block{
		Id: id.String(),
	})
}

func (m *LabelNamesResponseHints) AddQueriedBlock(id ulid.ULID) {
	m.QueriedBlocks = append(m.QueriedBlocks, Block{
		Id: id.String(),
	})
}

func (m *LabelValuesResponseHints) AddQueriedBlock(id ulid.ULID) {
	m.QueriedBlocks = append(m.QueriedBlocks, Block{
		Id: id.String(),
	})
}

func (m *QueryStats) Merge(other *QueryStats) {
	m.BlocksQueried += other.BlocksQueried
	m.MergeDuration += other.MergeDuration
	m.GetAllDuration += other.GetAllDuration
	m.MergedSeriesCount += other.MergedSeriesCount
	m.MergedChunksCount += other.MergedChunksCount

	m.CachedPostingsCompressions += other.CachedPostingsCompressions
	m.CachedPostingsCompressionErrors += other.CachedPostingsCompressionErrors
	m.CachedPostingsCompressedSizeSum += other.CachedPostingsCompressedSizeSum
	m.CachedPostingsCompressionTimeSum += other.CachedPostingsCompressionTimeSum
	m.CachedPostingsDecompressions += other.CachedPostingsDecompressions
	m.CachedPostingsDecompressionErrors += other.CachedPostingsDecompressionErrors
	m.CachedPostingsDecompressionTimeSum += other.CachedPostingsDecompressionTimeSum

	m.PostingsFetched += other.PostingsFetched
	m.PostingsToFetch += other.PostingsToFetch
	m.PostingsFetchCount += other.PostingsFetchCount
	m.PostingsFetchedSizeSum += other.PostingsFetchedSizeSum
	m.PostingsFetchDurationSum += other.PostingsFetchDurationSum
	m.PostingsTouched += other.PostingsTouched
	m.PostingsTouchedSizeSum += other.PostingsTouchedSizeSum

	m.SeriesFetched += other.SeriesFetched
	m.SeriesFetchCount += other.SeriesFetchCount
	m.SeriesFetchedSizeSum += m.SeriesFetchedSizeSum
	m.SeriesFetchDurationSum += m.SeriesFetchDurationSum
	m.SeriesTouched += other.SeriesTouched
	m.SeriesTouchedSizeSum += other.SeriesTouchedSizeSum

	m.ChunksFetched += other.ChunksFetched
	m.ChunksFetchCount += other.ChunksFetchCount
	m.ChunksFetchedSizeSum += other.ChunksFetchedSizeSum
	m.ChunksFetchDurationSum += other.ChunksFetchDurationSum
	m.ChunksTouched += other.ChunksTouched
	m.ChunksTouchedSizeSum += other.ChunksTouchedSizeSum
}
