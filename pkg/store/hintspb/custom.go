// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hintspb

import (
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (m *SeriesResponseHints) AddQueriedBlock(id ulid.ULID) {
	m.QueriedBlocks = append(m.QueriedBlocks, &Block{
		Id: id.String(),
	})
}

func (m *LabelNamesResponseHints) AddQueriedBlock(id ulid.ULID) {
	m.QueriedBlocks = append(m.QueriedBlocks, &Block{
		Id: id.String(),
	})
}

func (m *LabelValuesResponseHints) AddQueriedBlock(id ulid.ULID) {
	m.QueriedBlocks = append(m.QueriedBlocks, &Block{
		Id: id.String(),
	})
}

func (m *QueryStats) Merge(other *QueryStats) {
	m.BlocksQueried += other.BlocksQueried
	m.MergedSeriesCount += other.MergedSeriesCount
	m.MergedChunksCount += other.MergedChunksCount
	m.DataDownloadedSizeSum += other.DataDownloadedSizeSum

	m.PostingsFetched += other.PostingsFetched
	m.PostingsToFetch += other.PostingsToFetch
	m.PostingsFetchCount += other.PostingsFetchCount
	m.PostingsFetchedSizeSum += other.PostingsFetchedSizeSum
	m.PostingsTouched += other.PostingsTouched
	m.PostingsTouchedSizeSum += other.PostingsTouchedSizeSum

	m.SeriesFetched += other.SeriesFetched
	m.SeriesFetchCount += other.SeriesFetchCount
	m.SeriesFetchedSizeSum += other.SeriesFetchedSizeSum
	m.SeriesTouched += other.SeriesTouched
	m.SeriesTouchedSizeSum += other.SeriesTouchedSizeSum

	m.ChunksFetched += other.ChunksFetched
	m.ChunksFetchCount += other.ChunksFetchCount
	m.ChunksFetchedSizeSum += other.ChunksFetchedSizeSum
	m.ChunksTouched += other.ChunksTouched
	m.ChunksTouchedSizeSum += other.ChunksTouchedSizeSum

	// Add durations using durationpb
	m.GetAllDuration = addDurations(m.GetAllDuration, other.GetAllDuration)
	m.MergeDuration = addDurations(m.MergeDuration, other.MergeDuration)
}

// addDurations adds two durationpb.Duration values.
func addDurations(a, b *durationpb.Duration) *durationpb.Duration {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return durationpb.New(a.AsDuration() + b.AsDuration())
}
