// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package hintspb

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestQueryStatsMerge(t *testing.T) {
	s := &QueryStats{
		BlocksQueried:          1,
		MergedSeriesCount:      1,
		MergedChunksCount:      1,
		DataDownloadedSizeSum:  1,
		PostingsFetched:        1,
		PostingsToFetch:        1,
		PostingsFetchCount:     1,
		PostingsFetchedSizeSum: 1,
		PostingsTouched:        1,
		PostingsTouchedSizeSum: 1,
		SeriesFetched:          1,
		SeriesFetchCount:       1,
		SeriesFetchedSizeSum:   1,
		SeriesTouched:          1,
		SeriesTouchedSizeSum:   1,
		ChunksFetched:          1,
		ChunksFetchCount:       1,
		ChunksFetchedSizeSum:   1,
		ChunksTouched:          1,
		ChunksTouchedSizeSum:   1,
		GetAllDuration:         &durationpb.Duration{Seconds: 1, Nanos: 1},
		MergeDuration:          &durationpb.Duration{Seconds: 1, Nanos: 1},
	}
	o := &QueryStats{
		BlocksQueried:          1,
		MergedSeriesCount:      1,
		MergedChunksCount:      1,
		DataDownloadedSizeSum:  1,
		PostingsFetched:        1,
		PostingsToFetch:        1,
		PostingsFetchCount:     1,
		PostingsFetchedSizeSum: 1,
		PostingsTouched:        1,
		PostingsTouchedSizeSum: 1,
		SeriesFetched:          1,
		SeriesFetchCount:       1,
		SeriesFetchedSizeSum:   1,
		SeriesTouched:          1,
		SeriesTouchedSizeSum:   1,
		ChunksFetched:          1,
		ChunksFetchCount:       1,
		ChunksFetchedSizeSum:   1,
		ChunksTouched:          1,
		ChunksTouchedSizeSum:   1,
		GetAllDuration:         &durationpb.Duration{Seconds: 1, Nanos: 1},
		MergeDuration:          &durationpb.Duration{Seconds: 1, Nanos: 1},
	}

	s.Merge(o)

	// Expected stats.
	e := &QueryStats{
		BlocksQueried:          2,
		MergedSeriesCount:      2,
		MergedChunksCount:      2,
		DataDownloadedSizeSum:  2,
		PostingsFetched:        2,
		PostingsToFetch:        2,
		PostingsFetchCount:     2,
		PostingsFetchedSizeSum: 2,
		PostingsTouched:        2,
		PostingsTouchedSizeSum: 2,
		SeriesFetched:          2,
		SeriesFetchCount:       2,
		SeriesFetchedSizeSum:   2,
		SeriesTouched:          2,
		SeriesTouchedSizeSum:   2,
		ChunksFetched:          2,
		ChunksFetchCount:       2,
		ChunksFetchedSizeSum:   2,
		ChunksTouched:          2,
		ChunksTouchedSizeSum:   2,
		GetAllDuration:         &durationpb.Duration{Seconds: 2, Nanos: 2},
		MergeDuration:          &durationpb.Duration{Seconds: 2, Nanos: 2},
	}
	testutil.Equals(t, e, s)
}
