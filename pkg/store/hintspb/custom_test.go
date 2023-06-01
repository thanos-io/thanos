package hintspb

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestQueryStatsMerge(t *testing.T) {
	s := &QueryStats{
		BlocksQueried:          1,
		MergedSeriesCount:      1,
		MergedChunksCount:      1,
		PostingsTouched:        1,
		PostingsTouchedSizeSum: 1,
		PostingsToFetch:        1,
		PostingsFetched:        1,
		PostingsFetchedSizeSum: 1,
		PostingsFetchCount:     1,
		SeriesTouched:          1,
		SeriesTouchedSizeSum:   1,
		SeriesFetched:          1,
		SeriesFetchedSizeSum:   1,
		SeriesFetchCount:       1,
		ChunksTouched:          1,
		ChunksTouchedSizeSum:   1,
		ChunksFetched:          1,
		ChunksFetchedSizeSum:   1,
		ChunksFetchCount:       1,
	}
	o := &QueryStats{
		BlocksQueried:          100,
		MergedSeriesCount:      100,
		MergedChunksCount:      100,
		PostingsTouched:        100,
		PostingsTouchedSizeSum: 100,
		PostingsToFetch:        100,
		PostingsFetched:        100,
		PostingsFetchedSizeSum: 100,
		PostingsFetchCount:     100,
		SeriesTouched:          100,
		SeriesTouchedSizeSum:   100,
		SeriesFetched:          100,
		SeriesFetchedSizeSum:   100,
		SeriesFetchCount:       100,
		ChunksTouched:          100,
		ChunksTouchedSizeSum:   100,
		ChunksFetched:          100,
		ChunksFetchedSizeSum:   100,
		ChunksFetchCount:       100,
	}
	s.Merge(o)
	expected := &QueryStats{
		BlocksQueried:          101,
		MergedSeriesCount:      101,
		MergedChunksCount:      101,
		PostingsTouched:        101,
		PostingsTouchedSizeSum: 101,
		PostingsToFetch:        101,
		PostingsFetched:        101,
		PostingsFetchedSizeSum: 101,
		PostingsFetchCount:     101,
		SeriesTouched:          101,
		SeriesTouchedSizeSum:   101,
		SeriesFetched:          101,
		SeriesFetchedSizeSum:   101,
		SeriesFetchCount:       101,
		ChunksTouched:          101,
		ChunksTouchedSizeSum:   101,
		ChunksFetched:          101,
		ChunksFetchedSizeSum:   101,
		ChunksFetchCount:       101,
	}
	testutil.Equals(t, expected, s)
}
