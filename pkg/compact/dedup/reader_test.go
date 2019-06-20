package dedup

import (
	"context"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestSampleSeries_ToChunkSeries(t *testing.T) {
	series := &SampleSeries{
		lset: labels.Labels{
			{Name: "b", Value: "1"},
			{Name: "a", Value: "1"},
		},
		samples: []*Sample{
			{timestamp: 7, value: rand.Float64()},
			{timestamp: 5, value: rand.Float64()},
			{timestamp: 9, value: rand.Float64()},
			{timestamp: 1, value: rand.Float64()},
			{timestamp: 2, value: rand.Float64()},
			{timestamp: 6, value: rand.Float64()},
		},
	}

	chunkSeries, err := series.ToChunkSeries()
	testutil.Ok(t, err)
	testutil.Assert(t, len(chunkSeries.chks) == 1, "chunk series conversion failed")
	testutil.Assert(t, chunkSeries.chks[0].MinTime == 1, "chunk series conversion failed")
	testutil.Assert(t, chunkSeries.chks[0].MaxTime == 9, "chunk series conversion failed")
}

func TestNewBlockReader(t *testing.T) {
	reader := createBlockReader(t)
	testutil.Assert(t, reader != nil, "new block reader failed")
	testutil.Assert(t, reader.ir != nil, "new block reader failed")
	testutil.Assert(t, reader.cr != nil, "new block reader failed")
	testutil.Assert(t, reader.postings != nil, "new block reader failed")
	testutil.Assert(t, len(reader.closers) == 3, "new block reader failed")
}

func TestBlockReader_Symbols(t *testing.T) {
	reader := createBlockReader(t)
	symbols, err := reader.Symbols()
	testutil.Ok(t, err)
	testutil.Assert(t, len(symbols) > 0, "new block reader failed")
}

func TestBlockReader_Close(t *testing.T) {
	reader := createBlockReader(t)
	err := reader.Close()
	testutil.Ok(t, err)
}

func createBlockReader(t *testing.T) *BlockReader {
	dataDir, err := ioutil.TempDir("", "thanos-dedup-streamed-block-reader")
	testutil.Ok(t, err)
	id := createBlock(t, context.Background(), dataDir, "r0")

	blockDir := filepath.Join(dataDir, id.String())

	reader, err := NewBlockReader(log.NewNopLogger(), blockDir)
	testutil.Ok(t, err)

	return reader
}
