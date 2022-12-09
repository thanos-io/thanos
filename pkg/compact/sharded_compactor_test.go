package compact

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
)

func TestCompaction_CompactWithSplitting(t *testing.T) {
	seriesCounts := []int{10, 1234}
	shardCounts := []uint64{1, 13}

	for _, series := range seriesCounts {
		dir, err := os.MkdirTemp("", "compact")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.RemoveAll(dir))
		}()

		ranges := [][2]int64{{0, 5000}, {3000, 8000}, {6000, 11000}, {9000, 14000}}

		// Generate blocks.
		var blockDirs []string
		var openBlocks []*tsdb.Block

		for _, r := range ranges {
			block, err := tsdb.OpenBlock(nil, makeBlock(t, dir, genSeries(series, 10, r[0], r[1])), nil)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, block.Close())
			}()

			openBlocks = append(openBlocks, block)
			blockDirs = append(blockDirs, block.Dir())
		}

		for _, shardCount := range shardCounts {
			t.Run(fmt.Sprintf("series=%d, shards=%d", series, shardCount), func(t *testing.T) {
				compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{0}, nil, nil)
				require.NoError(t, err)

				shardedCompactor := NewShardedCompactor(nil, log.NewNopLogger(), compactor, shardCount)
				blockIDs, err := shardedCompactor.Compact(dir, blockDirs, openBlocks)

				require.NoError(t, err)
				require.Equal(t, shardCount, uint64(len(blockIDs)))

				// Verify resulting blocks. We will iterate over all series in all blocks, and check two things:
				// 1) Make sure that each series in the block belongs to the block (based on sharding).
				// 2) Verify that total number of series over all blocks is correct.
				totalSeries := uint64(0)

				ts := uint64(0)
				for shardIndex, blockID := range blockIDs {
					// Some blocks may be empty, they will have zero block ID.
					if blockID == (ulid.ULID{}) {
						continue
					}

					// All blocks have the same timestamp.
					if ts == 0 {
						ts = blockID.Time()
					} else {
						require.Equal(t, ts, blockID.Time())
					}

					// Symbols found in series.
					seriesSymbols := map[string]struct{}{}

					// We always expect to find "" symbol in the symbols table even if it's not in the series.
					// Head compaction always includes it, and then it survives additional non-sharded compactions.
					// Our splitting compaction preserves it too.
					seriesSymbols[""] = struct{}{}

					block, err := tsdb.OpenBlock(log.NewNopLogger(), filepath.Join(dir, blockID.String()), nil)
					require.NoError(t, err)

					defer func() {
						require.NoError(t, block.Close())
					}()

					totalSeries += block.Meta().Stats.NumSeries

					idxr, err := block.Index()
					require.NoError(t, err)

					defer func() {
						require.NoError(t, idxr.Close())
					}()

					k, v := index.AllPostingsKey()
					p, err := idxr.Postings(k, v)
					require.NoError(t, err)

					var (
						lbls labels.Labels
						chks []chunks.Meta
					)
					for p.Next() {
						ref := p.At()
						require.NoError(t, idxr.Series(ref, &lbls, &chks))

						require.Equal(t, uint64(shardIndex), lbls.Hash()%shardCount)

						// Collect all symbols used by series.
						for _, l := range lbls {
							seriesSymbols[l.Name] = struct{}{}
							seriesSymbols[l.Value] = struct{}{}
						}
					}
					require.NoError(t, p.Err())

					// Check that all symbols in symbols table are actually used by series.
					symIt := idxr.Symbols()
					for symIt.Next() {
						w := symIt.At()
						_, ok := seriesSymbols[w]
						require.True(t, ok, "not found in series: '%s'", w)
						delete(seriesSymbols, w)
					}

					// Check that symbols table covered all symbols found from series.
					require.Equal(t, 0, len(seriesSymbols))
				}

				require.Equal(t, uint64(series), totalSeries)

				// Source blocks are *not* deletable.
				for _, b := range openBlocks {
					require.False(t, b.Meta().Compaction.Deletable)
				}
			})
		}
	}
}

const (
	defaultLabelName  = "labelName"
	defaultLabelValue = "labelValue"
)

type sample struct {
	t int64
	v float64
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() float64 {
	return s.v
}

// genSeries generates series with a given number of labels and values.
func genSeries(totalSeries, labelCount int, mint, maxt int64) []storage.Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]storage.Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t++ {
			samples = append(samples, sample{t: t, v: rand.Float64()})
		}
		series[i] = storage.NewListSeries(labels.FromMap(lbls), samples)
	}
	return series
}

// createBlock creates a block with given set of series and returns its dir.
func makeBlock(tb testing.TB, dir string, series []storage.Series) string {
	blockDir, err := tsdb.CreateBlock(series, dir, 0, log.NewNopLogger())
	require.NoError(tb, err)
	return blockDir
}
