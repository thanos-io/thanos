// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/objtesting"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
)

func newTestFetcherMetrics() *FetcherMetrics {
	return &FetcherMetrics{
		Synced:   extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"}),
		Modified: extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"modified"}),
	}
}

type ulidFilter struct {
	ulidToDelete *ulid.ULID
}

func (f *ulidFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	if _, ok := metas[*f.ulidToDelete]; ok {
		synced.WithLabelValues("filtered").Inc()
		delete(metas, *f.ulidToDelete)
	}
	return nil
}

func ULID(i int) ulid.ULID { return ulid.MustNew(uint64(i), nil) }

func ULIDs(is ...int) []ulid.ULID {
	ret := []ulid.ULID{}
	for _, i := range is {
		ret = append(ret, ULID(i))
	}

	return ret
}

func TestMetaFetcher_Fetch(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		dir := t.TempDir()

		var ulidToDelete ulid.ULID
		noopLogger := log.NewNopLogger()
		insBkt := objstore.WithNoopInstr(bkt)

		r := prometheus.NewRegistry()

		recursiveLister := NewRecursiveLister(noopLogger, insBkt)
		recursiveBaseFetcher, err := NewBaseFetcher(noopLogger, 20, insBkt, recursiveLister, dir, r)
		testutil.Ok(t, err)

		recursiveFetcher := recursiveBaseFetcher.NewMetaFetcher(r, []MetadataFilter{
			&ulidFilter{ulidToDelete: &ulidToDelete},
		}, nil)

		for _, tcase := range []struct {
			name                  string
			do                    func(cleanCache func())
			filterULID            ulid.ULID
			expectedMetas         []ulid.ULID
			expectedCorruptedMeta []ulid.ULID
			expectedNoMeta        []ulid.ULID
			expectedFiltered      int
			expectedMetaErr       error
			expectedCacheBusts    int
			expectedSyncs         int

			// If this is set then use it.
			fetcher     *MetaFetcher
			baseFetcher *BaseFetcher
		}{
			{
				name: "empty bucket",
				do:   func(_ func()) {},

				expectedMetas:         ULIDs(),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(),
			},
			{
				name: "3 metas in bucket",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 1
					meta.ULID = ULID(1)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))

					meta.ULID = ULID(2)
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))

					meta.ULID = ULID(3)
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(),
			},
			{
				name: "meta 2 and 3 have corrupted data on disk ",
				do: func(cleanCache func()) {
					testutil.Ok(t, os.Remove(filepath.Join(dir, "meta-syncer", ULID(2).String(), MetaFilename)))

					f, err := os.OpenFile(filepath.Join(dir, "meta-syncer", ULID(3).String(), MetaFilename), os.O_WRONLY, os.ModePerm)
					testutil.Ok(t, err)

					_, err = f.WriteString("{ almost")
					testutil.Ok(t, err)
					testutil.Ok(t, f.Close())
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(),
			},
			{
				name: "block without meta",
				do: func(_ func()) {
					testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(4).String(), "some-file"), bytes.NewBuffer([]byte("something"))))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(),
				expectedNoMeta:        ULIDs(4),
			},
			{
				name: "corrupted meta.json",
				do: func(_ func()) {
					testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(5).String(), MetaFilename), bytes.NewBuffer([]byte("{ not a json"))))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
			},

			{
				name:       "filter not existing ulid",
				do:         func(_ func()) {},
				filterULID: ULID(10),

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
			},
			{
				name: "filter ulid 1",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 1
					meta.ULID = ULID(1)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},
				filterULID: ULID(1),

				expectedMetas:         ULIDs(2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
				expectedFiltered:      1,
			},
			{
				name: "use recursive lister",
				do: func(cleanCache func()) {
					cleanCache()
				},
				fetcher:     recursiveFetcher,
				baseFetcher: recursiveBaseFetcher,

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
			},
			{
				name: "update timestamp, expect a cache bust",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 1
					meta.MaxTime = 123456
					meta.ULID = ULID(1)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},
				fetcher:     recursiveFetcher,
				baseFetcher: recursiveBaseFetcher,

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
				expectedFiltered:      0,
				expectedCacheBusts:    1,
				expectedSyncs:         2,
			},
			{
				name: "error: not supported meta version",
				do: func(_ func()) {
					var meta metadata.Meta
					meta.Version = 20
					meta.ULID = ULID(7)

					var buf bytes.Buffer
					testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
					testutil.Ok(t, bkt.Upload(ctx, path.Join(meta.ULID.String(), metadata.MetaFilename), &buf))
				},

				expectedMetas:         ULIDs(1, 2, 3),
				expectedCorruptedMeta: ULIDs(5),
				expectedNoMeta:        ULIDs(4),
				expectedMetaErr:       errors.New("incomplete view: unexpected meta file: 00000000070000000000000000/meta.json version: 20"),
			},
		} {
			if ok := t.Run(tcase.name, func(t *testing.T) {
				r := prometheus.NewRegistry()

				var fetcher *MetaFetcher
				var baseFetcher *BaseFetcher

				if tcase.baseFetcher != nil {
					baseFetcher = tcase.baseFetcher
				} else {
					lister := NewConcurrentLister(noopLogger, insBkt)
					bf, err := NewBaseFetcher(noopLogger, 20, insBkt, lister, dir, r)
					testutil.Ok(t, err)

					baseFetcher = bf
				}

				if tcase.fetcher != nil {
					fetcher = tcase.fetcher
				} else {
					fetcher = baseFetcher.NewMetaFetcher(r, []MetadataFilter{
						&ulidFilter{ulidToDelete: &ulidToDelete},
					}, nil)
				}

				tcase.do(func() {
					baseFetcher.cached.Clear()
					testutil.Ok(t, os.RemoveAll(filepath.Join(dir, "meta-syncer")))
				})

				ulidToDelete = tcase.filterULID
				metas, partial, err := fetcher.Fetch(ctx)
				if tcase.expectedMetaErr != nil {
					testutil.NotOk(t, err)
					testutil.Equals(t, tcase.expectedMetaErr.Error(), err.Error())
				} else {
					testutil.Ok(t, err)
				}

				{
					metasSlice := make([]ulid.ULID, 0, len(metas))
					for id, m := range metas {
						testutil.Assert(t, m != nil, "meta is nil")
						metasSlice = append(metasSlice, id)
					}
					sort.Slice(metasSlice, func(i, j int) bool {
						return metasSlice[i].Compare(metasSlice[j]) < 0
					})
					testutil.Equals(t, tcase.expectedMetas, metasSlice)
				}

				{
					partialSlice := make([]ulid.ULID, 0, len(partial))
					for id := range partial {

						partialSlice = append(partialSlice, id)
					}
					sort.Slice(partialSlice, func(i, j int) bool {
						return partialSlice[i].Compare(partialSlice[j]) >= 0
					})
					expected := append([]ulid.ULID{}, tcase.expectedCorruptedMeta...)
					expected = append(expected, tcase.expectedNoMeta...)
					sort.Slice(expected, func(i, j int) bool {
						return expected[i].Compare(expected[j]) >= 0
					})
					testutil.Equals(t, expected, partialSlice)
				}

				expectedFailures := 0
				if tcase.expectedMetaErr != nil {
					expectedFailures = 1
				}

				testutil.Equals(t, float64(max(1, tcase.expectedSyncs)), promtest.ToFloat64(baseFetcher.syncs))
				testutil.Equals(t, float64(tcase.expectedCacheBusts), promtest.ToFloat64(baseFetcher.cacheBusts))
				testutil.Equals(t, float64(max(1, tcase.expectedSyncs)), promtest.ToFloat64(fetcher.metrics.Syncs))
				testutil.Equals(t, float64(len(tcase.expectedMetas)), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(LoadedMeta)))
				testutil.Equals(t, float64(len(tcase.expectedNoMeta)), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(NoMeta)))
				testutil.Equals(t, float64(tcase.expectedFiltered), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues("filtered")))
				testutil.Equals(t, 0.0, promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(labelExcludedMeta)))
				testutil.Equals(t, 0.0, promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(timeExcludedMeta)))
				testutil.Equals(t, float64(expectedFailures), promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(FailedMeta)))
				testutil.Equals(t, 0.0, promtest.ToFloat64(fetcher.metrics.Synced.WithLabelValues(tooFreshMeta)))
			}); !ok {
				return
			}
		}
	})
}

func TestLabelShardedMetaFilter_Filter_Basic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	relabelContentYaml := `
    - action: drop
      regex: "A"
      source_labels:
      - cluster
    - action: keep
      regex: "keepme"
      source_labels:
      - message
    `
	relabelConfig, err := ParseRelabelConfig([]byte(relabelContentYaml), SelectorSupportedRelabelActions)
	testutil.Ok(t, err)

	f := NewLabelShardedMetaFilter(relabelConfig)

	input := map[ulid.ULID]*metadata.Meta{
		ULID(1): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "B", "message": "keepme"},
			},
		},
		ULID(2): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"something": "A", "message": "keepme"},
			},
		},
		ULID(3): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "A", "message": "keepme"},
			},
		},
		ULID(4): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "A", "something": "B", "message": "keepme"},
			},
		},
		ULID(5): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "B"},
			},
		},
		ULID(6): {
			Thanos: metadata.Thanos{
				Labels: map[string]string{"cluster": "B", "message": "keepme"},
			},
		},
	}
	expected := map[ulid.ULID]*metadata.Meta{
		ULID(1): input[ULID(1)],
		ULID(2): input[ULID(2)],
		ULID(6): input[ULID(6)],
	}

	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 3.0, promtest.ToFloat64(m.Synced.WithLabelValues(labelExcludedMeta)))
	testutil.Equals(t, expected, input)

}

func TestLabelShardedMetaFilter_Filter_Hashmod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	relabelContentYamlFmt := `
    - action: hashmod
      source_labels: ["%s"]
      target_label: shard
      modulus: 3
    - action: keep
      source_labels: ["shard"]
      regex: %d
`
	for i := range 3 {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			relabelConfig, err := ParseRelabelConfig(fmt.Appendf(nil, relabelContentYamlFmt, BlockIDLabel, i), SelectorSupportedRelabelActions)
			testutil.Ok(t, err)

			f := NewLabelShardedMetaFilter(relabelConfig)

			input := map[ulid.ULID]*metadata.Meta{
				ULID(1): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "B", "message": "keepme"},
					},
				},
				ULID(2): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"something": "A", "message": "keepme"},
					},
				},
				ULID(3): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "A", "message": "keepme"},
					},
				},
				ULID(4): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "A", "something": "B", "message": "keepme"},
					},
				},
				ULID(5): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "B"},
					},
				},
				ULID(6): {
					Thanos: metadata.Thanos{
						Labels: map[string]string{"cluster": "B", "message": "keepme"},
					},
				},
				ULID(7):  {},
				ULID(8):  {},
				ULID(9):  {},
				ULID(10): {},
				ULID(11): {},
				ULID(12): {},
				ULID(13): {},
				ULID(14): {},
				ULID(15): {},
			}
			expected := map[ulid.ULID]*metadata.Meta{}
			switch i {
			case 0:
				expected = map[ulid.ULID]*metadata.Meta{
					ULID(2):  input[ULID(2)],
					ULID(6):  input[ULID(6)],
					ULID(11): input[ULID(11)],
					ULID(13): input[ULID(13)],
				}
			case 1:
				expected = map[ulid.ULID]*metadata.Meta{
					ULID(5):  input[ULID(5)],
					ULID(7):  input[ULID(7)],
					ULID(10): input[ULID(10)],
					ULID(12): input[ULID(12)],
					ULID(14): input[ULID(14)],
					ULID(15): input[ULID(15)],
				}
			case 2:
				expected = map[ulid.ULID]*metadata.Meta{
					ULID(1): input[ULID(1)],
					ULID(3): input[ULID(3)],
					ULID(4): input[ULID(4)],
					ULID(8): input[ULID(8)],
					ULID(9): input[ULID(9)],
				}
			}
			deleted := len(input) - len(expected)

			m := newTestFetcherMetrics()
			testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

			testutil.Equals(t, expected, input)
			testutil.Equals(t, float64(deleted), promtest.ToFloat64(m.Synced.WithLabelValues(labelExcludedMeta)))

		})

	}
}

func TestTimePartitionMetaFilter_Filter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	mint := time.Unix(0, 1*time.Millisecond.Nanoseconds())
	maxt := time.Unix(0, 10*time.Millisecond.Nanoseconds())
	f := NewTimePartitionMetaFilter(model.TimeOrDurationValue{Time: &mint}, model.TimeOrDurationValue{Time: &maxt})

	input := map[ulid.ULID]*metadata.Meta{
		ULID(1): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 0,
				MaxTime: 1,
			},
		},
		ULID(2): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 1,
				MaxTime: 10,
			},
		},
		ULID(3): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 2,
				MaxTime: 30,
			},
		},
		ULID(4): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 0,
				MaxTime: 30,
			},
		},
		ULID(5): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: -1,
				MaxTime: 0,
			},
		},
		ULID(6): {
			BlockMeta: tsdb.BlockMeta{
				MinTime: 20,
				MaxTime: 30,
			},
		},
	}
	expected := map[ulid.ULID]*metadata.Meta{
		ULID(1): input[ULID(1)],
		ULID(2): input[ULID(2)],
		ULID(3): input[ULID(3)],
		ULID(4): input[ULID(4)],
	}

	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 2.0, promtest.ToFloat64(m.Synced.WithLabelValues(timeExcludedMeta)))
	testutil.Equals(t, expected, input)

}

type sourcesAndResolution struct {
	sources    []ulid.ULID
	resolution int64
}

func TestDeduplicateFilter_Filter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for _, tcase := range []struct {
		name     string
		input    map[ulid.ULID]*sourcesAndResolution
		expected []ulid.ULID
	}{
		{
			name: "3 non compacted blocks in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(2): {
					sources:    []ulid.ULID{ULID(2)},
					resolution: 0,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(3)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(1),
				ULID(2),
				ULID(3),
			},
		},
		{
			name: "compacted block with sources in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(5)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(4),
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "two compacted blocks with same sources",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(5): {
					sources:    []ulid.ULID{ULID(5)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(3),
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "two compacted blocks with overlapping sources",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "3 non compacted blocks and compacted block of level 2 in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(2): {
					sources:    []ulid.ULID{ULID(2)},
					resolution: 0,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(3)},
					resolution: 0,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(2), ULID(1), ULID(3)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(4),
				ULID(6),
			},
		},
		{
			name: "3 compacted blocks of level 2 and one compacted block of level 3 in bucket",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(1), ULID(2), ULID(3)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(6), ULID(4), ULID(5)},
					resolution: 0,
				},
				ULID(14): {
					sources:    []ulid.ULID{ULID(14)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(13): {
					sources:    []ulid.ULID{ULID(1), ULID(6), ULID(2), ULID(3), ULID(5), ULID(7), ULID(4), ULID(8), ULID(9)},
					resolution: 0,
				},
				ULID(12): {
					sources:    []ulid.ULID{ULID(7), ULID(9), ULID(8)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(14),
				ULID(13),
			},
		},
		{
			name: "compacted blocks with overlapping sources",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(8): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2), ULID(4)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2), ULID(4)},
					resolution: 0,
				},
				ULID(7): {
					sources:    []ulid.ULID{ULID(3), ULID(1), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(6),
			},
		},
		{
			name: "compacted blocks of level 3 with overlapping sources of equal length",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(1), ULID(2), ULID(6), ULID(7)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(6), ULID(8), ULID(1), ULID(2)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(10),
				ULID(11),
			},
		},
		{
			name: "compacted blocks of level 3 with overlapping sources of different length",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(6), ULID(7), ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(2), ULID(3), ULID(1)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(10),
				ULID(11),
			},
		},
		{
			name: "blocks with same sources and different resolutions",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(2): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 1000,
				},
				ULID(3): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 10000,
				},
			},
			expected: []ulid.ULID{
				ULID(1),
				ULID(2),
				ULID(3),
			},
		},
		{
			name: "compacted blocks with overlapping sources and different resolutions",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(6): {
					sources:    []ulid.ULID{ULID(6)},
					resolution: 10000,
				},
				ULID(4): {
					sources:    []ulid.ULID{ULID(1), ULID(3), ULID(2)},
					resolution: 0,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(2), ULID(3), ULID(1)},
					resolution: 1000,
				},
			},
			expected: []ulid.ULID{
				ULID(4),
				ULID(5),
				ULID(6),
			},
		},
		{
			name: "compacted blocks of level 3 with overlapping sources of different length and different resolutions",
			input: map[ulid.ULID]*sourcesAndResolution{
				ULID(10): {
					sources:    []ulid.ULID{ULID(7), ULID(5), ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(12): {
					sources:    []ulid.ULID{ULID(6), ULID(7), ULID(1)},
					resolution: 10000,
				},
				ULID(1): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 0,
				},
				ULID(13): {
					sources:    []ulid.ULID{ULID(1)},
					resolution: 10000,
				},
				ULID(5): {
					sources:    []ulid.ULID{ULID(1), ULID(2)},
					resolution: 0,
				},
				ULID(11): {
					sources:    []ulid.ULID{ULID(2), ULID(3), ULID(1)},
					resolution: 0,
				},
			},
			expected: []ulid.ULID{
				ULID(10),
				ULID(11),
				ULID(12),
			},
		},
	} {
		f := NewDeduplicateFilter(1)
		if ok := t.Run(tcase.name, func(t *testing.T) {
			m := newTestFetcherMetrics()
			metas := make(map[ulid.ULID]*metadata.Meta)
			inputLen := len(tcase.input)
			for id, metaInfo := range tcase.input {
				metas[id] = &metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						ULID: id,
						Compaction: tsdb.BlockMetaCompaction{
							Sources: metaInfo.sources,
						},
					},
					Thanos: metadata.Thanos{
						Downsample: metadata.ThanosDownsample{
							Resolution: metaInfo.resolution,
						},
					},
				}
			}
			testutil.Ok(t, f.Filter(ctx, metas, m.Synced, nil))
			compareSliceWithMapKeys(t, metas, tcase.expected)
			testutil.Equals(t, float64(inputLen-len(tcase.expected)), promtest.ToFloat64(m.Synced.WithLabelValues(duplicateMeta)))
		}); !ok {
			return
		}
	}
}

func TestReplicaLabelRemover_Modify(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	for _, tcase := range []struct {
		name                string
		input               map[ulid.ULID]*metadata.Meta
		expected            map[ulid.ULID]*metadata.Meta
		modified            float64
		replicaLabelRemover *ReplicaLabelRemover
	}{
		{
			name: "without replica labels",
			input: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			expected: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			modified:            0,
			replicaLabelRemover: NewReplicaLabelRemover(log.NewNopLogger(), []string{"replica", "rule_replica"}),
		},
		{
			name: "with replica labels",
			input: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "cluster1", "message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "cluster1", "rule_replica": "rule1", "message": "something"}}},
				ULID(4): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "cluster1", "rule_replica": "rule1"}}},
			},
			expected: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(4): {Thanos: metadata.Thanos{Labels: map[string]string{"replica": "deduped"}}},
			},
			modified:            5.0,
			replicaLabelRemover: NewReplicaLabelRemover(log.NewNopLogger(), []string{"replica", "rule_replica"}),
		},
		{
			name: "no replica label specified in the ReplicaLabelRemover",
			input: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			expected: map[ulid.ULID]*metadata.Meta{
				ULID(1): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(2): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something"}}},
				ULID(3): {Thanos: metadata.Thanos{Labels: map[string]string{"message": "something1"}}},
			},
			modified:            0,
			replicaLabelRemover: NewReplicaLabelRemover(log.NewNopLogger(), []string{}),
		},
	} {
		m := newTestFetcherMetrics()
		testutil.Ok(t, tcase.replicaLabelRemover.Filter(ctx, tcase.input, nil, m.Modified))

		testutil.Equals(t, tcase.modified, promtest.ToFloat64(m.Modified.WithLabelValues(replicaRemovedMeta)))
		testutil.Equals(t, tcase.expected, tcase.input)
	}
}

func compareSliceWithMapKeys(tb testing.TB, m map[ulid.ULID]*metadata.Meta, s []ulid.ULID) {
	_, file, line, _ := runtime.Caller(1)
	matching := len(m) == len(s)

	for _, val := range s {
		if m[val] == nil {
			matching = false
			break
		}
	}

	if !matching {
		var mapKeys []ulid.ULID
		for id := range m {
			mapKeys = append(mapKeys, id)
		}
		fmt.Printf("\033[31m%s:%d:\n\n\texp keys: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, mapKeys, s)
		tb.FailNow()
	}
}

type ulidBuilder struct {
	entropy *rand.Rand

	created []ulid.ULID
}

func (u *ulidBuilder) ULID(t time.Time) ulid.ULID {
	if u.entropy == nil {
		source := rand.NewSource(1234)
		u.entropy = rand.New(source)
	}

	id := ulid.MustNew(ulid.Timestamp(t), u.entropy)
	u.created = append(u.created, id)
	return id
}

func TestConsistencyDelayMetaFilter_Filter_0(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	u := &ulidBuilder{}
	now := time.Now()

	input := map[ulid.ULID]*metadata.Meta{
		// Fresh blocks.
		u.ULID(now):                       {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},

		// For now non-delay delete sources, should be ignored by consistency delay.
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-1 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},

		// 29m.
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},

		// For now non-delay delete sources, should be ignored by consistency delay.
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-29 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},

		// 30m.
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-30 * time.Minute)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},

		// 30m+.
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.SidecarSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.ReceiveSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.RulerSource}},
		u.ULID(now):                      {Thanos: metadata.Thanos{UploadTime: time.Now().Add(-20 * time.Hour), Source: metadata.RulerSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.BucketRepairSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.CompactorSource}},
		u.ULID(now.Add(-20 * time.Hour)): {Thanos: metadata.Thanos{Source: metadata.CompactorRepairSource}},
	}

	t.Run("consistency 0 (turned off)", func(t *testing.T) {
		m := newTestFetcherMetrics()
		expected := map[ulid.ULID]*metadata.Meta{}
		// Copy all.
		for _, id := range u.created {
			expected[id] = input[id]
		}

		reg := prometheus.NewRegistry()
		f := NewConsistencyDelayMetaFilter(nil, 0*time.Second, reg)
		testutil.Equals(t, map[string]float64{"consistency_delay_seconds{}": 0.0}, extprom.CurrentGaugeValuesFor(t, reg, "consistency_delay_seconds"))

		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
		testutil.Equals(t, 0.0, promtest.ToFloat64(m.Synced.WithLabelValues(tooFreshMeta)))
		testutil.Equals(t, expected, input)
	})

	t.Run("consistency 30m.", func(t *testing.T) {
		m := newTestFetcherMetrics()
		expected := map[ulid.ULID]*metadata.Meta{}
		// Only certain sources and those with 30m or more age go through.
		for i, id := range u.created {
			// Younger than 30m.
			if i < 13 {
				if input[id].Thanos.Source != metadata.BucketRepairSource &&
					input[id].Thanos.Source != metadata.CompactorSource &&
					input[id].Thanos.Source != metadata.CompactorRepairSource {
					continue
				}
			}
			expected[id] = input[id]
		}

		reg := prometheus.NewRegistry()
		f := NewConsistencyDelayMetaFilter(nil, 30*time.Minute, reg)
		testutil.Equals(t, map[string]float64{"consistency_delay_seconds{}": (30 * time.Minute).Seconds()}, extprom.CurrentGaugeValuesFor(t, reg, "consistency_delay_seconds"))

		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
		testutil.Equals(t, float64(len(u.created)-len(expected)), promtest.ToFloat64(m.Synced.WithLabelValues(tooFreshMeta)))
		testutil.Equals(t, expected, input)
	})
}

func TestIgnoreDeletionMarkFilter_Filter(t *testing.T) {
	objtesting.ForeachStore(t, func(t *testing.T, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		now := time.Now()
		f := NewIgnoreDeletionMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 48*time.Hour, 32)

		shouldFetch := &metadata.DeletionMark{
			ID:           ULID(1),
			DeletionTime: now.Add(-15 * time.Hour).Unix(),
			Version:      1,
		}

		shouldIgnore := &metadata.DeletionMark{
			ID:           ULID(2),
			DeletionTime: now.Add(-60 * time.Hour).Unix(),
			Version:      1,
		}

		var buf bytes.Buffer
		testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldFetch))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldFetch.ID.String(), metadata.DeletionMarkFilename), &buf))

		testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldIgnore))
		testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnore.ID.String(), metadata.DeletionMarkFilename), &buf))

		testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(3).String(), metadata.DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))

		input := map[ulid.ULID]*metadata.Meta{
			ULID(1): {},
			ULID(2): {},
			ULID(3): {},
			ULID(4): {},
		}

		expected := map[ulid.ULID]*metadata.Meta{
			ULID(1): {},
			ULID(3): {},
			ULID(4): {},
		}

		m := newTestFetcherMetrics()
		testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
		testutil.Equals(t, 1.0, promtest.ToFloat64(m.Synced.WithLabelValues(MarkedForDeletionMeta)))
		testutil.Equals(t, expected, input)
	})
}

func BenchmarkDeduplicateFilter_Filter(b *testing.B) {

	var (
		reg   prometheus.Registerer
		count uint64
		cases []map[ulid.ULID]*metadata.Meta
	)

	dedupFilter := NewDeduplicateFilter(1)
	synced := extprom.NewTxGaugeVec(reg, prometheus.GaugeOpts{}, []string{"state"})

	for blocksNum := 10; blocksNum <= 10000; blocksNum *= 10 {

		var ctx context.Context
		// blocksNum number of blocks with all of them unique ULID and unique 100 sources.
		cases = append(cases, make(map[ulid.ULID]*metadata.Meta, blocksNum))
		for i := 0; i < blocksNum; i++ {

			id := ulid.MustNew(count, nil)
			count++

			cases[0][id] = &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID: id,
				},
			}

			for range 100 {
				cases[0][id].Compaction.Sources = append(cases[0][id].Compaction.Sources, ulid.MustNew(count, nil))
				count++
			}
		}

		// Case for running 3x resolution as they can be run concurrently.
		// blocksNum number of blocks. all of them with unique ULID and unique 100 cases.
		cases = append(cases, make(map[ulid.ULID]*metadata.Meta, 3*blocksNum))

		for i := 0; i < blocksNum; i++ {
			for _, res := range []int64{0, 5 * 60 * 1000, 60 * 60 * 1000} {

				id := ulid.MustNew(count, nil)
				count++
				cases[1][id] = &metadata.Meta{
					BlockMeta: tsdb.BlockMeta{
						ULID: id,
					},
					Thanos: metadata.Thanos{
						Downsample: metadata.ThanosDownsample{Resolution: res},
					},
				}
				for range 100 {
					cases[1][id].Compaction.Sources = append(cases[1][id].Compaction.Sources, ulid.MustNew(count, nil))
					count++
				}

			}
		}

		b.Run(fmt.Sprintf("Block-%d", blocksNum), func(b *testing.B) {
			for _, tcase := range cases {
				b.ResetTimer()
				b.Run("", func(b *testing.B) {
					for n := 0; n <= b.N; n++ {
						_ = dedupFilter.Filter(ctx, tcase, synced, nil)
						testutil.Equals(b, 0, len(dedupFilter.DuplicateIDs()))
					}
				})
			}
		})
	}
}

func Test_ParseRelabelConfig(t *testing.T) {
	_, err := ParseRelabelConfig([]byte(`
    - action: drop
      regex: "A"
      source_labels:
      - cluster
    `), SelectorSupportedRelabelActions)
	testutil.Ok(t, err)

	_, err = ParseRelabelConfig([]byte(`
    - action: labelmap
      regex: "A"
    `), SelectorSupportedRelabelActions)
	testutil.NotOk(t, err)
	testutil.Equals(t, "unsupported relabel action: labelmap", err.Error())
}

func TestDeletionMarkFilter_HoldsOntoMarks(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	now := time.Now()
	f := NewIgnoreDeletionMarkFilter(log.NewNopLogger(), objstore.WithNoopInstr(bkt), 48*time.Hour, 32)

	shouldFetch := &metadata.DeletionMark{
		ID:           ULID(1),
		DeletionTime: now.Add(-15 * time.Hour).Unix(),
		Version:      1,
	}

	shouldIgnore := &metadata.DeletionMark{
		ID:           ULID(2),
		DeletionTime: now.Add(-60 * time.Hour).Unix(),
		Version:      1,
	}

	var buf bytes.Buffer
	testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldFetch))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldFetch.ID.String(), metadata.DeletionMarkFilename), &buf))

	buf.Truncate(0)

	md := &metadata.Meta{
		Thanos: metadata.Thanos{
			Version: 1,
		},
	}
	testutil.Ok(t, json.NewEncoder(&buf).Encode(md))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldFetch.ID.String(), "meta.json"), &buf))

	testutil.Ok(t, json.NewEncoder(&buf).Encode(&shouldIgnore))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(shouldIgnore.ID.String(), metadata.DeletionMarkFilename), &buf))

	testutil.Ok(t, bkt.Upload(ctx, path.Join(ULID(3).String(), metadata.DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))

	input := map[ulid.ULID]*metadata.Meta{
		ULID(1): {},
		ULID(2): {},
		ULID(3): {},
		ULID(4): {},
	}

	expected := map[ulid.ULID]*metadata.Meta{
		ULID(1): {},
		ULID(3): {},
		ULID(4): {},
	}

	m := newTestFetcherMetrics()
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))
	testutil.Equals(t, 1.0, promtest.ToFloat64(m.Synced.WithLabelValues(MarkedForDeletionMeta)))
	testutil.Equals(t, expected, input)

	testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))

	testutil.Ok(t, bkt.Delete(ctx, path.Join(shouldFetch.ID.String(), metadata.DeletionMarkFilename)))
	input = map[ulid.ULID]*metadata.Meta{
		ULID(1): {},
		ULID(2): {},
		ULID(3): {},
		ULID(4): {},
	}
	testutil.Ok(t, f.Filter(ctx, input, m.Synced, nil))

	testutil.Equals(t, 2, len(f.DeletionMarkBlocks()))
}

func TestRecursiveLister_MetaJsonOrderIsIrrelevant(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	blockID := ULID(1)

	// Upload files in the order a bucket impl would return them (alphabetical):
	// 1. chunks/000001 ("c" < "m")
	// 2. index ("i" < "m")
	// 3. meta.json
	// 4. no-compact-mark.json ("n" > "m") ← This comes AFTER meta.json
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), "chunks", "000001"), bytes.NewBuffer([]byte("chunks"))))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), "index"), bytes.NewBuffer([]byte("index"))))

	var meta metadata.Meta
	meta.Version = 1
	meta.ULID = blockID
	var buf bytes.Buffer
	testutil.Ok(t, json.NewEncoder(&buf).Encode(&meta))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), MetaFilename), &buf))
	testutil.Ok(t, bkt.Upload(ctx, path.Join(blockID.String(), metadata.NoCompactMarkFilename), bytes.NewBuffer([]byte("{}"))))

	// Create a RecursiveLister
	logger := log.NewNopLogger()
	insBkt := objstore.WithNoopInstr(bkt)
	lister := NewRecursiveLister(logger, insBkt)

	// Get active and partial blocks
	activeBlocksCh := make(chan ActiveBlockFetchData, 10)
	partialBlocks, err := lister.GetActiveAndPartialBlockIDs(ctx, activeBlocksCh)
	testutil.Ok(t, err)
	close(activeBlocksCh)

	// Drain the channel
	var activeBlocks []ulid.ULID
	for block := range activeBlocksCh {
		activeBlocks = append(activeBlocks, block.ULID)
	}

	testutil.Equals(t, 1, len(activeBlocks))
	testutil.Equals(t, blockID, activeBlocks[0])
	isPartial := partialBlocks[blockID]
	if isPartial {
		t.Errorf("Block %s has meta.json but was incorrectly marked as partial", blockID)
	}
}

func TestIgnoreConvertedBlocksMetaFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := log.NewNopLogger()

	t.Run("filters converted blocks", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		insBkt := objstore.WithNoopInstr(bkt)

		filter := &IgnoreParquetConvertedBlocksFilter{
			bkt:         insBkt,
			concurrency: 2,
			logger:      logger,
		}

		metaPbContent := encodeParquetMetadata([]ulid.ULID{ULID(1), ULID(2)})

		testutil.Ok(t, bkt.Upload(ctx, "parquet-block-1/meta.pb", bytes.NewBuffer(metaPbContent)))

		metaPbContent2 := encodeParquetMetadata([]ulid.ULID{ULID(3)})
		testutil.Ok(t, bkt.Upload(ctx, "another-location/subdir/meta.pb", bytes.NewBuffer(metaPbContent2)))

		input := map[ulid.ULID]*metadata.Meta{
			ULID(1): {},
			ULID(2): {},
			ULID(3): {},
			ULID(4): {},
			ULID(5): {},
		}

		expected := map[ulid.ULID]*metadata.Meta{
			ULID(4): {},
			ULID(5): {},
		}

		m := newTestFetcherMetrics()
		testutil.Ok(t, filter.Filter(ctx, input, m.Synced, nil))

		testutil.Equals(t, expected, input)
		testutil.Equals(t, 3.0, promtest.ToFloat64(m.Synced.WithLabelValues("parquet-converted")))
	})

	t.Run("nil bucket does nothing", func(t *testing.T) {
		filter := &IgnoreParquetConvertedBlocksFilter{
			bkt:         nil,
			concurrency: 2,
			logger:      logger,
		}

		input := map[ulid.ULID]*metadata.Meta{
			ULID(1): {},
			ULID(2): {},
			ULID(3): {},
		}

		expected := map[ulid.ULID]*metadata.Meta{
			ULID(1): {},
			ULID(2): {},
			ULID(3): {},
		}

		m := newTestFetcherMetrics()
		testutil.Ok(t, filter.Filter(ctx, input, m.Synced, nil))

		testutil.Equals(t, expected, input)
		testutil.Equals(t, 0.0, promtest.ToFloat64(m.Synced.WithLabelValues("parquet-converted")))
	})
}

func encodeParquetMetadata(convertedBlockIDs []ulid.ULID) []byte {
	m := mp.Get()

	mm := m.MessageMarshaler()
	for _, id := range convertedBlockIDs {
		mm.AppendString(6, id.String())
	}
	defer mp.Put(m)

	return m.Marshal(nil)
}

var mp easyproto.MarshalerPool
