// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package compactv2

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/logutil"
)

func TestCompactor_WriteSeries_e2e(t *testing.T) {
	if testing.
		Short() {
		t.Skip("too slow for testing.Short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := log.NewLogfmtLogger(os.Stderr)
	for _, tcase := range []struct {
		name string

		input     [][]seriesSamples
		modifiers []Modifier
		dryRun    bool

		expected        []seriesSamples
		expectedErr     error
		expectedStats   tsdb.BlockStats
		expectedChanges string
	}{
		{
			name:        "empty block",
			expectedErr: errors.New("cannot write from no readers"),
		},
		{
			name: "1 blocks no modify",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedStats: tsdb.BlockStats{
				NumSamples: 18,
				NumSeries:  3,
				NumChunks:  4,
			},
		},
		{
			name: "2 blocks compact no modify",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}}},
				},
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{10, 12}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "4"),
						chunks: [][]sample{{{10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 12}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "4"),
					chunks: [][]sample{{{10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedStats: tsdb.BlockStats{
				NumSamples: 21,
				NumSeries:  4,
				NumChunks:  7,
			},
		},
		{
			name: "1 blocks + delete modifier, empty deletion request",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier()},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedStats: tsdb.BlockStats{
				NumSamples: 18,
				NumSeries:  3,
				NumChunks:  4,
			},
		},
		{
			name: "1 blocks + delete modifier, deletion request no deleting anything",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "0")},
					Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: math.MaxInt64}},
				}, metadata.DeletionRequest{
					Matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")},
					Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: -1}},
				})},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedStats: tsdb.BlockStats{
				NumSamples: 18,
				NumSeries:  3,
				NumChunks:  4,
			},
		},
		{
			name: "1 blocks + delete modifier, deletion request no deleting anything - by specifying no intervals.",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "0")},
				}, metadata.DeletionRequest{
					Matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")},
					Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: -1}},
				})},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedStats: tsdb.BlockStats{
				NumSamples: 18,
				NumSeries:  3,
				NumChunks:  4,
			},
		},
		{
			name: "1 blocks + delete modifier, delete second series",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "2")},
				}, metadata.DeletionRequest{
					Matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")},
					Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: -1}},
				})},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedChanges: "Deleted {a=\"2\"} [{0 20}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 12,
				NumSeries:  2,
				NumChunks:  2,
			},
		},
		{
			name: "1 blocks + delete modifier, delete second series and part of first 3rd",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "2")},
				}, metadata.DeletionRequest{
					Matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "1")},
					Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: -1}},
				}, metadata.DeletionRequest{
					Matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "a", "3")},
					Intervals: tombstones.Intervals{{Mint: 10, Maxt: 11}},
				})},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {20, 20}}}},
			},
			expectedChanges: "Deleted {a=\"2\"} [{0 20}]\nDeleted {a=\"3\"} [{10 11}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 10,
				NumSeries:  2,
				NumChunks:  2,
			},
		},
		{
			name: "1 blocks + delete modifier, intervals delete all samples of a chunk without covering it",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}}, {{30, 30}, {40, 40}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{10, 10}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", "1|2")},
					Intervals: tombstones.Intervals{{Mint: 9, Maxt: 12}, {Mint: 19, Maxt: 21}, {Mint: 40, Maxt: 40}},
				})},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{30, 30}}}},
			},
			expectedChanges: "Deleted {a=\"1\"} [{10 12} {19 20} {40 40}]\nDeleted {a=\"2\"} [{10 12} {19 20}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 4,
				NumSeries:  1,
				NumChunks:  2,
			},
		},
		{
			name: "1 blocks + delete modifier, deletion request contains multiple matchers, delete second series",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1", "b", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "1", "b", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
						labels.MustNewMatcher(labels.MatchEqual, "b", "2"),
					},
				})},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1", "b", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedChanges: "Deleted {a=\"1\", b=\"2\"} [{0 20}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 12,
				NumSeries:  2,
				NumChunks:  2,
			},
		},
		{
			name: "1 blocks + delete modifier. For deletion request, full match is required. Delete the first two series",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1", "b", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "1", "b", "2", "foo", "bar"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("b", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("c", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
						labels.MustNewMatcher(labels.MatchEqual, "b", "2"),
					},
				})},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("b", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("c", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 12}, {11, 11}, {20, 20}}}},
			},
			expectedChanges: "Deleted {a=\"1\", b=\"2\"} [{0 20}]\nDeleted {a=\"1\", b=\"2\", foo=\"bar\"} [{0 20}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 18,
				NumSeries:  3,
				NumChunks:  3,
			},
		},
		{
			name: "1 blocks + delete modifier. Deletion request contains non-equal matchers.",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2", "foo", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2", "foo", "bar"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "3", "foo", "baz"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("foo", "bat"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},

					// Label a is present but with an empty value.
					{lset: labels.FromMap(map[string]string{"a": "", "foo": "bat"}),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
					// Series with unrelated labels.
					{lset: labels.FromStrings("c", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithDeletionModifier(
				metadata.DeletionRequest{
					Matchers: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchNotEqual, "a", "1"),
						labels.MustNewMatcher(labels.MatchRegexp, "foo", "^ba.$"),
					},
				})},
			expected: []seriesSamples{
				{lset: labels.FromMap(map[string]string{"a": "", "foo": "bat"}),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("a", "2", "foo", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("c", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
				{lset: labels.FromStrings("foo", "bat"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 11}, {11, 11}, {20, 20}}}},
			},
			expectedChanges: "Deleted {a=\"2\", foo=\"bar\"} [{0 20}]\nDeleted {a=\"3\", foo=\"baz\"} [{0 20}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 36,
				NumSeries:  6,
				NumChunks:  12,
			},
		},
		{
			name: "1 block + relabel modifier, two chunks from the same series are merged into one larger chunk",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}}}},
				},
			},
			// Not used in this test case.
			modifiers: []Modifier{WithRelabelModifier(
				&relabel.Config{
					Action:               relabel.Drop,
					Regex:                relabel.MustNewRegexp("no-match"),
					SourceLabels:         model.LabelNames{"a"},
					NameValidationScheme: model.UTF8Validation,
				},
			)},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
			},
			expectedStats: tsdb.BlockStats{
				NumSamples: 6,
				NumSeries:  1,
				NumChunks:  1,
			},
		},
		{
			name: "1 block + relabel modifier, delete first series",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
					{lset: labels.FromStrings("a", "3"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 13}, {11, 11}, {20, 20}}}},
				},
			},
			modifiers: []Modifier{WithRelabelModifier(
				&relabel.Config{
					Action:               relabel.Drop,
					Regex:                relabel.MustNewRegexp("1"),
					SourceLabels:         model.LabelNames{"a"},
					NameValidationScheme: model.UTF8Validation,
				},
			)},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "2"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 13}, {11, 11}, {20, 20}}}},
			},
			expectedChanges: "Deleted {a=\"1\"} [{0 20}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 13,
				NumSeries:  2,
				NumChunks:  2,
			},
		},
		{
			name: "1 block + relabel modifier, series reordered",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, -1}, {2, -2}, {10, -10}, {11, -11}, {20, -20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
				},
			},
			// {a="1"} will be relabeled to {a="3"} while {a="2"} will be relabeled to {a="0"}.
			modifiers: []Modifier{WithRelabelModifier(
				&relabel.Config{
					Action:               relabel.Replace,
					Regex:                relabel.MustNewRegexp("1"),
					SourceLabels:         model.LabelNames{"a"},
					TargetLabel:          "a",
					Replacement:          "3",
					NameValidationScheme: model.UTF8Validation,
				},
				&relabel.Config{
					Action:               relabel.Replace,
					Regex:                relabel.MustNewRegexp("2"),
					SourceLabels:         model.LabelNames{"a"},
					TargetLabel:          "a",
					Replacement:          "0",
					NameValidationScheme: model.UTF8Validation,
				},
			)},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "0"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
				{lset: labels.FromStrings("a", "3"),
					chunks: [][]sample{{{0, 0}, {1, -1}, {2, -2}, {10, -10}, {11, -11}, {20, -20}}}},
			},
			expectedChanges: "Relabelled {a=\"1\"} {a=\"3\"}\nRelabelled {a=\"2\"} {a=\"0\"}\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 13,
				NumSeries:  2,
				NumChunks:  2,
			},
		},
		{
			name: "1 block + relabel modifier, series deleted because of no labels left after relabel",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
				},
				{
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
				},
			},
			// Drop all label name "a".
			modifiers: []Modifier{WithRelabelModifier(
				&relabel.Config{
					Action:               relabel.LabelDrop,
					Regex:                relabel.MustNewRegexp("a"),
					NameValidationScheme: model.UTF8Validation,
				},
			)},
			expected:        nil,
			expectedChanges: "Deleted {a=\"1\"} [{0 25}]\nDeleted {a=\"2\"} [{0 25}]\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 0,
				NumSeries:  0,
				NumChunks:  0,
			},
		},
		{
			name: "1 block + relabel modifier, series 1 is deleted because of no labels left after relabel",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
				},
				{
					{lset: labels.FromStrings("a", "2", "b", "1"),
						chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
				},
			},
			// Drop all label name "a".
			modifiers: []Modifier{WithRelabelModifier(
				&relabel.Config{
					Action:               relabel.LabelDrop,
					Regex:                relabel.MustNewRegexp("a"),
					NameValidationScheme: model.UTF8Validation,
				},
			)},
			expected: []seriesSamples{
				{lset: labels.FromStrings("b", "1"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
			},
			expectedChanges: "Deleted {a=\"1\"} [{0 25}]\nRelabelled {a=\"2\", b=\"1\"} {b=\"1\"}\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 7,
				NumSeries:  1,
				NumChunks:  1,
			},
		},
		{
			name: "1 block + relabel modifier, series merged after relabeling",
			input: [][]seriesSamples{
				{
					{lset: labels.FromStrings("a", "1"),
						chunks: [][]sample{{{1, 1}, {2, 2}, {10, 10}, {20, 20}}}},
					{lset: labels.FromStrings("a", "2"),
						chunks: [][]sample{{{0, 0}, {2, 2}, {3, 3}}, {{4, 4}, {11, 11}, {20, 20}, {25, 25}}}},
				},
			},
			// Replace values of label name "a" with "0".
			modifiers: []Modifier{WithRelabelModifier(
				&relabel.Config{
					Action:               relabel.Replace,
					Regex:                relabel.MustNewRegexp("1|2"),
					SourceLabels:         model.LabelNames{"a"},
					TargetLabel:          "a",
					Replacement:          "0",
					NameValidationScheme: model.UTF8Validation,
				},
			)},
			expected: []seriesSamples{
				{lset: labels.FromStrings("a", "0"),
					chunks: [][]sample{{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {10, 10}, {11, 11}, {20, 20}, {25, 25}}}},
			},
			expectedChanges: "Relabelled {a=\"1\"} {a=\"0\"}\nRelabelled {a=\"2\"} {a=\"0\"}\n",
			expectedStats: tsdb.BlockStats{
				NumSamples: 9,
				NumSeries:  1,
				NumChunks:  1,
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			chunkPool := chunkenc.NewPool()

			changes := bytes.Buffer{}
			changeLog := &changeLog{w: &changes}
			var s *Compactor
			if tcase.dryRun {
				s = NewDryRun(tmpDir, logger, changeLog, chunkPool)
			} else {
				s = New(tmpDir, logger, changeLog, chunkPool)
			}

			var series int
			var blocks []block.Reader
			for _, b := range tcase.input {
				series += len(b)
				id := ulid.MustNew(uint64(len(blocks)+1), nil)
				bdir := filepath.Join(tmpDir, id.String())
				testutil.Ok(t, os.MkdirAll(bdir, os.ModePerm))
				testutil.Ok(t, createBlockSeries(bdir, b))
				// Meta does not matter, but let's create for OpenBlock to work.
				testutil.Ok(t, metadata.Meta{BlockMeta: tsdb.BlockMeta{Version: 1, ULID: id}}.WriteToDir(logger, bdir))
				block, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), bdir, chunkPool, nil)
				testutil.Ok(t, err)
				blocks = append(blocks, block)
			}

			id := ulid.MustNew(uint64(len(blocks)+1), nil)
			d, err := block.NewDiskWriter(ctx, logger, filepath.Join(tmpDir, id.String()))
			testutil.Ok(t, err)
			p := NewProgressLogger(logger, series)
			if tcase.expectedErr != nil {
				err := s.WriteSeries(ctx, blocks, d, p, tcase.modifiers...)
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, s.WriteSeries(ctx, blocks, d, p, tcase.modifiers...))

			testutil.Ok(t, os.MkdirAll(filepath.Join(tmpDir, id.String()), os.ModePerm))
			stats, err := d.Flush()
			testutil.Ok(t, err)

			testutil.Equals(t, tcase.expectedChanges, changes.String())
			testutil.Equals(t, tcase.expectedStats, stats)
			testutil.Equals(t, tcase.expected, readBlockSeries(t, filepath.Join(tmpDir, id.String())))
		})
	}
}

type sample struct {
	t int64
	v float64
}

type seriesSamples struct {
	lset   labels.Labels
	chunks [][]sample
}

func readBlockSeries(t *testing.T, bDir string) []seriesSamples {
	ctx := context.Background()

	indexr, err := index.NewFileReader(filepath.Join(bDir, block.IndexFilename), index.DecodePostingsRaw)
	testutil.Ok(t, err)
	defer indexr.Close()

	chunkr, err := chunks.NewDirReader(filepath.Join(bDir, block.ChunksDirname), nil)
	testutil.Ok(t, err)
	defer chunkr.Close()

	k, v := index.AllPostingsKey()
	all, err := indexr.Postings(ctx, k, v)
	testutil.Ok(t, err)
	all = indexr.SortedPostings(all)

	var builder labels.ScratchBuilder
	var series []seriesSamples
	var chks []chunks.Meta
	for all.Next() {
		s := seriesSamples{}
		testutil.Ok(t, indexr.Series(all.At(), &builder, &chks))
		s.lset = builder.Labels()

		for _, c := range chks {
			// Ignore iterable as it should be nil.
			c.Chunk, _, err = chunkr.ChunkOrIterable(c)
			testutil.Ok(t, err)

			var chk []sample
			iter := c.Chunk.Iterator(nil)
			for iter.Next() != chunkenc.ValNone {
				sa := sample{}
				sa.t, sa.v = iter.At()
				chk = append(chk, sa)
			}
			testutil.Ok(t, iter.Err())
			s.chunks = append(s.chunks, chk)
		}
		series = append(series, s)
	}
	testutil.Ok(t, all.Err())
	return series
}

func createBlockSeries(bDir string, inputSeries []seriesSamples) (err error) {
	d, err := block.NewDiskWriter(context.TODO(), log.NewNopLogger(), bDir)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = d.Flush()
			_ = os.RemoveAll(bDir)
		}
	}()

	sort.Slice(inputSeries, func(i, j int) bool {
		return labels.Compare(inputSeries[i].lset, inputSeries[j].lset) < 0
	})

	// Gather symbols.
	symbols := map[string]struct{}{}
	for _, input := range inputSeries {
		input.lset.Range(func(l labels.Label) {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		})
	}

	symbolsSlice := make([]string, 0, len(symbols))
	for s := range symbols {
		symbolsSlice = append(symbolsSlice, s)
	}
	sort.Strings(symbolsSlice)
	for _, s := range symbolsSlice {
		if err := d.AddSymbol(s); err != nil {
			return err
		}
	}
	var ref storage.SeriesRef
	for _, input := range inputSeries {
		var chks []chunks.Meta
		for _, chk := range input.chunks {
			x := chunkenc.NewXORChunk()
			a, err := x.Appender()
			if err != nil {
				return err
			}
			for _, sa := range chk {
				a.Append(sa.t, sa.v)
			}
			chks = append(chks, chunks.Meta{Chunk: x, MinTime: chk[0].t, MaxTime: chk[len(chk)-1].t})
		}
		if err := d.WriteChunks(chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		if err := d.AddSeries(ref, input.lset, chks...); err != nil {
			return errors.Wrap(err, "add series")
		}
		ref++
	}

	if _, err = d.Flush(); err != nil {
		return errors.Wrap(err, "flush")
	}
	return nil
}

func TestCompactor_WriteSeries_DownsampledBlock(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	tmpDir := t.TempDir()

	// Raw block with 3 series and a 10s interval over 2h, so that downsampling to 5m produces aggregated chunks.
	var input []seriesSamples
	for _, v := range []string{"1", "2", "3"} {
		var smpls []sample
		for ts := int64(0); ts < time.Hour.Milliseconds()*2; ts += 10 * 1000 {
			smpls = append(smpls, sample{t: ts, v: float64(ts)})
		}
		input = append(input, seriesSamples{lset: labels.FromStrings("a", v), chunks: [][]sample{smpls}})
	}
	rawID := ulid.MustNew(1, nil)
	rawDir := filepath.Join(tmpDir, rawID.String())
	testutil.Ok(t, os.MkdirAll(rawDir, os.ModePerm))
	testutil.Ok(t, createBlockSeries(rawDir, input))
	rawMeta := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{Version: 1, ULID: rawID, MinTime: 0, MaxTime: time.Hour.Milliseconds() * 2},
		Thanos:    metadata.Thanos{Labels: map[string]string{"ext": "1"}},
	}
	testutil.Ok(t, rawMeta.WriteToDir(logger, rawDir))
	rawBlock, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), rawDir, chunkenc.NewPool(), nil)
	testutil.Ok(t, err)
	t.Cleanup(func() { testutil.Ok(t, rawBlock.Close()) })

	dsID, err := downsample.Downsample(ctx, logger, &rawMeta, rawBlock, tmpDir, downsample.ResLevel1)
	testutil.Ok(t, err)
	dsDir := filepath.Join(tmpDir, dsID.String())

	deleteA2 := WithDeletionModifier(metadata.DeletionRequest{
		Matchers: metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "a", "2")},
	})

	t.Run("whole series deletion keeps the other chunks byte-identical", func(t *testing.T) {
		chunkPool := downsample.NewPool()
		b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), dsDir, chunkPool, nil)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, b.Close()) }()

		outDir := filepath.Join(tmpDir, ulid.MustNew(3, nil).String())
		d, err := block.NewDiskWriter(ctx, logger, outDir)
		testutil.Ok(t, err)
		changes := bytes.Buffer{}
		comp := New(tmpDir, logger, &changeLog{w: &changes}, chunkPool)
		testutil.Ok(t, comp.WriteSeries(ctx, []block.Reader{b}, d, NewProgressLogger(logger, 3), deleteA2))
		testutil.Ok(t, os.MkdirAll(outDir, os.ModePerm))
		stats, err := d.Flush()
		testutil.Ok(t, err)
		testutil.Equals(t, uint64(2), stats.NumSeries)

		src := readBlockChunkBytes(t, dsDir)
		testutil.Equals(t, 3, len(src))
		for _, chks := range src {
			for _, c := range chks {
				testutil.Equals(t, byte(downsample.ChunkEncAggr), c[0])
			}
		}
		delete(src, labels.FromStrings("a", "2").String())
		testutil.Equals(t, src, readBlockChunkBytes(t, outDir))
	})

	t.Run("undecodable chunks return an error instead of panicking", func(t *testing.T) {
		// The Prometheus pool cannot decode aggregated chunks.
		chunkPool := chunkenc.NewPool()
		b, err := tsdb.OpenBlock(logutil.GoKitLogToSlog(logger), dsDir, chunkPool, nil)
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, b.Close()) }()

		d, err := block.NewDiskWriter(ctx, logger, filepath.Join(tmpDir, ulid.MustNew(4, nil).String()))
		testutil.Ok(t, err)
		changes := bytes.Buffer{}
		comp := New(tmpDir, logger, &changeLog{w: &changes}, chunkPool)
		err = comp.WriteSeries(ctx, []block.Reader{b}, d, NewProgressLogger(logger, 3), deleteA2)
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), "cannot populate chunk"), "unexpected error: %v", err)
	})
}

// readBlockChunkBytes returns the raw encoding and bytes of every chunk, by series labels.
func readBlockChunkBytes(t *testing.T, bDir string) map[string][][]byte {
	ctx := context.Background()

	indexr, err := index.NewFileReader(filepath.Join(bDir, block.IndexFilename), index.DecodePostingsRaw)
	testutil.Ok(t, err)
	defer indexr.Close()

	chunkr, err := chunks.NewDirReader(filepath.Join(bDir, block.ChunksDirname), downsample.NewPool())
	testutil.Ok(t, err)
	defer chunkr.Close()

	k, v := index.AllPostingsKey()
	all, err := indexr.Postings(ctx, k, v)
	testutil.Ok(t, err)

	var builder labels.ScratchBuilder
	var chks []chunks.Meta
	res := map[string][][]byte{}
	for all.Next() {
		testutil.Ok(t, indexr.Series(all.At(), &builder, &chks))
		lset := builder.Labels().String()
		for _, c := range chks {
			chk, _, err := chunkr.ChunkOrIterable(c)
			testutil.Ok(t, err)
			res[lset] = append(res[lset], append([]byte{byte(chk.Encoding())}, chk.Bytes()...))
		}
	}
	testutil.Ok(t, all.Err())
	return res
}
