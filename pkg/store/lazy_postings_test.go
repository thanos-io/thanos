// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"path"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestKeysToFetchFromPostingGroups(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		pgs              []*postingGroup
		expectedLabels   []labels.Label
		expectedMatchers []*labels.Matcher
	}{
		{
			name: "empty group",
			pgs: []*postingGroup{
				{
					addKeys:    []string{},
					removeKeys: []string{},
				},
			},
			expectedLabels: []labels.Label{},
		},
		{
			name: "empty groups",
			pgs: []*postingGroup{
				{
					addKeys:    []string{},
					removeKeys: []string{},
				},
				{
					addKeys:    []string{},
					removeKeys: []string{},
				},
				{
					addKeys:    []string{},
					removeKeys: []string{},
				},
			},
			expectedLabels: []labels.Label{},
		},
		{
			name: "group with add keys",
			pgs: []*postingGroup{
				{
					name:       "test",
					addKeys:    []string{"foo", "bar"},
					removeKeys: []string{},
				},
			},
			expectedLabels: []labels.Label{{Name: "test", Value: "foo"}, {Name: "test", Value: "bar"}},
		},
		{
			name: "group with remove keys",
			pgs: []*postingGroup{
				{
					name:       "test",
					addKeys:    []string{},
					removeKeys: []string{"foo", "bar"},
				},
			},
			expectedLabels: []labels.Label{{Name: "test", Value: "foo"}, {Name: "test", Value: "bar"}},
		},
		{
			name: "group with both add and remove keys",
			pgs: []*postingGroup{
				{
					name:       "test",
					addKeys:    []string{"foo", "bar"},
					removeKeys: []string{"a", "b"},
				},
			},
			expectedLabels: []labels.Label{
				{Name: "test", Value: "foo"}, {Name: "test", Value: "bar"},
				{Name: "test", Value: "a"}, {Name: "test", Value: "b"},
			},
		},
		{
			name: "groups with both add keys",
			pgs: []*postingGroup{
				{
					name:    "test",
					addKeys: []string{"foo", "bar"},
				},
				{
					name:    "foo",
					addKeys: []string{"bar"},
				},
			},
			expectedLabels: []labels.Label{
				{Name: "test", Value: "foo"}, {Name: "test", Value: "bar"},
				{Name: "foo", Value: "bar"},
			},
		},
		{
			name: "groups with add and remove keys",
			pgs: []*postingGroup{
				{
					name:    "test",
					addKeys: []string{"foo", "bar"},
				},
				{
					name:       "foo",
					removeKeys: []string{"bar"},
				},
			},
			expectedLabels: []labels.Label{
				{Name: "test", Value: "foo"}, {Name: "test", Value: "bar"},
				{Name: "foo", Value: "bar"},
			},
		},
		{
			name: "lazy posting group with empty matchers",
			pgs: []*postingGroup{
				{
					name:     "test",
					addKeys:  []string{"foo", "bar"},
					matchers: []*labels.Matcher{},
					lazy:     true,
				},
			},
			expectedLabels:   []labels.Label{},
			expectedMatchers: []*labels.Matcher{},
		},
		{
			name: "lazy posting group",
			pgs: []*postingGroup{
				{
					name:     "test",
					addKeys:  []string{"foo", "bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
					lazy:     true,
				},
			},
			expectedLabels:   []labels.Label{},
			expectedMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
		},
		{
			name: "multiple lazy posting groups",
			pgs: []*postingGroup{
				{
					name:     "test",
					addKeys:  []string{"foo", "bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
					lazy:     true,
				},
				{
					name:     "job",
					addKeys:  []string{"prometheus"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus.*")},
					lazy:     true,
				},
			},
			expectedLabels: []labels.Label{},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus.*"),
			},
		},
		{
			name: "multiple non lazy and lazy posting groups",
			pgs: []*postingGroup{
				{
					name:     "test",
					addKeys:  []string{"foo", "bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
				},
				{
					name:     "test",
					addKeys:  []string{"foo", "bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
					lazy:     true,
				},
				{
					name:     "job",
					addKeys:  []string{"prometheus"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus.*")},
					lazy:     true,
				},
			},
			expectedLabels: []labels.Label{{Name: "test", Value: "foo"}, {Name: "test", Value: "bar"}},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus.*"),
			},
		},
		{
			name: "multiple non lazy and lazy posting groups with lazy posting groups in the middle",
			pgs: []*postingGroup{
				{
					name:     "test",
					addKeys:  []string{"foo", "bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")},
				},
				{
					name:     "cluster",
					addKeys:  []string{"bar"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "cluster", "bar")},
					lazy:     true,
				},
				{
					name:     "env",
					addKeys:  []string{"beta", "gamma", "prod"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "env", "beta|gamma|prod")},
					lazy:     true,
				},
				{
					name:     "job",
					addKeys:  []string{"prometheus"},
					matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus.*")},
				},
			},
			expectedLabels: []labels.Label{{Name: "test", Value: "foo"}, {Name: "test", Value: "bar"}, {Name: "job", Value: "prometheus"}},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "cluster", "bar"),
				labels.MustNewMatcher(labels.MatchRegexp, "env", "beta|gamma|prod"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, matchers := keysToFetchFromPostingGroups(tc.pgs)
			testutil.Equals(t, tc.expectedLabels, keys)
			testutil.Assert(t, len(tc.expectedMatchers) == len(matchers))
			for i := 0; i < len(tc.expectedMatchers); i++ {
				testutil.Equals(t, tc.expectedMatchers[i].String(), matchers[i].String())
			}
		})
	}
}

type mockIndexHeaderReader struct {
	postings map[string]map[string]index.Range
	err      error
}

func (h *mockIndexHeaderReader) Close() error { return nil }

func (h *mockIndexHeaderReader) IndexVersion() (int, error) { return 0, nil }

func (h *mockIndexHeaderReader) PostingsOffsets(name string, value ...string) ([]index.Range, error) {
	ranges := make([]index.Range, 0)
	if _, ok := h.postings[name]; !ok {
		return nil, nil
	}
	for _, val := range value {
		if rng, ok := h.postings[name][val]; ok {
			ranges = append(ranges, rng)
		} else {
			ranges = append(ranges, indexheader.NotFoundRange)
		}
	}
	return ranges, h.err
}

func (h *mockIndexHeaderReader) PostingsOffset(name string, value string) (index.Range, error) {
	return index.Range{}, nil
}

func (h *mockIndexHeaderReader) LookupSymbol(ctx context.Context, o uint32) (string, error) {
	return "", nil
}

func (h *mockIndexHeaderReader) LabelValues(name string) ([]string, error) { return nil, nil }

func (h *mockIndexHeaderReader) LabelNames() ([]string, error) { return nil, nil }

func TestOptimizePostingsFetchByDownloadedBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	bkt, err := filesystem.NewBucket(dir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

	logger := log.NewNopLogger()
	inputError := errors.New("random")
	blockID := ulid.MustNew(1, nil)
	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: blockID},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
	}
	for _, tc := range []struct {
		name                          string
		inputPostings                 map[string]map[string]index.Range
		inputError                    error
		postingGroups                 []*postingGroup
		seriesMaxSize                 int64
		seriesMatchRatio              float64
		postingGroupMaxKeySeriesRatio float64
		expectedPostingGroups         []*postingGroup
		expectedEmptyPosting          bool
		expectedError                 string
	}{
		{
			name: "empty posting group",
		},
		{
			name: "one posting group",
			postingGroups: []*postingGroup{
				{name: "foo"},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "foo"},
			},
		},
		{
			name: "posting offsets return error",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}},
			},
			inputError:       inputError,
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: nil,
			expectedError:         "postings offsets for foo: random",
		},
		{
			name:             "posting offsets empty with add keys, expect empty posting",
			inputPostings:    map[string]map[string]index.Range{},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: nil,
			expectedEmptyPosting:  true,
		},
		{
			name: "posting group label with add keys doesn't exist, return empty postings",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: nil,
			expectedEmptyPosting:  true,
		},
		{
			name: "posting group label with remove keys doesn't exist, noop",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", removeKeys: []string{"foo"}, addAll: true},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "bar", removeKeys: []string{"foo"}, cardinality: 0, addAll: true},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
			},
		},
		{
			name: "posting group label with add keys exist but no matching value, expect empty posting",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"baz": index.Range{Start: 8, End: 16}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: nil,
			expectedEmptyPosting:  true,
		},
		{
			name: "posting group label with remove keys exist but no matching value, noop",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"baz": index.Range{Start: 8, End: 16}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", removeKeys: []string{"foo"}, addAll: true},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "bar", removeKeys: []string{"foo"}, cardinality: 0, addAll: true},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
			},
		},
		{
			name: "posting group keys partial exist",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo", "buz"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "bar", addKeys: []string{"foo", "buz"}, cardinality: 1, existentKeys: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
			},
		},
		{
			name: "two posting groups with add keys, small postings and large series size",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "bar", addKeys: []string{"foo"}, cardinality: 1, existentKeys: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
			},
		},
		{
			name: "two posting groups with add keys, posting group not marked as lazy due to some add keys don't exist",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}},
			},
			postingGroupMaxKeySeriesRatio: 2,
			expectedPostingGroups: []*postingGroup{
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}, cardinality: 1, existentKeys: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
			},
		},
		{
			name: "two posting groups with add keys, first posting group not marked as lazy even though exceeding 2 keys due to we always mark first posting group as non lazy",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 108}},
				"bar": {"foo": index.Range{Start: 108, End: 116}, "bar": index.Range{Start: 116, End: 124}, "baz": index.Range{Start: 124, End: 132}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}},
			},
			postingGroupMaxKeySeriesRatio: 2,
			expectedPostingGroups: []*postingGroup{
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}, cardinality: 3, existentKeys: 3},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 26, existentKeys: 1},
			},
		},
		{
			name: "two posting groups with add keys, one posting group with too many keys not marked as lazy due to postingGroupMaxKeySeriesRatio not set",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}, "bar": index.Range{Start: 16, End: 24}, "baz": index.Range{Start: 24, End: 32}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}},
			},
			postingGroupMaxKeySeriesRatio: 0,
			expectedPostingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}, cardinality: 3, existentKeys: 3},
			},
		},
		{
			name: "two posting groups with add keys, one posting group marked as lazy due to exceeding postingGroupMaxKeySeriesRatio",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}, "bar": index.Range{Start: 16, End: 24}, "baz": index.Range{Start: 24, End: 32}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}},
			},
			postingGroupMaxKeySeriesRatio: 2,
			expectedPostingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}, cardinality: 3, existentKeys: 3, lazy: true},
			},
		},
		{
			name: "two posting groups with remove keys, minAddKeysToMarkLazy won't be applied",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}, "baz": index.Range{Start: 16, End: 24}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}},
				{addAll: true, name: "bar", removeKeys: []string{"baz", "foo"}},
			},
			expectedPostingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{addAll: true, name: "bar", removeKeys: []string{"baz", "foo"}, cardinality: 2, existentKeys: 2},
			},
		},
		{
			// This test case won't be optimized in real case because it is add all
			// so doesn't make sense to optimize postings fetching anyway.
			name: "two posting groups with remove keys, small postings and large series size",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}},
				{addAll: true, name: "bar", removeKeys: []string{"foo"}},
			},
			expectedPostingGroups: []*postingGroup{
				{addAll: true, name: "bar", removeKeys: []string{"foo"}, cardinality: 1, existentKeys: 1},
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
			},
		},
		{
			name: "one group with remove keys and another one with add keys. Always add the addKeys posting group to avoid fetching all postings",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 1000012}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 250000, existentKeys: 1},
			},
		},
		{
			name: "two posting groups with add keys, very small series size, making one posting group lazy",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 16}},
			},
			seriesMaxSize:    1,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "bar", addKeys: []string{"foo"}, cardinality: 1, existentKeys: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1, lazy: true},
			},
		},
		{
			name: "two posting groups with add keys, one small posting group and a very large posting group, large one become lazy",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 8}},
				"bar": {"foo": index.Range{Start: 8, End: 1000012}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 250000, existentKeys: 1, lazy: true},
			},
		},
		{
			name: "two posting groups with add keys, group not marked as lazy because of lower series match ratio",
			inputPostings: map[string]map[string]index.Range{
				"foo": {"bar": index.Range{End: 44}},
				"bar": {"foo": index.Range{Start: 44, End: 5052}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.1,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}, cardinality: 10, existentKeys: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 1251, existentKeys: 1, lazy: false},
			},
		},
		{
			name: "three posting groups with add keys, two small posting group and a very large posting group, large one become lazy",
			inputPostings: map[string]map[string]index.Range{
				"foo":     {"bar": index.Range{End: 8}},
				"bar":     {"foo": index.Range{Start: 8, End: 1000012}},
				"cluster": {"us": index.Range{Start: 1000012, End: 1000020}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
				{name: "cluster", addKeys: []string{"us"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "cluster", addKeys: []string{"us"}, cardinality: 1, existentKeys: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 250000, existentKeys: 1, lazy: true},
			},
		},
		{
			name: "three posting groups with add keys, middle posting group marked as lazy due to too many add keys",
			inputPostings: map[string]map[string]index.Range{
				"foo":     {"bar": index.Range{End: 8}},
				"bar":     {"bar": index.Range{Start: 8, End: 16}, "baz": index.Range{Start: 16, End: 24}, "foo": index.Range{Start: 24, End: 32}},
				"cluster": {"us": index.Range{Start: 32, End: 108}},
			},
			seriesMaxSize:                 1000,
			seriesMatchRatio:              0.5,
			postingGroupMaxKeySeriesRatio: 2,
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}},
				{name: "cluster", addKeys: []string{"us"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}, cardinality: 3, existentKeys: 3, lazy: true},
				{name: "cluster", addKeys: []string{"us"}, cardinality: 18, existentKeys: 1},
			},
		},
		{
			name: "three posting groups with add keys, bar not marked as lazy even though too many add keys due to first positive posting group sorted by cardinality",
			inputPostings: map[string]map[string]index.Range{
				"foo":     {"bar": index.Range{End: 8}},
				"bar":     {"bar": index.Range{Start: 8, End: 16}, "baz": index.Range{Start: 16, End: 24}, "foo": index.Range{Start: 24, End: 32}},
				"cluster": {"us": index.Range{Start: 32, End: 108}},
			},
			seriesMaxSize:                 1000,
			seriesMatchRatio:              0.5,
			postingGroupMaxKeySeriesRatio: 2,
			postingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}},
				{name: "cluster", addKeys: []string{"us"}},
			},
			expectedPostingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"bar", "baz", "foo"}, cardinality: 3, existentKeys: 3},
				{name: "cluster", addKeys: []string{"us"}, cardinality: 18, existentKeys: 1},
			},
		},
		{
			name: "three posting groups with either add or remove keys, two small posting group and a very large posting group, large one become lazy",
			inputPostings: map[string]map[string]index.Range{
				"foo":     {"bar": index.Range{End: 8}},
				"bar":     {"foo": index.Range{Start: 8, End: 1000012}},
				"cluster": {"us": index.Range{Start: 1000012, End: 1000020}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}},
				{addAll: true, name: "bar", removeKeys: []string{"foo"}},
				{name: "cluster", addKeys: []string{"us"}},
			},
			expectedPostingGroups: []*postingGroup{
				{name: "cluster", addKeys: []string{"us"}, cardinality: 1, existentKeys: 1},
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{addAll: true, name: "bar", removeKeys: []string{"foo"}, cardinality: 250000, existentKeys: 1, lazy: true},
			},
		},
		{
			name: "four posting groups with either add or remove keys, negative matcher group has the lowest cardinality, only the largest group is lazy",
			inputPostings: map[string]map[string]index.Range{
				"foo":     {"bar": index.Range{End: 8}},
				"bar":     {"foo": index.Range{Start: 8, End: 2012}},
				"baz":     {"foo": index.Range{Start: 2012, End: 4020}},
				"cluster": {"us": index.Range{Start: 4020, End: 1004024}},
			},
			seriesMaxSize:    1000,
			seriesMatchRatio: 0.5,
			postingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
				{name: "baz", addKeys: []string{"foo"}},
				{name: "cluster", addKeys: []string{"us"}},
			},
			expectedPostingGroups: []*postingGroup{
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1, existentKeys: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 500, existentKeys: 1},
				{name: "baz", addKeys: []string{"foo"}, cardinality: 501, existentKeys: 1},
				{name: "cluster", addKeys: []string{"us"}, cardinality: 250000, existentKeys: 1, lazy: true},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headerReader := &mockIndexHeaderReader{postings: tc.inputPostings, err: tc.inputError}
			registry := prometheus.NewRegistry()
			block, err := newBucketBlock(ctx, newBucketStoreMetrics(registry), meta, bkt, path.Join(dir, blockID.String()), nil, nil, headerReader, nil, nil, nil)
			testutil.Ok(t, err)
			ir := newBucketIndexReader(block, logger)
			dummyCounter := promauto.With(registry).NewCounter(prometheus.CounterOpts{Name: "test"})
			dummyCounterVec := promauto.With(registry).NewCounterVec(prometheus.CounterOpts{Name: "test_counter_vec"}, []string{"reason"})
			pgs, emptyPosting, err := optimizePostingsFetchByDownloadedBytes(ir, tc.postingGroups, tc.seriesMaxSize, tc.seriesMatchRatio, tc.postingGroupMaxKeySeriesRatio, dummyCounter, dummyCounterVec)
			if err != nil {
				testutil.Equals(t, tc.expectedError, err.Error())
				return
			}
			testutil.Equals(t, tc.expectedEmptyPosting, emptyPosting)
			testutil.Equals(t, tc.expectedPostingGroups, pgs)
			var c int64
			for _, pg := range pgs {
				if pg.lazy {
					c += pg.cardinality
				}
			}
			testutil.Equals(t, float64(4*c), promtest.ToFloat64(dummyCounter))
		})
	}
}

func TestMergeFetchedPostings(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name               string
		fetchedPostings    []index.Postings
		postingGroups      []*postingGroup
		expectedSeriesRefs []storage.SeriesRef
	}{
		{
			name: "empty fetched postings and posting groups",
		},
		{
			name:            "single posting group with 1 add key",
			fetchedPostings: []index.Postings{index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
			},
			expectedSeriesRefs: []storage.SeriesRef{1, 2, 3, 4, 5},
		},
		{
			name: "single posting group with multiple add keys, merge",
			fetchedPostings: []index.Postings{
				index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}),
				index.NewListPostings([]storage.SeriesRef{6, 7, 8, 9}),
			},
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar", "baz"}},
			},
			expectedSeriesRefs: []storage.SeriesRef{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name: "multiple posting groups with add key, intersect",
			fetchedPostings: []index.Postings{
				index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}),
				index.NewListPostings([]storage.SeriesRef{1, 2, 4}),
			},
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}},
			},
			expectedSeriesRefs: []storage.SeriesRef{1, 2, 4},
		},
		{
			name: "posting group with remove keys",
			fetchedPostings: []index.Postings{
				index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}),
				index.NewListPostings([]storage.SeriesRef{1, 2, 4}),
			},
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", removeKeys: []string{"foo"}, addAll: true},
			},
			expectedSeriesRefs: []storage.SeriesRef{3, 5},
		},
		{
			name: "multiple posting groups with add key and ignore lazy posting groups",
			fetchedPostings: []index.Postings{
				index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}),
			},
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}, lazy: true},
				{name: "baz", addKeys: []string{"foo"}, lazy: true},
				{name: "job", addKeys: []string{"foo"}, lazy: true},
			},
			expectedSeriesRefs: []storage.SeriesRef{1, 2, 3, 4, 5},
		},
		{
			name: "multiple posting groups with add key and non consecutive lazy posting groups",
			fetchedPostings: []index.Postings{
				index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5}),
				index.NewListPostings([]storage.SeriesRef{1, 2, 4}),
			},
			postingGroups: []*postingGroup{
				{name: "foo", addKeys: []string{"bar"}},
				{name: "bar", addKeys: []string{"foo"}, lazy: true},
				{name: "baz", addKeys: []string{"foo"}},
				{name: "job", addKeys: []string{"foo"}, lazy: true},
			},
			expectedSeriesRefs: []storage.SeriesRef{1, 2, 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := mergeFetchedPostings(ctx, tc.fetchedPostings, tc.postingGroups)
			res, err := index.ExpandPostings(p)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSeriesRefs, res)
		})
	}
}
