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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func TestKeysToFetchFromPostingGroups(t *testing.T) {
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, matchers := keysToFetchFromPostingGroups(tc.pgs)
			testutil.Equals(t, tc.expectedLabels, keys)
			testutil.Equals(t, tc.expectedMatchers, matchers)
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

func (h *mockIndexHeaderReader) LookupSymbol(o uint32) (string, error) { return "", nil }

func (h *mockIndexHeaderReader) LabelValues(name string) ([]string, error) { return nil, nil }

func (h *mockIndexHeaderReader) LabelNames() ([]string, error) { return nil, nil }

func TestOptimizePostingsFetchByDownloadedBytes(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	dir := t.TempDir()
	bkt, err := filesystem.NewBucket(dir)
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, bkt.Close()) }()

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
		name                  string
		inputPostings         map[string]map[string]index.Range
		inputError            error
		postingGroups         []*postingGroup
		seriesMaxSize         int64
		seriesMatchRatio      float64
		expectedPostingGroups []*postingGroup
		expectedEmptyPosting  bool
		expectedError         string
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
			name: "posting offsets empty",
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
			name: "posting group label doesn't exist",
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
				{name: "bar", addKeys: []string{"foo", "buz"}, cardinality: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1},
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
				{name: "bar", addKeys: []string{"foo"}, cardinality: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1},
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
				{addAll: true, name: "bar", removeKeys: []string{"foo"}, cardinality: 1},
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1},
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
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 250000},
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
				{name: "bar", addKeys: []string{"foo"}, cardinality: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1, lazy: true},
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
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 250000, lazy: true},
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
				{name: "cluster", addKeys: []string{"us"}, cardinality: 1},
				{name: "foo", addKeys: []string{"bar"}, cardinality: 1},
				{name: "bar", addKeys: []string{"foo"}, cardinality: 250000, lazy: true},
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
				{name: "cluster", addKeys: []string{"us"}, cardinality: 1},
				{addAll: true, name: "foo", removeKeys: []string{"bar"}, cardinality: 1},
				{addAll: true, name: "bar", removeKeys: []string{"foo"}, cardinality: 250000, lazy: true},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headerReader := &mockIndexHeaderReader{postings: tc.inputPostings, err: tc.inputError}
			block, err := newBucketBlock(ctx, logger, newBucketStoreMetrics(nil), meta, bkt, path.Join(dir, blockID.String()), nil, nil, headerReader, nil, nil, nil)
			testutil.Ok(t, err)
			ir := newBucketIndexReader(block)
			pgs, emptyPosting, err := optimizePostingsFetchByDownloadedBytes(ir, tc.postingGroups, tc.seriesMaxSize, tc.seriesMatchRatio)
			if err != nil {
				testutil.Equals(t, tc.expectedError, err.Error())
				return
			}
			testutil.Equals(t, tc.expectedEmptyPosting, emptyPosting)
			testutil.Equals(t, tc.expectedPostingGroups, pgs)
		})
	}
}
