// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMatchMeta(t *testing.T) {
	ulid1 := ulid.MustNew(0, nil)
	ulid2 := ulid.MustNew(10, nil)
	for _, tcase := range []struct {
		name             string
		tombstone        *Tombstone
		meta             *metadata.Meta
		expectedMatchers *metadata.Matchers
		expectedMatched  bool
	}{
		{
			name: "empty tombstone matchers, time range overlapped",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{},
				MinTime:  0,
				MaxTime:  20,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 0,
					MaxTime: 10,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{},
			expectedMatched:  true,
		},
		{
			name: "empty tombstone matchers, time range same",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{},
				MinTime:  0,
				MaxTime:  9,
			},
			// TSDB block metadata time is [minTime, maxTime) so it is actually [0, 9]
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 0,
					MaxTime: 10,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{},
			expectedMatched:  true,
		},
		{
			name: "empty tombstone matchers, unmatched time range",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{},
				MinTime:  0,
				MaxTime:  1,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: nil,
			expectedMatched:  false,
		},
		{
			name: "empty tombstone matchers, unmatched time range 2",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{},
				MinTime:  30,
				MaxTime:  40,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 20,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: nil,
			expectedMatched:  false,
		},
		{
			name: "tombstone matchers unrelated to external labels",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
				MinTime:  5,
				MaxTime:  25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
			expectedMatched:  true,
		},
		{
			name: "tombstone matchers matches external labels",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "cluster", "one")},
				MinTime:  5,
				MaxTime:  25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{},
			expectedMatched:  true,
		},
		{
			name: "tombstone matchers matches external labels",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "cluster", "one"),
					labels.MustNewMatcher(labels.MatchEqual, "__test__", "up")},
				MinTime: 5,
				MaxTime: 25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "__test__", "up")},
			expectedMatched:  true,
		},
		{
			name: "tombstone matchers don't match external labels",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "cluster", "two")},
				MinTime:  5,
				MaxTime:  25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: nil,
			expectedMatched:  false,
		},
		{
			name: "tombstone matchers match block ID",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, block.BlockIDLabel, ulid1.String())},
				MinTime:  5,
				MaxTime:  25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{},
			expectedMatched:  true,
		},
		{
			name: "tombstone matchers match block ID and external labels",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, block.BlockIDLabel, ulid1.String()),
					labels.MustNewMatcher(labels.MatchEqual, "cluster", "one")},
				MinTime: 5,
				MaxTime: 25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{},
			expectedMatched:  true,
		},
		{
			name: "tombstone matchers don't match block ID",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, block.BlockIDLabel, ulid2.String()),
					labels.MustNewMatcher(labels.MatchEqual, "cluster", "one")},
				MinTime: 5,
				MaxTime: 25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: nil,
			expectedMatched:  false,
		},
		{
			name: "tombstone matchers match block ID, external labels and additional matchers",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, block.BlockIDLabel, ulid1.String()),
					labels.MustNewMatcher(labels.MatchEqual, "cluster", "one"),
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
				},
				MinTime: 5,
				MaxTime: 25,
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid1,
					MinTime: 10,
					MaxTime: 30,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{"cluster": "one"},
				},
			},
			expectedMatchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
			expectedMatched:  true,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			matchers, ok := tcase.tombstone.MatchMeta(tcase.meta)
			testutil.Equals(t, tcase.expectedMatched, ok)
			testutil.Equals(t, tcase.expectedMatchers, matchers)
		})
	}
}

func TestMatchLabels(t *testing.T) {
	for _, tcase := range []struct {
		name             string
		tombstone        *Tombstone
		labels           map[string]string
		expectedMatchers *metadata.Matchers
		expectedMatched  bool
	}{
		{
			name: "empty tombstone matchers",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{},
			},
			labels:           map[string]string{},
			expectedMatchers: &metadata.Matchers{},
			expectedMatched:  true,
		},
		{
			name: "unrelated matchers",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "tenant", "default")},
			},
			labels:           map[string]string{"cluster": "one"},
			expectedMatchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "tenant", "default")},
			expectedMatched:  true,
		},
		{
			name: "matched external label matchers",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "cluster", "one")},
			},
			labels:           map[string]string{"cluster": "one"},
			expectedMatchers: &metadata.Matchers{},
			expectedMatched:  true,
		},
		{
			name: "unmatched external label matchers",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "cluster", "one")},
			},
			labels:           map[string]string{"cluster": "two"},
			expectedMatchers: nil,
			expectedMatched:  false,
		},
		{
			name: "unmatched external label matchers with two matchers",
			tombstone: &Tombstone{
				Matchers: &metadata.Matchers{labels.MustNewMatcher(labels.MatchEqual, "cluster", "one"),
					labels.MustNewMatcher(labels.MatchEqual, "region", "eu")},
			},
			labels:           map[string]string{"cluster": "two", "region": "eu"},
			expectedMatchers: nil,
			expectedMatched:  false,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			matchers, ok := tcase.tombstone.MatchLabels(labels.FromMap(tcase.labels))
			testutil.Equals(t, tcase.expectedMatched, ok)
			testutil.Equals(t, tcase.expectedMatchers, matchers)
		})
	}
}
