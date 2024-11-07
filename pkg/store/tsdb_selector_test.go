// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sort"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestMatchersForLabelSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		labelSets []labels.Labels
		want      []storepb.LabelMatcher
	}{
		{
			name:      "empty label sets",
			labelSets: nil,
			want:      []storepb.LabelMatcher{},
		},
		{
			name: "single label set with single label",
			labelSets: []labels.Labels{
				labels.FromStrings("a", "1"),
			},
			want: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1"},
			},
		},
		{
			name: "multiple labels with same label name",
			labelSets: []labels.Labels{
				labels.FromStrings("a", "1"),
				labels.FromStrings("a", "2"),
			},
			want: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
			},
		},
		{
			name: "multiple labels with different label name",
			labelSets: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
			},
			want: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1"},
				{Type: storepb.LabelMatcher_RE, Name: "b", Value: "2"},
			},
		},
		{
			name: "multiple label sets with same label name",
			labelSets: []labels.Labels{
				labels.FromStrings("a", "1"),
				labels.FromStrings("a", "2"),
			},
			want: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
			},
		},
		{
			name: "multiple label sets with different label name",
			labelSets: []labels.Labels{
				labels.FromStrings("a", "1"),
				labels.FromStrings("b", "2"),
			},
			want: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|^$"},
				{Type: storepb.LabelMatcher_RE, Name: "b", Value: "2|^$"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matchers := MatchersForLabelSets(tt.labelSets)
			sort.Slice(matchers, func(i, j int) bool {
				return matchers[i].Name < matchers[j].Name
			})

			testutil.Equals(t, tt.want, matchers)
		})
	}
}
