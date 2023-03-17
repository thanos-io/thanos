// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
)

func TestRemoteEngine_LabelSets(t *testing.T) {
	tests := []struct {
		name          string
		labelSets     []labels.Labels
		replicaLabels []string
		expected      []labels.Labels
	}{
		{
			name:      "empty label sets",
			labelSets: []labels.Labels{},
			expected:  []labels.Labels{},
		},
		{
			name:          "empty label sets with replica labels",
			labelSets:     []labels.Labels{},
			replicaLabels: []string{"replica"},
			expected:      []labels.Labels{},
		},
		{
			name:      "non-empty label sets",
			labelSets: []labels.Labels{labels.FromStrings("a", "1")},
			expected:  []labels.Labels{labels.FromStrings("a", "1")},
		},
		{
			name:          "non-empty label sets with replica labels",
			labelSets:     []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			replicaLabels: []string{"a"},
			expected:      []labels.Labels{labels.FromStrings("b", "2")},
		},
		{
			name:          "replica labels not in label sets",
			labelSets:     []labels.Labels{labels.FromStrings("a", "1", "c", "2")},
			replicaLabels: []string{"a", "b"},
			expected:      []labels.Labels{labels.FromStrings("c", "2")},
		},
	}

	for _, test := range tests {
		client := Client{labelSets: test.labelSets}
		engine := newRemoteEngine(log.NewNopLogger(), client, Opts{
			ReplicaLabels: test.replicaLabels,
		})

		testutil.Equals(t, test.expected, engine.LabelSets())
	}
}
