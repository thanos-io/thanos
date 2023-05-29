// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"math"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func TestRemoteEngine_LabelSets(t *testing.T) {
	tests := []struct {
		name          string
		tsdbInfos     []infopb.TSDBInfo
		replicaLabels []string
		expected      []labels.Labels
	}{
		{
			name:      "empty label sets",
			tsdbInfos: []infopb.TSDBInfo{},
			expected:  []labels.Labels{},
		},
		{
			name:          "empty label sets with replica labels",
			tsdbInfos:     []infopb.TSDBInfo{},
			replicaLabels: []string{"replica"},
			expected:      []labels.Labels{},
		},
		{
			name: "non-empty label sets",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels: zLabelSetFromStrings("a", "1"),
			}},
			expected: []labels.Labels{labels.FromStrings("a", "1")},
		},
		{
			name: "non-empty label sets with replica labels",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels: zLabelSetFromStrings("a", "1", "b", "2"),
			}},
			replicaLabels: []string{"a"},
			expected:      []labels.Labels{labels.FromStrings("b", "2")},
		},
		{
			name: "replica labels not in label sets",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels: zLabelSetFromStrings("a", "1", "c", "2"),
				},
			},
			replicaLabels: []string{"a", "b"},
			expected:      []labels.Labels{labels.FromStrings("c", "2")},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client := NewClient(nil, "", testCase.tsdbInfos)
			engine := newRemoteEngine(log.NewNopLogger(), client, Opts{
				ReplicaLabels: testCase.replicaLabels,
			})

			testutil.Equals(t, testCase.expected, engine.LabelSets())
		})
	}
}

func TestRemoteEngine_MinT(t *testing.T) {
	tests := []struct {
		name          string
		tsdbInfos     []infopb.TSDBInfo
		replicaLabels []string
		expected      int64
	}{
		{
			name:      "empty label sets",
			tsdbInfos: []infopb.TSDBInfo{},
			expected:  math.MaxInt64,
		},
		{
			name:          "empty label sets with replica labels",
			tsdbInfos:     []infopb.TSDBInfo{},
			replicaLabels: []string{"replica"},
			expected:      math.MaxInt64,
		},
		{
			name: "non-empty label sets",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels:  zLabelSetFromStrings("a", "1"),
				MinTime: 30,
			}},
			expected: 30,
		},
		{
			name: "non-empty label sets with replica labels",
			tsdbInfos: []infopb.TSDBInfo{{
				Labels:  zLabelSetFromStrings("a", "1", "b", "2"),
				MinTime: 30,
			}},
			replicaLabels: []string{"a"},
			expected:      30,
		},
		{
			name: "replicated labelsets with different mint",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "1"),
					MinTime: 30,
				},
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "2"),
					MinTime: 60,
				},
			},
			replicaLabels: []string{"replica"},
			expected:      60,
		},
		{
			name: "multiple replicated labelsets with different mint",
			tsdbInfos: []infopb.TSDBInfo{
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "1"),
					MinTime: 30,
				},
				{
					Labels:  zLabelSetFromStrings("a", "1", "replica", "2"),
					MinTime: 60,
				},
				{
					Labels:  zLabelSetFromStrings("a", "2", "replica", "1"),
					MinTime: 80,
				},
				{
					Labels:  zLabelSetFromStrings("a", "2", "replica", "2"),
					MinTime: 120,
				},
			},
			replicaLabels: []string{"replica"},
			expected:      60,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client := NewClient(nil, "", testCase.tsdbInfos)
			engine := newRemoteEngine(log.NewNopLogger(), client, Opts{
				ReplicaLabels: testCase.replicaLabels,
			})

			testutil.Equals(t, testCase.expected, engine.MinT())
		})
	}
}

func zLabelSetFromStrings(ss ...string) labelpb.ZLabelSet {
	return labelpb.ZLabelSet{
		Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(ss...)),
	}
}
