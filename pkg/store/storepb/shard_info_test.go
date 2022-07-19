// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"sync"
	"testing"

	"github.com/alecthomas/units"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func TestShardInfo_MatchesSeries(t *testing.T) {
	series := labelpb.ZLabelsFromPromLabels(labels.FromStrings(
		"pod", "nginx",
		"node", "node-1",
		"container", "nginx",
	))

	tests := []struct {
		name      string
		shardInfo *ShardInfo
		series    []labelpb.ZLabel
		matches   bool
	}{
		{
			name:      "nil shard info",
			shardInfo: nil,
			matches:   true,
		},
		{
			name:   "one shard only",
			series: series,
			shardInfo: &ShardInfo{
				ShardIndex:  0,
				TotalShards: 1,
				By:          true,
				Labels:      nil,
			},
			matches: true,
		},
		{
			name:   "shard by empty sharding labels",
			series: series,
			shardInfo: &ShardInfo{
				ShardIndex:  0,
				TotalShards: 2,
				By:          true,
				Labels:      nil,
			},
			matches: false,
		},
		{
			name:   "shard without empty sharding labels",
			series: series,
			shardInfo: &ShardInfo{
				ShardIndex:  0,
				TotalShards: 2,
				By:          false,
				Labels:      nil,
			},
			matches: true,
		},
		{
			name:   "shard by labels for shard 0",
			series: series,
			shardInfo: &ShardInfo{
				ShardIndex:  0,
				TotalShards: 2,
				By:          true,
				Labels:      []string{"pod", "node"},
			},
			matches: false,
		},
		{
			name:   "shard by labels for shard 1",
			series: series,
			shardInfo: &ShardInfo{
				ShardIndex:  1,
				TotalShards: 2,
				By:          true,
				Labels:      []string{"pod", "node"},
			},
			matches: true,
		},
		{
			name:   "shard without labels for shard 0",
			series: series,
			shardInfo: &ShardInfo{
				ShardIndex:  0,
				TotalShards: 2,
				By:          false,
				Labels:      []string{"node"},
			},
			matches: true,
		},
		{
			name:   "shard without labels for shard 1",
			series: series,
			shardInfo: &ShardInfo{
				ShardIndex:  1,
				TotalShards: 2,
				By:          false,
				Labels:      []string{"node"},
			},
			matches: false,
		},
	}

	buffers := sync.Pool{New: func() interface{} {
		b := make([]byte, 0, 10*units.Kilobyte)
		return &b
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matcher := test.shardInfo.Matcher(&buffers)
			defer matcher.Close()
			isMatch := matcher.MatchesZLabels(test.series)
			if isMatch != test.matches {
				t.Fatalf("invalid result, got %t, want %t", isMatch, test.matches)
			}
		})
	}
}
