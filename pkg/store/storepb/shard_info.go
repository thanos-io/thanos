// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

var sep = []byte{'\xff'}

type ShardMatcher struct {
	buf              *[]byte
	buffers          *sync.Pool
	shardingLabelset map[string]struct{}

	isSharded   bool
	by          bool
	totalShards int64
	shardIndex  int64
}

func (s *ShardMatcher) IsSharded() bool {
	return s.isSharded
}

func (s *ShardMatcher) Close() {
	if s == nil {
		return
	}
	if s.buffers != nil {
		s.buffers.Put(s.buf)
	}
}

// MatchesLabels checks if the given Prometheus labels match the shard.
func (s *ShardMatcher) MatchesLabels(lbls labels.Labels) bool {
	// Match all series when query is not sharded
	if s == nil || !s.isSharded {
		return true
	}

	*s.buf = (*s.buf)[:0]
	lbls.Range(func(lbl labels.Label) {
		if shardByLabelName(s.shardingLabelset, lbl.Name, s.by) {
			*s.buf = append(*s.buf, lbl.Name...)
			*s.buf = append(*s.buf, sep[0])
			*s.buf = append(*s.buf, lbl.Value...)
			*s.buf = append(*s.buf, sep[0])
		}
	})

	hash := xxhash.Sum64(*s.buf)
	return hash%uint64(s.totalShards) == uint64(s.shardIndex)
}

// MatchesLabelPointers checks if the given label pointers match the shard.
func (s *ShardMatcher) MatchesLabelPointers(lbls []*labelpb.Label) bool {
	// Match all series when query is not sharded
	if s == nil || !s.isSharded {
		return true
	}

	*s.buf = (*s.buf)[:0]
	for _, lbl := range lbls {
		if lbl == nil {
			continue
		}
		if shardByLabelName(s.shardingLabelset, lbl.Name, s.by) {
			*s.buf = append(*s.buf, lbl.Name...)
			*s.buf = append(*s.buf, sep[0])
			*s.buf = append(*s.buf, lbl.Value...)
			*s.buf = append(*s.buf, sep[0])
		}
	}

	hash := xxhash.Sum64(*s.buf)
	return hash%uint64(s.totalShards) == uint64(s.shardIndex)
}

func shardByLabelName(labelSet map[string]struct{}, name string, groupingBy bool) bool {
	_, shardHasLabel := labelSet[name]
	if groupingBy && shardHasLabel {
		return true
	}

	groupingWithout := !groupingBy
	if groupingWithout && !shardHasLabel {
		return true
	}

	return false
}

func (m *ShardInfo) Matcher(buffers *sync.Pool) *ShardMatcher {
	if m == nil || m.TotalShards < 1 {
		return &ShardMatcher{
			isSharded: false,
		}
	}

	return &ShardMatcher{
		isSharded:        true,
		buf:              buffers.Get().(*[]byte),
		buffers:          buffers,
		shardingLabelset: m.labelSet(),
		by:               m.By,
		totalShards:      m.TotalShards,
		shardIndex:       m.ShardIndex,
	}
}

func (m *ShardInfo) labelSet() map[string]struct{} {
	if m == nil {
		return nil
	}
	labelSet := make(map[string]struct{})
	if m == nil || m.Labels == nil {
		return labelSet
	}

	for _, label := range m.Labels {
		labelSet[label] = struct{}{}
	}

	return labelSet
}
