// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

const SeriesHash = "__thanos_series_hash"

type ProjectionMatcher struct {
	buf      *[]byte
	buffers  *sync.Pool
	grouping bool
	labelset map[string]struct{}
	extLset  map[string]struct{}
	by       bool
}

func (m *ProjectionInfo) Matcher(buffers *sync.Pool, extLset labels.Labels) *ProjectionMatcher {
	if m == nil {
		return &ProjectionMatcher{
			grouping: false,
		}
	}

	pm := &ProjectionMatcher{
		grouping: m.Grouping,
		buf:      buffers.Get().(*[]byte),
		buffers:  buffers,
		by:       m.By,
		labelset: m.labelSet(),
		extLset:  make(map[string]struct{}, len(extLset)),
	}
	for _, lbl := range extLset {
		pm.extLset[lbl.Name] = struct{}{}
	}
	return pm
}

func (m *ProjectionInfo) labelSet() map[string]struct{} {
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

func (s *ProjectionMatcher) ModifyLabels(lbls labels.Labels, calculateHash bool) labels.Labels {
	if !s.grouping {
		return lbls
	}
	*s.buf = (*s.buf)[:0]
	output := labels.NewBuilder(nil)
	for _, lbl := range lbls {
		if _, ok := s.extLset[lbl.Name]; ok {
			output.Set(lbl.Name, lbl.Value)
			continue
		}
		if shardByLabel(s.labelset, labelpb.ZLabel{Name: lbl.Name, Value: lbl.Value}, s.by) {
			output.Set(lbl.Name, lbl.Value)
		} else if calculateHash {
			*s.buf = append(*s.buf, lbl.Name...)
			*s.buf = append(*s.buf, sep[0])
			*s.buf = append(*s.buf, lbl.Value...)
			*s.buf = append(*s.buf, sep[0])
		}
	}

	if calculateHash {
		hash := xxhash.Sum64(*s.buf)
		output.Set(SeriesHash, fmt.Sprintf("%d", hash))
	}
	return output.Labels(nil)
}

func (s *ProjectionMatcher) Close() {
	if s.buffers != nil {
		s.buffers.Put(s.buf)
	}
}
