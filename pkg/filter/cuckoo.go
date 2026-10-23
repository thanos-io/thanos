// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filter

import (
	"sync"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	cuckoo "github.com/seiflotfy/cuckoofilter"
)

type CuckooMetricNameStoreFilter struct {
	filter *cuckoo.Filter
	mtx    sync.RWMutex
}

func NewCuckooMetricNameStoreFilter(capacity uint) *CuckooMetricNameStoreFilter {
	return &CuckooMetricNameStoreFilter{
		filter: cuckoo.NewFilter(capacity),
	}
}

func (f *CuckooMetricNameStoreFilter) Matches(matchers []*labels.Matcher) bool {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	var constraints [][]string
	for _, m := range matchers {
		if m.Name != labels.MetricName {
			continue
		}

		switch m.Type {
		case labels.MatchEqual:
			constraints = append(constraints, []string{m.Value})
		case labels.MatchRegexp:
			vs := m.SetMatches()
			if len(vs) == 0 {
				continue
			}
			constraints = append(constraints, vs)
		}
	}

	if len(constraints) == 0 {
		return true
	}

	for _, values := range constraints {
		matches := false
		for _, value := range values {
			if f.lookup(value) {
				matches = true
				break
			}
		}
		if !matches {
			return false
		}
	}

	return true
}

func (f *CuckooMetricNameStoreFilter) lookup(v string) bool {
	return f.filter.Lookup(unsafe.Slice(unsafe.StringData(v), len(v)))
}

func (f *CuckooMetricNameStoreFilter) ResetAndSet(values ...string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.filter.Reset()
	for _, value := range values {
		f.filter.Insert(unsafe.Slice(unsafe.StringData(value), len(value)))
	}
}
