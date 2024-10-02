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

	for _, m := range matchers {
		if m.Type == labels.MatchEqual && m.Name == labels.MetricName {
			return f.filter.Lookup([]byte(m.Value))
		}
	}

	return true
}

func (f *CuckooMetricNameStoreFilter) ResetAndSet(values ...string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.filter.Reset()
	for _, value := range values {
		f.filter.Insert(unsafe.Slice(unsafe.StringData(value), len(value)))
	}
}
