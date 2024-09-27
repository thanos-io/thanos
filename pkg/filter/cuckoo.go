// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filter

import (
	"sync"

	cuckoo "github.com/seiflotfy/cuckoofilter"
)

type CuckooFilterMetricNameFilter struct {
	filter *cuckoo.Filter
	mtx    sync.RWMutex
}

func NewCuckooFilterMetricNameFilter(capacity uint) *CuckooFilterMetricNameFilter {
	return &CuckooFilterMetricNameFilter{
		filter: cuckoo.NewFilter(capacity),
	}
}

func (f *CuckooFilterMetricNameFilter) MatchesMetricName(metricName string) bool {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.filter.Lookup([]byte(metricName))
}

func (f *CuckooFilterMetricNameFilter) ResetAddMetricName(metricNames ...string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	f.filter.Reset()
	for _, metricName := range metricNames {
		f.filter.Insert([]byte(metricName))
	}
}
