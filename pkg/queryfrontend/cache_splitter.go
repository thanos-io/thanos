// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

// constSplitter is a utility for using a constant split interval when determining cache keys.
type constSplitter time.Duration

// TODO(yeya24): Add other request params as request key.
// GenerateCacheKey generates a cache key based on the Request and interval.
func (t constSplitter) GenerateCacheKey(_ string, r queryrange.Request) string {
	currentInterval := r.GetStart() / time.Duration(t).Milliseconds()
	if tr, ok := r.(*ThanosRequest); ok {
		return fmt.Sprintf("%s:%d:%d:%d", tr.Query, tr.Step, currentInterval, tr.MaxSourceResolution)
	}
	return fmt.Sprintf("%s:%d:%d", r.GetQuery(), r.GetStep(), currentInterval)
}
