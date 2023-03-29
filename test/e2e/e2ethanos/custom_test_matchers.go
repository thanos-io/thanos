// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	e2emon "github.com/efficientgo/e2e/monitoring"
)

// Between is a MetricValueExpectation function for WaitSumMetrics that returns true if given single sum is between
// the lower and upper bounds (non-inclusive, as in `lower < x < upper`).
func Between(lower, upper float64) e2emon.MetricValueExpectation {
	return func(sums ...float64) bool {
		if len(sums) != 1 {
			panic("between: expected one value")
		}
		return sums[0] > lower && sums[0] < upper
	}
}
