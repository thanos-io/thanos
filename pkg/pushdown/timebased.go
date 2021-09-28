// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package pushdown

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/pushdown/querypb"
	"github.com/thanos-io/thanos/pkg/store"
)

// TimeBasedPushdown is a type of pushdown that pushes
// down a query when a timerange of a query only
// matches one QueryAPI.
type TimeBasedPushdown struct {
	stores         func() []store.Client
	matchedQueries prometheus.Counter
}

func NewTimeBasedPushdown(
	stores func() []store.Client,
	reg prometheus.Registerer,
) *TimeBasedPushdown {
	ret := &TimeBasedPushdown{
		stores: stores,
	}
	if reg != nil {
		ret.matchedQueries = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Name: "matches_total",
				Help: "How many queries were matched and were pushed down as a result",
			},
		)
	}
	return ret
}

// Match finds a querypb.QueryClient that we can push down the query
// to if it is the only partially matching QueryAPI & StoreAPI tuple.
// It returns false if nothing has been found.
func (pa *TimeBasedPushdown) Match(startNS, endNS int64) (querypb.QueryClient, bool) {
	stores := pa.stores()

	var overlappingStore store.Client

	for _, st := range stores {
		mint, maxt := st.TimeRange()

		if !(mint <= endNS/int64(time.Millisecond) && startNS/int64(time.Millisecond) <= maxt) {
			continue
		}

		// Multiple partially overlapping StoreAPIs. We have to ask them all.
		if overlappingStore != nil {
			return nil, false
		}

		overlappingStore = st
	}

	if overlappingStore == nil {
		return nil, false
	}

	if qapi := overlappingStore.QueryAPI(); qapi != nil {
		pa.matchedQueries.Inc()
		return qapi, true
	}

	return nil, false
}
