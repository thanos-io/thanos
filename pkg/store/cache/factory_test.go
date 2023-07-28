// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

func TestIndexCacheMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	commonMetrics := newCommonMetrics(reg)

	memcached := newMockedMemcachedClient(nil)
	_, err := NewRemoteIndexCache(log.NewNopLogger(), memcached, commonMetrics, reg)
	testutil.Ok(t, err)
	conf := []byte(`
max_size: 10MB
max_item_size: 1MB
`)
	// Make sure that the in memory cache does not register the same metrics of the remote index cache.
	// If so, we should move those metrics to the `commonMetrics`
	_, err = NewInMemoryIndexCache(log.NewNopLogger(), commonMetrics, reg, conf)
	testutil.Ok(t, err)
}
