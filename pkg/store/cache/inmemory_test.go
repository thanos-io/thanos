// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Tests out the index cache implementation.
package storecache

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestNewInMemoryIndexCache(t *testing.T) {
	// Should return error on invalid YAML config.
	conf := []byte("invalid")
	cache, err := NewInMemoryIndexCache(log.NewNopLogger(), nil, nil, conf)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*InMemoryIndexCache)(nil), cache)

	// Should instance an in-memory index cache with default config
	// on empty YAML config.
	conf = []byte{}
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, nil, conf)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(DefaultInMemoryIndexCacheConfig.MaxItemSize), cache.maxItemSizeBytes)

	// Should instance an in-memory index cache with specified YAML config.s with units.
	conf = []byte(`
max_size: 1MB
max_item_size: 2KB
`)
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, nil, conf)
	testutil.Ok(t, err)
	testutil.Equals(t, uint64(2*1024), cache.maxItemSizeBytes)

	// Should instance an in-memory index cache with specified YAML config.s with units.
	conf = []byte(`
max_size: 2KB
max_item_size: 1MB
`)
	cache, err = NewInMemoryIndexCache(log.NewNopLogger(), nil, nil, conf)
	testutil.NotOk(t, err)
	testutil.Equals(t, (*InMemoryIndexCache)(nil), cache)
	// testutil.Equals(t, uint64(1024*1024), cache.maxSizeBytes)
	// testutil.Equals(t, uint64(2*1024), cache.maxItemSizeBytes)

	// testutil.Equals(t, uint64(1024*1024), cache.maxItemSizeBytes)
	// testutil.Equals(t, uint64(2*1024), cache.maxSizeBytes)
}

func TestInMemoryIndexCache_UpdateItem(t *testing.T) {
	var errorLogs []string
	errorLogger := log.LoggerFunc(func(kvs ...interface{}) error {
		var lvl string
		for i := 0; i < len(kvs); i += 2 {
			if kvs[i] == "level" {
				lvl = fmt.Sprint(kvs[i+1])
				break
			}
		}
		if lvl != "error" {
			return nil
		}
		var buf bytes.Buffer
		defer func() { errorLogs = append(errorLogs, buf.String()) }()
		return log.NewLogfmtLogger(&buf).Log(kvs...)
	})

	metrics := prometheus.NewRegistry()
	cache, err := NewInMemoryIndexCacheWithConfig(log.NewSyncLogger(errorLogger), nil, metrics, InMemoryIndexCacheConfig{
		MaxItemSize: 1024,
		MaxSize:     1024,
	})
	testutil.Ok(t, err)

	uid := func(id storage.SeriesRef) ulid.ULID { return ulid.MustNew(uint64(id), nil) }
	lbl := labels.Label{Name: "foo", Value: "bar"}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")
	ctx := context.Background()

	for _, tt := range []struct {
		typ string
		set func(storage.SeriesRef, []byte)
		get func(storage.SeriesRef) ([]byte, bool)
	}{
		{
			typ: cacheTypePostings,
			set: func(id storage.SeriesRef, b []byte) { cache.StorePostings(uid(id), lbl, b, tenancy.DefaultTenant) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiPostings(ctx, uid(id), []labels.Label{lbl}, tenancy.DefaultTenant)
				b, ok := hits[lbl]

				return b, ok
			},
		},
		{
			typ: cacheTypeSeries,
			set: func(id storage.SeriesRef, b []byte) { cache.StoreSeries(uid(id), id, b, tenancy.DefaultTenant) },
			get: func(id storage.SeriesRef) ([]byte, bool) {
				hits, _ := cache.FetchMultiSeries(ctx, uid(id), []storage.SeriesRef{id}, tenancy.DefaultTenant)
				b, ok := hits[id]

				return b, ok
			},
		},
		{
			typ: cacheTypeExpandedPostings,
			set: func(id storage.SeriesRef, b []byte) {
				cache.StoreExpandedPostings(uid(id), []*labels.Matcher{matcher}, b, tenancy.DefaultTenant)
			},
			get: func(id storage.SeriesRef) ([]byte, bool) {
				return cache.FetchExpandedPostings(ctx, uid(id), []*labels.Matcher{matcher}, tenancy.DefaultTenant)
			},
		},
	} {
		t.Run(tt.typ, func(t *testing.T) {
			defer func() { errorLogs = nil }()

			// Set value.
			tt.set(0, []byte{0})
			buf, ok := tt.get(0)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0}, buf)
			testutil.Equals(t, []string(nil), errorLogs)

			// Set the same value again.
			tt.set(0, []byte{0})
			buf, ok = tt.get(0)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0}, buf)
			testutil.Equals(t, []string(nil), errorLogs)

			// Set a larger value.
			tt.set(1, []byte{0, 1})
			buf, ok = tt.get(1)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0, 1}, buf)
			testutil.Equals(t, []string(nil), errorLogs)

			// Mutations to existing values will be ignored.
			tt.set(1, []byte{1, 2})
			buf, ok = tt.get(1)
			testutil.Equals(t, true, ok)
			testutil.Equals(t, []byte{0, 1}, buf)
			testutil.Equals(t, []string(nil), errorLogs)
		})
	}
}
