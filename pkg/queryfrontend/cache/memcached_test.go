// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestNewMemcachedCache(t *testing.T) {
	for _, tc := range []struct {
		name      string
		conf      string
		expectErr bool
	}{
		{
			name: "address and dns update interval",
			conf: `
addresses: ["localhost:11211"]
dns_provider_update_interval: 1s
`,
			expectErr: false,
		},
		{
			name: "validity",
			conf: `
addresses: ["localhost:11211"]
dns_provider_update_interval: 1s
validity: 6h
`,
			expectErr: false,
		},
		{
			name: "address not set",
			conf: `
dns_provider_update_interval: 1s
`,
			expectErr: true,
		},
		{
			name: "failed to unmarshal because of invalid field",
			conf: `
aaa: 111
dns_provider_update_interval: 1s
`,
			expectErr: true,
		},
	} {
		_, err := newMemcachedCache([]byte(tc.conf), log.NewNopLogger(), nil)
		if tc.expectErr {
			testutil.NotOk(t, err)
		} else {
			testutil.Ok(t, err)
		}
	}
}

func TestMemcachedResponseCache(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := []struct {
		name            string
		keys            []string
		bufs            [][]byte
		mockedErr       error
		fetchKeys       []string
		expectedHits    map[string][]byte
		expectedMissing []string
	}{
		{
			name:            "should return no hits on empty cache",
			fetchKeys:       []string{key1, key2},
			expectedHits:    map[string][]byte{},
			expectedMissing: []string{key1, key2},
		},
		{
			name:      "should return no misses on 100% hit ratio",
			keys:      []string{key1},
			bufs:      [][]byte{value1},
			fetchKeys: []string{key1},
			expectedHits: map[string][]byte{
				key1: value1,
			},
			expectedMissing: []string{},
		},
		{
			name:      "100% hit ratio with more keys",
			keys:      []string{key1, key2, key3},
			bufs:      [][]byte{value1, value2, value3},
			fetchKeys: []string{key1, key2, key3},
			expectedHits: map[string][]byte{
				key1: value1,
				key2: value2,
				key3: value3,
			},
			expectedMissing: []string{},
		},
		{
			name:            "should return hits and misses on partial hits",
			keys:            []string{key1},
			bufs:            [][]byte{value1},
			fetchKeys:       []string{key1, key3},
			expectedHits:    map[string][]byte{key1: value1},
			expectedMissing: []string{key3},
		},
		{
			name:            "should return no hits on memcached error",
			mockedErr:       errors.New("mocked error"),
			keys:            []string{key1},
			bufs:            [][]byte{value1},
			fetchKeys:       []string{key1},
			expectedHits:    make(map[string][]byte),
			expectedMissing: []string{key1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			memcached := cache.NewMockedMemcachedClient(tc.mockedErr)
			c := newMemcachedCacheWithClient(memcached, time.Hour, log.NewNopLogger(), nil)

			ctx := context.Background()
			c.Store(ctx, tc.keys, tc.bufs)

			found, bufs, missing := c.Fetch(ctx, tc.fetchKeys)
			hits := make(map[string][]byte, len(bufs))
			for i, buf := range bufs {
				hits[found[i]] = buf
			}
			testutil.Equals(t, tc.expectedHits, hits)
			testutil.Equals(t, tc.expectedMissing, missing)

			// Assert on metrics.
			testutil.Equals(t, float64(len(tc.fetchKeys)), prom_testutil.ToFloat64(c.requests))
			testutil.Equals(t, float64(len(tc.expectedHits)), prom_testutil.ToFloat64(c.hits))
		})
	}
}
