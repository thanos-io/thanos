// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/store/cache/cachekey"
	"github.com/thanos-io/thanos/pkg/testutil"
)

// Benchmark retrieval of one key from one groupcache node.
func BenchmarkGroupcacheRetrieval(b *testing.B) {
	reg := prometheus.NewRegistry()
	router := route.New()

	bkt := objstore.NewInMemBucket()
	b.Cleanup(func() { bkt.Close() })

	payload := strings.Repeat("foobar", 16*1024/6)

	testutil.Ok(b, bkt.Upload(context.Background(), "test", strings.NewReader(payload)))

	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	testutil.Ok(b, err)

	listenerAddr := listener.Addr().String()
	port := strings.Split(listenerAddr, ":")[1]

	selfURL := fmt.Sprintf("http://localhost:%v", port)

	cachingBucketConfig := NewCachingBucketConfig()
	cachingBucketConfig.CacheGet("test", nil, func(s string) bool {
		return true
	}, 16*1024, 0*time.Second, 0*time.Second, 0*time.Second)

	groupCache, err := NewGroupcacheWithConfig(
		log.NewJSONLogger(os.Stderr),
		reg,
		GroupcacheConfig{
			Peers:           []string{selfURL},
			SelfURL:         selfURL,
			GroupcacheGroup: "groupName",
			DNSSDResolver:   dns.MiekgdnsResolverType,
			DNSInterval:     30 * time.Second,
			MaxSize:         model.Bytes(16 * 1024),
		},
		"/_groupcache/",
		router,
		bkt,
		cachingBucketConfig,
	)
	testutil.Ok(b, err)

	go func() { _ = http.Serve(listener, router) }()
	b.Cleanup(func() { listener.Close() })
	cachingBucketConfig.SetCacheImplementation(groupCache)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp, err := http.Get(selfURL + fmt.Sprintf("/_groupcache/groupName/%v", cachekey.BucketCacheKey{Verb: "content", Name: "test"}))
		testutil.Ok(b, err)
		testutil.Ok(b, resp.Body.Close())
	}
}
