// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"crypto/tls"
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
	"github.com/thanos-io/objstore"
	galaxyhttp "github.com/vimeo/galaxycache/http"
	"golang.org/x/net/http2"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/prober"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/store/cache/cachekey"
	"github.com/thanos-io/thanos/pkg/testutil"
)

const basePath = `/_groupcache/`
const groupName = `groupName`
const selfURLH1 = "http://localhost:12345"
const selfURLH2C = "http://localhost:12346"

func TestMain(m *testing.M) {
	reg := prometheus.NewRegistry()
	router := route.New()

	bkt := objstore.NewInMemBucket()
	defer bkt.Close()

	payload := strings.Repeat("foobar", 16*1024/6)

	if err := bkt.Upload(context.Background(), "test", strings.NewReader(payload)); err != nil {
		fmt.Printf("failed to upload: %s\n", err.Error())
		os.Exit(1)
	}

	httpServer := httpserver.New(log.NewNopLogger(), nil, component.Bucket, &prober.HTTPProbe{},
		httpserver.WithListen("0.0.0.0:12345"))
	httpServer.Handle("/", router)
	httpServerH2C := httpserver.New(log.NewNopLogger(), nil, component.Bucket, &prober.HTTPProbe{},
		httpserver.WithListen("0.0.0.0:12346"), httpserver.WithEnableH2C(true))
	httpServerH2C.Handle("/", router)

	cachingBucketConfig := NewCachingBucketConfig()
	cachingBucketConfig.CacheGet("test", nil, func(s string) bool {
		return true
	}, 16*1024, 0*time.Second, 0*time.Second, 0*time.Second)

	groupCache, err := NewGroupcacheWithConfig(
		log.NewJSONLogger(os.Stderr),
		reg,
		GroupcacheConfig{
			Peers:           []string{selfURLH1},
			SelfURL:         selfURLH1,
			GroupcacheGroup: groupName,
			DNSSDResolver:   dns.MiekgdnsResolverType,
			DNSInterval:     30 * time.Second,
			MaxSize:         model.Bytes(16 * 1024),
		},
		basePath,
		router,
		bkt,
		cachingBucketConfig,
	)
	if err != nil {
		fmt.Printf("failed creating group cache: %s\n", err.Error())
		os.Exit(1)
	}

	go func() {
		if err = httpServer.ListenAndServe(); err != nil {
			fmt.Printf("failed to listen: %s\n", err.Error())
			os.Exit(1)
		}
	}()
	go func() {
		if err = httpServerH2C.ListenAndServe(); err != nil {
			fmt.Printf("failed to listen: %s\n", err.Error())
			os.Exit(1)
		}
	}()

	defer httpServer.Shutdown(nil)
	defer httpServerH2C.Shutdown(nil)

	cachingBucketConfig.SetCacheImplementation(groupCache)

	exitVal := m.Run()

	os.Exit(exitVal)
}

// Benchmark retrieval of one key from one groupcache node.
func BenchmarkGroupcacheRetrieval(b *testing.B) {
	b.Run("h2c", func(b *testing.B) {
		fetcher := galaxyhttp.NewHTTPFetchProtocol(&galaxyhttp.HTTPOptions{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
			BasePath: basePath,
		})

		f, err := fetcher.NewFetcher(selfURLH2C)
		testutil.Ok(b, err)

		b.Run("seq", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, _, err = f.Fetch(context.Background(), groupName, cachekey.BucketCacheKey{Verb: "content", Name: "test"}.String())
				testutil.Ok(b, err)
			}
		})
		b.Run("parallel=500", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			ch := make(chan struct{})

			for i := 0; i < 500; i++ {
				go func() {
					for range ch {
						_, _, err = f.Fetch(context.Background(), groupName, cachekey.BucketCacheKey{Verb: "content", Name: "test"}.String())
						testutil.Ok(b, err)
					}
				}()
			}

			for i := 0; i < b.N; i++ {
				ch <- struct{}{}
			}
			close(ch)
		})
	})
	b.Run("h1, max one TCP connection", func(b *testing.B) {
		fetcher := galaxyhttp.NewHTTPFetchProtocol(&galaxyhttp.HTTPOptions{
			BasePath: basePath,
			Transport: &http.Transport{
				MaxConnsPerHost:     1,
				MaxIdleConnsPerHost: 1,
			},
		})

		f, err := fetcher.NewFetcher(selfURLH1)
		testutil.Ok(b, err)

		b.Run("seq", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, _, err = f.Fetch(context.Background(), groupName, cachekey.BucketCacheKey{Verb: "content", Name: "test"}.String())
				testutil.Ok(b, err)
			}
		})

		b.Run("parallel=500", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			ch := make(chan struct{})

			for i := 0; i < 500; i++ {
				go func() {
					for range ch {
						_, _, err = f.Fetch(context.Background(), groupName, cachekey.BucketCacheKey{Verb: "content", Name: "test"}.String())
						testutil.Ok(b, err)
					}
				}()
			}

			for i := 0; i < b.N; i++ {
				ch <- struct{}{}
			}
			close(ch)
		})
	})
	b.Run("h1, unlimited TCP connections", func(b *testing.B) {
		fetcher := galaxyhttp.NewHTTPFetchProtocol(&galaxyhttp.HTTPOptions{
			BasePath:  basePath,
			Transport: &http.Transport{},
		})

		f, err := fetcher.NewFetcher(selfURLH1)
		testutil.Ok(b, err)

		b.Run("seq", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, _, err = f.Fetch(context.Background(), groupName, cachekey.BucketCacheKey{Verb: "content", Name: "test"}.String())
				testutil.Ok(b, err)
			}
		})

		b.Run("parallel=500", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			ch := make(chan struct{})

			for i := 0; i < 500; i++ {
				go func() {
					for range ch {
						_, _, err = f.Fetch(context.Background(), groupName, cachekey.BucketCacheKey{Verb: "content", Name: "test"}.String())
						testutil.Ok(b, err)
					}
				}()
			}

			for i := 0; i < b.N; i++ {
				ch <- struct{}{}
			}
			close(ch)
		})
	})

}
