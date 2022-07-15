// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/objstore"
	"github.com/vimeo/galaxycache"
	galaxyhttp "github.com/vimeo/galaxycache/http"
	"golang.org/x/net/http2"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/cache/cachekey"
)

type Groupcache struct {
	galaxy   *galaxycache.Galaxy
	universe *galaxycache.Universe
	logger   log.Logger
	timeout  time.Duration
}

// GroupcacheConfig holds the in-memory cache config.
type GroupcacheConfig struct {
	// Addresses of statically configured peers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect store API servers through respective DNS lookups.
	// Typically, you'd want something like `dns+http://thanos-store:42/`.
	Peers []string `yaml:"peers"`

	// Address of ourselves in the peer list. This needs to be set to `http://external-ip:HTTP_PORT`
	// of the current instance.
	SelfURL string `yaml:"self_url"`

	// Maximum size of the hot in-memory cache.
	MaxSize model.Bytes `yaml:"max_size"`

	// Group's name. All of the instances need to be using the same group and point to the same bucket.
	GroupcacheGroup string `yaml:"groupcache_group"`

	// DNS SD resolver to use.
	DNSSDResolver dns.ResolverType `yaml:"dns_sd_resolver"`

	// How often we should resolve the addresses.
	DNSInterval time.Duration `yaml:"dns_interval"`

	// Timeout specifies the read/write timeout.
	Timeout time.Duration `yaml:"timeout"`
}

var (
	DefaultGroupcacheConfig = GroupcacheConfig{
		MaxSize:       250 * 1024 * 1024,
		DNSSDResolver: dns.GolangResolverType,
		DNSInterval:   1 * time.Minute,
		Timeout:       2 * time.Second,
	}
)

// parseGroupcacheConfig unmarshals a buffer into a GroupcacheConfig with default values.
func parseGroupcacheConfig(conf []byte) (GroupcacheConfig, error) {
	config := DefaultGroupcacheConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return GroupcacheConfig{}, err
	}

	if len(config.Peers) == 0 {
		config.Peers = append(config.Peers, config.SelfURL)
	}

	for i, peer := range config.Peers {
		// Workaround for https://github.com/thanos-community/galaxycache/blob/master/http/http.go#L205-L210.
		// If the peer has a slash at the end then the router redirects
		// and then the request fails.
		if strings.HasSuffix(peer, "/") {
			return GroupcacheConfig{}, fmt.Errorf("peer %d must not have a trailing slash (%s)", i, peer)
		}
	}
	if strings.HasSuffix(config.SelfURL, "/") {
		return GroupcacheConfig{}, fmt.Errorf("self URL %s must not have a trailing slash", config.SelfURL)
	}

	return config, nil
}

// NewGroupcache creates a new Groupcache instance.
func NewGroupcache(logger log.Logger, reg prometheus.Registerer, conf []byte, basepath string, r *route.Router, bucket objstore.Bucket, cfg *CachingBucketConfig) (*Groupcache, error) {
	config, err := parseGroupcacheConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewGroupcacheWithConfig(logger, reg, config, basepath, r, bucket, cfg)
}

// NewGroupcacheWithConfig creates a new Groupcache instance with the given config.
func NewGroupcacheWithConfig(logger log.Logger, reg prometheus.Registerer, conf GroupcacheConfig, basepath string, r *route.Router, bucket objstore.Bucket,
	cfg *CachingBucketConfig) (*Groupcache, error) {
	httpProto := galaxyhttp.NewHTTPFetchProtocol(&galaxyhttp.HTTPOptions{
		BasePath: basepath,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	})
	universe := galaxycache.NewUniverse(httpProto, conf.SelfURL)

	dnsGroupcacheProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_store_groupcache_", reg),
		dns.ResolverType(conf.DNSSDResolver),
	)
	ticker := time.NewTicker(conf.DNSInterval)

	go func() {
		for {
			if err := dnsGroupcacheProvider.Resolve(context.Background(), conf.Peers); err != nil {
				level.Error(logger).Log("msg", "failed to resolve addresses for groupcache", "err", err)
			} else {
				err := universe.Set(dnsGroupcacheProvider.Addresses()...)
				if err != nil {
					level.Error(logger).Log("msg", "failed to set peers for groupcache", "err", err)
				}
			}

			<-ticker.C
		}
	}()

	mux := http.NewServeMux()
	galaxyhttp.RegisterHTTPHandler(universe, &galaxyhttp.HTTPOptions{
		BasePath: basepath,
	}, mux)
	r.Get(filepath.Join(basepath, conf.GroupcacheGroup, "*key"), mux.ServeHTTP)

	galaxy := universe.NewGalaxy(conf.GroupcacheGroup, int64(conf.MaxSize), galaxycache.GetterFunc(
		func(ctx context.Context, id string, dest galaxycache.Codec) error {
			parsedData, err := cachekey.ParseBucketCacheKey(id)
			if err != nil {
				return err
			}

			switch parsedData.Verb {
			case cachekey.AttributesVerb:
				_, attrCfg := cfg.FindAttributesConfig(parsedData.Name)
				if attrCfg == nil {
					panic("caching bucket layer must not call on unconfigured paths")
				}

				attrs, err := bucket.Attributes(ctx, parsedData.Name)
				if err != nil {
					return err
				}

				finalAttrs, err := json.Marshal(attrs)
				if err != nil {
					return err
				}

				return dest.UnmarshalBinary(finalAttrs, time.Now().Add(attrCfg.TTL))
			case cachekey.IterVerb:
				_, iterCfg := cfg.FindIterConfig(parsedData.Name)
				if iterCfg == nil {
					panic("caching bucket layer must not call on unconfigured paths")
				}

				var list []string
				if err := bucket.Iter(ctx, parsedData.Name, func(s string) error {
					list = append(list, s)
					return nil
				}); err != nil {
					return err
				}

				encodedList, err := json.Marshal(list)
				if err != nil {
					return err
				}

				return dest.UnmarshalBinary(encodedList, time.Now().Add(iterCfg.TTL))
			case cachekey.ContentVerb:
				_, contentCfg := cfg.FindGetConfig(parsedData.Name)
				if contentCfg == nil {
					panic("caching bucket layer must not call on unconfigured paths")
				}
				rc, err := bucket.Get(ctx, parsedData.Name)
				if err != nil {
					return err
				}
				defer runutil.CloseWithLogOnErr(logger, rc, "closing get")

				b, err := ioutil.ReadAll(rc)
				if err != nil {
					return err
				}

				return dest.UnmarshalBinary(b, time.Now().Add(contentCfg.ContentTTL))
			case cachekey.ExistsVerb:
				_, existsCfg := cfg.FindExistConfig(parsedData.Name)
				if existsCfg == nil {
					panic("caching bucket layer must not call on unconfigured paths")
				}
				exists, err := bucket.Exists(ctx, parsedData.Name)
				if err != nil {
					return err
				}

				if exists {
					return dest.UnmarshalBinary([]byte(strconv.FormatBool(exists)), time.Now().Add(existsCfg.ExistsTTL))
				} else {
					return dest.UnmarshalBinary([]byte(strconv.FormatBool(exists)), time.Now().Add(existsCfg.DoesntExistTTL))
				}

			case cachekey.SubrangeVerb:
				_, subrangeCfg := cfg.FindGetRangeConfig(parsedData.Name)
				if subrangeCfg == nil {
					panic("caching bucket layer must not call on unconfigured paths")
				}
				rc, err := bucket.GetRange(ctx, parsedData.Name, parsedData.Start, parsedData.End-parsedData.Start)
				if err != nil {
					return err
				}
				defer runutil.CloseWithLogOnErr(logger, rc, "closing get_range")

				b, err := ioutil.ReadAll(rc)
				if err != nil {
					return err
				}

				return dest.UnmarshalBinary(b, time.Now().Add(subrangeCfg.SubrangeTTL))

			}

			return nil
		},
	))

	RegisterCacheStatsCollector(galaxy, &conf, reg)

	return &Groupcache{
		logger:   logger,
		galaxy:   galaxy,
		universe: universe,
		timeout:  conf.Timeout,
	}, nil
}

// unsafeByteCodec is a byte slice type that implements Codec.
type unsafeByteCodec struct {
	bytes  []byte
	expire time.Time
}

// MarshalBinary returns the contained byte-slice.
func (c *unsafeByteCodec) MarshalBinary() ([]byte, time.Time, error) {
	return c.bytes, c.expire, nil
}

// UnmarshalBinary to provided data so they share the same backing array
// this is a generally unsafe performance optimization, but safe in our
// case because we always use ioutil.ReadAll(). That is fine though
// because later that slice remains in our local cache.
// Used https://github.com/vimeo/galaxycache/pull/23/files as inspiration.
// TODO(GiedriusS): figure out if pooling could be used somehow by hooking into
// eviction.
func (c *unsafeByteCodec) UnmarshalBinary(data []byte, expire time.Time) error {
	c.bytes = data
	c.expire = expire
	return nil
}

func (c *Groupcache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	// Noop since cache is already filled during fetching.
}

func (c *Groupcache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	data := map[string][]byte{}

	if c.timeout != 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
		ctx = timeoutCtx
		defer cancel()
	}

	for _, k := range keys {
		codec := unsafeByteCodec{}

		if err := c.galaxy.Get(ctx, k, &codec); err != nil {
			level.Debug(c.logger).Log("msg", "failed fetching data from groupcache", "err", err, "key", k)
			continue
		}

		retrievedData, _, err := codec.MarshalBinary()
		if err != nil {
			level.Debug(c.logger).Log("msg", "failed retrieving data", "err", err, "key", k)
			continue
		}

		if len(retrievedData) > 0 {
			data[k] = retrievedData
		}
	}

	return data
}

func (c *Groupcache) Name() string {
	return c.galaxy.Name()
}

type CacheStatsCollector struct {
	galaxy *galaxycache.Galaxy
	conf   *GroupcacheConfig

	// GalaxyCache Metric descriptions.
	bytes             *prometheus.Desc
	evictions         *prometheus.Desc
	items             *prometheus.Desc
	maxBytes          *prometheus.Desc
	gets              *prometheus.Desc
	loads             *prometheus.Desc
	peerLoads         *prometheus.Desc
	peerLoadErrors    *prometheus.Desc
	backendLoads      *prometheus.Desc
	backendLoadErrors *prometheus.Desc
	cacheHits         *prometheus.Desc
}

// RegisterCacheStatsCollector registers a groupcache metrics collector.
func RegisterCacheStatsCollector(galaxy *galaxycache.Galaxy, conf *GroupcacheConfig, reg prometheus.Registerer) {
	// Cache metrics.
	bytes := prometheus.NewDesc("thanos_cache_groupcache_bytes", "The number of bytes in the main cache.", []string{"cache"}, nil)
	evictions := prometheus.NewDesc("thanos_cache_groupcache_evictions_total", "The number items evicted from the cache.", []string{"cache"}, nil)
	items := prometheus.NewDesc("thanos_cache_groupcache_items", "The number of items in the cache.", []string{"cache"}, nil)

	// Configuration Metrics.
	maxBytes := prometheus.NewDesc("thanos_cache_groupcache_max_bytes", "The max number of bytes in the cache.", nil, nil)

	// GroupCache metrics.
	gets := prometheus.NewDesc("thanos_cache_groupcache_get_requests_total", "Total number of get requests, including from peers.", nil, nil)
	loads := prometheus.NewDesc("thanos_cache_groupcache_loads_total", "Total number of loads from backend (gets - cacheHits).", nil, nil)
	peerLoads := prometheus.NewDesc("thanos_cache_groupcache_peer_loads_total", "Total number of loads from peers (remote load or remote cache hit).", nil, nil)
	peerLoadErrors := prometheus.NewDesc("thanos_cache_groupcache_peer_load_errors_total", "Total number of errors from peer loads.", nil, nil)
	backendLoads := prometheus.NewDesc("thanos_cache_groupcache_backend_loads_total", "Total number of direct backend loads.", nil, nil)
	backendLoadErrors := prometheus.NewDesc("thanos_cache_groupcache_backend_load_errors_total", "Total number of errors on direct backend loads.", nil, nil)
	cacheHits := prometheus.NewDesc("thanos_cache_groupcache_hits_total", "Total number of cache hits.", []string{"type"}, nil)

	collector := &CacheStatsCollector{
		galaxy:            galaxy,
		conf:              conf,
		bytes:             bytes,
		evictions:         evictions,
		items:             items,
		maxBytes:          maxBytes,
		gets:              gets,
		loads:             loads,
		peerLoads:         peerLoads,
		peerLoadErrors:    peerLoadErrors,
		backendLoads:      backendLoads,
		backendLoadErrors: backendLoadErrors,
		cacheHits:         cacheHits,
	}
	reg.MustRegister(collector)
}

func (s *CacheStatsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, cache := range []galaxycache.CacheType{galaxycache.MainCache, galaxycache.HotCache} {
		cacheStats := s.galaxy.CacheStats(cache)
		ch <- prometheus.MustNewConstMetric(s.bytes, prometheus.GaugeValue, float64(cacheStats.Bytes), cache.String())
		ch <- prometheus.MustNewConstMetric(s.evictions, prometheus.GaugeValue, float64(cacheStats.Evictions), cache.String())
		ch <- prometheus.MustNewConstMetric(s.items, prometheus.GaugeValue, float64(cacheStats.Items), cache.String())
	}

	ch <- prometheus.MustNewConstMetric(s.maxBytes, prometheus.GaugeValue, float64(s.conf.MaxSize))
	ch <- prometheus.MustNewConstMetric(s.gets, prometheus.CounterValue, float64(s.galaxy.Stats.Gets.Get()))
	ch <- prometheus.MustNewConstMetric(s.loads, prometheus.CounterValue, float64(s.galaxy.Stats.Loads.Get()))
	ch <- prometheus.MustNewConstMetric(s.peerLoads, prometheus.CounterValue, float64(s.galaxy.Stats.PeerLoads.Get()))
	ch <- prometheus.MustNewConstMetric(s.peerLoadErrors, prometheus.CounterValue, float64(s.galaxy.Stats.PeerLoadErrors.Get()))
	ch <- prometheus.MustNewConstMetric(s.backendLoads, prometheus.CounterValue, float64(s.galaxy.Stats.BackendLoads.Get()))
	ch <- prometheus.MustNewConstMetric(s.backendLoadErrors, prometheus.CounterValue, float64(s.galaxy.Stats.BackendLoadErrors.Get()))
	ch <- prometheus.MustNewConstMetric(s.cacheHits, prometheus.CounterValue, float64(s.galaxy.Stats.MaincacheHits.Get()), galaxycache.MainCache.String())
	ch <- prometheus.MustNewConstMetric(s.cacheHits, prometheus.CounterValue, float64(s.galaxy.Stats.HotcacheHits.Get()), galaxycache.HotCache.String())
}

func (s *CacheStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(s, ch)
}
