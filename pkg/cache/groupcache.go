// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/cache/cachekey"
	"github.com/vimeo/galaxycache"
	galaxyhttp "github.com/vimeo/galaxycache/http"
	"gopkg.in/yaml.v2"
)

type Groupcache struct {
	galaxy   *galaxycache.Galaxy
	universe *galaxycache.Universe
	logger   log.Logger
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
}

var (
	DefaultGroupcacheConfig = GroupcacheConfig{
		MaxSize:       250 * 1024 * 1024,
		DNSSDResolver: dns.GolangResolverType,
		DNSInterval:   1 * time.Minute,
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

	return config, nil
}

// NewGroupcache creates a new Groupcache instance.
func NewGroupcache(logger log.Logger, reg prometheus.Registerer, conf []byte, basepath string, r *route.Router, bucket objstore.Bucket) (*Groupcache, error) {
	config, err := parseGroupcacheConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewGroupcacheWithConfig(logger, reg, config, basepath, r, bucket)
}

// NewGroupcacheWithConfig creates a new Groupcache instance with the given config.
func NewGroupcacheWithConfig(logger log.Logger, reg prometheus.Registerer, conf GroupcacheConfig, basepath string, r *route.Router, bucket objstore.Bucket) (*Groupcache, error) {
	dnsGroupcacheProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_store_groupcache_", reg),
		dns.ResolverType(conf.DNSSDResolver),
	)

	httpProto := galaxyhttp.NewHTTPFetchProtocol(&galaxyhttp.HTTPOptions{
		BasePath: basepath,
	})
	universe := galaxycache.NewUniverse(httpProto, conf.SelfURL)

	ticker := time.NewTicker(conf.DNSInterval)

	go func() {
		for {
			if err := dnsGroupcacheProvider.Resolve(context.Background(), conf.Peers); err != nil {
				level.Error(logger).Log("msg", "failed to resolve addresses for groupcache", "err", err)
			} else {
				universe.Set(dnsGroupcacheProvider.Addresses()...)
			}

			<-ticker.C
		}
	}()

	mux := http.NewServeMux()
	galaxyhttp.RegisterHTTPHandler(universe, &galaxyhttp.HTTPOptions{
		BasePath: basepath,
	}, mux)
	r.Get(basepath, mux.ServeHTTP)

	galaxy := universe.NewGalaxy(conf.GroupcacheGroup, int64(conf.MaxSize), galaxycache.GetterFunc(
		func(ctx context.Context, id string, dest galaxycache.Codec) error {
			parsedData, err := cachekey.ParseBucketCacheKey(id)
			if err != nil {
				return err
			}

			switch parsedData.Verb {
			case cachekey.AttributesVerb:
				attrs, err := bucket.Attributes(ctx, parsedData.Name)
				if err != nil {
					return err
				}

				finalAttrs, err := json.Marshal(attrs)
				if err != nil {
					return err
				}
				err = dest.UnmarshalBinary(finalAttrs)
				if err != nil {
					return err
				}
			case cachekey.IterVerb:
				// Not supported.

				return nil
			case cachekey.ContentVerb:
				rc, err := bucket.Get(ctx, parsedData.Name)
				if err != nil {
					return err
				}
				defer runutil.CloseWithLogOnErr(logger, rc, "closing get")

				b, err := ioutil.ReadAll(rc)
				if err != nil {
					return err
				}

				err = dest.UnmarshalBinary(b)
				if err != nil {
					return err
				}
			case cachekey.ExistsVerb:
				exists, err := bucket.Exists(ctx, parsedData.Name)
				if err != nil {
					return err
				}

				err = dest.UnmarshalBinary([]byte(strconv.FormatBool(exists)))
				if err != nil {
					return err
				}
			case cachekey.SubrangeVerb:
				rc, err := bucket.GetRange(ctx, parsedData.Name, parsedData.Start, parsedData.End-parsedData.Start)
				if err != nil {
					return err
				}
				defer runutil.CloseWithLogOnErr(logger, rc, "closing get_range")

				b, err := ioutil.ReadAll(rc)
				if err != nil {
					return err
				}

				err = dest.UnmarshalBinary(b)
				if err != nil {
					return err
				}
			}

			return nil
		},
	))

	return &Groupcache{
		logger:   logger,
		galaxy:   galaxy,
		universe: universe,
	}, nil
}

func (c *Groupcache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	// Noop since cache is already filled during fetching.
}

func (c *Groupcache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	data := map[string][]byte{}

	for _, k := range keys {
		codec := galaxycache.ByteCodec{}

		if err := c.galaxy.Get(ctx, k, &codec); err != nil {
			level.Error(c.logger).Log("msg", "failed fetching data from groupcache", "err", err, "key", k)
			continue
		}

		data[k] = codec
	}

	return data
}

func (c *Groupcache) Name() string {
	return c.galaxy.Name()
}
