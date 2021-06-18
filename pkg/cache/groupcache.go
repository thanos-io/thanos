// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/mailgun/groupcache/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"gopkg.in/yaml.v2"
)

type Groupcache struct {
	dns    *dns.Provider
	Pool   *groupcache.HTTPPool
	group  *groupcache.Group
	logger log.Logger
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

type ExistsVerb struct {
	Name string
}

type ContentVerb struct {
	Name string
}

type IterVerb struct {
	Name string
}

type SubrangeVerb struct {
	Name       string
	Start, End int64
}

type AttributesVerb struct {
	Name string
}

func ParseGroupcacheKey(key string) interface{} {
	if strings.HasPrefix(key, "attrs:") {
		return &AttributesVerb{Name: key[6:]}
	}
	if strings.HasPrefix(key, "iter:") {
		return &IterVerb{Name: key[5:]}
	}
	if strings.HasPrefix(key, "exists:") {
		return &ExistsVerb{Name: key[7:]}
	}
	if strings.HasPrefix(key, "content:") {
		return &ContentVerb{Name: key[8:]}
	}
	if strings.HasPrefix(key, "subrange:") {
		r := regexp.MustCompile(`subrange:(?P<Name>.+):(?P<Start>\d+):(?P<End>\d+)`)
		matches := r.FindStringSubmatch(key)

		start, _ := strconv.ParseInt(matches[2], 10, 64)
		end, _ := strconv.ParseInt(matches[3], 10, 64)

		return &SubrangeVerb{Name: matches[1], Start: start, End: end}
	}

	panic("unsupported verb")
}

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
func NewGroupcache(name string, logger log.Logger, reg prometheus.Registerer, conf []byte, groupname, basepath string, r *route.Router, bucket objstore.Bucket) (*Groupcache, error) {
	config, err := parseGroupcacheConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewGroupcacheWithConfig(name, logger, reg, config, groupname, basepath, r, bucket)
}

// NewGroupcacheWithConfig creates a new Groupcache instance with the given config.
func NewGroupcacheWithConfig(name string, logger log.Logger, reg prometheus.Registerer, conf GroupcacheConfig, groupname, basepath string, r *route.Router, bucket objstore.Bucket) (*Groupcache, error) {
	dnsGroupcacheProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_store_groupcache_", reg),
		dns.ResolverType(conf.DNSSDResolver),
	)

	pool := groupcache.NewHTTPPoolOpts(conf.SelfURL, &groupcache.HTTPPoolOptions{
		BasePath: basepath,
	})

	ticker := time.NewTicker(conf.DNSInterval)

	go func() {
		for {
			if err := dnsGroupcacheProvider.Resolve(context.Background(), conf.Peers); err != nil {
				level.Error(logger).Log("msg", "failed to resolve addresses for groupcache", "err", err)
			} else {
				pool.Set(dnsGroupcacheProvider.Addresses()...)
			}

			<-ticker.C
		}
	}()

	r.Get(basepath, pool.ServeHTTP)

	group := groupcache.NewGroup(conf.GroupcacheGroup, int64(conf.MaxSize), groupcache.GetterFunc(
		func(ctx context.Context, id string, dest groupcache.Sink) error {
			parsedData := ParseGroupcacheKey(id)

			switch v := parsedData.(type) {
			case *AttributesVerb:
				attrs, err := bucket.Attributes(ctx, v.Name)
				if err != nil {
					return err
				}

				finalAttrs, err := json.Marshal(attrs)
				if err != nil {
					return err
				}
				err = dest.SetString(string(finalAttrs), time.Now().Add(5*time.Minute))
				if err != nil {
					return err
				}
			case *IterVerb:
				// Not supported.

				return nil
			case *ContentVerb:
				rc, err := bucket.Get(ctx, v.Name)
				if err != nil {
					return err
				}
				defer runutil.CloseWithLogOnErr(logger, rc, "closing get")

				b, err := ioutil.ReadAll(rc)
				if err != nil {
					return err
				}

				err = dest.SetBytes(b, time.Now().Add(5*time.Minute))
				if err != nil {
					return err
				}
			case *ExistsVerb:
				exists, err := bucket.Exists(ctx, v.Name)
				if err != nil {
					return err
				}

				err = dest.SetString(strconv.FormatBool(exists), time.Now().Add(5*time.Minute))
				if err != nil {
					return err
				}
			case *SubrangeVerb:
				rc, err := bucket.GetRange(ctx, v.Name, v.Start, v.End-v.Start)
				if err != nil {
					return err
				}
				defer runutil.CloseWithLogOnErr(logger, rc, "closing get_range")

				b, err := ioutil.ReadAll(rc)
				if err != nil {
					return err
				}

				err = dest.SetBytes(b, time.Now().Add(5*time.Minute))
				if err != nil {
					return err
				}
			}

			return nil
		},
	))

	return &Groupcache{
		dns:    dnsGroupcacheProvider,
		Pool:   pool,
		group:  group,
		logger: logger,
	}, nil
}

func (c *Groupcache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	// Noop since cache is already filled during fetching.
}

func (c *Groupcache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	data := map[string][]byte{}

	for _, k := range keys {
		var keyData []byte

		if err := c.group.Get(ctx, k, groupcache.AllocatingByteSliceSink(&keyData)); err != nil {
			level.Error(c.logger).Log("msg", "failed fetching data from groupcache", "err", err, "key", k)
			return nil
		}

		data[k] = keyData
	}

	return data
}
