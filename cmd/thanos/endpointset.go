// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/cache"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/query"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// fileContent is the interface of methods that we need from extkingpin.PathOrContent.
// We need to abstract it for now so we can implement a default if the user does not provide one.
type fileContent interface {
	Content() ([]byte, error)
	Path() string
}

// Maps to crypto/tls.Config fields.
// See https://pkg.go.dev/crypto/tls#Config
type tlsConfig struct {
	Enabled                  *bool   `yaml:"enabled"`
	InsecureSkipVerification *bool   `yaml:"insecure_skip_verify"`
	CertFile                 *string `yaml:"cert_file"`
	KeyFile                  *string `yaml:"key_file"`
	CAFile                   *string `yaml:"ca_file"`
	MinVersion               *string `yaml:"min_version"`
}

type clientConfig struct {
	TLSConfig   tlsConfig `yaml:"tls_config"`
	ServerName  string    `yaml:"server_name"`
	Compression string    `yaml:"compression"`
}

type endpointSettings struct {
	Strict        bool         `yaml:"strict"`
	Group         bool         `yaml:"group"`
	Address       string       `yaml:"address"`
	ServiceConfig string       `yaml:"service_config"`
	ClientConfig  clientConfig `yaml:"client_config"`
}

type EndpointConfig struct {
	Endpoints []endpointSettings `yaml:"endpoints"`
}

type endpointConfigProvider struct {
	mu  sync.Mutex
	cfg EndpointConfig

	// statically defined endpoints from flags for backwards compatibility
	endpoints            []string
	endpointGroups       []string
	strictEndpoints      []string
	strictEndpointGroups []string
}

func (cfg *clientConfig) UseGlobalTLSOpts() bool {
	return cfg.TLSConfig.Enabled == nil &&
		cfg.TLSConfig.CertFile == nil &&
		cfg.TLSConfig.KeyFile == nil &&
		cfg.TLSConfig.CAFile == nil &&
		cfg.ServerName == "" &&
		cfg.TLSConfig.MinVersion == nil &&
		cfg.TLSConfig.InsecureSkipVerification == nil
}

// hasTLSConfig returns true if user specified any TLS-specific configuration.
// This is it set TLS even if 'enabled' is not explicitly set.
func (tc *tlsConfig) hasTLSConfig() bool {
	return tc.CertFile != nil || tc.KeyFile != nil || tc.CAFile != nil || tc.InsecureSkipVerification != nil
}

// applyDefaults sets default values for nil pointer fields.
// If fallback is provided, uses those values before falling back to defaults.
// Call this only after UseGlobalTLSOpts() check, otherwise that check will always return false.
func (tc *tlsConfig) applyDefaults(fallback *tlsConfig) {
	if tc.Enabled == nil {
		// If user provided any TLS config (cert/key/CA/insecure_skip_verify),
		// we assume they want TLS enabled rather than using global's enabled value
		if tc.hasTLSConfig() {
			defaultVal := true
			tc.Enabled = &defaultVal
		} else if fallback != nil && fallback.Enabled != nil {
			tc.Enabled = fallback.Enabled
		} else {
			defaultVal := false
			tc.Enabled = &defaultVal
		}
	}
	if tc.InsecureSkipVerification == nil {
		if fallback != nil && fallback.InsecureSkipVerification != nil {
			tc.InsecureSkipVerification = fallback.InsecureSkipVerification
		} else {
			defaultVal := false
			tc.InsecureSkipVerification = &defaultVal
		}
	}
	if tc.CertFile == nil {
		if fallback != nil && fallback.CertFile != nil {
			tc.CertFile = fallback.CertFile
		} else {
			defaultVal := ""
			tc.CertFile = &defaultVal
		}
	}
	if tc.KeyFile == nil {
		if fallback != nil && fallback.KeyFile != nil {
			tc.KeyFile = fallback.KeyFile
		} else {
			defaultVal := ""
			tc.KeyFile = &defaultVal
		}
	}
	if tc.CAFile == nil {
		if fallback != nil && fallback.CAFile != nil {
			tc.CAFile = fallback.CAFile
		} else {
			defaultVal := ""
			tc.CAFile = &defaultVal
		}
	}
	if tc.MinVersion == nil {
		if fallback != nil && fallback.MinVersion != nil {
			tc.MinVersion = fallback.MinVersion
		} else {
			defaultVal := "1.3"
			tc.MinVersion = &defaultVal
		}
	}
}

func (cfg *clientConfig) validateCompression() error {
	if cfg.Compression == "" {
		cfg.Compression = "none"
	}
	if cfg.Compression != "none" && cfg.Compression != "snappy" {
		return errors.Newf("invalid compression: %s, must be 'none' or 'snappy'", cfg.Compression)
	}
	return nil
}

func (er *endpointConfigProvider) config() EndpointConfig {
	er.mu.Lock()
	defer er.mu.Unlock()

	res := EndpointConfig{Endpoints: make([]endpointSettings, len(er.cfg.Endpoints))}
	copy(res.Endpoints, er.cfg.Endpoints)
	return res
}

func (er *endpointConfigProvider) parse(configFile fileContent) (EndpointConfig, error) {
	content, err := configFile.Content()
	if err != nil {
		return EndpointConfig{}, errors.Wrapf(err, "unable to load config content: %s", configFile.Path())
	}
	var cfg EndpointConfig
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return EndpointConfig{}, errors.Wrapf(err, "unable to unmarshal config content: %s", configFile.Path())
	}
	return cfg, nil
}

func (er *endpointConfigProvider) addStaticEndpoints(cfg *EndpointConfig) {
	for _, e := range er.endpoints {
		cfg.Endpoints = append(cfg.Endpoints, endpointSettings{
			Address: e,
		})
	}
	for _, e := range er.endpointGroups {
		cfg.Endpoints = append(cfg.Endpoints, endpointSettings{
			Address: e,
			Group:   true,
		})
	}
	for _, e := range er.strictEndpoints {
		cfg.Endpoints = append(cfg.Endpoints, endpointSettings{
			Address: e,
			Strict:  true,
		})
	}
	for _, e := range er.strictEndpointGroups {
		cfg.Endpoints = append(cfg.Endpoints, endpointSettings{
			Address: e,
			Group:   true,
			Strict:  true,
		})
	}
}

func validateEndpointConfig(cfg *EndpointConfig) error {
	for i := range cfg.Endpoints {
		ecfg := &cfg.Endpoints[i]
		if dns.IsDynamicNode(ecfg.Address) && ecfg.Strict {
			return errors.Newf("%s is a dynamically specified endpoint i.e. it uses SD and that is not permitted under strict mode.", ecfg.Address)
		}
		if !ecfg.Group && len(ecfg.ServiceConfig) != 0 {
			return errors.Newf("%s service_config is only valid for endpoint groups.", ecfg.Address)
		}
		if err := ecfg.ClientConfig.validateCompression(); err != nil {
			return errors.Wrapf(err, "endpoint %s", ecfg.Address)
		}
	}
	return nil
}

func newEndpointConfigProvider(
	logger log.Logger,
	configFile fileContent,
	configReloadInterval time.Duration,
	staticEndpoints []string,
	staticEndpointGroups []string,
	staticStrictEndpoints []string,
	staticStrictEndpointGroups []string,
) (*endpointConfigProvider, error) {
	res := &endpointConfigProvider{
		endpoints:            staticEndpoints,
		endpointGroups:       staticEndpointGroups,
		strictEndpoints:      staticStrictEndpoints,
		strictEndpointGroups: staticStrictEndpointGroups,
	}

	if configFile == nil {
		configFile = extkingpin.NewNopConfig()
	}

	cfg, err := res.parse(configFile)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load config file")
	}
	res.addStaticEndpoints(&cfg)
	if err := validateEndpointConfig(&cfg); err != nil {
		return nil, errors.Wrapf(err, "unable to validate endpoints")
	}
	res.cfg = cfg

	// only static endpoints
	if len(configFile.Path()) == 0 {
		return res, nil
	}

	if err := extkingpin.PathContentReloader(context.Background(), configFile, logger, func() {
		res.mu.Lock()
		defer res.mu.Unlock()

		level.Info(logger).Log("msg", "reloading endpoint config")
		cfg, err := res.parse(configFile)
		if err != nil {
			level.Error(logger).Log("msg", "failed to reload endpoint config", "err", err)
			return
		}
		res.addStaticEndpoints(&cfg)
		if err := validateEndpointConfig(&cfg); err != nil {
			level.Error(logger).Log("msg", "failed to validate endpoint config", "err", err)
			return
		}
		res.cfg = cfg
	}, configReloadInterval); err != nil {
		return nil, errors.Wrapf(err, "unable to create config reloader")
	}
	return res, nil
}

func setupEndpointSet(
	g *run.Group,
	comp component.Component,
	reg prometheus.Registerer,
	logger log.Logger,
	configFile fileContent,
	configReloadInterval time.Duration,
	legacyFileSDFiles []string,
	legacyFileSDInterval time.Duration,
	legacyEndpoints []string,
	legacyEndpointGroups []string,
	legacyStrictEndpoints []string,
	legacyStrictEndpointGroups []string,
	dnsSDResolver string,
	dnsSDInterval time.Duration,
	unhealthyTimeout time.Duration,
	endpointTimeout time.Duration,
	queryTimeout time.Duration,
	dialOpts []grpc.DialOption,
	globalTLSConfig *tlsConfig,
	globalTLSOpt grpc.DialOption,
	globalCompression string,
	injectTestAddresses []string,
	queryConnMetricLabels ...string,
) (*query.EndpointSet, error) {
	configProvider, err := newEndpointConfigProvider(
		logger,
		configFile,
		configReloadInterval,
		legacyEndpoints,
		legacyEndpointGroups,
		legacyStrictEndpoints,
		legacyStrictEndpointGroups,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load config initially")
	}
	// Register resolver for the "thanos:///" scheme for endpoint-groups
	dns.RegisterGRPCResolver(
		logger,
		dns.NewProvider(
			logger,
			extprom.WrapRegistererWithPrefix(fmt.Sprintf("thanos_%s_endpoint_groups_", comp), reg),
			dns.ResolverType(dnsSDResolver),
		),
		dnsSDInterval,
		injectTestAddresses,
	)
	dnsEndpointProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix(fmt.Sprintf("thanos_%s_endpoints_", comp), reg),
		dns.ResolverType(dnsSDResolver),
	)
	duplicatedEndpoints := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: fmt.Sprintf("thanos_%s_duplicated_endpoint_addresses_total", comp),
		Help: "The number of times a duplicated endpoint addresses is detected from the different configs",
	})

	removeDuplicateEndpointSpecs := func(specs []*query.GRPCEndpointSpec) []*query.GRPCEndpointSpec {
		set := make(map[string]*query.GRPCEndpointSpec)
		for _, spec := range specs {
			addr := spec.Addr()
			if _, ok := set[addr]; ok {
				level.Warn(logger).Log("msg", "Duplicate endpoint address is provided", "addr", addr)
				duplicatedEndpoints.Inc()
			}
			set[addr] = spec
		}
		deduplicated := make([]*query.GRPCEndpointSpec, 0, len(set))
		for _, value := range set {
			deduplicated = append(deduplicated, value)
		}
		return deduplicated
	}
	var fileSD *file.Discovery
	if len(legacyFileSDFiles) > 0 {
		conf := &file.SDConfig{
			Files:           legacyFileSDFiles,
			RefreshInterval: model.Duration(legacyFileSDInterval),
		}
		var err error
		if fileSD, err = file.NewDiscovery(conf, logutil.GoKitLogToSlog(logger), conf.NewDiscovererMetrics(reg, discovery.NewRefreshMetrics(reg))); err != nil {
			return nil, fmt.Errorf("unable to create new legacy file sd config: %w", err)
		}
	}
	legacyFileSDCache := cache.New()

	// Perform initial DNS resolution before starting periodic updates.
	// This ensures that DNS providers have addresses when the first endpoint update runs.
	{
		resolveCtx, resolveCancel := context.WithTimeout(context.Background(), dnsSDInterval)
		defer resolveCancel()

		level.Info(logger).Log("msg", "performing initial DNS resolution for endpoints")

		endpointConfig := configProvider.config()
		addresses := make([]string, 0, len(endpointConfig.Endpoints))
		for _, ecfg := range endpointConfig.Endpoints {
			// Only resolve non-group dynamic endpoints here.
			// Group endpoints are resolved by the gRPC resolver in its Build() method.
			if addr := ecfg.Address; dns.IsDynamicNode(addr) && !ecfg.Group {
				addresses = append(addresses, addr)
			}
		}
		// Note: legacyFileSDCache will be empty at this point since file SD hasn't started yet
		if len(addresses) > 0 {
			if err := dnsEndpointProvider.Resolve(resolveCtx, addresses, true); err != nil {
				level.Error(logger).Log("msg", "initial DNS resolution failed", "err", err)
			}
		}

		level.Info(logger).Log("msg", "initial DNS resolution completed")
	}

	ctx, cancel := context.WithCancel(context.Background())

	if fileSD != nil {
		fileSDUpdates := make(chan []*targetgroup.Group)

		g.Add(func() error {
			fileSD.Run(ctx, fileSDUpdates)
			return nil

		}, func(err error) {
			cancel()
		})

		g.Add(func() error {
			for {
				select {
				case update := <-fileSDUpdates:
					// Discoverers sometimes send nil updates so need to check for it to avoid panics.
					if update == nil {
						continue
					}
					legacyFileSDCache.Update(update)
				case <-ctx.Done():
					return nil
				}
			}
		}, func(err error) {
			cancel()
		})
	}

	{
		g.Add(func() error {
			return runutil.Repeat(dnsSDInterval, ctx.Done(), func() error {
				ctxUpdateIter, cancelUpdateIter := context.WithTimeout(ctx, dnsSDInterval)
				defer cancelUpdateIter()

				endpointConfig := configProvider.config()

				addresses := make([]string, 0, len(endpointConfig.Endpoints))
				for _, ecfg := range endpointConfig.Endpoints {
					if addr := ecfg.Address; dns.IsDynamicNode(addr) && !ecfg.Group {
						addresses = append(addresses, addr)
					}
				}
				addresses = append(addresses, legacyFileSDCache.Addresses()...)
				if err := dnsEndpointProvider.Resolve(ctxUpdateIter, addresses, true); err != nil {
					level.Error(logger).Log("msg", "failed to resolve addresses for endpoints", "err", err)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	endpointset := query.NewEndpointSet(time.Now, logger, reg, func() []*query.GRPCEndpointSpec {
		endpointConfig := configProvider.config()

		specs := make([]*query.GRPCEndpointSpec, 0)
		// groups and non dynamic endpoints
		for _, ecfg := range endpointConfig.Endpoints {
			strict, group, addr, tlsEnabled := ecfg.Strict, ecfg.Group, ecfg.Address, ecfg.ClientConfig.TLSConfig.Enabled
			var tlsOpt grpc.DialOption
			useGlobalConfig := ecfg.ClientConfig.UseGlobalTLSOpts()
			if useGlobalConfig {
				tlsOpt = globalTLSOpt
			} else if tlsEnabled == nil {
				// Merging endpoint config with global config as fallback
				ecfg.ClientConfig.TLSConfig.applyDefaults(globalTLSConfig)

				if *ecfg.ClientConfig.TLSConfig.Enabled {
					var err error
					tlsOpt, err = extgrpc.StoreClientTLSCredentials(
						logger,
						*ecfg.ClientConfig.TLSConfig.Enabled,
						*ecfg.ClientConfig.TLSConfig.InsecureSkipVerification,
						*ecfg.ClientConfig.TLSConfig.CertFile,
						*ecfg.ClientConfig.TLSConfig.KeyFile,
						*ecfg.ClientConfig.TLSConfig.CAFile,
						ecfg.ClientConfig.ServerName,
						*ecfg.ClientConfig.TLSConfig.MinVersion,
					)
					if err != nil {
						level.Error(logger).Log("msg", "skipping endpoint due to TLS configuration error", "addr", addr, "err", err)
						continue
					}
				} else {
					// TLS not enabled after merging, use cleartext
					tlsOpt, _ = extgrpc.StoreClientTLSCredentials(logger, false, false, "", "", "", "", "")
				}
			} else if *tlsEnabled {
				// applying defaults for missing TLS config values
				ecfg.ClientConfig.TLSConfig.applyDefaults(nil)

				var err error
				tlsOpt, err = extgrpc.StoreClientTLSCredentials(
					logger,
					*ecfg.ClientConfig.TLSConfig.Enabled,
					*ecfg.ClientConfig.TLSConfig.InsecureSkipVerification,
					*ecfg.ClientConfig.TLSConfig.CertFile,
					*ecfg.ClientConfig.TLSConfig.KeyFile,
					*ecfg.ClientConfig.TLSConfig.CAFile,
					ecfg.ClientConfig.ServerName,
					*ecfg.ClientConfig.TLSConfig.MinVersion,
				)
				if err != nil {
					level.Error(logger).Log("msg", "skipping endpoint due to TLS configuration error", "addr", addr, "err", err)
					continue
				}
			} else {
				// If tlsEnabled is disabled we will use cleartext
				tlsOpt, _ = extgrpc.StoreClientTLSCredentials(logger, false, false, "", "", "", "", "")
			}
			endpointDialOpts := append(dialOpts, tlsOpt)

			var compression string
			if useGlobalConfig {
				compression = globalCompression
			} else {
				compression = ecfg.ClientConfig.Compression
			}
			if compression != "none" {
				endpointDialOpts = append(endpointDialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(compression)))
			}

			if group {
				specs = append(specs, query.NewGRPCEndpointSpec(fmt.Sprintf("thanos:///%s", addr), strict, append(endpointDialOpts, extgrpc.EndpointGroupGRPCOpts(ecfg.ServiceConfig)...)...))
			} else if !dns.IsDynamicNode(addr) {
				specs = append(specs, query.NewGRPCEndpointSpec(addr, strict, endpointDialOpts...))
			}
		}
		// dynamic endpoints
		for _, addr := range dnsEndpointProvider.Addresses() {
			dynamicDialOpts := append(dialOpts, globalTLSOpt)
			if globalCompression != "none" {
				dynamicDialOpts = append(dynamicDialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(globalCompression)))
			}
			specs = append(specs, query.NewGRPCEndpointSpec(addr, false, dynamicDialOpts...))
		}
		return removeDuplicateEndpointSpecs(specs)
	}, unhealthyTimeout, endpointTimeout, queryTimeout, queryConnMetricLabels...)

	g.Add(func() error {
		return runutil.Repeat(endpointTimeout, ctx.Done(), func() error {
			ctxIter, cancelIter := context.WithTimeout(ctx, endpointTimeout)
			defer cancelIter()

			endpointset.Update(ctxIter)
			return nil
		})
	}, func(error) {
		cancel()
	})

	return endpointset, nil
}
