// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/exthttp"
	thanosmodel "github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/reloader"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"

	"gopkg.in/alecthomas/kingpin.v2"
)

func registerSidecar(m map[string]setupFunc, app *kingpin.Application) {
	cmd := app.Command(component.Sidecar.String(), "sidecar for Prometheus server")

	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := regGRPCFlags(cmd)

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API. For better performance use local network.").
		Default("http://localhost:9090").URL()

	promReadyTimeout := cmd.Flag("prometheus.ready_timeout", "Maximum time to wait for the Prometheus instance to start up").
		Default("10m").Duration()

	connectionPoolSize := cmd.Flag("receive.connection-pool-size", "Controls the http MaxIdleConns. Default is 0, which is unlimited").Int()
	connectionPoolSizePerHost := cmd.Flag("receive.connection-pool-size-per-host", "Controls the http MaxIdleConnsPerHost").Default("100").Int()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	reloaderCfgFile := cmd.Flag("reloader.config-file", "Config file watched by the reloader.").
		Default("").String()

	reloaderCfgOutputFile := cmd.Flag("reloader.config-envsubst-file", "Output file for environment variable substituted config file.").
		Default("").String()

	reloaderRuleDirs := cmd.Flag("reloader.rule-dir", "Rule directories for the reloader to refresh (repeated field).").Strings()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", false)

	uploadCompacted := cmd.Flag("shipper.upload-compacted", "If true sidecar will try to upload compacted blocks as well. Useful for migration purposes. Works only if compaction is disabled on Prometheus. Do it once and then disable the flag when done.").Default("false").Bool()

	ignoreBlockSize := cmd.Flag("shipper.ignore-unequal-block-size", "If true sidecar will not require prometheus min and max block size flags to be set to the same value. Only use this if you want to keep long retention and compaction enabled on your Prometheus instance, as in the worst case it can result in ~2h data loss for your Thanos bucket storage.").Default("false").Hidden().Bool()

	minTime := thanosmodel.TimeOrDuration(cmd.Flag("min-time", "Start of time range limit to serve. Thanos sidecar will serve only metrics, which happened later than this value. Option can be a constant time in RFC3339 format or time duration relative to current time, such as -1d or 2h45m. Valid duration units are ms, s, m, h, d, w, y.").
		Default("0000-01-01T00:00:00Z"))

	m[component.Sidecar.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ <-chan struct{}, _ bool) error {
		rl := reloader.New(
			log.With(logger, "component", "reloader"),
			reloader.ReloadURLFromBase(*promURL),
			*reloaderCfgFile,
			*reloaderCfgOutputFile,
			*reloaderRuleDirs,
		)

		return runSidecar(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			*promURL,
			*promReadyTimeout,
			*dataDir,
			objStoreConfig,
			rl,
			*uploadCompacted,
			*ignoreBlockSize,
			component.Sidecar,
			*minTime,
			*connectionPoolSize,
			*connectionPoolSizePerHost,
		)
	}
}

func runSidecar(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	promURL *url.URL,
	promReadyTimeout time.Duration,
	dataDir string,
	objStoreConfig *extflag.PathOrContent,
	reloader *reloader.Reloader,
	uploadCompacted bool,
	ignoreBlockSize bool,
	comp component.Component,
	limitMinTime thanosmodel.TimeOrDurationValue,
	connectionPoolSize int,
	connectionPoolSizePerHost int,
) error {
	var m = &promMetadata{
		promURL: promURL,

		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		mint: limitMinTime.PrometheusTimestamp(),
		maxt: math.MaxInt64,

		limitMinTime: limitMinTime,
		client:       promclient.NewWithTracingClient(logger, "thanos-sidecar"),
	}

	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return errors.Wrap(err, "getting object store config")
	}

	var uploads = true
	if len(confContentYaml) == 0 {
		level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
		uploads = false
	}

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg)),
	)

	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(httpBindAddr),
		httpserver.WithGracePeriod(httpGracePeriod),
	)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	// Setup all the concurrent groups.
	{
		promUp := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its Prometheus peer.",
		})
		lastHeartbeat := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "thanos_sidecar_last_heartbeat_success_time_seconds",
			Help: "Second timestamp of the last successful heartbeat.",
		})
		reg.MustRegister(promUp, lastHeartbeat)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// Only check Prometheus's flags when upload is enabled.
			if uploads {
				// Check prometheus's flags to ensure sane sidecar flags.
				if err := validatePrometheus(ctx, m.client, logger, ignoreBlockSize, m); err != nil {
					return errors.Wrap(err, "validate Prometheus flags")
				}
			}

			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				if err := m.UpdateLabels(ctx); err != nil {
					level.Warn(logger).Log(
						"msg", "failed to fetch initial external labels. Is Prometheus running? Retrying",
						"err", err,
					)
					promUp.Set(0)
					statusProber.NotReady(err)
					return err
				}

				level.Info(logger).Log(
					"msg", "successfully loaded prometheus external labels",
					"external_labels", m.Labels().String(),
				)
				promUp.Set(1)
				statusProber.Ready()
				lastHeartbeat.SetToCurrentTime()
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			if len(m.Labels()) == 0 {
				return errors.New("no external labels configured on Prometheus server, uniquely identifying external labels must be configured")
			}

			// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
			// the external labels we apply.
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				if err := m.UpdateLabels(iterCtx); err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					promUp.Set(1)
					lastHeartbeat.SetToCurrentTime()
				}

				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return reloader.Watch(ctx)
		}, func(error) {
			cancel()
		})
	}

	{
		t := exthttp.NewTransport()
		t.MaxIdleConnsPerHost = connectionPoolSizePerHost
		t.MaxIdleConns = connectionPoolSize
		c := &http.Client{Transport: tracing.HTTPTripperware(logger, t)}

		promStore, err := store.NewPrometheusStore(logger, c, promURL, component.Sidecar, m.Labels, m.Timestamps)
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, comp, grpcProbe, promStore, promStore,
			grpcserver.WithListen(grpcBindAddr),
			grpcserver.WithGracePeriod(grpcGracePeriod),
			grpcserver.WithTLSConfig(tlsCfg),
		)
		g.Add(func() error {
			statusProber.Ready()
			return s.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			s.Shutdown(err)
		})
	}

	if uploads {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Sidecar.String())
		if err != nil {
			return err
		}

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		if err := promclient.IsWALDirAccessible(dataDir); err != nil {
			level.Error(logger).Log("err", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			extLabelsCtx, cancel := context.WithTimeout(ctx, promReadyTimeout)
			defer cancel()

			if err := runutil.Retry(2*time.Second, extLabelsCtx.Done(), func() error {
				if len(m.Labels()) == 0 {
					return errors.New("not uploading as no external labels are configured yet - is Prometheus healthy/reachable?")
				}
				return nil
			}); err != nil {
				return errors.Wrapf(err, "aborting as no external labels found after waiting %s", promReadyTimeout)
			}

			var s *shipper.Shipper
			if uploadCompacted {
				s = shipper.NewWithCompacted(logger, reg, dataDir, bkt, m.Labels, metadata.SidecarSource)
			} else {
				s = shipper.New(logger, reg, dataDir, bkt, m.Labels, metadata.SidecarSource)
			}

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if uploaded, err := s.Sync(ctx); err != nil {
					level.Warn(logger).Log("err", err, "uploaded", uploaded)
				}

				minTime, _, err := s.Timestamps()
				if err != nil {
					level.Warn(logger).Log("msg", "reading timestamps failed", "err", err)
					return nil
				}
				m.UpdateTimestamps(minTime, math.MaxInt64)
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "starting sidecar")
	return nil
}

func validatePrometheus(ctx context.Context, client *promclient.Client, logger log.Logger, ignoreBlockSize bool, m *promMetadata) error {
	var (
		flagErr error
		flags   promclient.Flags
	)

	if err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
		if flags, flagErr = client.ConfiguredFlags(ctx, m.promURL); flagErr != nil && flagErr != promclient.ErrFlagEndpointNotFound {
			level.Warn(logger).Log("msg", "failed to get Prometheus flags. Is Prometheus running? Retrying", "err", flagErr)
			return errors.Wrapf(flagErr, "fetch Prometheus flags")
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "fetch Prometheus flags")
	}

	if flagErr != nil {
		level.Warn(logger).Log("msg", "failed to check Prometheus flags, due to potentially older Prometheus. No extra validation is done.", "err", flagErr)
		return nil
	}

	// Check if compaction is disabled.
	if flags.TSDBMinTime != flags.TSDBMaxTime {
		if !ignoreBlockSize {
			return errors.Errorf("found that TSDB Max time is %s and Min time is %s. "+
				"Compaction needs to be disabled (storage.tsdb.min-block-duration = storage.tsdb.max-block-duration)", flags.TSDBMaxTime, flags.TSDBMinTime)
		}
		level.Warn(logger).Log("msg", "flag to ignore Prometheus min/max block duration flags differing is being used. If the upload of a 2h block fails and a Prometheus compaction happens that block may be missing from your Thanos bucket storage.")
	}
	// Check if block time is 2h.
	if flags.TSDBMinTime != model.Duration(2*time.Hour) {
		level.Warn(logger).Log("msg", "found that TSDB block time is not 2h. Only 2h block time is recommended.", "block-time", flags.TSDBMinTime)
	}

	return nil
}

type promMetadata struct {
	promURL *url.URL

	mtx    sync.Mutex
	mint   int64
	maxt   int64
	labels labels.Labels

	limitMinTime thanosmodel.TimeOrDurationValue

	client *promclient.Client
}

func (s *promMetadata) UpdateLabels(ctx context.Context) error {
	elset, err := s.client.ExternalLabels(ctx, s.promURL)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.labels = elset
	return nil
}

func (s *promMetadata) UpdateTimestamps(mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if mint < s.limitMinTime.PrometheusTimestamp() {
		mint = s.limitMinTime.PrometheusTimestamp()
	}

	s.mint = mint
	s.maxt = maxt
}

func (s *promMetadata) Labels() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *promMetadata) LabelsPB() []storepb.Label {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := make([]storepb.Label, 0, len(s.labels))
	for _, l := range s.labels {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return lset
}

func (s *promMetadata) Timestamps() (mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.mint, s.maxt
}
