package main

import (
	"context"
	"math"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/block/metadata"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
	"github.com/improbable-eng/thanos/pkg/promclient"
	"github.com/improbable-eng/thanos/pkg/reloader"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func registerSidecar(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for Prometheus server")

	grpcBindAddr, httpBindAddr, cert, key, clientCA, newPeerFn := regCommonServerFlags(cmd)

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API. For better performance use local network.").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	reloaderCfgFile := cmd.Flag("reloader.config-file", "Config file watched by the reloader.").
		Default("").String()

	reloaderCfgOutputFile := cmd.Flag("reloader.config-envsubst-file", "Output file for environment variable substituted config file.").
		Default("").String()

	reloaderRuleDirs := cmd.Flag("reloader.rule-dir", "Rule directories for the reloader to refresh (repeated field).").Strings()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", false)

	uploadCompacted := cmd.Flag("shipper.upload-compacted", "[Experimental] If true sidecar will try to upload compacted blocks as well. Useful for migration purposes. Works only if compaction is disabled on Prometheus.").Default("false").Hidden().Bool()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		rl := reloader.New(
			log.With(logger, "component", "reloader"),
			reloader.ReloadURLFromBase(*promURL),
			*reloaderCfgFile,
			*reloaderCfgOutputFile,
			*reloaderRuleDirs,
		)
		peer, err := newPeerFn(logger, reg, false, "", false)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return runSidecar(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*cert,
			*key,
			*clientCA,
			*httpBindAddr,
			*promURL,
			*dataDir,
			objStoreConfig,
			peer,
			rl,
			*uploadCompacted,
		)
	}
}

func runSidecar(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	cert string,
	key string,
	clientCA string,
	httpBindAddr string,
	promURL *url.URL,
	dataDir string,
	objStoreConfig *pathOrContent,
	peer cluster.Peer,
	reloader *reloader.Reloader,
	uploadCompacted bool,
) error {
	var m = &promMetadata{
		promURL: promURL,

		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		mint: 0,
		maxt: math.MaxInt64,
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
				if err := validatePrometheus(ctx, logger, m); err != nil {
					return errors.Wrap(err, "validate Prometheus flags")
				}
			}

			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				if err := m.UpdateLabels(ctx, logger); err != nil {
					level.Warn(logger).Log(
						"msg", "failed to fetch initial external labels. Is Prometheus running? Retrying",
						"err", err,
					)
					promUp.Set(0)
					return err
				}

				level.Info(logger).Log(
					"msg", "successfully loaded prometheus external labels",
					"external_labels", m.Labels().String(),
				)
				promUp.Set(1)
				lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			if len(m.Labels()) == 0 {
				return errors.New("no external labels configured on Prometheus server, uniquely identifying external labels must be configured")
			}

			// New gossip cluster.
			mint, maxt := m.Timestamps()
			if err = peer.Join(cluster.PeerTypeSource, cluster.PeerMetadata{
				Labels:  m.LabelsPB(),
				MinTime: mint,
				MaxTime: maxt,
			}); err != nil {
				return errors.Wrap(err, "join cluster")
			}

			// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
			// the external labels we apply.
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				if err := m.UpdateLabels(iterCtx, logger); err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					// Update gossip.
					peer.SetLabels(m.LabelsPB())

					promUp.Set(1)
					lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				}

				return nil
			})
		}, func(error) {
			cancel()
			peer.Close(2 * time.Second)
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
	if err := metricHTTPListenGroup(g, logger, reg, httpBindAddr); err != nil {
		return err
	}
	{
		l, err := net.Listen("tcp", grpcBindAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", component.Sidecar.String())

		var client http.Client

		promStore, err := store.NewPrometheusStore(
			logger, &client, promURL, component.Sidecar, m.Labels, m.Timestamps)
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		opts, err := defaultGRPCServerOpts(logger, reg, tracer, cert, key, clientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}
		s := grpc.NewServer(opts...)
		storepb.RegisterStoreServer(s, promStore)

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			runutil.CloseWithLogOnErr(logger, l, "store gRPC listener")
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

		if err := promclient.IsWALDirAccesible(dataDir); err != nil {
			level.Error(logger).Log("err", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

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
				} else {
					m.UpdateTimestamps(minTime, math.MaxInt64)

					mint, maxt := m.Timestamps()
					peer.SetTimestamps(mint, maxt)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	level.Info(logger).Log("msg", "starting sidecar", "peer", peer.Name())
	return nil
}

func validatePrometheus(ctx context.Context, logger log.Logger, m *promMetadata) error {
	var (
		flagErr error
		flags   promclient.Flags
	)

	if err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
		if flags, flagErr = promclient.ConfiguredFlags(ctx, logger, m.promURL); flagErr != nil && flagErr != promclient.ErrFlagEndpointNotFound {
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
		return errors.Errorf("found that TSDB Max time is %s and Min time is %s. "+
			"Compaction needs to be disabled (storage.tsdb.min-block-duration = storage.tsdb.max-block-duration)", flags.TSDBMaxTime, flags.TSDBMinTime)
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
}

func (s *promMetadata) UpdateLabels(ctx context.Context, logger log.Logger) error {
	elset, err := promclient.ExternalLabels(ctx, logger, s.promURL)
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
