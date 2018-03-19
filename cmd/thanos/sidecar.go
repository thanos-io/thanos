package main

import (
	"context"
	"encoding/json"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/improbable-eng/thanos/pkg/reloader"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

func registerSidecar(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for Prometheus server")

	grpcAddr := cmd.Flag("grpc-address", "listen address for gRPC endpoints").
		Default(defaultGRPCAddr).String()

	httpAddr := cmd.Flag("http-address", "listen address for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").String()

	var s3config s3.Config

	cmd.Flag("s3.bucket", "S3-Compatible API bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("S3_BUCKET").StringVar(&s3config.Bucket)

	cmd.Flag("s3.endpoint", "S3-Compatible API endpoint for stored blocks.").
		PlaceHolder("<api-url>").Envar("S3_ENDPOINT").StringVar(&s3config.Endpoint)

	cmd.Flag("s3.access-key", "Access key for an S3-Compatible API.").
		PlaceHolder("<key>").Envar("S3_ACCESS_KEY").StringVar(&s3config.AccessKey)

	s3config.SecretKey = os.Getenv("S3_SECRET_KEY")

	cmd.Flag("s3.insecure", "Whether to use an insecure connection with an S3-Compatible API.").
		Default("false").Envar("S3_INSECURE").BoolVar(&s3config.Insecure)

	cmd.Flag("s3.signature-version2", "Whether to use S3 Signature Version 2; otherwise Signature Version 4 will be used.").
		Default("false").Envar("S3_SIGNATURE_VERSION2").BoolVar(&s3config.SignatureV2)

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	gossipInterval := cmd.Flag("cluster.gossip-interval", "interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.").
		Default(cluster.DefaultGossipInterval.String()).Duration()

	pushPullInterval := cmd.Flag("cluster.pushpull-interval", "interval for gossip state syncs . Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.").
		Default(cluster.DefaultPushPullInterval.String()).Duration()

	reloaderCfgFile := cmd.Flag("reloader.config-file", "config file watched by the reloader").
		Default("").String()

	reloaderCfgSubstFile := cmd.Flag("reloader.config-envsubst-file", "output file for environment variable substituted config file").
		Default("").String()

	reloaderRuleDir := cmd.Flag("reloader.rule-dir", "rule directory for the reloader to refresh").String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		rl := reloader.New(
			log.With(logger, "component", "reloader"),
			reloader.ReloadURLFromBase(*promURL),
			*reloaderCfgFile,
			*reloaderCfgSubstFile,
			*reloaderRuleDir,
		)
		peer, err := cluster.New(logger, reg, *clusterBindAddr, *clusterAdvertiseAddr, *peers, false, *gossipInterval, *pushPullInterval)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return runSidecar(
			g,
			logger,
			reg,
			tracer,
			*grpcAddr,
			*httpAddr,
			*promURL,
			*dataDir,
			*gcsBucket,
			&s3config,
			peer,
			rl,
		)
	}
}

func runSidecar(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcAddr string,
	httpAddr string,
	promURL *url.URL,
	dataDir string,
	gcsBucket string,
	s3Config *s3.Config,
	peer *cluster.Peer,
	reloader *reloader.Reloader,
) error {
	var externalLabels = &extLabelSet{promURL: promURL}

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
			// Blocking query of external labels before joining as a Source Peer into gossip.
			// We retry infinitely until we reach and fetch labels from our Prometheus.
			err := runutil.Retry(2*time.Second, ctx.Done(), func() error {
				err := externalLabels.Update(ctx)
				if err != nil {
					level.Warn(logger).Log(
						"msg", "failed to fetch initial external labels. Is Prometheus running? Retrying",
						"err", err,
					)
					promUp.Set(0)
					return err
				}

				promUp.Set(1)
				lastHeartbeat.Set(float64(time.Now().Unix()))
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			// New gossip cluster.
			err = peer.Join(
				cluster.PeerState{
					Type:    cluster.PeerTypeSource,
					APIAddr: grpcAddr,
					Metadata: cluster.PeerMetadata{
						Labels: externalLabels.GetPB(),
						// Start out with the full time range. The shipper will constrain it later.
						// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
						MinTime: 0,
						MaxTime: math.MaxInt64,
					},
				},
			)
			if err != nil {
				return errors.Wrap(err, "join cluster")
			}

			// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
			// the external labels we apply.
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				iterCtx, iterCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer iterCancel()

				err := externalLabels.Update(iterCtx)
				if err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					// Update gossip.
					peer.SetLabels(externalLabels.GetPB())

					promUp.Set(1)
					lastHeartbeat.Set(float64(time.Now().Unix()))
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
	{
		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)

		l, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return errors.Wrap(err, "listen metrics address")
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve metrics")
		}, func(error) {
			l.Close()
		})
	}
	{
		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", "store")

		var client http.Client

		promStore, err := store.NewPrometheusStore(
			logger, prometheus.DefaultRegisterer, &client, promURL, externalLabels.Get)
		if err != nil {
			return errors.Wrap(err, "create Prometheus store")
		}

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, promStore)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})
	}

	var (
		bkt    objstore.Bucket
		bucket string
		// closeFn gets called when the sync loop ends to close clients, clean up, etc
		closeFn      = func() error { return nil }
		uploads bool = true
	)

	// The background shipper continuously scans the data directory and uploads
	// new blocks to Google Cloud Storage or an S3-compatible storage service.
	if gcsBucket != "" {
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}

		bkt = gcs.NewBucket(gcsBucket, gcsClient.Bucket(gcsBucket), reg)
		closeFn = gcsClient.Close
		bucket = gcsBucket
	} else if s3Config.Validate() == nil {
		var err error
		bkt, err = s3.NewBucket(s3Config, reg)
		if err != nil {
			return errors.Wrap(err, "create s3 client")
		}

		bucket = s3Config.Bucket
	} else {
		uploads = false
		level.Info(logger).Log("msg", "No GCS or S3 bucket were configured, uploads will be disabled")
	}

	if uploads {
		bkt = objstore.BucketWithMetrics(bucket, bkt, reg)

		s := shipper.New(logger, nil, dataDir, bkt, externalLabels.Get)

		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer closeFn()

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				s.Sync(ctx)

				minTime, _, err := s.Timestamps()
				if err != nil {
					level.Warn(logger).Log("msg", "reading timestamps failed", "err", err)
				} else {
					peer.SetTimestamps(minTime, math.MaxInt64)
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

type extLabelSet struct {
	promURL *url.URL

	mtx    sync.Mutex
	labels labels.Labels
}

func (s *extLabelSet) Update(ctx context.Context) error {
	elset, err := queryExternalLabels(ctx, s.promURL)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	s.labels = elset
	s.mtx.Unlock()

	return nil
}

func (s *extLabelSet) Get() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *extLabelSet) GetPB() []storepb.Label {
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

func queryExternalLabels(ctx context.Context, base *url.URL) (labels.Labels, error) {
	u := *base
	u.Path = path.Join(u.Path, "/api/v1/status/config")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrapf(err, "request config against %s", u.String())
	}
	defer resp.Body.Close()

	var d struct {
		Data struct {
			YAML string `json:"yaml"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, errors.Wrap(err, "decode response")
	}
	var cfg struct {
		Global struct {
			ExternalLabels map[string]string `yaml:"external_labels"`
		} `yaml:"global"`
	}
	if err := yaml.Unmarshal([]byte(d.Data.YAML), &cfg); err != nil {
		return nil, errors.Wrap(err, "parse Prometheus config")
	}
	return labels.FromMap(cfg.Global.ExternalLabels), nil
}
