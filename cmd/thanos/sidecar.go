package main

import (
	"context"
	"encoding/json"
	"math"
	"net"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/objstore/client"
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

	grpcAddr := cmd.Flag("grpc-address", "Listen address for gRPC endpoints.").
		Default(defaultGRPCAddr).String()

	httpAddr := cmd.Flag("http-address", "Listen address for HTTP endpoints.").
		Default(defaultHTTPAddr).String()

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API.").
		Default("http://localhost:9090").URL()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty, sidecar won't store any block inside Google Cloud Storage.").
		PlaceHolder("<bucket>").String()

	s3Config := s3.RegisterS3Params(cmd)

	peers := cmd.Flag("cluster.peers", "Initial peers to join the cluster. It can be either <ip:port>, or <domain:port>.").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "Listen address for cluster.").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "Explicit address to advertise in cluster.").
		String()

	gossipInterval := cmd.Flag("cluster.gossip-interval", "Interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.").
		Default(cluster.DefaultGossipInterval.String()).Duration()

	pushPullInterval := cmd.Flag("cluster.pushpull-interval", "Interval for gossip state syncs. Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.").
		Default(cluster.DefaultPushPullInterval.String()).Duration()

	reloaderCfgFile := cmd.Flag("reloader.config-file", "Config file watched by the reloader.").
		Default("").String()

	reloaderCfgSubstFile := cmd.Flag("reloader.config-envsubst-file", "Output file for environment variable substituted config file.").
		Default("").String()

	reloaderRuleDir := cmd.Flag("reloader.rule-dir", "Rule directory for the reloader to refresh.").String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
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
			s3Config,
			peer,
			rl,
			name,
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
	component string,
) error {
	var externalLabels = &extLabelSet{promURL: promURL}
	var uploads = true

	// The background shipper continuously scans the data directory and uploads
	// new blocks to Google Cloud Storage or an S3-compatible storage service.
	bkt, closeFn, err := client.NewBucket(&gcsBucket, *s3Config, reg, component)
	if err != nil && err != client.ErrNotFound {
		return err
	}

	if err == client.ErrNotFound {
		level.Info(logger).Log("msg", "No GCS or S3 bucket was configured, uploads will be disabled")
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
				lastHeartbeat.Set(float64(time.Now().UnixNano()) / 1e9)
				return nil
			})
			if err != nil {
				return errors.Wrap(err, "initial external labels query")
			}

			if len(externalLabels.Get()) == 0 {
				return errors.New("no external labels configured on Prometheus server, uniquely identifying external labels must be configured")
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
	{
		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		if uploads {
			mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method != "POST" {
					w.WriteHeader(http.StatusMethodNotAllowed)
					json.NewEncoder(w).Encode(&response{
						Status:    statusError,
						ErrorType: "not_allowed",
					})
					return
				}

				apiErr := func() *apiError {
					resp, err := http.Post(promURL.String()+snapshotPath, "", nil)
					if err != nil {
						return &apiError{err: err, typ: errorUnavailable}
					}

					sResponse := &response{}
					err = json.NewDecoder(resp.Body).Decode(sResponse)
					if err != nil {
						return &apiError{err: err, typ: errorInternal}
					}

					if sResponse.Status != statusSuccess {
						return &apiError{err: err, typ: errorInternal}
					}

					sDir := path.Join(dataDir, "snapshots", sResponse.Data.Name)
					meta, _ := shipper.ReadMetaFile(dataDir)
					if meta != nil {
						shipper.WriteMetaFile(sDir, meta)
					}

					s := shipper.New(logger, nil, sDir, bkt, externalLabels.Get)
					s.Sync(context.Background())
					return nil
				}()

				if apiErr != nil {
					var code int
					switch apiErr.typ {
					case errorUnavailable:
						code = http.StatusServiceUnavailable
					case errorInternal:
						code = http.StatusInternalServerError
					default:
						code = http.StatusInternalServerError
					}
					w.WriteHeader(code)
					json.NewEncoder(w).Encode(&response{
						Status:    statusError,
						ErrorType: apiErr.typ,
						Error:     apiErr.err.Error(),
					})
					return
				}

				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(&response{
					Status: statusSuccess,
				})
			})
		}

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

	if uploads {
		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				closeFn()
			}
		}()

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

type resStatus string

const (
	snapshotPath = "/api/v1/admin/tsdb/snapshot"
)

const (
	statusSuccess resStatus = "success"
	statusError             = "error"
)

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type errorType string

const (
	errorInternal    errorType = "internal"
	errorUnavailable           = "unavailable"
)

type response struct {
	Status    resStatus        `json:"status"`
	Data      snapshotResponse `json:"data,omitempty"`
	ErrorType errorType        `json:"errorType,omitempty"`
	Error     string           `json:"error,omitempty"`
	Warnings  []string         `json:"warnings,omitempty"`
}

type snapshotResponse struct {
	Name string `json:"name"`
}
