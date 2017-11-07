package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/improbable-eng/promlts/pkg/shipper"
	"github.com/improbable-eng/promlts/pkg/store"
	"github.com/improbable-eng/promlts/pkg/store/storepb"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/oklog/pkg/group"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

func registerSidecar(m map[string]runFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "sidecar for Prometheus server")

	apiAddr := cmd.Flag("api-address", "listen address for the store API").
		Default(":19090").String()

	metricsAddr := cmd.Flag("metrics-address", "metrics address for the sidecar").
		Default(":19091").String()

	promURL := cmd.Flag("prometheus.url", "URL at which to reach Prometheus's API").
		Default("http://localhost:9090").String()

	dataDir := cmd.Flag("tsdb.path", "data directory of TSDB").
		Default("./data").String()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty sidecar won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").String()

	m[name] = func(logger log.Logger, reg prometheus.Registerer) error {
		return runSidecar(logger, reg, *apiAddr, *metricsAddr, *promURL, *dataDir, *gcsBucket)
	}
}

func runSidecar(
	logger log.Logger,
	reg prometheus.Registerer,
	apiAddr string,
	metricsAddr string,
	promURL string,
	dataDir string,
	gcsBucket string,
) error {
	level.Info(logger).Log("msg", "starting sidecar")

	baseURL, err := url.Parse(promURL)
	if err != nil {
		return errors.Wrap(err, "invalid Prometheus base URL")
	}

	var extLabelsMu sync.RWMutex
	var externalLabels labels.Labels

	getExternalLabels := func() labels.Labels {
		extLabelsMu.Lock()
		defer extLabelsMu.Unlock()
		return externalLabels
	}

	var g group.Group
	{
		mux := http.NewServeMux()
		mux.Handle("/metrics", prometheus.Handler())

		l, err := net.Listen("tcp", metricsAddr)
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
		l, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", "proxy")

		var client http.Client

		proxy, err := store.NewPrometheusProxy(
			logger, prometheus.DefaultRegisterer, &client, baseURL, getExternalLabels)

		if err != nil {
			return errors.Wrap(err, "create Prometheus proxy")
		}

		s := grpc.NewServer()

		storepb.RegisterStoreServer(s, proxy)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})
	}
	// Periodically query the Prometheus config. We use this as a heartbeat as well as for updating
	// the external labels we apply.
	{
		promUp := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "promlts_sidecar_prometheus_up",
			Help: "Boolean indicator whether the sidecar can reach its Prometheus peer.",
		})
		lastHeartbeat := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "promlts_sidecar_last_heartbeat_success_time_seconds",
			Help: "Second timestamp of the last successful heartbeat.",
		})
		reg.MustRegister(promUp, lastHeartbeat)

		ctx, cancel := context.WithCancel(context.Background())
		tick := time.NewTicker(30 * time.Second)

		g.Add(func() error {
			for {
				elset, err := queryExternalLabels(ctx, baseURL)
				if err != nil {
					level.Warn(logger).Log("msg", "heartbeat failed", "err", err)
					promUp.Set(0)
				} else {
					promUp.Set(1)

					extLabelsMu.Lock()
					externalLabels = elset
					extLabelsMu.Unlock()

					lastHeartbeat.Set(float64(time.Now().Unix()))
				}
				select {
				case <-ctx.Done():
					return nil
				case <-tick.C:
				}
			}
		}, func(error) {
			cancel()
			tick.Stop()
		})
	}

	if gcsBucket != "" {
		// The background shipper continuously scans the data directory and uploads
		// new found blocks to Google Cloud Storage.

		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}
		defer gcsClient.Close()

		remote := shipper.NewGCSRemote(logger, nil, gcsClient.Bucket(gcsBucket))
		s := shipper.New(logger, nil, dataDir, remote, shipper.IsULIDDir)

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return errors.Wrap(s.Run(ctx, 30*time.Second), "run block shipper")
		}, func(error) {
			cancel()
		})
	} else {
		level.Info(logger).Log("msg", "No GCS bucket were configured, GCS uploads will be disabled")
	}
	// Listen for termination signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}
	return g.Run()
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
		return nil, errors.Wrap(err, "request config")
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
