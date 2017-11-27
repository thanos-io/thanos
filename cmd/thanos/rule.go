package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage/tsdb"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerRule registers a rule command.
func registerRule(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "query node exposing PromQL enabled Query API with data retrieved from multiple store nodes")

	labelStrs := cmd.Flag("label", "labels applying to all generated metrics (repeated)").
		PlaceHolder("<name>=\"<value>\"").Strings()

	dataDir := cmd.Flag("data-dir", "data directory").Default("data/").String()

	ruleDir := cmd.Flag("rule-dir", "directory containing rule files").
		Default("rules/").String()

	httpAddr := cmd.Flag("http-address", "listen host:port for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	grpcAddr := cmd.Flag("grpc-address", "listen host:port for gRPC endpoints").
		Default(defaultGRPCAddr).String()

	evalInterval := cmd.Flag("eval-interval", "the default evaluation interval to use").
		Default("30s").Duration()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry) error {
		peer, err := cluster.Join(
			logger,
			reg,
			*clusterBindAddr,
			*clusterAdvertiseAddr,
			*peers,
			cluster.PeerState{
				Type:    cluster.PeerTypeSource,
				APIAddr: *grpcAddr,
			},
			true,
		)
		if err != nil {
			return errors.Wrap(err, "join cluster")
		}
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}
		return runRule(g, logger, reg, lset, *httpAddr, *grpcAddr, *evalInterval, *dataDir, *ruleDir, peer)
	}
}

// runRule runs a rule evaluation component that continously evaluates alerting and recording
// rules. It sends alert notifications and writes TSDB data for results like a regular Prometheus server.
func runRule(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	lset labels.Labels,
	httpAddr string,
	grpcAddr string,
	evalInterval time.Duration,
	dataDir string,
	ruleDir string,
	peer *cluster.Peer,
) error {
	db, err := tsdb.Open(dataDir, log.With(logger, "component", "tsdb"), reg, &tsdb.Options{
		MinBlockDuration: model.Duration(2 * time.Hour),
		MaxBlockDuration: model.Duration(2 * time.Hour),
		Retention:        model.Duration(48 * time.Hour),
		NoLockfile:       true,
		WALFlushInterval: 30 * time.Second,
	})
	if err != nil {
		return errors.Wrap(err, "open TSDB")
	}
	{
		done := make(chan struct{})
		g.Add(func() error {
			<-done
			return db.Close()
		}, func(error) {
			close(done)
		})
	}

	// Hit the HTTP query API of query peers in randomized order until we get a result
	// back or the context get canceled.
	queryFn := func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		peers := peer.PeerStates(cluster.PeerTypeQuery)

		for _, i := range rand.Perm(len(peers)) {
			vec, err := queryPrometheusInstant(ctx, peers[i].APIAddr, q, t)
			if err != nil {
				return nil, err
			}
			return vec, nil
		}
		return nil, errors.Errorf("no query peer reachable")
	}

	var mgr *rules.Manager

	{
		ctx, cancel := context.WithCancel(context.Background())

		mgr = rules.NewManager(&rules.ManagerOptions{
			Context:     ctx,
			Query:       queryFn,
			Notify:      printAlertNotifications,
			Logger:      log.With(logger, "component", "rules"),
			Appendable:  tsdb.Adapter(db, 0),
			ExternalURL: nil,
		})
		g.Add(func() error {
			mgr.Run()
			<-ctx.Done()
			mgr.Stop()
			return nil
		}, func(error) {
			cancel()
		})
	}

	reload := make(chan struct{}, 1)
	{
		cancel := make(chan struct{})
		reload <- struct{}{} // initial reload

		g.Add(func() error {
			for {
				select {
				case <-cancel:
					return errors.New("canceled")
				case <-reload:
				}

				var files []string
				// We recursively pick all files in the rule directory.
				filepath.Walk(ruleDir, func(p string, fi os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if !fi.IsDir() {
						files = append(files, p)
					}
					return nil
				})
				level.Info(logger).Log("msg", "reload rule files", "numFiles", len(files))

				if err := mgr.Update(evalInterval, files); err != nil {
					level.Error(logger).Log("msg", "reloading rules failed", "err", err)
				}
			}
		}, func(error) {
			close(cancel)
		})
	}
	{
		cancel := make(chan struct{})

		g.Add(func() error {
			c := make(chan os.Signal, 1)
			for {
				signal.Notify(c, syscall.SIGHUP)
				select {
				case <-c:
					select {
					case reload <- struct{}{}:
					default:
					}
				case <-cancel:
					return errors.New("canceled")
				}
			}
		}, func(error) {
			close(cancel)
		})
	}
	{
		l, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return errors.Wrap(err, "listen API address")
		}
		logger := log.With(logger, "component", "store")

		store := store.NewTSDBStore(logger, reg, db, lset)

		met := grpc_prometheus.NewServerMetrics()
		met.EnableHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets([]float64{
				0.001, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4,
			}),
		)
		s := grpc.NewServer(
			grpc.UnaryInterceptor(met.UnaryServerInterceptor()),
			grpc.StreamInterceptor(met.StreamServerInterceptor()),
		)
		storepb.RegisterStoreServer(s, store)
		reg.MustRegister(met)

		g.Add(func() error {
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			s.Stop()
			l.Close()
		})
	}
	// Start the HTTP server for debugging and metrics.
	{
		router := route.New()

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		mux.Handle("/", router)

		l, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return errors.Wrapf(err, "listen on address %s", httpAddr)
		}

		g.Add(func() error {
			return errors.Wrap(http.Serve(l, mux), "serve query")
		}, func(error) {
			l.Close()
		})
	}

	level.Info(logger).Log("msg", "starting query node")
	return nil
}

func queryPrometheusInstant(ctx context.Context, addr, query string, t time.Time) (promql.Vector, error) {
	u, err := url.Parse(fmt.Sprintf("http://%s/api/v1/query", addr))
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Add("query", query)
	params.Add("time", t.Format(time.RFC3339Nano))
	u.RawQuery = params.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Always try to decode a vector. Scalar rules won't work for now and arguably
	// have no relevant use case.
	var m struct {
		Data struct {
			Result model.Vector `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	vec := make(promql.Vector, 0, len(m.Data.Result))

	for _, e := range m.Data.Result {
		lset := make(promlabels.Labels, 0, len(e.Metric))

		for k, v := range e.Metric {
			lset = append(lset, promlabels.Label{
				Name:  string(k),
				Value: string(v),
			})
		}
		sort.Sort(lset)

		vec = append(vec, promql.Sample{
			Metric: lset,
			Point:  promql.Point{T: int64(e.Timestamp), V: float64(e.Value)},
		})
	}
	return vec, nil
}

func printAlertNotifications(ctx context.Context, expr string, alerts ...*rules.Alert) error {
	fmt.Fprintf(os.Stdout, "%d alerts for %s\n", len(alerts), expr)
	for _, a := range alerts {
		fmt.Fprintf(os.Stdout, "  labels: %s val: %f\n", a.Labels, a.Value)
	}
	return nil
}

func parseFlagLabels(s []string) (labels.Labels, error) {
	var lset labels.Labels
	for _, l := range s {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("unrecognized label %q", l)
		}
		val, err := strconv.Unquote(parts[1])
		if err != nil {
			return nil, errors.Wrap(err, "unquote label value")
		}
		lset = append(lset, labels.Label{Name: parts[0], Value: val})
	}
	return lset, nil
}
