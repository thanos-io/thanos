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
	"sync"
	"syscall"
	"time"

	"github.com/improbable-eng/thanos/pkg/runutil"

	"math"

	"path"

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/improbable-eng/thanos/pkg/alert"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/shipper"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/tracing"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
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

	ruleDir := cmd.Flag("rule-dir", "directory containing rule files. Only .yml and .yaml file extensions are accepted. All directories and `..` or `.` files are ignored.").
		Default("rules/").String()

	httpAddr := cmd.Flag("http-address", "listen host:port for HTTP endpoints").
		Default(defaultHTTPAddr).String()

	grpcAddr := cmd.Flag("grpc-address", "listen host:port for gRPC endpoints").
		Default(defaultGRPCAddr).String()

	evalInterval := cmd.Flag("eval-interval", "the default evaluation interval to use").
		Default("30s").Duration()
	tsdbBlockDuration := cmd.Flag("tsdb.block-duration", "block duration for TSDB block").
		Default("2h").Duration()
	tsdbRetention := cmd.Flag("tsdb.retention", "block retention time on local disk").
		Default("48h").Duration()

	alertmgrs := cmd.Flag("alertmanagers.url", "Alertmanager URLs to push firing alerts to. The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Alertmanager IPs through respective DNS lookups. The port defaults to 9093 or the SRV record's value. The URL path is used as a prefix for the regular Alertmanager API path.").
		Strings()

	gcsBucket := cmd.Flag("gcs.bucket", "Google Cloud Storage bucket name for stored blocks. If empty ruler won't store any block inside Google Cloud Storage").
		PlaceHolder("<bucket>").String()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
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

		tsdbOpts := &tsdb.Options{
			MinBlockDuration: model.Duration(*tsdbBlockDuration),
			MaxBlockDuration: model.Duration(*tsdbBlockDuration),
			Retention:        model.Duration(*tsdbRetention),
			NoLockfile:       true,
			WALFlushInterval: 30 * time.Second,
		}
		return runRule(g, logger, reg, tracer, lset, *alertmgrs, *httpAddr, *grpcAddr, *evalInterval, *dataDir, *ruleDir, peer, *gcsBucket, tsdbOpts)
	}
}

// runRule runs a rule evaluation component that continously evaluates alerting and recording
// rules. It sends alert notifications and writes TSDB data for results like a regular Prometheus server.
func runRule(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	lset labels.Labels,
	alertmgrURLs []string,
	httpAddr string,
	grpcAddr string,
	evalInterval time.Duration,
	dataDir string,
	ruleDir string,
	peer *cluster.Peer,
	gcsBucket string,
	tsdbOpts *tsdb.Options,
) error {
	db, err := tsdb.Open(dataDir, log.With(logger, "component", "tsdb"), reg, tsdbOpts)
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
			vec, err := queryPrometheusInstant(ctx, logger, peers[i].APIAddr, q, t)
			if err != nil {
				return nil, err
			}
			return vec, nil
		}
		return nil, errors.Errorf("no query peer reachable")
	}

	// Run rule evaluation and alert notifications.
	var (
		alertmgrs = newAlertmanagerSet(alertmgrURLs, nil)
		alertQ    = alert.NewQueue(logger, reg, 10000, 100, labelsTSDBToProm(lset))
		mgr       *rules.Manager
	)
	{
		ctx, cancel := context.WithCancel(context.Background())
		ctx = tracing.ContextWithTracer(ctx, tracer)

		notify := func(ctx context.Context, expr string, alerts ...*rules.Alert) error {
			res := make([]*alert.Alert, 0, len(alerts))
			for _, alrt := range alerts {
				// Only send actually firing alerts.
				if alrt.State == rules.StatePending {
					continue
				}
				a := &alert.Alert{
					StartsAt:    alrt.FiringAt,
					Labels:      alrt.Labels,
					Annotations: alrt.Annotations,
				}
				if !alrt.ResolvedAt.IsZero() {
					a.EndsAt = alrt.ResolvedAt
				}
				res = append(res, a)
			}
			alertQ.Push(res)

			return nil
		}
		mgr = rules.NewManager(&rules.ManagerOptions{
			Context:     ctx,
			Query:       queryFn,
			Notify:      notify,
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
	{
		sdr := alert.NewSender(logger, reg, alertmgrs.get, nil)
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			for {
				sdr.Send(ctx, alertQ.Pop(ctx.Done()))

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
		}, func(error) {
			cancel()
		})
	}
	{
		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if err := alertmgrs.update(ctx); err != nil {
					level.Warn(logger).Log("msg", "refreshing Alertmanagers failed", "err", err)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	}

	// Handle reload and termination interrupts.
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

				files, err := getRulesFiles(logger, ruleDir)
				if err != nil {
					level.Error(logger).Log("msg", "get rules files failed", "err", err)
					continue
				}

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

	// Start HTTP and gRPC servers.
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
			grpc.UnaryInterceptor(
				grpc_middleware.ChainUnaryServer(
					met.UnaryServerInterceptor(),
					tracing.UnaryServerInterceptor(tracer),
				),
			),
			grpc.StreamInterceptor(
				grpc_middleware.ChainStreamServer(
					met.StreamServerInterceptor(),
					tracing.StreamServerInterceptor(tracer),
				),
			),
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

	if gcsBucket != "" {
		// The background shipper continuously scans the data directory and uploads
		// new found blocks to Google Cloud Storage.
		gcsClient, err := storage.NewClient(context.Background())
		if err != nil {
			return errors.Wrap(err, "create GCS client")
		}

		bkt := gcs.NewBucket(gcsClient.Bucket(gcsBucket), reg, gcsBucket)
		s := shipper.New(logger, nil, dataDir, bkt, func() labels.Labels {
			// We don't need external labels here, replica label if any will be appended to TSDB directly.
			return labels.Labels{}
		}, func(mint int64) {
			peer.SetTimestamps(mint, math.MaxInt64)
		})

		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer gcsClient.Close()

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				s.Sync(ctx)
				return nil
			})
		}, func(error) {
			cancel()
		})
	} else {
		level.Info(logger).Log("msg", "No GCS bucket were configured, GCS uploads will be disabled")
	}

	level.Info(logger).Log("msg", "starting query node")
	return nil
}

func queryPrometheusInstant(ctx context.Context, logger log.Logger, addr, query string, t time.Time) (promql.Vector, error) {
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

	span, ctx := tracing.StartSpan(ctx, "/rule_instant_query HTTP[client]")
	defer span.Finish()

	req = req.WithContext(ctx)

	client := &http.Client{
		Transport: tracing.HTTPTripperware(logger, http.DefaultTransport),
	}
	resp, err := client.Do(req)
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

// getRulesFiles picks recursively all .yml or .yaml files (with symlink support) from the rule directory.
// It also ignores dirs and files with `.` and `..` prefixes.
func getRulesFiles(logger log.Logger, ruleDir string) ([]string, error) {
	var files []string
	err := filepath.Walk(ruleDir, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(fi.Name(), ".") {
			// Ignore . and .. files/dirs (e.g Configmap mounting are leaving the original objects in the `..data` dir).
			return nil
		}

		if fi.IsDir() {
			return nil
		}

		// Follow symlink and double check if it is not a directory.
		if fi.Mode()&os.ModeSymlink != 0 {
			followedPath, err := os.Readlink(p)
			if err != nil {
				level.Warn(logger).Log("msg", "failed to follow symlink. Ignoring rule.", "path", p, "err", err)
				return nil
			}

			if !path.IsAbs(followedPath) {
				// relative path. append dir.
				followedPath = path.Join(path.Dir(p), followedPath)
			}

			info, err := os.Lstat(followedPath)
			if err != nil {
				fmt.Println(followedPath)
				level.Warn(logger).Log("msg", "failed to lstat target symlink. Ignoring rule.", "path", followedPath, "err", err)
				return nil
			}

			if info.IsDir() {
				// It is a directory, so ignore it. Otherwise symlink will be automatically followed.
				return nil
			}
		}

		if path.Ext(fi.Name()) == ".yml" || path.Ext(fi.Name()) == ".yaml" {
			files = append(files, p)
		}

		return nil
	})
	return files, err
}

type alertmanagerSet struct {
	resolver *net.Resolver
	addrs    []string
	mtx      sync.Mutex
	current  []*url.URL
}

func newAlertmanagerSet(addrs []string, resolver *net.Resolver) *alertmanagerSet {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &alertmanagerSet{
		resolver: resolver,
		addrs:    addrs,
	}
}

func (s *alertmanagerSet) get() []*url.URL {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.current
}

const defaultAlertmanagerPort = 9093

func (s *alertmanagerSet) update(ctx context.Context) error {
	var res []*url.URL

	for _, addr := range s.addrs {
		u, err := url.Parse(addr)
		if err != nil {
			return errors.Wrapf(err, "parse URL %q", addr)
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil {
			host, port = u.Host, ""
		}
		var (
			hosts  []string
			proto  = u.Scheme
			lookup = "none"
		)
		if ps := strings.SplitN(u.Scheme, "+", 2); len(ps) == 2 {
			lookup, proto = ps[0], ps[1]
		}
		switch lookup {
		case "dns":
			if port == "" {
				port = strconv.Itoa(defaultAlertmanagerPort)
			}
			ips, err := s.resolver.LookupIPAddr(ctx, host)
			if err != nil {
				return errors.Wrapf(err, "lookup IP addresses %q", host)
			}
			for _, ip := range ips {
				hosts = append(hosts, net.JoinHostPort(ip.String(), port))
			}
		case "dnssrv":
			_, recs, err := s.resolver.LookupSRV(ctx, "", proto, host)
			if err != nil {
				return errors.Wrapf(err, "lookup SRV records %q", host)
			}
			for _, rec := range recs {
				// Only use port from SRV record if no explicit port was specified.
				if port == "" {
					port = strconv.Itoa(int(rec.Port))
				}
				hosts = append(hosts, net.JoinHostPort(rec.Target, port))
			}
		case "none":
			if port == "" {
				port = strconv.Itoa(defaultAlertmanagerPort)
			}
			hosts = append(hosts, net.JoinHostPort(host, port))
		default:
			return errors.Errorf("invalid lookup scheme %q", lookup)
		}

		for _, h := range hosts {
			res = append(res, &url.URL{
				Scheme: proto,
				Host:   h,
				Path:   u.Path,
				User:   u.User,
			})
		}
	}

	s.mtx.Lock()
	s.current = res
	s.mtx.Unlock()

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

func labelsTSDBToProm(lset labels.Labels) (res promlabels.Labels) {
	for _, l := range lset {
		res = append(res, promlabels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res
}
