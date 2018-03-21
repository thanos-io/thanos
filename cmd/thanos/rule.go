package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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

	"cloud.google.com/go/storage"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/alert"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/improbable-eng/thanos/pkg/objstore/gcs"
	"github.com/improbable-eng/thanos/pkg/objstore/s3"
	"github.com/improbable-eng/thanos/pkg/runutil"
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
	cmd := app.Command(name, "ruler evaluating Prometheus rules against given Query nodes, exposing Store API and storing old blocks in bucket")

	labelStrs := cmd.Flag("label", "labels applying to all generated metrics (repeated)").
		PlaceHolder("<name>=\"<value>\"").Strings()

	dataDir := cmd.Flag("data-dir", "data directory").Default("data/").String()

	ruleFiles := cmd.Flag("rule-file", "rule files that should be used by rule manager. Can be in glob format (repeated)").
		Default("rules/").Strings()

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

	s3Bucket := cmd.Flag("s3.bucket", "S3-Compatible API bucket name for stored blocks.").
		PlaceHolder("<bucket>").Envar("S3_BUCKET").String()

	s3Endpoint := cmd.Flag("s3.endpoint", "S3-Compatible API endpoint for stored blocks.").
		PlaceHolder("<api-url>").Envar("S3_ENDPOINT").String()

	s3AccessKey := cmd.Flag("s3.access-key", "Access key for an S3-Compatible API.").
		PlaceHolder("<key>").Envar("S3_ACCESS_KEY").String()

	s3SecretKey := os.Getenv("S3_SECRET_KEY")

	s3Insecure := cmd.Flag("s3.insecure", "Whether to use an insecure connection with an S3-Compatible API.").
		Default("false").Envar("S3_INSECURE").Bool()

	s3SignatureV2 := cmd.Flag("s3.signature-version2", "Whether to use S3 Signature Version 2; otherwise Signature Version 4 will be used.").
		Default("false").Envar("S3_SIGNATURE_VERSION2").Bool()

	peers := cmd.Flag("cluster.peers", "initial peers to join the cluster. It can be either <ip:port>, or <domain:port>").Strings()

	clusterBindAddr := cmd.Flag("cluster.address", "listen address for cluster").
		Default(defaultClusterAddr).String()

	gossipInterval := cmd.Flag("cluster.gossip-interval", "interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.").
		Default(cluster.DefaultGossipInterval.String()).Duration()

	pushPullInterval := cmd.Flag("cluster.pushpull-interval", "interval for gossip state syncs . Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.").
		Default(cluster.DefaultPushPullInterval.String()).Duration()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "explicit address to advertise in cluster").
		String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer) error {
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}
		peer, err := cluster.New(logger, reg, *clusterBindAddr, *clusterAdvertiseAddr, *peers, false, *gossipInterval, *pushPullInterval)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration: model.Duration(*tsdbBlockDuration),
			MaxBlockDuration: model.Duration(*tsdbBlockDuration),
			Retention:        model.Duration(*tsdbRetention),
			NoLockfile:       true,
			WALFlushInterval: 30 * time.Second,
		}
		return runRule(g, logger, reg, tracer, lset, *alertmgrs, *httpAddr, *grpcAddr, *evalInterval, *dataDir, *ruleFiles, peer, *gcsBucket, *s3Bucket, *s3Endpoint, *s3AccessKey, s3SecretKey, *s3Insecure, *s3SignatureV2, tsdbOpts)
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
	ruleFiles []string,
	peer *cluster.Peer,
	gcsBucket string,
	s3Bucket string,
	s3Endpoint string,
	s3AccessKey string,
	s3SecretKey string,
	s3Insecure bool,
	s3SignatureV2 bool,
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
		var ids []string
		for id := range peers {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i int, j int) bool {
			return strings.Compare(ids[i], ids[j]) < 0
		})

		for _, i := range rand.Perm(len(ids)) {
			vec, err := queryPrometheusInstant(ctx, logger, peers[ids[i]].APIAddr, q, t)
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
					StartsAt:    alrt.FiredAt,
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
			QueryFunc:   queryFn,
			NotifyFunc:  notify,
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
		var storeLset []storepb.Label
		for _, l := range lset {
			storeLset = append(storeLset, storepb.Label{Name: l.Name, Value: l.Value})
		}

		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			err := peer.Join(cluster.PeerState{
				Type:    cluster.PeerTypeSource,
				APIAddr: grpcAddr,
				Metadata: cluster.PeerMetadata{
					Labels: storeLset,
					// Start out with the full time range. The shipper will constrain it later.
					// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
					MinTime: 0,
					MaxTime: math.MaxInt64,
				},
			})
			if err != nil {
				return errors.Wrap(err, "join cluster")
			}

			<-ctx.Done()
			return nil
		}, func(error) {
			cancel()
			peer.Close(5 * time.Second)
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

				level.Debug(logger).Log("msg", "configured rule files", "files", strings.Join(ruleFiles, ","))
				var files []string
				for _, pat := range ruleFiles {
					fs, err := filepath.Glob(pat)
					if err != nil {
						// The only error can be a bad pattern.
						level.Error(logger).Log("msg", "retrieving rule files failed. Ignoring file.", "pattern", pat, "err", err)
						continue
					}
					files = append(files, fs...)
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

		s := grpc.NewServer(defaultGRPCServerOpts(logger, reg, tracer)...)
		storepb.RegisterStoreServer(s, store)

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

	var (
		bkt    objstore.Bucket
		bucket string
		// closeFn gets called when the sync loop ends to close clients, clean up, etc
		closeFn = func() error { return nil }
		uploads = true
	)

	s3Config := &s3.Config{
		Bucket:      s3Bucket,
		Endpoint:    s3Endpoint,
		AccessKey:   s3AccessKey,
		SecretKey:   s3SecretKey,
		Insecure:    s3Insecure,
		SignatureV2: s3SignatureV2,
	}

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
		bkt, err = s3.NewBucket(s3Config, reg)
		if err != nil {
			return errors.Wrap(err, "create s3 client")
		}

		bucket = s3Config.Bucket
	} else {
		level.Info(logger).Log("msg", "No GCS or S3 bucket configured, uploads will be disabled")
		uploads = false
	}

	if uploads {
		bkt = objstore.BucketWithMetrics(bucket, bkt, reg)

		s := shipper.New(logger, nil, dataDir, bkt, func() labels.Labels { return lset })

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

	level.Info(logger).Log("msg", "starting rule node", "peer", peer.Name())
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
	params.Add("dedup", "true")
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
