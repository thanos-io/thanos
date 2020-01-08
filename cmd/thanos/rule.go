package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage/tsdb"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/discovery/cache"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/extflag"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/promclient"
	thanosrule "github.com/thanos-io/thanos/pkg/rule"
	v1 "github.com/thanos-io/thanos/pkg/rule/api"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/ui"
	"gopkg.in/alecthomas/kingpin.v2"
)

// registerRule registers a rule command.
func registerRule(m map[string]setupFunc, app *kingpin.Application) {
	comp := component.Rule
	cmd := app.Command(comp.String(), "ruler evaluating Prometheus rules against given Query nodes, exposing Store API and storing old blocks in bucket")

	httpBindAddr, httpGracePeriod := regHTTPFlags(cmd)
	grpcBindAddr, grpcGracePeriod, grpcCert, grpcKey, grpcClientCA := regGRPCFlags(cmd)

	labelStrs := cmd.Flag("label", "Labels to be applied to all generated metrics (repeated). Similar to external labels for Prometheus, used to identify ruler and its blocks as unique source.").
		PlaceHolder("<name>=\"<value>\"").Strings()

	dataDir := cmd.Flag("data-dir", "data directory").Default("data/").String()

	ruleFiles := cmd.Flag("rule-file", "Rule files that should be used by rule manager. Can be in glob format (repeated).").
		Default("rules/").Strings()
	resendDelay := modelDuration(cmd.Flag("resend-delay", "Minimum amount of time to wait before resending an alert to Alertmanager.").
		Default("1m"))
	evalInterval := modelDuration(cmd.Flag("eval-interval", "The default evaluation interval to use.").
		Default("30s"))
	tsdbBlockDuration := modelDuration(cmd.Flag("tsdb.block-duration", "Block duration for TSDB block.").
		Default("2h"))
	tsdbRetention := modelDuration(cmd.Flag("tsdb.retention", "Block retention time on local disk.").
		Default("48h"))

	alertmgrs := cmd.Flag("alertmanagers.url", "Alertmanager replica URLs to push firing alerts. Ruler claims success if push to at least one alertmanager from discovered succeeds. The scheme should not be empty e.g `http` might be used. The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Alertmanager IPs through respective DNS lookups. The port defaults to 9093 or the SRV record's value. The URL path is used as a prefix for the regular Alertmanager API path.").
		Strings()
	alertmgrsTimeout := cmd.Flag("alertmanagers.send-timeout", "Timeout for sending alerts to Alertmanager").Default("10s").Duration()
	alertmgrsConfig := extflag.RegisterPathOrContent(cmd, "alertmanagers.config", "YAML file that contains alerting configuration. See format details: https://thanos.io/components/rule.md/#configuration. If defined, it takes precedence over the '--alertmanagers.url' and '--alertmanagers.send-timeout' flags.", false)
	alertmgrsDNSSDInterval := modelDuration(cmd.Flag("alertmanagers.sd-dns-interval", "Interval between DNS resolutions of Alertmanager hosts.").
		Default("30s"))

	alertQueryURL := cmd.Flag("alert.query-url", "The external Thanos Query URL that would be set in all alerts 'Source' field").String()

	alertExcludeLabels := cmd.Flag("alert.label-drop", "Labels by name to drop before sending to alertmanager. This allows alert to be deduplicated on replica label (repeated). Similar Prometheus alert relabelling").
		Strings()
	webRoutePrefix := cmd.Flag("web.route-prefix", "Prefix for API and UI endpoints. This allows thanos UI to be served on a sub-path. This option is analogous to --web.route-prefix of Promethus.").Default("").String()
	webExternalPrefix := cmd.Flag("web.external-prefix", "Static prefix for all HTML links and redirect URLs in the UI query web interface. Actual endpoints are still served on / or the web.route-prefix. This allows thanos UI to be served behind a reverse proxy that strips a URL sub-path.").Default("").String()
	webPrefixHeaderName := cmd.Flag("web.prefix-header", "Name of HTTP request header used for dynamic prefixing of UI links and redirects. This option is ignored if web.external-prefix argument is set. Security risk: enable this option only if a reverse proxy in front of thanos is resetting the header. The --web.prefix-header=X-Forwarded-Prefix option can be useful, for example, if Thanos UI is served via Traefik reverse proxy with PathPrefixStrip option enabled, which sends the stripped prefix value in X-Forwarded-Prefix header. This allows thanos UI to be served on a sub-path.").Default("").String()

	objStoreConfig := regCommonObjStoreFlags(cmd, "", false)

	queries := cmd.Flag("query", "Addresses of statically configured query API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect query API servers through respective DNS lookups.").
		PlaceHolder("<query>").Strings()

	fileSDFiles := cmd.Flag("query.sd-files", "Path to file that contain addresses of query peers. The path can be a glob pattern (repeatable).").
		PlaceHolder("<path>").Strings()

	fileSDInterval := modelDuration(cmd.Flag("query.sd-interval", "Refresh interval to re-read file SD files. (used as a fallback)").
		Default("5m"))

	dnsSDInterval := modelDuration(cmd.Flag("query.sd-dns-interval", "Interval between DNS resolutions.").
		Default("30s"))

	dnsSDResolver := cmd.Flag("query.sd-dns-resolver", "Resolver to use. Possible options: [golang, miekgdns]").
		Default("golang").Hidden().String()

	m[comp.String()] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		lset, err := parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}
		alertQueryURL, err := url.Parse(*alertQueryURL)
		if err != nil {
			return errors.Wrap(err, "parse alert query url")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration:  *tsdbBlockDuration,
			MaxBlockDuration:  *tsdbBlockDuration,
			RetentionDuration: *tsdbRetention,
			NoLockfile:        true,
		}

		lookupQueries := map[string]struct{}{}
		for _, q := range *queries {
			if _, ok := lookupQueries[q]; ok {
				return errors.Errorf("Address %s is duplicated for --query flag.", q)
			}

			lookupQueries[q] = struct{}{}
		}

		var fileSD *file.Discovery
		if len(*fileSDFiles) > 0 {
			conf := &file.SDConfig{
				Files:           *fileSDFiles,
				RefreshInterval: *fileSDInterval,
			}
			fileSD = file.NewDiscovery(conf, logger)
		}

		if fileSD == nil && len(*queries) == 0 {
			return errors.Errorf("No --query parameter was given.")
		}

		return runRule(g,
			logger,
			reg,
			tracer,
			lset,
			*alertmgrs,
			*alertmgrsTimeout,
			alertmgrsConfig,
			time.Duration(*alertmgrsDNSSDInterval),
			*grpcBindAddr,
			time.Duration(*grpcGracePeriod),
			*grpcCert,
			*grpcKey,
			*grpcClientCA,
			*httpBindAddr,
			time.Duration(*httpGracePeriod),
			*webRoutePrefix,
			*webExternalPrefix,
			*webPrefixHeaderName,
			time.Duration(*resendDelay),
			time.Duration(*evalInterval),
			*dataDir,
			*ruleFiles,
			objStoreConfig,
			tsdbOpts,
			alertQueryURL,
			*alertExcludeLabels,
			*queries,
			fileSD,
			time.Duration(*dnsSDInterval),
			*dnsSDResolver,
			comp,
		)
	}
}

// runRule runs a rule evaluation component that continuously evaluates alerting and recording
// rules. It sends alert notifications and writes TSDB data for results like a regular Prometheus server.
func runRule(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	lset labels.Labels,
	alertmgrURLs []string,
	alertmgrsTimeout time.Duration,
	alertmgrsConfig *extflag.PathOrContent,
	alertmgrsDNSSDInterval time.Duration,
	grpcBindAddr string,
	grpcGracePeriod time.Duration,
	grpcCert string,
	grpcKey string,
	grpcClientCA string,
	httpBindAddr string,
	httpGracePeriod time.Duration,
	webRoutePrefix string,
	webExternalPrefix string,
	webPrefixHeaderName string,
	resendDelay time.Duration,
	evalInterval time.Duration,
	dataDir string,
	ruleFiles []string,
	objStoreConfig *extflag.PathOrContent,
	tsdbOpts *tsdb.Options,
	alertQueryURL *url.URL,
	alertExcludeLabels []string,
	queryAddrs []string,
	fileSD *file.Discovery,
	dnsSDInterval time.Duration,
	dnsSDResolver string,
	comp component.Component,
) error {
	configSuccess := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_rule_config_last_reload_successful",
		Help: "Whether the last configuration reload attempt was successful.",
	})
	configSuccessTime := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_rule_config_last_reload_success_timestamp_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	})
	duplicatedQuery := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_rule_duplicated_query_address",
		Help: "The number of times a duplicated query addresses is detected from the different configs in rule",
	})
	rulesLoaded := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "thanos_rule_loaded_rules",
			Help: "Loaded rules partitioned by file and group",
		},
		[]string{"strategy", "file", "group"},
	)
	ruleEvalWarnings := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "thanos_rule_evaluation_with_warnings_total",
			Help: "The total number of rule evaluation that were successful but had warnings which can indicate partial error.",
		}, []string{"strategy"},
	)
	ruleEvalWarnings.WithLabelValues(strings.ToLower(storepb.PartialResponseStrategy_ABORT.String()))
	ruleEvalWarnings.WithLabelValues(strings.ToLower(storepb.PartialResponseStrategy_WARN.String()))

	reg.MustRegister(configSuccess)
	reg.MustRegister(configSuccessTime)
	reg.MustRegister(duplicatedQuery)
	reg.MustRegister(rulesLoaded)
	reg.MustRegister(ruleEvalWarnings)

	for _, addr := range queryAddrs {
		if addr == "" {
			return errors.New("static querier address cannot be empty")
		}
	}

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

	// FileSD query addresses.
	fileSDCache := cache.New()

	dnsProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_ruler_query_apis_", reg),
		dns.ResolverType(dnsSDResolver),
	)

	// Build the Alertmanager clients.
	alertmgrsConfigYAML, err := alertmgrsConfig.Content()
	if err != nil {
		return err
	}
	var (
		alertingCfg alert.AlertingConfig
		alertmgrs   []*alert.Alertmanager
	)
	if len(alertmgrsConfigYAML) > 0 {
		if len(alertmgrURLs) != 0 {
			return errors.New("--alertmanagers.url and --alertmanagers.config* flags cannot be defined at the same time")
		}
		alertingCfg, err = alert.LoadAlertingConfig(alertmgrsConfigYAML)
		if err != nil {
			return err
		}
	} else {
		// Build the Alertmanager configuration from the legacy flags.
		for _, addr := range alertmgrURLs {
			cfg, err := alert.BuildAlertmanagerConfig(logger, addr, alertmgrsTimeout)
			if err != nil {
				return err
			}
			alertingCfg.Alertmanagers = append(alertingCfg.Alertmanagers, cfg)
		}
	}

	if len(alertingCfg.Alertmanagers) == 0 {
		level.Warn(logger).Log("msg", "no alertmanager configured")
	}

	amProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_ruler_alertmanagers_", reg),
		dns.ResolverType(dnsSDResolver),
	)
	for _, cfg := range alertingCfg.Alertmanagers {
		// Each Alertmanager client has a different list of targets thus each needs its own DNS provider.
		am, err := alert.NewAlertmanager(logger, cfg, amProvider.Clone())
		if err != nil {
			return err
		}
		alertmgrs = append(alertmgrs, am)
	}

	// Run rule evaluation and alert notifications.
	var (
		alertQ  = alert.NewQueue(logger, reg, 10000, 100, labelsTSDBToProm(lset), alertExcludeLabels)
		ruleMgr = thanosrule.NewManager(dataDir)
	)
	{
		notify := func(ctx context.Context, expr string, alerts ...*rules.Alert) {
			res := make([]*alert.Alert, 0, len(alerts))
			for _, alrt := range alerts {
				// Only send actually firing alerts.
				if alrt.State == rules.StatePending {
					continue
				}
				a := &alert.Alert{
					StartsAt:     alrt.FiredAt,
					Labels:       alrt.Labels,
					Annotations:  alrt.Annotations,
					GeneratorURL: alertQueryURL.String() + strutil.TableLinkForExpression(expr),
				}
				if !alrt.ResolvedAt.IsZero() {
					a.EndsAt = alrt.ResolvedAt
				} else {
					a.EndsAt = alrt.ValidUntil
				}
				res = append(res, a)
			}
			alertQ.Push(res)
		}
		st := tsdb.Adapter(db, 0)

		opts := rules.ManagerOptions{
			NotifyFunc:  notify,
			Logger:      log.With(logger, "component", "rules"),
			Appendable:  st,
			ExternalURL: nil,
			TSDB:        st,
			ResendDelay: resendDelay,
		}

		// TODO(bwplotka): Hide this behind thanos rules.Manager.
		for _, strategy := range storepb.PartialResponseStrategy_value {
			s := storepb.PartialResponseStrategy(strategy)

			ctx, cancel := context.WithCancel(context.Background())
			ctx = tracing.ContextWithTracer(ctx, tracer)

			opts := opts
			opts.Registerer = extprom.WrapRegistererWith(prometheus.Labels{"strategy": strings.ToLower(s.String())}, reg)
			opts.Context = ctx
			opts.QueryFunc = queryFunc(logger, dnsProvider, duplicatedQuery, ruleEvalWarnings, s)

			mgr := rules.NewManager(&opts)
			ruleMgr.SetRuleManager(s, mgr)
			g.Add(func() error {
				mgr.Run()
				<-ctx.Done()

				return nil
			}, func(error) {
				cancel()
				mgr.Stop()
			})
		}
	}
	// Discover and resolve Alertmanager addresses.
	{
		for i := range alertmgrs {
			am := alertmgrs[i]
			ctx, cancel := context.WithCancel(context.Background())
			g.Add(func() error {
				am.Discover(ctx)
				return nil
			}, func(error) {
				cancel()
			})

			g.Add(func() error {
				return runutil.Repeat(alertmgrsDNSSDInterval, ctx.Done(), func() error {
					am.Resolve(ctx)
					return nil
				})
			}, func(error) {
				cancel()
			})
		}
	}
	// Run the alert sender.
	{
		clients := make([]alert.AlertmanagerClient, len(alertmgrs))
		for i := range alertmgrs {
			clients[i] = alertmgrs[i]
		}
		sdr := alert.NewSender(logger, reg, clients)
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
	// Run File Service Discovery and update the query addresses when the files are modified.
	if fileSD != nil {
		var fileSDUpdates chan []*targetgroup.Group
		ctxRun, cancelRun := context.WithCancel(context.Background())

		fileSDUpdates = make(chan []*targetgroup.Group)

		g.Add(func() error {
			fileSD.Run(ctxRun, fileSDUpdates)
			return nil
		}, func(error) {
			cancelRun()
		})

		ctxUpdate, cancelUpdate := context.WithCancel(context.Background())
		g.Add(func() error {
			for {
				select {
				case update := <-fileSDUpdates:
					// Discoverers sometimes send nil updates so need to check for it to avoid panics.
					if update == nil {
						continue
					}
					fileSDCache.Update(update)
				case <-ctxUpdate.Done():
					return nil
				}
			}
		}, func(error) {
			cancelUpdate()
			close(fileSDUpdates)
		})
	}

	// Handle reload and termination interrupts.
	reload := make(chan struct{}, 1)
	{
		cancel := make(chan struct{})
		reload <- struct{}{} // Initial reload.

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

				if err := ruleMgr.Update(evalInterval, files); err != nil {
					configSuccess.Set(0)
					level.Error(logger).Log("msg", "reloading rules failed", "err", err)
					continue
				}

				configSuccess.Set(1)
				configSuccessTime.Set(float64(time.Now().UnixNano()) / 1e9)

				rulesLoaded.Reset()
				for _, group := range ruleMgr.RuleGroups() {
					rulesLoaded.WithLabelValues(group.PartialResponseStrategy.String(), group.File(), group.Name()).Set(float64(len(group.Rules())))
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
	// Periodically update the addresses from static flags and file SD by resolving them using DNS SD if necessary.
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			return runutil.Repeat(dnsSDInterval, ctx.Done(), func() error {
				dnsProvider.Resolve(ctx, append(fileSDCache.Addresses(), queryAddrs...))
				return nil
			})
		}, func(error) {
			cancel()
		})
	}
	statusProber := prober.New(comp, logger, prometheus.WrapRegistererWithPrefix("thanos_", reg))
	// Start gRPC server.
	{
		store := store.NewTSDBStore(logger, reg, db, component.Rule, lset)

		tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), grpcCert, grpcKey, grpcClientCA)
		if err != nil {
			return errors.Wrap(err, "setup gRPC server")
		}

		s := grpcserver.New(logger, reg, tracer, comp, store,
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
	// Start UI & metrics HTTP server.
	{
		router := route.New()

		// Redirect from / to /webRoutePrefix.
		if webRoutePrefix != "" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, webRoutePrefix, http.StatusFound)
			})
		}

		router.WithPrefix(webRoutePrefix).Post("/-/reload", func(w http.ResponseWriter, r *http.Request) {
			reload <- struct{}{}
		})

		flagsMap := map[string]string{
			// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
			"web.external-prefix": webExternalPrefix,
			"web.prefix-header":   webPrefixHeaderName,
		}

		ins := extpromhttp.NewInstrumentationMiddleware(reg)

		ui.NewRuleUI(logger, reg, ruleMgr, alertQueryURL.String(), flagsMap).Register(router.WithPrefix(webRoutePrefix), ins)

		api := v1.NewAPI(logger, reg, ruleMgr)
		api.Register(router.WithPrefix(path.Join(webRoutePrefix, "/api/v1")), tracer, logger, ins)

		// Initiate HTTP listener providing metrics endpoint and readiness/liveness probes.
		srv := httpserver.New(logger, reg, comp, statusProber,
			httpserver.WithListen(httpBindAddr),
			httpserver.WithGracePeriod(httpGracePeriod),
		)
		srv.Handle("/", router)

		g.Add(func() error {
			statusProber.Healthy()

			return srv.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			defer statusProber.NotHealthy(err)

			srv.Shutdown(err)
		})
	}

	confContentYaml, err := objStoreConfig.Content()
	if err != nil {
		return err
	}

	if len(confContentYaml) > 0 {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		bkt, err := client.NewBucket(logger, confContentYaml, reg, component.Rule.String())
		if err != nil {
			return err
		}

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		s := shipper.New(logger, nil, dataDir, bkt, func() labels.Labels { return lset }, metadata.RulerSource)

		ctx, cancel := context.WithCancel(context.Background())

		g.Add(func() error {
			defer runutil.CloseWithLogOnErr(logger, bkt, "bucket client")

			return runutil.Repeat(30*time.Second, ctx.Done(), func() error {
				if _, err := s.Sync(ctx); err != nil {
					level.Warn(logger).Log("err", err)
				}
				return nil
			})
		}, func(error) {
			cancel()
		})
	} else {
		level.Info(logger).Log("msg", "no supported bucket was configured, uploads will be disabled")
	}

	level.Info(logger).Log("msg", "starting rule node")
	return nil
}

func parseFlagLabels(s []string) (labels.Labels, error) {
	var lset labels.Labels
	for _, l := range s {
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("unrecognized label %q", l)
		}
		if !model.LabelName.IsValid(model.LabelName(string(parts[0]))) {
			return nil, errors.Errorf("unsupported format for label %s", l)
		}
		val, err := strconv.Unquote(parts[1])
		if err != nil {
			return nil, errors.Wrap(err, "unquote label value")
		}
		lset = append(lset, labels.Label{Name: parts[0], Value: val})
	}
	return lset, nil
}

func labelsTSDBToProm(lset labels.Labels) (res labels.Labels) {
	for _, l := range lset {
		res = append(res, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res
}

func removeDuplicateQueryAddrs(logger log.Logger, duplicatedQueriers prometheus.Counter, addrs []string) []string {
	set := make(map[string]struct{})
	for _, addr := range addrs {
		if _, ok := set[addr]; ok {
			level.Warn(logger).Log("msg", "duplicate query address is provided - %v", addr)
			duplicatedQueriers.Inc()
		}
		set[addr] = struct{}{}
	}

	deduplicated := make([]string, 0, len(set))
	for key := range set {
		deduplicated = append(deduplicated, key)
	}
	return deduplicated
}

// queryFunc returns query function that hits the HTTP query API of query peers in randomized order until we get a result
// back or the context get canceled.
func queryFunc(
	logger log.Logger,
	dnsProvider *dns.Provider,
	duplicatedQuery prometheus.Counter,
	ruleEvalWarnings *prometheus.CounterVec,
	partialResponseStrategy storepb.PartialResponseStrategy,
) rules.QueryFunc {
	var spanID string

	switch partialResponseStrategy {
	case storepb.PartialResponseStrategy_WARN:
		spanID = "/rule_instant_query HTTP[client]"
	case storepb.PartialResponseStrategy_ABORT:
		spanID = "/rule_instant_query_part_resp_abort HTTP[client]"
	default:
		// Programming error will be caught by tests.
		panic(errors.Errorf("unknown partial response strategy %v", partialResponseStrategy).Error())
	}

	return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		// Add DNS resolved addresses from static flags and file SD.
		// TODO(bwplotka): Consider generating addresses in *url.URL.
		addrs := dnsProvider.Addresses()

		addrs = removeDuplicateQueryAddrs(logger, duplicatedQuery, addrs)

		for _, i := range rand.Perm(len(addrs)) {
			u, err := url.Parse(fmt.Sprintf("http://%s", addrs[i]))
			if err != nil {
				return nil, errors.Wrapf(err, "url parse %s", addrs[i])
			}

			span, ctx := tracing.StartSpan(ctx, spanID)
			v, warns, err := promclient.PromqlQueryInstant(ctx, logger, u, q, t, promclient.QueryOptions{
				Deduplicate:             true,
				PartialResponseStrategy: partialResponseStrategy,
			})
			span.Finish()

			if err != nil {
				level.Error(logger).Log("err", err, "query", q)
			} else {
				if len(warns) > 0 {
					ruleEvalWarnings.WithLabelValues(strings.ToLower(partialResponseStrategy.String())).Inc()
					// TODO(bwplotka): Propagate those to UI, probably requires changing rule manager code ):
					level.Warn(logger).Log("warnings", strings.Join(warns, ", "), "query", q)
				}
				return v, nil
			}
		}
		return nil, errors.Errorf("no query peer reachable")
	}
}
