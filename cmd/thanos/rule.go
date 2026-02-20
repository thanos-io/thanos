// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"maps"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	texttemplate "text/template"
	"time"

	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/agent"
	"github.com/prometheus/prometheus/util/compression"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"
	"github.com/thanos-io/promql-engine/execution/parse"

	"github.com/thanos-io/thanos/pkg/alert"
	v1 "github.com/thanos-io/thanos/pkg/api/rule"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/clientconfig"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/compressutil"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extannotations"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extprom"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/extpromql"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/logging"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/prober"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/query"
	thanosrules "github.com/thanos-io/thanos/pkg/rules"
	"github.com/thanos-io/thanos/pkg/runutil"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/tls"
	"github.com/thanos-io/thanos/pkg/tracing"
	"github.com/thanos-io/thanos/pkg/ui"
)

type ruleConfig struct {
	http    httpConfig
	grpc    grpcConfig
	web     webConfig
	shipper shipperConfig

	query              queryConfig
	queryConfigYAML    []byte
	grpcQueryEndpoints []string

	alertmgr               alertMgrConfig
	alertmgrsConfigYAML    []byte
	alertQueryURL          *url.URL
	alertRelabelConfigYAML []byte

	rwConfig *extflag.PathOrContent

	resendDelay        time.Duration
	evalInterval       time.Duration
	queryOffset        time.Duration
	outageTolerance    time.Duration
	forGracePeriod     time.Duration
	ruleFiles          []string
	objStoreConfig     *extflag.PathOrContent
	dataDir            string
	lset               labels.Labels
	ignoredLabelNames  []string
	storeRateLimits    store.SeriesSelectLimits
	ruleConcurrentEval int64

	extendedFunctionsEnabled   bool
	EnableFeatures             []string
	tsdbEnableNativeHistograms bool
}

type Expression struct {
	Expr string
}

func (rc *ruleConfig) registerFlag(cmd extkingpin.FlagClause) {
	rc.http.registerFlag(cmd)
	rc.grpc.registerFlag(cmd)
	rc.web.registerFlag(cmd)
	rc.shipper.registerFlag(cmd)
	rc.query.registerFlag(cmd)
	rc.alertmgr.registerFlag(cmd)
	rc.storeRateLimits.RegisterFlags(cmd)
}

// registerRule registers a rule command.
func registerRule(app *extkingpin.App) {
	comp := component.Rule
	cmd := app.Command(comp.String(), "Ruler evaluating Prometheus rules against given Query nodes, exposing Store API and storing old blocks in bucket.")

	conf := &ruleConfig{}
	conf.registerFlag(cmd)

	labelStrs := cmd.Flag("label", "Labels to be applied to all generated metrics (repeated). Similar to external labels for Prometheus, used to identify ruler and its blocks as unique source.").
		PlaceHolder("<name>=\"<value>\"").Strings()
	tsdbBlockDuration := extkingpin.ModelDuration(cmd.Flag("tsdb.block-duration", "Block duration for TSDB block.").
		Default("2h"))
	tsdbRetention := extkingpin.ModelDuration(cmd.Flag("tsdb.retention", "Block retention time on local disk.").
		Default("48h"))
	noLockFile := cmd.Flag("tsdb.no-lockfile", "Do not create lockfile in TSDB data directory. In any case, the lockfiles will be deleted on next startup.").Default("false").Bool()
	walCompression := cmd.Flag("tsdb.wal-compression", "Compress the tsdb WAL.").Default("true").Bool()

	cmd.Flag("data-dir", "data directory").Default("data/").StringVar(&conf.dataDir)
	cmd.Flag("rule-file", "Rule files that should be used by rule manager. Can be in glob format (repeated). Note that rules are not automatically detected, use SIGHUP or do HTTP POST /-/reload to re-read them.").
		Default("rules/").StringsVar(&conf.ruleFiles)
	cmd.Flag("resend-delay", "Minimum amount of time to wait before resending an alert to Alertmanager.").
		Default("1m").DurationVar(&conf.resendDelay)
	cmd.Flag("eval-interval", "The default evaluation interval to use.").
		Default("1m").DurationVar(&conf.evalInterval)
	cmd.Flag("rule-query-offset", "The default rule group query_offset duration to use.").
		Default("0s").DurationVar(&conf.queryOffset)
	cmd.Flag("for-outage-tolerance", "Max time to tolerate prometheus outage for restoring \"for\" state of alert.").
		Default("1h").DurationVar(&conf.outageTolerance)
	cmd.Flag("for-grace-period", "Minimum duration between alert and restored \"for\" state. This is maintained only for alerts with configured \"for\" time greater than grace period.").
		Default("10m").DurationVar(&conf.forGracePeriod)
	cmd.Flag("restore-ignored-label", "Label names to be ignored when restoring alerts from the remote storage. This is only used in stateless mode.").
		StringsVar(&conf.ignoredLabelNames)
	cmd.Flag("rule-concurrent-evaluation", "How many rules can be evaluated concurrently. Default is 1.").Default("1").Int64Var(&conf.ruleConcurrentEval)

	cmd.Flag("grpc-query-endpoint", "Addresses of Thanos gRPC query API servers (repeatable). The scheme may be prefixed with 'dns+' or 'dnssrv+' to detect Thanos API servers through respective DNS lookups.").
		PlaceHolder("<endpoint>").StringsVar(&conf.grpcQueryEndpoints)

	cmd.Flag("query.enable-x-functions", "Whether to enable extended rate functions (xrate, xincrease and xdelta). Only has effect when used with Thanos engine.").Default("false").BoolVar(&conf.extendedFunctionsEnabled)
	cmd.Flag("enable-feature", "Comma separated feature names to enable. Valid options for now: promql-experimental-functions (enables promql experimental functions for ruler)").Default("").StringsVar(&conf.EnableFeatures)

	cmd.Flag("tsdb.enable-native-histograms",
		"(Deprecated) Enables the ingestion of native histograms. This flag is a no-op now and will be removed in the future. Native histogram ingestion is always enabled.").
		Default("true").BoolVar(&conf.tsdbEnableNativeHistograms)

	conf.rwConfig = extflag.RegisterPathOrContent(cmd, "remote-write.config", "YAML config for the remote-write configurations, that specify servers where samples should be sent to (see https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write). This automatically enables stateless mode for ruler and no series will be stored in the ruler's TSDB. If an empty config (or file) is provided, the flag is ignored and ruler is run with its own TSDB.", extflag.WithEnvSubstitution())

	conf.objStoreConfig = extkingpin.RegisterCommonObjStoreFlags(cmd, "", false)

	reqLogConfig := extkingpin.RegisterRequestLoggingFlags(cmd)

	var err error
	cmd.Setup(func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, reload <-chan struct{}, _ bool) error {
		conf.lset, err = parseFlagLabels(*labelStrs)
		if err != nil {
			return errors.Wrap(err, "parse labels")
		}

		conf.alertQueryURL, err = url.Parse(*conf.alertmgr.alertQueryURL)
		if err != nil {
			return errors.Wrap(err, "parse alert query url")
		}

		tsdbOpts := &tsdb.Options{
			MinBlockDuration:  int64(time.Duration(*tsdbBlockDuration) / time.Millisecond),
			MaxBlockDuration:  int64(time.Duration(*tsdbBlockDuration) / time.Millisecond),
			RetentionDuration: int64(time.Duration(*tsdbRetention) / time.Millisecond),
			NoLockfile:        *noLockFile,
			WALCompression:    compressutil.ParseCompressionType(*walCompression, compression.Snappy),
		}

		agentOpts := &agent.Options{
			WALCompression: compressutil.ParseCompressionType(*walCompression, compression.Snappy),
			NoLockfile:     *noLockFile,
		}

		// Parse and check query configuration.
		lookupQueries := map[string]struct{}{}
		for _, q := range conf.query.addrs {
			if _, ok := lookupQueries[q]; ok {
				return errors.Errorf("Address %s is duplicated for --query flag.", q)
			}

			lookupQueries[q] = struct{}{}
		}

		conf.queryConfigYAML, err = conf.query.configPath.Content()
		if err != nil {
			return err
		}

		if len(conf.query.sdFiles) == 0 && len(conf.query.addrs) == 0 && len(conf.queryConfigYAML) == 0 && len(conf.grpcQueryEndpoints) == 0 {
			return errors.New("no query configuration parameter was given")
		}
		if (len(conf.query.sdFiles) != 0 || len(conf.query.addrs) != 0 || len(conf.grpcQueryEndpoints) != 0) && len(conf.queryConfigYAML) != 0 {
			return errors.New("--query/--query.sd-files/--grpc-query-endpoint and --query.config* parameters cannot be defined at the same time")
		}

		// Parse and check alerting configuration.
		conf.alertmgrsConfigYAML, err = conf.alertmgr.configPath.Content()
		if err != nil {
			return err
		}
		if len(conf.alertmgrsConfigYAML) != 0 && len(conf.alertmgr.alertmgrURLs) != 0 {
			return errors.New("--alertmanagers.url and --alertmanagers.config* parameters cannot be defined at the same time")
		}

		conf.alertRelabelConfigYAML, err = conf.alertmgr.alertRelabelConfigPath.Content()
		if err != nil {
			return err
		}

		httpLogOpts, err := logging.ParseHTTPOptions(reqLogConfig)
		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		grpcLogOpts, logFilterMethods, err := logging.ParsegRPCOptions(reqLogConfig)

		if err != nil {
			return errors.Wrap(err, "error while parsing config for request logging")
		}

		return runRule(
			g,
			logger,
			reg,
			tracer,
			comp,
			*conf,
			reload,
			getFlagsMap(cmd.Flags()),
			httpLogOpts,
			grpcLogOpts,
			logFilterMethods,
			tsdbOpts,
			agentOpts,
		)
	})
}

// RuleMetrics defines Thanos Ruler metrics.
type RuleMetrics struct {
	configSuccess     prometheus.Gauge
	configSuccessTime prometheus.Gauge
	duplicatedQuery   prometheus.Counter
	rulesLoaded       *prometheus.GaugeVec
	ruleEvalWarnings  *prometheus.CounterVec
}

func newRuleMetrics(reg *prometheus.Registry) *RuleMetrics {
	m := new(RuleMetrics)

	factory := promauto.With(reg)
	m.configSuccess = factory.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_rule_config_last_reload_successful",
		Help: "Whether the last configuration reload attempt was successful.",
	})
	m.configSuccessTime = factory.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_rule_config_last_reload_success_timestamp_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	})
	m.duplicatedQuery = factory.NewCounter(prometheus.CounterOpts{
		Name: "thanos_rule_duplicated_query_addresses_total",
		Help: "The number of times a duplicated query addresses is detected from the different configs in rule.",
	})
	m.rulesLoaded = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "thanos_rule_loaded_rules",
			Help: "Loaded rules partitioned by file and group.",
		},
		[]string{"strategy", "file", "group"},
	)
	m.ruleEvalWarnings = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "thanos_rule_evaluation_with_warnings_total",
			Help: "The total number of rule evaluation that were successful but had non PromQL warnings which can indicate partial error.",
		}, []string{"strategy"},
	)
	m.ruleEvalWarnings.WithLabelValues(strings.ToLower(storepb.PartialResponseStrategy_ABORT.String()))
	m.ruleEvalWarnings.WithLabelValues(strings.ToLower(storepb.PartialResponseStrategy_WARN.String()))

	return m
}

// runRule runs a rule evaluation component that continuously evaluates alerting and recording
// rules. It sends alert notifications and writes TSDB data for results like a regular Prometheus server.
func runRule(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	comp component.Component,
	conf ruleConfig,
	reloadSignal <-chan struct{},
	flagsMap map[string]string,
	httpLogOpts []logging.Option,
	grpcLogOpts []grpc_logging.Option,
	logFilterMethods []string,
	tsdbOpts *tsdb.Options,
	agentOpts *agent.Options,
) error {
	metrics := newRuleMetrics(reg)

	var queryCfg []clientconfig.Config
	var err error
	if len(conf.queryConfigYAML) > 0 {
		queryCfg, err = clientconfig.LoadConfigs(conf.queryConfigYAML)
		if err != nil {
			return errors.Wrap(err, "query configuration")
		}
	} else {
		queryCfg, err = clientconfig.BuildConfigFromHTTPAddresses(conf.query.addrs)
		if err != nil {
			return errors.Wrap(err, "query configuration")
		}

		// Build the query configuration from the legacy query flags.
		var fileSDConfigs []clientconfig.HTTPFileSDConfig
		if len(conf.query.sdFiles) > 0 {
			fileSDConfigs = append(fileSDConfigs, clientconfig.HTTPFileSDConfig{
				Files:           conf.query.sdFiles,
				RefreshInterval: model.Duration(conf.query.sdInterval),
			})
			queryCfg = append(queryCfg,
				clientconfig.Config{
					HTTPConfig: clientconfig.HTTPConfig{
						EndpointsConfig: clientconfig.HTTPEndpointsConfig{
							Scheme:        "http",
							FileSDConfigs: fileSDConfigs,
						},
					},
				},
			)
		}

		grpcQueryCfg, err := clientconfig.BuildConfigFromGRPCAddresses(conf.grpcQueryEndpoints)
		if err != nil {
			return errors.Wrap(err, "query configuration")
		}
		queryCfg = append(queryCfg, grpcQueryCfg...)
	}

	if err := validateTemplate(*conf.alertmgr.alertSourceTemplate); err != nil {
		return errors.Wrap(err, "invalid alert source template")
	}

	queryProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_rule_query_apis_", reg),
		dns.ResolverType(conf.query.dnsSDResolver),
	)
	var (
		queryClients    []*clientconfig.HTTPClient
		promClients     []*promclient.Client
		grpcEndpointSet *query.EndpointSet
		grpcEndpoints   []string
	)

	queryClientMetrics := extpromhttp.NewClientMetrics(extprom.WrapRegistererWith(prometheus.Labels{"client": "query"}, reg))

	for _, cfg := range queryCfg {
		if cfg.HTTPConfig.NotEmpty() {
			cfg.HTTPConfig.HTTPClientConfig.ClientMetrics = queryClientMetrics
			c, err := clientconfig.NewHTTPClient(cfg.HTTPConfig.HTTPClientConfig, "query")
			if err != nil {
				return fmt.Errorf("failed to create HTTP query client: %w", err)
			}
			c.Transport = tracing.HTTPTripperware(logger, c.Transport)
			queryClient, err := clientconfig.NewClient(logger, cfg.HTTPConfig.EndpointsConfig, c, queryProvider.Clone())
			if err != nil {
				return fmt.Errorf("failed to create query client: %w", err)
			}
			queryClients = append(queryClients, queryClient)
			promClients = append(promClients, promclient.NewClient(queryClient, logger, "thanos-rule"))
			// Discover and resolve query addresses.
			addDiscoveryGroups(g, queryClient, conf.query.dnsSDInterval, logger)
		}

		if cfg.GRPCConfig != nil {
			grpcEndpoints = append(grpcEndpoints, cfg.GRPCConfig.EndpointAddrs...)
		}
	}

	if len(grpcEndpoints) > 0 {
		dialOpts, err := extgrpc.StoreClientGRPCOpts(
			logger,
			reg,
			tracer,
		)
		if err != nil {
			return err
		}

		tlsDialOpts, err := extgrpc.StoreClientTLSCredentials(logger, false, false, "", "", "", "", "")
		if err != nil {
			return err
		}

		// No TLS config for rule component
		noTLSConfig := &tlsConfig{}

		grpcEndpointSet, err = setupEndpointSet(
			g,
			comp,
			reg,
			logger,
			nil,
			1*time.Minute,
			nil,
			1*time.Minute,
			grpcEndpoints,
			nil,
			nil,
			nil,
			conf.query.dnsSDResolver,
			conf.query.dnsSDInterval,
			5*time.Minute,
			5*time.Second,
			conf.evalInterval,
			dialOpts,
			noTLSConfig,
			tlsDialOpts,
			"", // no global compression
			[]string{},
		)
		if err != nil {
			return err
		}
	}

	var (
		appendable storage.Appendable
		queryable  storage.Queryable
		tsdbDB     *tsdb.DB
		agentDB    *agent.DB
	)

	rwCfgYAML, err := conf.rwConfig.Content()
	if err != nil {
		return err
	}

	if len(rwCfgYAML) > 0 {
		var rwCfg struct {
			RemoteWriteConfigs []*config.RemoteWriteConfig `yaml:"remote_write,omitempty"`
		}
		if err := yaml.Unmarshal(rwCfgYAML, &rwCfg); err != nil {
			return errors.Wrapf(err, "failed to parse remote write config %v", string(rwCfgYAML))
		}

		slogger := logutil.GoKitLogToSlog(logger)
		// flushDeadline is set to 1m, but it is for metadata watcher only so not used here.
		// TODO: add type and unit labels support?
		remoteStore := remote.NewStorage(slogger, reg, func() (int64, error) {
			return 0, nil
		}, conf.dataDir, 1*time.Minute, &readyScrapeManager{}, false)
		if err := remoteStore.ApplyConfig(&config.Config{
			GlobalConfig: config.GlobalConfig{
				ExternalLabels: labelsTSDBToProm(conf.lset),
			},
			RemoteWriteConfigs: rwCfg.RemoteWriteConfigs,
		}); err != nil {
			return errors.Wrap(err, "applying config to remote storage")
		}

		agentDB, err = agent.Open(slogger, reg, remoteStore, conf.dataDir, agentOpts)
		if err != nil {
			return errors.Wrap(err, "start remote write agent db")
		}
		fanoutStore := storage.NewFanout(slogger, agentDB, remoteStore)
		appendable = fanoutStore
		// Use a separate queryable to restore the ALERTS firing states.
		// We cannot use remoteStore directly because it uses remote read for
		// query. However, remote read is not implemented in Thanos Receiver.
		queryable = thanosrules.NewPromClientsQueryable(logger, queryClients, promClients, conf.query.httpMethod, conf.query.step, conf.ignoredLabelNames)
	} else {
		tsdbDB, err = tsdb.Open(conf.dataDir, logutil.GoKitLogToSlog(log.With(logger, "component", "tsdb")), reg, tsdbOpts, nil)
		if err != nil {
			return errors.Wrap(err, "open TSDB")
		}

		level.Debug(logger).Log("msg", "removing storage lock file if any")
		if err := removeLockfileIfAny(logger, conf.dataDir); err != nil {
			return errors.Wrap(err, "remove storage lock files")
		}

		{
			done := make(chan struct{})
			g.Add(func() error {
				<-done
				return tsdbDB.Close()
			}, func(error) {
				close(done)
			})
		}
		appendable = tsdbDB
		queryable = tsdbDB
	}

	// Build the Alertmanager clients.
	var alertingCfg alert.AlertingConfig
	if len(conf.alertmgrsConfigYAML) > 0 {
		alertingCfg, err = alert.LoadAlertingConfig(conf.alertmgrsConfigYAML)
		if err != nil {
			return err
		}
	} else {
		// Build the Alertmanager configuration from the legacy flags.
		for _, addr := range conf.alertmgr.alertmgrURLs {
			cfg, err := alert.BuildAlertmanagerConfig(addr, conf.alertmgr.alertmgrsTimeout)
			if err != nil {
				return err
			}
			alertingCfg.Alertmanagers = append(alertingCfg.Alertmanagers, cfg)
		}
	}

	if len(alertingCfg.Alertmanagers) == 0 {
		level.Warn(logger).Log("msg", "no alertmanager configured")
	}

	var alertRelabelConfigs []*relabel.Config
	if len(conf.alertRelabelConfigYAML) > 0 {
		alertRelabelConfigs, err = alert.LoadRelabelConfigs(conf.alertRelabelConfigYAML)
		if err != nil {
			return err
		}
	}

	amProvider := dns.NewProvider(
		logger,
		extprom.WrapRegistererWithPrefix("thanos_rule_alertmanagers_", reg),
		dns.ResolverType(conf.query.dnsSDResolver),
	)
	var alertmgrs []*alert.Alertmanager
	amClientMetrics := extpromhttp.NewClientMetrics(
		extprom.WrapRegistererWith(prometheus.Labels{"client": "alertmanager"}, reg),
	)
	for _, cfg := range alertingCfg.Alertmanagers {
		cfg.HTTPClientConfig.ClientMetrics = amClientMetrics
		c, err := clientconfig.NewHTTPClient(cfg.HTTPClientConfig, "alertmanager")
		if err != nil {
			return err
		}
		c.Transport = tracing.HTTPTripperware(logger, c.Transport)
		// Each Alertmanager client has a different list of targets thus each needs its own DNS provider.
		amClient, err := clientconfig.NewClient(logger, cfg.EndpointsConfig, c, amProvider.Clone())
		if err != nil {
			return err
		}
		// Discover and resolve Alertmanager addresses.
		addDiscoveryGroups(g, amClient, conf.alertmgr.alertmgrsDNSSDInterval, logger)

		alertmgrs = append(alertmgrs, alert.NewAlertmanager(logger, amClient, time.Duration(cfg.Timeout), cfg.APIVersion))
	}

	var (
		ruleMgr *thanosrules.Manager
		alertQ  = alert.NewQueue(logger, reg, 10000, 100, labelsTSDBToProm(conf.lset), conf.alertmgr.alertExcludeLabels, alertRelabelConfigs)
	)
	{
		if conf.extendedFunctionsEnabled {
			maps.Copy(parser.Functions, parse.XFunctions)
		}

		if len(conf.EnableFeatures) > 0 {
			for _, feature := range conf.EnableFeatures {
				if feature == promqlExperimentalFunctions {
					parser.EnableExperimentalFunctions = true
					level.Info(logger).Log("msg", "Experimental PromQL functions enabled.", "option", promqlExperimentalFunctions)
				}
			}
		}

		// Run rule evaluation and alert notifications.
		notifyFunc := func(ctx context.Context, expr string, alerts ...*rules.Alert) {
			res := make([]*notifier.Alert, 0, len(alerts))
			for _, alrt := range alerts {
				// Only send actually firing alerts.
				if alrt.State == rules.StatePending {
					continue
				}
				expressionURL, err := tableLinkForExpression(*conf.alertmgr.alertSourceTemplate, expr)
				if err != nil {
					level.Warn(logger).Log("msg", "failed to generate link for expression", "expr", expr, "err", err)
				}
				a := &notifier.Alert{
					StartsAt:     alrt.FiredAt,
					Labels:       alrt.Labels,
					Annotations:  alrt.Annotations,
					GeneratorURL: conf.alertQueryURL.String() + expressionURL,
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

		managerOpts := rules.ManagerOptions{
			NotifyFunc:             notifyFunc,
			Logger:                 logutil.GoKitLogToSlog(logger),
			Appendable:             appendable,
			ExternalURL:            nil,
			Queryable:              queryable,
			ResendDelay:            conf.resendDelay,
			OutageTolerance:        conf.outageTolerance,
			ForGracePeriod:         conf.forGracePeriod,
			DefaultRuleQueryOffset: func() time.Duration { return conf.queryOffset },
		}
		if conf.ruleConcurrentEval > 1 {
			managerOpts.MaxConcurrentEvals = conf.ruleConcurrentEval
			managerOpts.ConcurrentEvalsEnabled = true
		}

		ctx, cancel := context.WithCancel(context.Background())
		logger = log.With(logger, "component", "rules")
		ruleMgr = thanosrules.NewManager(
			tracing.ContextWithTracer(ctx, tracer),
			reg,
			conf.dataDir,
			managerOpts,
			queryFuncCreator(logger, queryClients, promClients, grpcEndpointSet, metrics.duplicatedQuery, metrics.ruleEvalWarnings, conf.query.httpMethod, conf.query.doNotAddThanosParams),
			conf.lset,
			// In our case the querying URL is the external URL because in Prometheus
			// --web.external-url points to it i.e. it points at something where the user
			// could execute the alert or recording rule's expression and get results.
			conf.alertQueryURL.String(),
		)

		// Schedule rule manager that evaluates rules.
		g.Add(func() error {
			ruleMgr.Run()
			<-ctx.Done()

			return nil
		}, func(err error) {
			cancel()
			ruleMgr.Stop()
		})
	}
	// Run the alert sender.
	{
		sdr := alert.NewSender(logger, reg, alertmgrs)
		ctx, cancel := context.WithCancel(context.Background())
		ctx = tracing.ContextWithTracer(ctx, tracer)

		g.Add(func() error {
			for {
				tracing.DoInSpan(ctx, "/send_alerts", func(ctx context.Context) {
					sdr.Send(ctx, alertQ.Pop(ctx.Done()))
				})

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

	// Handle reload and termination interrupts.
	reloadWebhandler := make(chan chan error)
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			// Initialize rules.
			if err := reloadRules(logger, conf.ruleFiles, ruleMgr, conf.evalInterval, metrics); err != nil {
				level.Error(logger).Log("msg", "initialize rules failed", "err", err)
				return err
			}
			for {
				select {
				case <-reloadSignal:
					if err := reloadRules(logger, conf.ruleFiles, ruleMgr, conf.evalInterval, metrics); err != nil {
						level.Error(logger).Log("msg", "reload rules by sighup failed", "err", err)
					}
				case reloadMsg := <-reloadWebhandler:
					err := reloadRules(logger, conf.ruleFiles, ruleMgr, conf.evalInterval, metrics)
					if err != nil {
						level.Error(logger).Log("msg", "reload rules by webhandler failed", "err", err)
					}
					reloadMsg <- err
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}, func(error) {
			cancel()
		})
	}

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, extprom.WrapRegistererWithPrefix("thanos_", reg)),
	)

	// Start gRPC server.
	tlsCfg, err := tls.NewServerConfig(log.With(logger, "protocol", "gRPC"), conf.grpc.tlsSrvCert, conf.grpc.tlsSrvKey, conf.grpc.tlsSrvClientCA, conf.grpc.tlsMinVersion)
	if err != nil {
		return errors.Wrap(err, "setup gRPC server")
	}

	options := []grpcserver.Option{
		grpcserver.WithServer(thanosrules.RegisterRulesServer(ruleMgr)),
		grpcserver.WithListen(conf.grpc.bindAddress),
		grpcserver.WithGracePeriod(conf.grpc.gracePeriod),
		grpcserver.WithGracePeriod(conf.grpc.maxConnectionAge),
		grpcserver.WithTLSConfig(tlsCfg),
	}
	infoOptions := []info.ServerOptionFunc{info.WithRulesInfoFunc()}
	if tsdbDB != nil {
		tsdbStore := store.NewTSDBStore(logger, tsdbDB, component.Rule, conf.lset)
		infoOptions = append(
			infoOptions,
			info.WithLabelSetFunc(func() []labelpb.ZLabelSet {
				return tsdbStore.LabelSet()
			}),
			info.WithStoreInfoFunc(func() (*infopb.StoreInfo, error) {
				if httpProbe.IsReady() {
					mint, maxt := tsdbStore.TimeRange()
					return &infopb.StoreInfo{
						MinTime:                      mint,
						MaxTime:                      maxt,
						SupportsSharding:             true,
						SupportsWithoutReplicaLabels: true,
						TsdbInfos:                    tsdbStore.TSDBInfos(),
					}, nil
				}
				return nil, errors.New("Not ready")
			}),
		)
		storeServer := store.NewLimitedStoreServer(store.NewInstrumentedStoreServer(reg, tsdbStore), reg, conf.storeRateLimits)
		options = append(options, grpcserver.WithServer(store.RegisterStoreServer(storeServer, logger)))
	}

	options = append(options, grpcserver.WithServer(
		info.RegisterInfoServer(info.NewInfoServer(component.Rule.String(), infoOptions...)),
	))
	s := grpcserver.New(logger, reg, tracer, grpcLogOpts, logFilterMethods, comp, grpcProbe, options...)

	g.Add(func() error {
		statusProber.Ready()
		return s.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		s.Shutdown(err)
	})

	// Start UI & metrics HTTP server.
	{
		router := route.New()

		// RoutePrefix must always start with '/'.
		conf.web.routePrefix = "/" + strings.Trim(conf.web.routePrefix, "/")

		// Redirect from / to /webRoutePrefix.
		if conf.web.routePrefix != "/" {
			router.Get("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, conf.web.routePrefix, http.StatusFound)
			})
			router = router.WithPrefix(conf.web.routePrefix)
		}

		router.Post("/-/reload", func(w http.ResponseWriter, r *http.Request) {
			reloadMsg := make(chan error)
			reloadWebhandler <- reloadMsg
			if err := <-reloadMsg; err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})

		ins := extpromhttp.NewInstrumentationMiddleware(reg, nil)

		// Configure Request Logging for HTTP calls.
		logMiddleware := logging.NewHTTPServerMiddleware(logger, httpLogOpts...)

		// TODO(bplotka in PR #513 review): pass all flags, not only the flags needed by prefix rewriting.
		ui.NewRuleUI(logger, reg, ruleMgr, conf.alertQueryURL.String(), conf.web.externalPrefix, conf.web.prefixHeaderName).Register(router, ins)

		api := v1.NewRuleAPI(logger, reg, thanosrules.NewGRPCClient(ruleMgr), ruleMgr, conf.web.disableCORS, flagsMap)
		api.Register(router.WithPrefix("/api/v1"), tracer, logger, ins, logMiddleware)

		srv := httpserver.New(logger, reg, comp, httpProbe,
			httpserver.WithListen(conf.http.bindAddress),
			httpserver.WithGracePeriod(time.Duration(conf.http.gracePeriod)),
			httpserver.WithTLSConfig(conf.http.tlsConfig),
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

	confContentYaml, err := conf.objStoreConfig.Content()
	if err != nil {
		return err
	}

	if len(confContentYaml) > 0 {
		// The background shipper continuously scans the data directory and uploads
		// new blocks to Google Cloud Storage or an S3-compatible storage service.
		bkt, err := client.NewBucket(logger, confContentYaml, component.Rule.String(), nil)
		if err != nil {
			return err
		}
		bkt = objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, extprom.WrapRegistererWithPrefix("thanos_", reg), bkt.Name()))

		// Ensure we close up everything properly.
		defer func() {
			if err != nil {
				runutil.CloseWithLogOnErr(logger, bkt, "bucket client")
			}
		}()

		s := shipper.New(
			bkt,
			conf.dataDir,
			shipper.WithLogger(logger),
			shipper.WithRegisterer(reg),
			shipper.WithSource(metadata.RulerSource),
			shipper.WithHashFunc(metadata.HashFunc(conf.shipper.hashFunc)),
			shipper.WithMetaFileName(conf.shipper.metaFileName),
			shipper.WithLabels(func() labels.Labels { return conf.lset }),
			shipper.WithAllowOutOfOrderUploads(conf.shipper.allowOutOfOrderUpload),
			shipper.WithSkipCorruptedBlocks(conf.shipper.skipCorruptedBlocks),
			shipper.WithUploadConcurrency(conf.shipper.uploadConcurrency),
		)

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

func removeLockfileIfAny(logger log.Logger, dataDir string) error {
	absdir, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(absdir, "lock")); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	level.Info(logger).Log("msg", "a leftover lockfile found and removed")
	return nil
}

func labelsTSDBToProm(lset labels.Labels) (res labels.Labels) {
	return lset.Copy()
}

func queryFuncCreator(
	logger log.Logger,
	queriers []*clientconfig.HTTPClient,
	promClients []*promclient.Client,
	grpcEndpointSet *query.EndpointSet,
	duplicatedQuery prometheus.Counter,
	ruleEvalWarnings *prometheus.CounterVec,
	httpMethod string,
	doNotAddThanosParams bool,
) func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {

	// queryFunc returns query function that hits the HTTP query API of query peers in randomized order until we get a result
	// back or the context get canceled.
	return func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
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

		return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
			for _, i := range rand.Perm(len(queriers)) {
				promClient := promClients[i]
				endpoints := thanosrules.RemoveDuplicateQueryEndpoints(logger, duplicatedQuery, queriers[i].Endpoints())
				for _, i := range rand.Perm(len(endpoints)) {
					span, ctx := tracing.StartSpan(ctx, spanID)
					v, warns, err := promClient.PromqlQueryInstant(ctx, endpoints[i], qs, t, promclient.QueryOptions{
						Deduplicate:             true,
						PartialResponseStrategy: partialResponseStrategy,
						Method:                  httpMethod,
						DoNotAddThanosParams:    doNotAddThanosParams,
					})
					span.Finish()

					if err != nil {
						level.Error(logger).Log("err", err, "query", qs)
						continue
					}

					warns = filterOutPromQLWarnings(warns, logger, qs)
					if len(warns) > 0 {
						ruleEvalWarnings.WithLabelValues(strings.ToLower(partialResponseStrategy.String())).Inc()
						// TODO(bwplotka): Propagate those to UI, probably requires changing rule manager code ):
						level.Warn(logger).Log("warnings", strings.Join(warns, ", "), "query", qs)
					}
					return v, nil
				}
			}

			if grpcEndpointSet != nil {
				queryAPIClients := grpcEndpointSet.GetQueryAPIClients()
				for _, i := range rand.Perm(len(queryAPIClients)) {
					e := query.NewRemoteEngine(logger, queryAPIClients[i], query.Opts{})
					expr, err := extpromql.ParseExpr(qs)
					if err != nil {
						level.Error(logger).Log("err", err, "query", qs)
						continue
					}
					q, err := e.NewInstantQuery(ctx, nil, expr, t)
					if err != nil {
						level.Error(logger).Log("err", err, "query", qs)
						continue
					}

					result := q.Exec(ctx)
					v, err := result.Vector()
					if err != nil {
						level.Error(logger).Log("err", err, "query", qs)
						continue
					}

					warnings := make([]string, 0, len(result.Warnings))
					for _, warn := range result.Warnings {
						warnings = append(warnings, warn.Error())
					}
					warnings = filterOutPromQLWarnings(warnings, logger, qs)
					if len(warnings) > 0 {
						ruleEvalWarnings.WithLabelValues(strings.ToLower(partialResponseStrategy.String())).Inc()
						level.Warn(logger).Log("warnings", strings.Join(warnings, ", "), "query", qs)
					}

					return v, nil
				}
			}
			return nil, errors.Errorf("no query API server reachable")
		}
	}
}

func addDiscoveryGroups(g *run.Group, c *clientconfig.HTTPClient, interval time.Duration, logger log.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		c.Discover(ctx)
		return nil
	}, func(error) {
		cancel()
	})

	g.Add(func() error {
		runutil.RepeatInfinitely(logger, interval, ctx.Done(), func() error {
			return c.Resolve(ctx)
		})
		return nil
	}, func(error) {
		cancel()
	})
}

func reloadRules(logger log.Logger,
	ruleFiles []string,
	ruleMgr *thanosrules.Manager,
	evalInterval time.Duration,
	metrics *RuleMetrics) error {
	level.Debug(logger).Log("msg", "configured rule files", "files", strings.Join(ruleFiles, ","))
	var (
		errs      errutil.MultiError
		files     []string
		seenFiles = make(map[string]struct{})
	)
	for _, pat := range ruleFiles {
		fs, err := filepath.Glob(pat)
		if err != nil {
			// The only error can be a bad pattern.
			errs.Add(errors.Wrapf(err, "retrieving rule files failed. Ignoring file. pattern %s", pat))
			continue
		}

		for _, fp := range fs {
			if _, ok := seenFiles[fp]; ok {
				continue
			}
			files = append(files, fp)
			seenFiles[fp] = struct{}{}
		}
	}

	level.Info(logger).Log("msg", "reload rule files", "numFiles", len(files))

	if err := ruleMgr.Update(evalInterval, files); err != nil {
		metrics.configSuccess.Set(0)
		errs.Add(errors.Wrap(err, "reloading rules failed"))
		return errs.Err()
	}

	metrics.configSuccess.Set(1)
	metrics.configSuccessTime.Set(float64(time.Now().UnixNano()) / 1e9)

	metrics.rulesLoaded.Reset()
	for _, group := range ruleMgr.RuleGroups() {
		metrics.rulesLoaded.WithLabelValues(group.PartialResponseStrategy.String(), group.OriginalFile, group.Name()).Set(float64(len(group.Rules())))
	}
	return errs.Err()
}

func tableLinkForExpression(tmpl string, expr string) (string, error) {
	// template example: "/graph?g0.expr={{.Expr}}&g0.tab=1"
	escapedExpression := url.QueryEscape(expr)

	escapedExpr := Expression{Expr: escapedExpression}
	t, err := texttemplate.New("url").Parse(tmpl)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse template")
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, escapedExpr); err != nil {
		return "", errors.Wrap(err, "failed to execute template")
	}
	return buf.String(), nil
}

func validateTemplate(tmplStr string) error {
	tmpl, err := template.New("test").Parse(tmplStr)
	if err != nil {
		return fmt.Errorf("failed to parse the template: %w", err)
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, Expression{Expr: "test_expr"})
	if err != nil {
		return fmt.Errorf("failed to execute the template: %w", err)
	}
	return nil
}

// Filter out PromQL related warnings from warning response and keep store related warnings only.
func filterOutPromQLWarnings(warns []string, logger log.Logger, query string) []string {
	storeWarnings := make([]string, 0, len(warns))
	for _, warn := range warns {
		if extannotations.IsPromQLAnnotation(warn) {
			level.Warn(logger).Log("warning", warn, "query", query)
			continue
		}
		storeWarnings = append(storeWarnings, warn)
	}
	return storeWarnings
}

// ReadyScrapeManager allows a scrape manager to be retrieved. Even if it's set at a later point in time.
type readyScrapeManager struct {
	mtx sync.RWMutex
	m   *scrape.Manager
}

// Set the scrape manager.
func (rm *readyScrapeManager) Set(m *scrape.Manager) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	rm.m = m
}

// Get the scrape manager. If is not ready, return an error.
func (rm *readyScrapeManager) Get() (*scrape.Manager, error) {
	rm.mtx.RLock()
	defer rm.mtx.RUnlock()

	if rm.m != nil {
		return rm.m, nil
	}

	return nil, ErrNotReady
}

// ErrNotReady is returned if the underlying scrape manager is not ready yet.
var ErrNotReady = errors.New("scrape manager not ready")
