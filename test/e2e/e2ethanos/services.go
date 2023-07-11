// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/efficientgo/core/backoff"
	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	e2emon "github.com/efficientgo/e2e/monitoring"
	e2eobs "github.com/efficientgo/e2e/observable"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/model/relabel"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/objstore/client"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/thanos-io/objstore/exthttp"

	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/httpconfig"

	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/receive"
)

const (
	infoLogLevel = "info"
)

// Same as default for now.
var defaultBackoffConfig = backoff.Config{
	Min:        300 * time.Millisecond,
	Max:        600 * time.Millisecond,
	MaxRetries: 50,
}

// TODO(bwplotka): Make strconv.Itoa(os.Getuid()) pattern in e2e?
func wrapWithDefaults(opt e2e.StartOptions) e2e.StartOptions {
	if opt.User == "" {
		opt.User = strconv.Itoa(os.Getuid())
	}
	if opt.WaitReadyBackoff == nil {
		opt.WaitReadyBackoff = &defaultBackoffConfig
	}
	return opt
}

const (
	// FeatureExemplarStorage is a feature flag that enables exemplar storage on Prometheus.
	FeatureExemplarStorage = "exemplar-storage"
)

// DefaultPrometheusImage sets default Prometheus image used in e2e service.
func DefaultPrometheusImage() string {
	return "quay.io/prometheus/prometheus:v2.41.0"
}

// DefaultAlertmanagerImage sets default Alertmanager image used in e2e service.
func DefaultAlertmanagerImage() string {
	return "quay.io/prometheus/alertmanager:v0.20.0"
}

// DefaultImage returns the local docker image to use to run Thanos.
func DefaultImage() string {
	// Get the Thanos image from the THANOS_IMAGE env variable.
	if os.Getenv("THANOS_IMAGE") != "" {
		return os.Getenv("THANOS_IMAGE")
	}

	return "thanos"
}

func defaultPromHttpConfig() string {
	// username: test, secret: test(bcrypt hash)
	return `basic_auth:
  username: test
  password: test
`
}

func NewPrometheus(e e2e.Environment, name, promConfig, webConfig, promImage string, enableFeatures ...string) *e2emon.InstrumentedRunnable {
	f := e.Runnable(name).WithPorts(map[string]int{"http": 9090}).Future()
	if err := os.MkdirAll(f.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create prometheus dir"))}
	}

	if err := os.WriteFile(filepath.Join(f.Dir(), "prometheus.yml"), []byte(promConfig), 0600); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "creating prom config"))}
	}

	if len(webConfig) > 0 {
		if err := os.WriteFile(filepath.Join(f.Dir(), "web-config.yml"), []byte(webConfig), 0600); err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "creating web-config"))}
		}
	}

	probe := e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200)
	args := e2e.BuildArgs(map[string]string{
		"--config.file":                     filepath.Join(f.InternalDir(), "prometheus.yml"),
		"--storage.tsdb.path":               f.InternalDir(),
		"--storage.tsdb.max-block-duration": "2h",
		"--log.level":                       infoLogLevel,
		"--web.listen-address":              ":9090",
	})

	if len(enableFeatures) > 0 {
		args = append(args, fmt.Sprintf("--enable-feature=%s", strings.Join(enableFeatures, ",")))
	}
	if len(webConfig) > 0 {
		args = append(args, fmt.Sprintf("--web.config.file=%s", filepath.Join(f.InternalDir(), "web-config.yml")))
		// If auth is enabled then prober would get 401 error.
		probe = e2e.NewHTTPReadinessProbe("http", "/-/ready", 401, 401)
	}
	return e2emon.AsInstrumented(f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     promImage,
		Command:   e2e.NewCommandWithoutEntrypoint("prometheus", args...),
		Readiness: probe,
	})), "http")
}

func NewPrometheusWithSidecar(e e2e.Environment, name, promConfig, webConfig, promImage, minTime string, enableFeatures ...string) (*e2emon.InstrumentedRunnable, *e2emon.InstrumentedRunnable) {
	return NewPrometheusWithSidecarCustomImage(e, name, promConfig, webConfig, promImage, minTime, DefaultImage(), enableFeatures...)
}

func NewPrometheusWithSidecarCustomImage(e e2e.Environment, name, promConfig, webConfig, promImage, minTime string, sidecarImage string, enableFeatures ...string) (*e2emon.InstrumentedRunnable, *e2emon.InstrumentedRunnable) {
	prom := NewPrometheus(e, name, promConfig, webConfig, promImage, enableFeatures...)

	args := map[string]string{
		"--debug.name":        fmt.Sprintf("sidecar-%v", name),
		"--grpc-address":      ":9091",
		"--grpc-grace-period": "0s",
		"--http-address":      ":8080",
		"--prometheus.url":    "http://" + prom.InternalEndpoint("http"),
		"--tsdb.path":         prom.InternalDir(),
		"--log.level":         "debug",
	}
	if len(webConfig) > 0 {
		args["--prometheus.http-client"] = defaultPromHttpConfig()
	}
	if minTime != "" {
		args["--min-time"] = minTime
	}
	sidecarRunnable := e.Runnable(fmt.Sprintf("sidecar-%s", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091}).
		Init(wrapWithDefaults(e2e.StartOptions{
			Image:     sidecarImage,
			Command:   e2e.NewCommand("sidecar", e2e.BuildArgs(args)...),
			Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		}))
	sidecar := e2emon.AsInstrumented(sidecarRunnable, "http")
	return prom, sidecar
}

type AvalancheOptions struct {
	MetricCount    string
	SeriesCount    string
	MetricInterval string
	SeriesInterval string
	ValueInterval  string

	RemoteURL           string
	RemoteWriteInterval string
	RemoteBatchSize     string
	RemoteRequestCount  string

	TenantID string
}

func NewAvalanche(e e2e.Environment, name string, o AvalancheOptions) *e2emon.InstrumentedRunnable {
	f := e.Runnable(name).WithPorts(map[string]int{"http": 9001}).Future()

	args := e2e.BuildArgs(map[string]string{
		"--metric-count":          o.MetricCount,
		"--series-count":          o.SeriesCount,
		"--remote-url":            o.RemoteURL,
		"--remote-write-interval": o.RemoteWriteInterval,
		"--remote-batch-size":     o.RemoteBatchSize,
		"--remote-requests-count": o.RemoteRequestCount,
		"--value-interval":        o.ValueInterval,
		"--metric-interval":       o.MetricInterval,
		"--series-interval":       o.SeriesInterval,
		"--remote-tenant-header":  "THANOS-TENANT",
		"--remote-tenant":         o.TenantID,
	})

	return e2emon.AsInstrumented(f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:   "quay.io/prometheuscommunity/avalanche:main",
		Command: e2e.NewCommandWithoutEntrypoint("avalanche", args...),
	})), "http")
}

func NewPrometheusWithJaegerTracingSidecarCustomImage(e e2e.Environment, name, promConfig, webConfig,
	promImage, minTime, sidecarImage, jaegerConfig string, enableFeatures ...string) (
	*e2emon.InstrumentedRunnable, *e2emon.InstrumentedRunnable) {
	prom := NewPrometheus(e, name, promConfig, webConfig, promImage, enableFeatures...)

	args := map[string]string{
		"--debug.name":        fmt.Sprintf("sidecar-%v", name),
		"--grpc-address":      ":9091",
		"--grpc-grace-period": "0s",
		"--http-address":      ":8080",
		"--prometheus.url":    "http://" + prom.InternalEndpoint("http"),
		"--tsdb.path":         prom.InternalDir(),
		"--log.level":         "debug",
		"--tracing.config":    jaegerConfig,
	}
	if len(webConfig) > 0 {
		args["--prometheus.http-client"] = defaultPromHttpConfig()
	}
	if minTime != "" {
		args["--min-time"] = minTime
	}

	sidecarRunnable := e.Runnable(fmt.Sprintf("sidecar-%s", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091}).
		Init(wrapWithDefaults(e2e.StartOptions{
			Image:     sidecarImage,
			Command:   e2e.NewCommand("sidecar", e2e.BuildArgs(args)...),
			Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		}))
	sidecar := e2emon.AsInstrumented(sidecarRunnable, "http")

	return prom, sidecar
}

type QuerierBuilder struct {
	name           string
	routePrefix    string
	externalPrefix string
	image          string

	storeAddresses          []string
	proxyStrategy           string
	disablePartialResponses bool
	fileSDStoreAddresses    []string
	ruleAddresses           []string
	metadataAddresses       []string
	envVars                 map[string]string
	targetAddresses         []string
	exemplarAddresses       []string
	enableFeatures          []string
	endpoints               []string
	strictEndpoints         []string

	engine    string
	queryMode string

	replicaLabels []string
	tracingConfig string

	telemetryDurationQuantiles []float64
	telemetrySamplesQuantiles  []float64
	telemetrySeriesQuantiles   []float64

	e2e.Linkable
	f e2e.FutureRunnable
}

func NewQuerierBuilder(e e2e.Environment, name string, storeAddresses ...string) *QuerierBuilder {
	f := e.Runnable(fmt.Sprintf("querier-%v", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091}).
		Future()
	return &QuerierBuilder{
		Linkable:       f,
		f:              f,
		name:           name,
		storeAddresses: storeAddresses,
		image:          DefaultImage(),
		replicaLabels:  []string{replicaLabel},
	}
}

func (q *QuerierBuilder) WithProxyStrategy(strategy string) *QuerierBuilder {
	q.proxyStrategy = strategy
	return q
}

func (q *QuerierBuilder) WithEnabledFeatures(enableFeatures []string) *QuerierBuilder {
	q.enableFeatures = enableFeatures
	return q
}

func (q *QuerierBuilder) WithImage(image string) *QuerierBuilder {
	q.image = image
	return q
}

func (q *QuerierBuilder) WithStoreAddresses(storeAddresses ...string) *QuerierBuilder {
	q.storeAddresses = storeAddresses
	return q
}

func (q *QuerierBuilder) WithFileSDStoreAddresses(fileSDStoreAddresses ...string) *QuerierBuilder {
	q.fileSDStoreAddresses = fileSDStoreAddresses
	return q
}

func (q *QuerierBuilder) WithRuleAddresses(ruleAddresses ...string) *QuerierBuilder {
	q.ruleAddresses = ruleAddresses
	return q
}

func (q *QuerierBuilder) WithTargetAddresses(targetAddresses ...string) *QuerierBuilder {
	q.targetAddresses = targetAddresses
	return q
}

func (q *QuerierBuilder) WithExemplarAddresses(exemplarAddresses ...string) *QuerierBuilder {
	q.exemplarAddresses = exemplarAddresses
	return q
}

func (q *QuerierBuilder) WithMetadataAddresses(metadataAddresses ...string) *QuerierBuilder {
	q.metadataAddresses = metadataAddresses
	return q
}

func (q *QuerierBuilder) WithEndpoints(endpoints ...string) *QuerierBuilder {
	q.endpoints = endpoints
	return q
}

func (q *QuerierBuilder) WithStrictEndpoints(strictEndpoints ...string) *QuerierBuilder {
	q.strictEndpoints = strictEndpoints
	return q
}

func (q *QuerierBuilder) WithRoutePrefix(routePrefix string) *QuerierBuilder {
	q.routePrefix = routePrefix
	return q
}

func (q *QuerierBuilder) WithExternalPrefix(externalPrefix string) *QuerierBuilder {
	q.externalPrefix = externalPrefix
	return q
}

func (q *QuerierBuilder) WithTracingConfig(tracingConfig string) *QuerierBuilder {
	q.tracingConfig = tracingConfig
	return q
}

// WithReplicaLabels replaces default [replica] replica label configuration for the querier.
func (q *QuerierBuilder) WithReplicaLabels(labels ...string) *QuerierBuilder {
	q.replicaLabels = labels
	return q
}

func (q *QuerierBuilder) WithDisablePartialResponses(disable bool) *QuerierBuilder {
	q.disablePartialResponses = disable
	return q
}

func (q *QuerierBuilder) WithEngine(engine string) *QuerierBuilder {
	q.engine = engine
	return q
}

func (q *QuerierBuilder) WithQueryMode(mode string) *QuerierBuilder {
	q.queryMode = mode
	return q
}

func (q *QuerierBuilder) WithEnvVars(envVars map[string]string) *QuerierBuilder {
	q.envVars = envVars
	return q
}

func (q *QuerierBuilder) WithTelemetryQuantiles(duration []float64, samples []float64, series []float64) *QuerierBuilder {
	q.telemetryDurationQuantiles = duration
	q.telemetrySamplesQuantiles = samples
	q.telemetrySeriesQuantiles = series
	return q
}

func (q *QuerierBuilder) Init() *e2emon.InstrumentedRunnable {
	args, err := q.collectArgs()
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(q.name, err)}
	}

	return e2emon.AsInstrumented(q.f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     q.image,
		Command:   e2e.NewCommand("query", args...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		EnvVars:   q.envVars,
	})), "http")
}

const replicaLabel = "replica"

func (q *QuerierBuilder) collectArgs() ([]string, error) {
	args := e2e.BuildArgs(map[string]string{
		"--debug.name":            fmt.Sprintf("querier-%v", q.name),
		"--grpc-address":          ":9091",
		"--grpc-grace-period":     "0s",
		"--http-address":          ":8080",
		"--store.sd-dns-interval": "5s",
		"--log.level":             infoLogLevel,
		"--query.max-concurrent":  "1",
		"--store.sd-interval":     "5s",
	})

	for _, repl := range q.replicaLabels {
		args = append(args, "--query.replica-label="+repl)
	}
	for _, addr := range q.storeAddresses {
		args = append(args, "--store="+addr)
	}
	for _, addr := range q.ruleAddresses {
		args = append(args, "--rule="+addr)
	}
	for _, addr := range q.targetAddresses {
		args = append(args, "--target="+addr)
	}
	for _, addr := range q.metadataAddresses {
		args = append(args, "--metadata="+addr)
	}
	for _, addr := range q.exemplarAddresses {
		args = append(args, "--exemplar="+addr)
	}
	for _, feature := range q.enableFeatures {
		args = append(args, "--enable-feature="+feature)
	}
	if q.proxyStrategy != "" {
		args = append(args, "--grpc.proxy-strategy="+q.proxyStrategy)
	}
	if q.disablePartialResponses {
		args = append(args, "--no-query.partial-response")
	}
	for _, addr := range q.endpoints {
		args = append(args, "--endpoint="+addr)
	}
	for _, addr := range q.strictEndpoints {
		args = append(args, "--endpoint-strict="+addr)
	}
	if len(q.fileSDStoreAddresses) > 0 {
		if err := os.MkdirAll(q.Dir(), 0750); err != nil {
			return nil, errors.Wrap(err, "create query dir failed")
		}

		fileSD := []*targetgroup.Group{{}}
		for _, a := range q.fileSDStoreAddresses {
			fileSD[0].Targets = append(fileSD[0].Targets, model.LabelSet{model.AddressLabel: model.LabelValue(a)})
		}

		b, err := yaml.Marshal(fileSD)
		if err != nil {
			return nil, err
		}

		if err := os.WriteFile(q.Dir()+"/filesd.yaml", b, 0600); err != nil {
			return nil, errors.Wrap(err, "creating query SD config failed")
		}

		args = append(args, "--store.sd-files="+filepath.Join(q.InternalDir(), "filesd.yaml"))
	}
	if q.routePrefix != "" {
		args = append(args, "--web.route-prefix="+q.routePrefix)
	}
	if q.externalPrefix != "" {
		args = append(args, "--web.external-prefix="+q.externalPrefix)
	}
	if q.tracingConfig != "" {
		args = append(args, "--tracing.config="+q.tracingConfig)
	}
	for _, bucket := range q.telemetryDurationQuantiles {
		args = append(args, "--query.telemetry.request-duration-seconds-quantiles="+strconv.FormatFloat(bucket, 'f', -1, 64))
	}
	for _, bucket := range q.telemetrySamplesQuantiles {
		args = append(args, "--query.telemetry.request-samples-quantiles="+strconv.FormatFloat(bucket, 'f', -1, 64))
	}
	for _, bucket := range q.telemetrySeriesQuantiles {
		args = append(args, "--query.telemetry.request-series-seconds-quantiles="+strconv.FormatFloat(bucket, 'f', -1, 64))
	}
	return args, nil
}

func RemoteWriteEndpoint(addr string) string { return fmt.Sprintf("http://%s/api/v1/receive", addr) }

func RemoteWriteEndpoints(addrs ...string) string {
	var endpoints []string
	for _, addr := range addrs {
		endpoints = append(endpoints, RemoteWriteEndpoint(addr))
	}
	return strings.Join(endpoints, ",")
}

type ReceiveBuilder struct {
	e2e.Linkable

	f e2e.FutureRunnable

	maxExemplars        int
	ingestion           bool
	limit               int
	tenantsLimits       receive.TenantsWriteLimitsConfig
	metaMonitoring      string
	metaMonitoringQuery string
	hashringConfigs     []receive.HashringConfig
	relabelConfigs      []*relabel.Config
	replication         int
	image               string
	nativeHistograms    bool
	labels              []string
}

func NewReceiveBuilder(e e2e.Environment, name string) *ReceiveBuilder {
	f := e.Runnable(fmt.Sprintf("receive-%v", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091, "remote-write": 8081}).
		Future()
	return &ReceiveBuilder{
		Linkable:    f,
		f:           f,
		replication: 1,
		image:       DefaultImage(),
	}
}

func (r *ReceiveBuilder) WithImage(image string) *ReceiveBuilder {
	r.image = image
	return r
}

func (r *ReceiveBuilder) WithExemplarsInMemStorage(maxExemplars int) *ReceiveBuilder {
	r.maxExemplars = maxExemplars
	r.ingestion = true
	return r
}

func (r *ReceiveBuilder) WithIngestionEnabled() *ReceiveBuilder {
	r.ingestion = true
	return r
}

func (r *ReceiveBuilder) WithLabel(name, value string) *ReceiveBuilder {
	r.labels = append(r.labels, fmt.Sprintf(`%s="%s"`, name, value))
	return r
}

func (r *ReceiveBuilder) WithRouting(replication int, hashringConfigs ...receive.HashringConfig) *ReceiveBuilder {
	r.hashringConfigs = hashringConfigs
	r.replication = replication
	return r
}

func (r *ReceiveBuilder) WithRelabelConfigs(relabelConfigs []*relabel.Config) *ReceiveBuilder {
	r.relabelConfigs = relabelConfigs
	return r
}

func (r *ReceiveBuilder) WithValidationEnabled(limit int, metaMonitoring string, tenantsLimits receive.TenantsWriteLimitsConfig, query ...string) *ReceiveBuilder {
	r.limit = limit
	r.metaMonitoring = metaMonitoring
	r.tenantsLimits = tenantsLimits
	if len(query) > 0 {
		r.metaMonitoringQuery = query[0]
	}
	return r
}

func (r *ReceiveBuilder) WithNativeHistograms() *ReceiveBuilder {
	r.nativeHistograms = true
	return r
}

// Init creates a Thanos Receive instance.
// If ingestion is enabled it will be configured for ingesting samples.
// If routing is configured (i.e. hashring configuration is provided) it routes samples to other receivers.
// If none, it errors out.
func (r *ReceiveBuilder) Init() *e2emon.InstrumentedRunnable {
	if !r.ingestion && len(r.hashringConfigs) == 0 {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.New("enable ingestion or configure routing for this receiver"))}
	}

	args := map[string]string{
		"--debug.name":           r.Name(),
		"--grpc-address":         ":9091",
		"--grpc-grace-period":    "0s",
		"--http-address":         ":8080",
		"--remote-write.address": ":8081",
		"--label":                fmt.Sprintf(`receive="%s"`, r.Name()),
		"--tsdb.path":            filepath.Join(r.InternalDir(), "data"),
		"--log.level":            infoLogLevel,
		"--tsdb.max-exemplars":   fmt.Sprintf("%v", r.maxExemplars),
	}

	if len(r.labels) > 0 {
		args["--label"] = fmt.Sprintf("%s,%s", args["--label"], strings.Join(r.labels, ","))
	}

	hashring := r.hashringConfigs
	if len(hashring) > 0 && r.ingestion {
		args["--receive.local-endpoint"] = r.InternalEndpoint("grpc")
	}

	if r.limit != 0 && r.metaMonitoring != "" {
		cfg := receive.RootLimitsConfig{
			WriteLimits: receive.WriteLimitsConfig{
				GlobalLimits: receive.GlobalLimitsConfig{
					MetaMonitoringURL:        r.metaMonitoring,
					MetaMonitoringLimitQuery: r.metaMonitoringQuery,
				},
				DefaultLimits: receive.DefaultLimitsConfig{
					HeadSeriesLimit: uint64(r.limit),
				},
			},
		}

		if r.tenantsLimits != nil {
			cfg.WriteLimits.TenantsLimits = r.tenantsLimits
		}

		b, err := yaml.Marshal(cfg)
		if err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrapf(err, "generate limiting file: %v", hashring))}
		}

		if err := os.WriteFile(filepath.Join(r.Dir(), "limits.yaml"), b, 0600); err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrap(err, "creating limitin config"))}
		}

		args["--receive.limits-config-file"] = filepath.Join(r.InternalDir(), "limits.yaml")
	}

	if err := os.MkdirAll(filepath.Join(r.Dir(), "data"), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrap(err, "create receive dir"))}
	}

	if len(hashring) > 0 {
		b, err := json.Marshal(hashring)
		if err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrapf(err, "generate hashring file: %v", hashring))}
		}

		if err := os.WriteFile(filepath.Join(r.Dir(), "hashrings.json"), b, 0600); err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrap(err, "creating receive config"))}
		}

		args["--receive.hashrings-file"] = filepath.Join(r.InternalDir(), "hashrings.json")
		args["--receive.hashrings-file-refresh-interval"] = "5s"
		args["--receive.replication-factor"] = strconv.Itoa(r.replication)
	}

	if len(r.relabelConfigs) > 0 {
		relabelConfigBytes, err := yaml.Marshal(r.relabelConfigs)
		if err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrapf(err, "generate relabel configs: %v", relabelConfigBytes))}
		}
		args["--receive.relabel-config"] = string(relabelConfigBytes)
	}

	if r.nativeHistograms {
		args["--tsdb.enable-native-histograms"] = ""
	}

	return e2emon.AsInstrumented(r.f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     r.image,
		Command:   e2e.NewCommand("receive", e2e.BuildKingpinArgs(args)...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	})), "http")
}

type RulerBuilder struct {
	e2e.Linkable

	f e2e.FutureRunnable

	amCfg                []alert.AlertmanagerConfig
	replicaLabel         string
	image                string
	resendDelay          string
	evalInterval         string
	forGracePeriod       string
	restoreIgnoredLabels []string
}

// NewRulerBuilder is a Ruler future that allows extra configuration before initialization.
func NewRulerBuilder(e e2e.Environment, name string) *RulerBuilder {
	f := e.Runnable(fmt.Sprintf("rule-%s", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091}).
		Future()
	return &RulerBuilder{
		replicaLabel: name,
		Linkable:     f,
		f:            f,
		image:        DefaultImage(),
	}
}

func (r *RulerBuilder) WithImage(image string) *RulerBuilder {
	r.image = image
	return r
}

func (r *RulerBuilder) WithAlertManagerConfig(amCfg []alert.AlertmanagerConfig) *RulerBuilder {
	r.amCfg = amCfg
	return r
}

func (r *RulerBuilder) WithReplicaLabel(replicaLabel string) *RulerBuilder {
	r.replicaLabel = replicaLabel
	return r
}

func (r *RulerBuilder) WithResendDelay(resendDelay string) *RulerBuilder {
	r.resendDelay = resendDelay
	return r
}

func (r *RulerBuilder) WithEvalInterval(evalInterval string) *RulerBuilder {
	r.evalInterval = evalInterval
	return r
}

func (r *RulerBuilder) WithForGracePeriod(forGracePeriod string) *RulerBuilder {
	r.forGracePeriod = forGracePeriod
	return r
}

func (r *RulerBuilder) WithRestoreIgnoredLabels(labels ...string) *RulerBuilder {
	r.restoreIgnoredLabels = labels
	return r
}

func (r *RulerBuilder) InitTSDB(internalRuleDir string, queryCfg []httpconfig.Config) *e2emon.InstrumentedRunnable {
	return r.initRule(internalRuleDir, queryCfg, nil)
}

func (r *RulerBuilder) InitStateless(internalRuleDir string, queryCfg []httpconfig.Config, remoteWriteCfg []*config.RemoteWriteConfig) *e2emon.InstrumentedRunnable {
	return r.initRule(internalRuleDir, queryCfg, remoteWriteCfg)
}

func (r *RulerBuilder) initRule(internalRuleDir string, queryCfg []httpconfig.Config, remoteWriteCfg []*config.RemoteWriteConfig) *e2emon.InstrumentedRunnable {
	if err := os.MkdirAll(r.f.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrap(err, "create rule dir"))}
	}

	amCfgBytes, err := yaml.Marshal(alert.AlertingConfig{
		Alertmanagers: r.amCfg,
	})
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrapf(err, "generate am file: %v", r.amCfg))}
	}

	queryCfgBytes, err := yaml.Marshal(queryCfg)
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrapf(err, "generate query file: %v", queryCfg))}
	}

	ruleArgs := map[string]string{
		"--debug.name":                    r.Name(),
		"--grpc-address":                  ":9091",
		"--grpc-grace-period":             "0s",
		"--http-address":                  ":8080",
		"--data-dir":                      r.InternalDir(),
		"--rule-file":                     filepath.Join(internalRuleDir, "*.yaml"),
		"--eval-interval":                 "1s",
		"--alertmanagers.config":          string(amCfgBytes),
		"--alertmanagers.sd-dns-interval": "1s",
		"--log.level":                     infoLogLevel,
		"--query.config":                  string(queryCfgBytes),
		"--query.sd-dns-interval":         "1s",
		"--resend-delay":                  "5s",
		"--for-grace-period":              "1s",
	}
	if r.replicaLabel != "" {
		ruleArgs["--label"] = fmt.Sprintf(`%s="%s"`, replicaLabel, r.replicaLabel)
	}

	if r.resendDelay != "" {
		ruleArgs["--resend-delay"] = r.resendDelay
	}

	if r.evalInterval != "" {
		ruleArgs["--eval-interval"] = r.evalInterval
	}

	if r.forGracePeriod != "" {
		ruleArgs["--for-grace-period"] = r.forGracePeriod
	}

	if remoteWriteCfg != nil {
		rwCfgBytes, err := yaml.Marshal(struct {
			RemoteWriteConfigs []*config.RemoteWriteConfig `yaml:"remote_write,omitempty"`
		}{remoteWriteCfg})
		if err != nil {
			return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(r.Name(), errors.Wrapf(err, "generate remote write config: %v", remoteWriteCfg))}
		}
		ruleArgs["--remote-write.config"] = string(rwCfgBytes)
	}

	args := e2e.BuildArgs(ruleArgs)

	for _, label := range r.restoreIgnoredLabels {
		args = append(args, "--restore-ignored-label="+label)
	}

	return e2emon.AsInstrumented(r.f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     r.image,
		Command:   e2e.NewCommand("rule", args...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	})), "http")
}

func NewAlertmanager(e e2e.Environment, name string) *e2emon.InstrumentedRunnable {
	f := e.Runnable(fmt.Sprintf("alertmanager-%v", name)).
		WithPorts(map[string]int{"http": 8080}).
		Future()

	if err := os.MkdirAll(f.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create am dir"))}
	}
	const config = `
route:
  group_by: ['alertname']
  group_wait: 1s
  group_interval: 1s
  receiver: 'null'
receivers:
- name: 'null'
`
	if err := os.WriteFile(filepath.Join(f.Dir(), "config.yaml"), []byte(config), 0600); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "creating alertmanager config file failed"))}
	}

	return e2emon.AsInstrumented(f.Init(wrapWithDefaults(e2e.StartOptions{
		Image: DefaultAlertmanagerImage(),
		Command: e2e.NewCommandWithoutEntrypoint("/bin/alertmanager", e2e.BuildArgs(map[string]string{
			"--config.file":         filepath.Join(f.InternalDir(), "config.yaml"),
			"--web.listen-address":  "0.0.0.0:8080",
			"--log.level":           infoLogLevel,
			"--storage.path":        f.InternalDir(),
			"--web.get-concurrency": "1",
			"--web.timeout":         "2m",
		})...),
		Readiness:        e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		User:             strconv.Itoa(os.Geteuid()),
		WaitReadyBackoff: &defaultBackoffConfig,
	})), "http")
}

func NewStoreGW(e e2e.Environment, name string, bucketConfig client.BucketConfig, cacheConfig, indexCacheConfig string, extArgs []string, relabelConfig ...relabel.Config) *e2emon.InstrumentedRunnable {
	f := e.Runnable(fmt.Sprintf("store-gw-%v", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091}).
		Future()

	if err := os.MkdirAll(f.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create store dir"))}
	}

	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrapf(err, "generate store config file: %v", bucketConfig))}
	}

	relabelConfigBytes, err := yaml.Marshal(relabelConfig)
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrapf(err, "generate store relabel file: %v", relabelConfig))}
	}

	args := append(e2e.BuildArgs(map[string]string{
		"--debug.name":        fmt.Sprintf("store-gw-%v", name),
		"--grpc-address":      ":9091",
		"--grpc-grace-period": "0s",
		"--http-address":      ":8080",
		"--log.level":         infoLogLevel,
		"--data-dir":          f.InternalDir(),
		"--objstore.config":   string(bktConfigBytes),
		// Accelerated sync time for quicker test (3m by default).
		"--sync-block-duration":               "3s",
		"--block-sync-concurrency":            "1",
		"--store.grpc.series-max-concurrency": "1",
		"--selector.relabel-config":           string(relabelConfigBytes),
		"--consistency-delay":                 "30m",
	}), extArgs...)

	if cacheConfig != "" {
		args = append(args, "--store.caching-bucket.config", cacheConfig)
	}

	if indexCacheConfig != "" {
		args = append(args, "--index-cache.config", indexCacheConfig)
	}

	return e2emon.AsInstrumented(f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     DefaultImage(),
		Command:   e2e.NewCommand("store", args...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	})), "http")
}

type CompactorBuilder struct {
	e2e.Linkable
	f e2e.FutureRunnable
}

func NewCompactorBuilder(e e2e.Environment, name string) *CompactorBuilder {
	f := e.Runnable(fmt.Sprintf("compact-%s", name)).
		WithPorts(map[string]int{"http": 8080}).
		Future()
	return &CompactorBuilder{
		Linkable: f,
		f:        f,
	}
}

func (c *CompactorBuilder) Init(bucketConfig client.BucketConfig, relabelConfig []relabel.Config, extArgs ...string) *e2emon.InstrumentedRunnable {
	if err := os.MkdirAll(c.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(c.Name(), errors.Wrap(err, "create compact dir"))}
	}

	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(c.Name(), errors.Wrapf(err, "generate compact config file: %v", bucketConfig))}
	}

	relabelConfigBytes, err := yaml.Marshal(relabelConfig)
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(c.Name(), errors.Wrapf(err, "generate compact relabel file: %v", relabelConfig))}
	}

	return e2emon.AsInstrumented(c.f.Init(wrapWithDefaults(e2e.StartOptions{
		Image: DefaultImage(),
		Command: e2e.NewCommand("compact", append(e2e.BuildArgs(map[string]string{
			"--debug.name":               c.Name(),
			"--log.level":                infoLogLevel,
			"--data-dir":                 c.InternalDir(),
			"--objstore.config":          string(bktConfigBytes),
			"--http-address":             ":8080",
			"--compact.cleanup-interval": "15s",
			"--selector.relabel-config":  string(relabelConfigBytes),
			"--wait":                     "",
		}), extArgs...)...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	})), "http")
}

func NewQueryFrontend(e e2e.Environment, name, downstreamURL string, config queryfrontend.Config, cacheConfig queryfrontend.CacheProviderConfig) *e2eobs.Observable {
	cacheConfigBytes, err := yaml.Marshal(cacheConfig)
	if err != nil {
		return &e2eobs.Observable{Runnable: e2e.NewFailedRunnable(name, errors.Wrapf(err, "marshal response cache config file: %v", cacheConfig))}
	}

	flags := map[string]string{
		"--debug.name":                        fmt.Sprintf("query-frontend-%s", name),
		"--http-address":                      ":8080",
		"--query-frontend.downstream-url":     downstreamURL,
		"--log.level":                         infoLogLevel,
		"--query-range.response-cache-config": string(cacheConfigBytes),
	}

	if !config.QueryRangeConfig.AlignRangeWithStep {
		flags["--no-query-range.align-range-with-step"] = ""
	}

	if config.NumShards > 0 {
		flags["--query-frontend.vertical-shards"] = strconv.Itoa(config.NumShards)
	}

	if config.QueryRangeConfig.MinQuerySplitInterval != 0 {
		flags["--query-range.min-split-interval"] = config.QueryRangeConfig.MinQuerySplitInterval.String()
		flags["--query-range.max-split-interval"] = config.QueryRangeConfig.MaxQuerySplitInterval.String()
		flags["--query-range.horizontal-shards"] = strconv.FormatInt(config.QueryRangeConfig.HorizontalShards, 10)
		flags["--query-range.split-interval"] = "0"
	}

	return e2eobs.AsObservable(e.Runnable(fmt.Sprintf("query-frontend-%s", name)).
		WithPorts(map[string]int{"http": 8080}).
		Init(e2e.StartOptions{
			Image:            DefaultImage(),
			Command:          e2e.NewCommand("query-frontend", e2e.BuildArgs(flags)...),
			Readiness:        e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		}), "http")
}

func NewReverseProxy(e e2e.Environment, name, tenantID, target string) *e2emon.InstrumentedRunnable {
	conf := fmt.Sprintf(`
events {
	worker_connections  1024;
}

http {
	server {
		listen 80;
		server_name _;

		location / {
			proxy_set_header THANOS-TENANT %s;
			proxy_pass %s;
		}
	}
}
`, tenantID, target)

	f := e.Runnable(fmt.Sprintf("nginx-%s", name)).
		WithPorts(map[string]int{"http": 80}).
		Future()

	if err := os.MkdirAll(f.Dir(), 0750); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "create store dir"))}
	}

	if err := os.WriteFile(filepath.Join(f.Dir(), "nginx.conf"), []byte(conf), 0600); err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrap(err, "creating nginx config file failed"))}
	}

	return e2emon.AsInstrumented(f.Init(e2e.StartOptions{
		Image:            "docker.io/nginx:1.21.1-alpine",
		Volumes:          []string{filepath.Join(f.Dir(), "/nginx.conf") + ":/etc/nginx/nginx.conf:ro"},
		WaitReadyBackoff: &defaultBackoffConfig,
	}), "http")
}

func NewMemcached(e e2e.Environment, name string) *e2emon.InstrumentedRunnable {
	return e2emon.AsInstrumented(e.Runnable(fmt.Sprintf("memcached-%s", name)).
		WithPorts(map[string]int{"memcached": 11211}).
		Init(e2e.StartOptions{
			Image:            "docker.io/memcached:1.6.3-alpine",
			Command:          e2e.NewCommand("memcached", []string{"-m 1024", "-I 1m", "-c 1024", "-v"}...),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		}), "memcached")
}

func NewToolsBucketWeb(
	e e2e.Environment,
	name string,
	bucketConfig client.BucketConfig,
	routePrefix,
	externalPrefix string,
	minTime string,
	maxTime string,
	relabelConfig string,
) *e2emon.InstrumentedRunnable {
	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return &e2emon.InstrumentedRunnable{Runnable: e2e.NewFailedRunnable(name, errors.Wrapf(err, "generate tools bucket web config file: %v", bucketConfig))}
	}

	f := e.Runnable(fmt.Sprintf("toolsBucketWeb-%s", name)).
		WithPorts(map[string]int{"http": 8080, "grpc": 9091}).
		Future()

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":      fmt.Sprintf("toolsBucketWeb-%s", name),
		"--http-address":    ":8080",
		"--log.level":       infoLogLevel,
		"--objstore.config": string(bktConfigBytes),
	})
	if routePrefix != "" {
		args = append(args, "--web.route-prefix="+routePrefix)
	}

	if externalPrefix != "" {
		args = append(args, "--web.external-prefix="+externalPrefix)
	}

	if minTime != "" {
		args = append(args, "--min-time="+minTime)
	}

	if maxTime != "" {
		args = append(args, "--max-time="+maxTime)
	}

	if relabelConfig != "" {
		args = append(args, "--selector.relabel-config="+relabelConfig)
	}

	args = append([]string{"bucket", "web"}, args...)

	return e2emon.AsInstrumented(f.Init(wrapWithDefaults(e2e.StartOptions{
		Image:     DefaultImage(),
		Command:   e2e.NewCommand("tools", args...),
		Readiness: e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	})), "http")
}

func NewS3Config(bucket, endpoint, basePath string) s3.Config {
	httpDefaultConf := s3.DefaultConfig.HTTPConfig
	httpDefaultConf.TLSConfig = exthttp.TLSConfig{
		CAFile:   filepath.Join(basePath, "certs", "CAs", "ca.crt"),
		CertFile: filepath.Join(basePath, "certs", "public.crt"),
		KeyFile:  filepath.Join(basePath, "certs", "private.key"),
	}

	return s3.Config{
		Bucket:           bucket,
		AccessKey:        e2edb.MinioAccessKey,
		SecretKey:        e2edb.MinioSecretKey,
		Endpoint:         endpoint,
		Insecure:         false,
		HTTPConfig:       httpDefaultConf,
		BucketLookupType: s3.AutoLookup,
	}
}

// NOTE: by using aggregation all results are now unsorted.
var QueryUpWithoutInstance = func() string { return "sum(up) without (instance)" }

// LocalPrometheusTarget is a constant to be used in the Prometheus config if you
// wish to enable Prometheus to scrape itself in a test.
const LocalPrometheusTarget = "localhost:9090"

// DefaultPromConfig returns Prometheus config that sets Prometheus to:
// * expose 2 external labels, source and replica.
// * optionally scrape self. This will produce up == 0 metric which we can assert on.
// * optionally remote write endpoint to write into.
func DefaultPromConfig(name string, replica int, remoteWriteEndpoint, ruleFile string, scrapeTargets ...string) string {
	var targets string
	if len(scrapeTargets) > 0 {
		targets = strings.Join(scrapeTargets, ",")
	}

	config := fmt.Sprintf(`
global:
  external_labels:
    prometheus: %v
    replica: %v
`, name, replica)

	if targets != "" {
		config = fmt.Sprintf(`
%s
scrape_configs:
- job_name: 'myself'
  # Quick scrapes for test purposes.
  scrape_interval: 1s
  scrape_timeout: 1s
  static_configs:
  - targets: [%s]
  relabel_configs:
  - source_labels: ['__address__']
    regex: '^localhost:80$'
    action: drop
`, config, targets)
	}

	if remoteWriteEndpoint != "" {
		config = fmt.Sprintf(`
%s
remote_write:`, config)
		for _, url := range strings.Split(remoteWriteEndpoint, ",") {
			config = fmt.Sprintf(`
%s
- url: "%s"
  # Don't spam receiver on mistake.
  queue_config:
    min_backoff: 2s
    max_backoff: 10s`, config, url)
		}
	}

	if ruleFile != "" {
		config = fmt.Sprintf(`
%s
rule_files:
-  "%s"
`, config, ruleFile)
	}

	return config
}

func NewRedis(e e2e.Environment, name string) e2e.Runnable {
	return e.Runnable(fmt.Sprintf("redis-%s", name)).WithPorts(map[string]int{"redis": 6379}).Init(
		e2e.StartOptions{
			Image:            "docker.io/redis:7.0.4-alpine",
			Command:          e2e.NewCommand("redis-server", "*:6379"),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)
}
