// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2ethanos

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/efficientgo/e2e"
	e2edb "github.com/efficientgo/e2e/db"
	"github.com/efficientgo/tools/core/pkg/backoff"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/thanos-io/thanos/pkg/httpconfig"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/alert"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/queryfrontend"
	"github.com/thanos-io/thanos/pkg/receive"
	"github.com/thanos-io/thanos/pkg/rules/remotewrite"
)

const (
	infoLogLevel       = "info"
	ContainerSharedDir = "/shared"
)

// Same as default for now.
var defaultBackoffConfig = backoff.Config{
	Min:        300 * time.Millisecond,
	Max:        600 * time.Millisecond,
	MaxRetries: 50,
}

const (
	FeatureExemplarStorage = "exemplar-storage"
)

// TODO(bwplotka): Run against multiple?
func DefaultPrometheusImage() string {
	return "quay.io/prometheus/prometheus:v2.29.2"
}

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

func NewPrometheus(e e2e.Environment, name, config, promImage string, enableFeatures ...string) (*e2e.InstrumentedRunnable, string, error) {
	dir := filepath.Join(e.SharedDir(), "data", "prometheus", name)
	container := filepath.Join(ContainerSharedDir, "data", "prometheus", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, "", errors.Wrap(err, "create prometheus dir")
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "prometheus.yml"), []byte(config), 0600); err != nil {
		return nil, "", errors.Wrap(err, "creating prom config failed")
	}

	args := e2e.BuildArgs(map[string]string{
		"--config.file":                     filepath.Join(container, "prometheus.yml"),
		"--storage.tsdb.path":               container,
		"--storage.tsdb.max-block-duration": "2h",
		"--log.level":                       infoLogLevel,
		"--web.listen-address":              ":9090",
	})

	if len(enableFeatures) > 0 {
		args = append(args, fmt.Sprintf("--enable-feature=%s", strings.Join(enableFeatures, ",")))
	}
	prom := e2e.NewInstrumentedRunnable(
		e,
		fmt.Sprintf("prometheus-%s", name),
		map[string]int{"http": 9090},
		"http").Init(
		e2e.StartOptions{
			Image:            promImage,
			Command:          e2e.NewCommandWithoutEntrypoint("prometheus", args...),
			Readiness:        e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)

	return prom, container, nil
}

func NewPrometheusWithSidecar(e e2e.Environment, name, config, promImage string, enableFeatures ...string) (*e2e.InstrumentedRunnable, *e2e.InstrumentedRunnable, error) {
	return NewPrometheusWithSidecarCustomImage(e, name, config, promImage, DefaultImage(), enableFeatures...)
}

func NewPrometheusWithSidecarCustomImage(e e2e.Environment, name, config, promImage string, sidecarImage string, enableFeatures ...string) (*e2e.InstrumentedRunnable, *e2e.InstrumentedRunnable, error) {
	prom, dataDir, err := NewPrometheus(e, name, config, promImage, enableFeatures...)
	if err != nil {
		return nil, nil, err
	}

	sidecar := NewService(
		e,
		fmt.Sprintf("sidecar-%s", name),
		sidecarImage,
		e2e.NewCommand("sidecar", e2e.BuildArgs(map[string]string{
			"--debug.name":        fmt.Sprintf("sidecar-%v", name),
			"--grpc-address":      ":9091",
			"--grpc-grace-period": "0s",
			"--http-address":      ":8080",
			"--prometheus.url":    "http://" + prom.InternalEndpoint("http"),
			"--tsdb.path":         dataDir,
			"--log.level":         infoLogLevel,
		})...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		8080,
		9091,
	)

	return prom, sidecar, nil
}

type QuerierBuilder struct {
	environment    e2e.Environment
	sharedDir      string
	name           string
	routePrefix    string
	externalPrefix string
	image          string

	storeAddresses       []string
	fileSDStoreAddresses []string
	ruleAddresses        []string
	metadataAddresses    []string
	targetAddresses      []string
	exemplarAddresses    []string
	enableFeatures       []string

	tracingConfig string
}

func NewQuerierBuilder(e e2e.Environment, name string, storeAddresses ...string) *QuerierBuilder {
	return &QuerierBuilder{
		environment:    e,
		sharedDir:      e.SharedDir(),
		name:           name,
		storeAddresses: storeAddresses,
		image:          DefaultImage(),
	}
}

func (q *QuerierBuilder) WithEnabledFeatures(enableFeatures []string) *QuerierBuilder {
	q.enableFeatures = enableFeatures
	return q
}

func (q *QuerierBuilder) WithImage(image string) *QuerierBuilder {
	q.image = image
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

func (q *QuerierBuilder) BuildUninitiated() *e2e.FutureInstrumentedRunnable {
	return newUninitiatedService(
		q.environment,
		fmt.Sprintf("querier-%v", q.name),
		8080,
		9091,
	)
}

func (q *QuerierBuilder) Initiate(service *e2e.FutureInstrumentedRunnable, storeAddresses ...string) (*e2e.InstrumentedRunnable, error) {
	q.storeAddresses = storeAddresses
	args, err := q.collectArgs()
	if err != nil {
		return nil, err
	}

	querier := initiateService(
		service,
		q.image,
		e2e.NewCommand("query", args...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	)

	return querier, nil
}

func (q *QuerierBuilder) Build() (*e2e.InstrumentedRunnable, error) {
	args, err := q.collectArgs()
	if err != nil {
		return nil, err
	}

	querier := NewService(
		q.environment,
		fmt.Sprintf("querier-%v", q.name),
		q.image,
		e2e.NewCommand("query", args...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		8080,
		9091,
	)

	return querier, nil
}

func (q *QuerierBuilder) collectArgs() ([]string, error) {
	const replicaLabel = "replica"

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":            fmt.Sprintf("querier-%v", q.name),
		"--grpc-address":          ":9091",
		"--grpc-grace-period":     "0s",
		"--http-address":          ":8080",
		"--query.replica-label":   replicaLabel,
		"--store.sd-dns-interval": "5s",
		"--log.level":             infoLogLevel,
		"--query.max-concurrent":  "1",
		"--store.sd-interval":     "5s",
	})
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

	if len(q.fileSDStoreAddresses) > 0 {
		queryFileSDDir := filepath.Join(q.sharedDir, "data", "querier", q.name)
		container := filepath.Join(ContainerSharedDir, "data", "querier", q.name)
		if err := os.MkdirAll(queryFileSDDir, 0750); err != nil {
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

		if err := ioutil.WriteFile(queryFileSDDir+"/filesd.yaml", b, 0600); err != nil {
			return nil, errors.Wrap(err, "creating query SD config failed")
		}

		args = append(args, "--store.sd-files="+filepath.Join(container, "filesd.yaml"))
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

	return args, nil
}

func RemoteWriteEndpoint(addr string) string { return fmt.Sprintf("http://%s/api/v1/receive", addr) }

// NewUninitiatedReceiver returns a future receiver that can be initiated. It is useful
// for obtaining a receiver address for hashring before the receiver is started.
func NewUninitiatedReceiver(e e2e.Environment, name string) *e2e.FutureInstrumentedRunnable {
	return newUninitiatedService(e, fmt.Sprintf("receive-%v", name), 8080, 9091, Port{Name: "remote-write", PortNum: 8081})
}

// NewRoutingAndIngestingReceiverFromService creates a Thanos Receive instances from an unitiated service.
// It is configured both for ingesting samples and routing samples to other receivers.
func NewRoutingAndIngestingReceiverFromService(service *e2e.FutureInstrumentedRunnable, sharedDir string, replicationFactor int, hashring ...receive.HashringConfig) (*e2e.InstrumentedRunnable, error) {
	var localEndpoint string
	if len(hashring) == 0 {
		localEndpoint = "0.0.0.0:9091"
		hashring = []receive.HashringConfig{{Endpoints: []string{localEndpoint}}}
	} else {
		localEndpoint = service.InternalEndpoint("grpc")
	}

	dir := filepath.Join(sharedDir, "data", "receive", service.Name())
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(ContainerSharedDir, "data", "receive", service.Name())
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	receiver := initiateService(
		service,
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                 service.Name(),
			"--grpc-address":               ":9091",
			"--grpc-grace-period":          "0s",
			"--http-address":               ":8080",
			"--remote-write.address":       ":8081",
			"--label":                      fmt.Sprintf(`receive="%s"`, service.Name()),
			"--tsdb.path":                  filepath.Join(container, "data"),
			"--log.level":                  infoLogLevel,
			"--receive.replication-factor": strconv.Itoa(replicationFactor),
			"--receive.local-endpoint":     localEndpoint,
			"--receive.hashrings":          string(b),
		})...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	)

	return receiver, nil
}

func NewRoutingAndIngestingReceiverWithConfigWatcher(service *e2e.FutureInstrumentedRunnable, sharedDir string, replicationFactor int, hashring ...receive.HashringConfig) (*e2e.InstrumentedRunnable, error) {
	var localEndpoint string
	if len(hashring) == 0 {
		localEndpoint = "0.0.0.0:9091"
		hashring = []receive.HashringConfig{{Endpoints: []string{localEndpoint}}}
	} else {
		localEndpoint = service.InternalEndpoint("grpc")
	}

	dir := filepath.Join(sharedDir, "data", "receive", service.Name())
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(ContainerSharedDir, "data", "receive", service.Name())
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "hashrings.json"), b, 0600); err != nil {
		return nil, errors.Wrap(err, "creating receive config")
	}

	receiver := initiateService(
		service,
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                              service.Name(),
			"--grpc-address":                            ":9091",
			"--grpc-grace-period":                       "0s",
			"--http-address":                            ":8080",
			"--remote-write.address":                    ":8081",
			"--label":                                   fmt.Sprintf(`receive="%s"`, service.Name()),
			"--tsdb.path":                               filepath.Join(container, "data"),
			"--log.level":                               infoLogLevel,
			"--receive.replication-factor":              strconv.Itoa(replicationFactor),
			"--receive.local-endpoint":                  localEndpoint,
			"--receive.hashrings-file":                  filepath.Join(container, "hashrings.json"),
			"--receive.hashrings-file-refresh-interval": "5s",
		})...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
	)

	return receiver, nil
}

// NewRoutingReceiver creates a Thanos Receive instance that is only configured to route to other receive instances. It has no local storage.
func NewRoutingReceiver(e e2e.Environment, name string, replicationFactor int, hashring ...receive.HashringConfig) (*e2e.InstrumentedRunnable, error) {

	if len(hashring) == 0 {
		return nil, errors.New("hashring should not be empty for receive-distributor mode")
	}

	dir := filepath.Join(e.SharedDir(), "data", "receive", name)
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	b, err := json.Marshal(hashring)
	if err != nil {
		return nil, errors.Wrapf(err, "generate hashring file: %v", hashring)
	}

	receiver := NewService(
		e,
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":                 fmt.Sprintf("receive-%v", name),
			"--grpc-address":               ":9091",
			"--grpc-grace-period":          "0s",
			"--http-address":               ":8080",
			"--remote-write.address":       ":8081",
			"--label":                      fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":                  filepath.Join(container, "data"),
			"--log.level":                  infoLogLevel,
			"--receive.replication-factor": strconv.Itoa(replicationFactor),
			"--receive.hashrings":          string(b),
		})...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		8080,
		9091,
		Port{Name: "remote-write", PortNum: 8081},
	)

	return receiver, nil
}

// NewIngestingReceiver creates a Thanos Receive instance that is only configured to ingest, not route to other receivers.
func NewIngestingReceiver(e e2e.Environment, name string) (*e2e.InstrumentedRunnable, error) {
	dir := filepath.Join(e.SharedDir(), "data", "receive", name)
	dataDir := filepath.Join(dir, "data")
	container := filepath.Join(ContainerSharedDir, "data", "receive", name)
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return nil, errors.Wrap(err, "create receive dir")
	}
	receiver := NewService(e,
		fmt.Sprintf("receive-%v", name),
		DefaultImage(),
		// TODO(bwplotka): BuildArgs should be interface.
		e2e.NewCommand("receive", e2e.BuildArgs(map[string]string{
			"--debug.name":           fmt.Sprintf("receive-%v", name),
			"--grpc-address":         ":9091",
			"--grpc-grace-period":    "0s",
			"--http-address":         ":8080",
			"--remote-write.address": ":8081",
			"--label":                fmt.Sprintf(`receive="%s"`, name),
			"--tsdb.path":            filepath.Join(container, "data"),
			"--log.level":            infoLogLevel,
		})...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		8080,
		9091,
		Port{Name: "remote-write", PortNum: 8081},
	)

	return receiver, nil
}

func NewRuler(e e2e.Environment, name, ruleSubDir string, amCfg []alert.AlertmanagerConfig, queryCfg []httpconfig.Config) (*e2e.InstrumentedRunnable, error) {
	dir := filepath.Join(e.SharedDir(), "data", "rule", name)
	container := filepath.Join(ContainerSharedDir, "data", "rule", name)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create rule dir")
	}

	amCfgBytes, err := yaml.Marshal(alert.AlertingConfig{
		Alertmanagers: amCfg,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "generate am file: %v", amCfg)
	}

	queryCfgBytes, err := yaml.Marshal(queryCfg)
	if err != nil {
		return nil, errors.Wrapf(err, "generate query file: %v", queryCfg)
	}

	ruler := NewService(e,
		fmt.Sprintf("rule-%v", name),
		DefaultImage(),
		e2e.NewCommand("rule", e2e.BuildArgs(map[string]string{
			"--debug.name":                    fmt.Sprintf("rule-%v", name),
			"--grpc-address":                  ":9091",
			"--grpc-grace-period":             "0s",
			"--http-address":                  ":8080",
			"--label":                         fmt.Sprintf(`replica="%s"`, name),
			"--data-dir":                      container,
			"--rule-file":                     filepath.Join(ContainerSharedDir, ruleSubDir, "*.yaml"),
			"--eval-interval":                 "1s",
			"--alertmanagers.config":          string(amCfgBytes),
			"--alertmanagers.sd-dns-interval": "1s",
			"--log.level":                     infoLogLevel,
			"--query.config":                  string(queryCfgBytes),
			"--query.sd-dns-interval":         "1s",
			"--resend-delay":                  "5s",
		})...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		8080,
		9091,
	)

	return ruler, nil
}

func NewAlertmanager(e e2e.Environment, name string) (*e2e.InstrumentedRunnable, error) {
	dir := filepath.Join(e.SharedDir(), "data", "am", name)
	container := filepath.Join(ContainerSharedDir, "data", "am", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create am dir")
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
	if err := ioutil.WriteFile(filepath.Join(dir, "config.yaml"), []byte(config), 0600); err != nil {
		return nil, errors.Wrap(err, "creating alertmanager config file failed")
	}

	s := e2e.NewInstrumentedRunnable(
		e, fmt.Sprintf("alertmanager-%v", name), map[string]int{"http": 8080}, "http").Init(
		e2e.StartOptions{
			Image: DefaultAlertmanagerImage(),
			Command: e2e.NewCommandWithoutEntrypoint("/bin/alertmanager", e2e.BuildArgs(map[string]string{
				"--config.file":         filepath.Join(container, "config.yaml"),
				"--web.listen-address":  "0.0.0.0:8080",
				"--log.level":           infoLogLevel,
				"--storage.path":        container,
				"--web.get-concurrency": "1",
				"--web.timeout":         "2m",
			})...),
			Readiness:        e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
			User:             strconv.Itoa(os.Geteuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)

	return s, nil
}

func NewStoreGW(e e2e.Environment, name string, bucketConfig client.BucketConfig, cacheConfig string, relabelConfig ...relabel.Config) (*e2e.InstrumentedRunnable, error) {
	dir := filepath.Join(e.SharedDir(), "data", "store", name)
	container := filepath.Join(ContainerSharedDir, "data", "store", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create store dir")
	}

	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate store config file: %v", bucketConfig)
	}

	relabelConfigBytes, err := yaml.Marshal(relabelConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate store relabel file: %v", relabelConfig)
	}

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":        fmt.Sprintf("store-gw-%v", name),
		"--grpc-address":      ":9091",
		"--grpc-grace-period": "0s",
		"--http-address":      ":8080",
		"--log.level":         infoLogLevel,
		"--data-dir":          container,
		"--objstore.config":   string(bktConfigBytes),
		// Accelerated sync time for quicker test (3m by default).
		"--sync-block-duration":               "3s",
		"--block-sync-concurrency":            "1",
		"--store.grpc.series-max-concurrency": "1",
		"--selector.relabel-config":           string(relabelConfigBytes),
		"--consistency-delay":                 "30m",
	})

	if cacheConfig != "" {
		args = append(args, "--store.caching-bucket.config", cacheConfig)
	}

	store := NewService(
		e,
		fmt.Sprintf("store-gw-%v", name),
		DefaultImage(),
		e2e.NewCommand("store", args...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		8080,
		9091,
	)

	return store, nil
}

func NewCompactor(e e2e.Environment, name string, bucketConfig client.BucketConfig, relabelConfig []relabel.Config, extArgs ...string) (*e2e.InstrumentedRunnable, error) {
	dir := filepath.Join(e.SharedDir(), "data", "compact", name)
	container := filepath.Join(ContainerSharedDir, "data", "compact", name)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create compact dir")
	}

	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate compact config file: %v", bucketConfig)
	}

	relabelConfigBytes, err := yaml.Marshal(relabelConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate compact relabel file: %v", relabelConfig)
	}

	compactor := e2e.NewInstrumentedRunnable(
		e, fmt.Sprintf("compact-%s", name), map[string]int{"http": 8080}, "http").Init(
		e2e.StartOptions{
			Image: DefaultImage(),
			Command: e2e.NewCommand("compact", append(e2e.BuildArgs(map[string]string{
				"--debug.name":              fmt.Sprintf("compact-%s", name),
				"--log.level":               infoLogLevel,
				"--data-dir":                container,
				"--objstore.config":         string(bktConfigBytes),
				"--http-address":            ":8080",
				"--block-sync-concurrency":  "20",
				"--selector.relabel-config": string(relabelConfigBytes),
				"--wait":                    "",
			}), extArgs...)...),
			Readiness:        e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)

	return compactor, nil
}

func NewQueryFrontend(e e2e.Environment, name, downstreamURL string, cacheConfig queryfrontend.CacheProviderConfig) (*e2e.InstrumentedRunnable, error) {
	cacheConfigBytes, err := yaml.Marshal(cacheConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal response cache config file: %v", cacheConfig)
	}

	args := e2e.BuildArgs(map[string]string{
		"--debug.name":                        fmt.Sprintf("query-frontend-%s", name),
		"--http-address":                      ":8080",
		"--query-frontend.downstream-url":     downstreamURL,
		"--log.level":                         infoLogLevel,
		"--query-range.response-cache-config": string(cacheConfigBytes),
	})

	queryFrontend := e2e.NewInstrumentedRunnable(
		e, fmt.Sprintf("query-frontend-%s", name), map[string]int{"http": 8080}, "http").Init(
		e2e.StartOptions{
			Image:            DefaultImage(),
			Command:          e2e.NewCommand("query-frontend", args...),
			Readiness:        e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)

	return queryFrontend, nil
}

func NewReverseProxy(e e2e.Environment, name, tenantID, target string) (*e2e.InstrumentedRunnable, error) {
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

	dir := filepath.Join(e.SharedDir(), "data", "nginx", name)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Wrap(err, "create store dir")
	}

	if err := ioutil.WriteFile(filepath.Join(dir, "nginx.conf"), []byte(conf), 0600); err != nil {
		return nil, errors.Wrap(err, "creating nginx config file failed")
	}

	nginx := e2e.NewInstrumentedRunnable(e, fmt.Sprintf("nginx-%s", name), map[string]int{"http": 80}, "http").Init(
		e2e.StartOptions{
			Image:            "docker.io/nginx:1.21.1-alpine",
			Volumes:          []string{filepath.Join(dir, "/nginx.conf") + ":/etc/nginx/nginx.conf:ro"},
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)

	return nginx, nil
}

// NewMinio returns minio server, used as a local replacement for S3.
// TODO(@matej-g): This is a temporary workaround for https://github.com/efficientgo/e2e/issues/11;
// after this is addresses fixed all calls should be replaced with e2edb.NewMinio.
func NewMinio(env e2e.Environment, name, bktName string) *e2e.InstrumentedRunnable {
	image := "minio/minio:RELEASE.2019-12-30T05-45-39Z"
	minioKESGithubContent := "https://raw.githubusercontent.com/minio/kes/master"
	commands := []string{
		"curl -sSL --tlsv1.2 -O '%s/root.key'	-O '%s/root.cert'",
		"mkdir -p /data/%s && minio server --address :%v --quiet /data",
	}

	return e2e.NewInstrumentedRunnable(
		env,
		name,
		map[string]int{"http": 8090},
		"http").Init(
		e2e.StartOptions{
			Image: image,
			// Create the required bucket before starting minio.
			Command:   e2e.NewCommandWithoutEntrypoint("sh", "-c", fmt.Sprintf(strings.Join(commands, " && "), minioKESGithubContent, minioKESGithubContent, bktName, 8090)),
			Readiness: e2e.NewHTTPReadinessProbe("http", "/minio/health/ready", 200, 200),
			EnvVars: map[string]string{
				"MINIO_ACCESS_KEY": e2edb.MinioAccessKey,
				"MINIO_SECRET_KEY": e2edb.MinioSecretKey,
				"MINIO_BROWSER":    "off",
				"ENABLE_HTTPS":     "0",
				// https://docs.min.io/docs/minio-kms-quickstart-guide.html
				"MINIO_KMS_KES_ENDPOINT":  "https://play.min.io:7373",
				"MINIO_KMS_KES_KEY_FILE":  "root.key",
				"MINIO_KMS_KES_CERT_FILE": "root.cert",
				"MINIO_KMS_KES_KEY_NAME":  "my-minio-key",
			},
		},
	)
}

func NewMemcached(e e2e.Environment, name string) *e2e.InstrumentedRunnable {
	memcached := e2e.NewInstrumentedRunnable(e, fmt.Sprintf("memcached-%s", name), map[string]int{"memcached": 11211}, "memcached").Init(
		e2e.StartOptions{
			Image:            "docker.io/memcached:1.6.3-alpine",
			Command:          e2e.NewCommand("memcached", []string{"-m 1024", "-I 1m", "-c 1024", "-v"}...),
			User:             strconv.Itoa(os.Getuid()),
			WaitReadyBackoff: &defaultBackoffConfig,
		},
	)

	return memcached
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
) (*e2e.InstrumentedRunnable, error) {
	bktConfigBytes, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "generate tools bucket web config file: %v", bucketConfig)
	}

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

	toolsBucketWeb := NewService(e,
		fmt.Sprintf("toolsBucketWeb-%s", name),
		DefaultImage(),
		e2e.NewCommand("tools", args...),
		e2e.NewHTTPReadinessProbe("http", "/-/ready", 200, 200),
		8080,
		9091,
	)

	return toolsBucketWeb, nil
}
