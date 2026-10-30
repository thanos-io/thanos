// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/agent"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/extpromql"
)

func TestRemoteWriteConfigReloader(t *testing.T) {
	t.Parallel()

	configContent := []byte("remote_write:\n- url: http://example.com/api/v1/write\n")
	var appliedConfig *config.Config
	reloader := &remoteWriteConfigReloader{
		loadConfig: func() ([]byte, error) {
			return configContent, nil
		},
		applyConfig: func(cfg *config.Config) error {
			appliedConfig = cfg
			return nil
		},
		externalLabels: labels.FromStrings("cluster", "test"),
	}

	reloadMsg := make(chan error)
	go reloadRemoteWriteConfig(log.NewNopLogger(), reloader, reloadMsg)
	testutil.Ok(t, <-reloadMsg)
	if appliedConfig == nil {
		t.Fatal("expected remote write config to be applied")
	}
	testutil.Equals(t, 1, len(appliedConfig.RemoteWriteConfigs))
	testutil.Equals(t, "http://example.com/api/v1/write", appliedConfig.RemoteWriteConfigs[0].URL.String())
	testutil.Equals(t, labels.FromStrings("cluster", "test"), appliedConfig.GlobalConfig.ExternalLabels)

	configContent = []byte("remote_write:\n- url: http://example.org/api/v1/write\n")
	testutil.Ok(t, reloader.reload())
	testutil.Equals(t, "http://example.org/api/v1/write", appliedConfig.RemoteWriteConfigs[0].URL.String())
}

func TestRemoteWriteReloadHandler(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		reloadErr      error
		expectedStatus int
	}{
		"success": {
			expectedStatus: http.StatusOK,
		},
		"error": {
			reloadErr:      errors.New("reload failed"),
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			reloadWebhandler := make(chan chan error)
			go func() {
				reloadMsg := <-reloadWebhandler
				reloadMsg <- tt.reloadErr
			}()

			req := httptest.NewRequest(http.MethodPost, "/-/reload-remote-write", nil)
			resp := httptest.NewRecorder()
			remoteWriteReloadHandler(reloadWebhandler).ServeHTTP(resp, req)

			testutil.Equals(t, tt.expectedStatus, resp.Code)
		})
	}
}

func TestRemoteWriteReloadRegistration(t *testing.T) {
	t.Parallel()

	t.Run("without remote write", func(t *testing.T) {
		router := route.New()
		registerRemoteWriteReloadHandler(router, newRemoteWriteReloadWebhandler(nil))

		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/-/reload-remote-write", nil)
		router.ServeHTTP(resp, req)

		testutil.Equals(t, http.StatusNotFound, resp.Code)
	})

	t.Run("with remote write", func(t *testing.T) {
		reloadWebhandler := newRemoteWriteReloadWebhandler(&remoteWriteConfigReloader{})
		router := route.New()
		registerRemoteWriteReloadHandler(router, reloadWebhandler)

		go func() {
			reloadMsg := <-reloadWebhandler
			reloadMsg <- nil
		}()

		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/-/reload-remote-write", nil)
		router.ServeHTTP(resp, req)

		testutil.Equals(t, http.StatusOK, resp.Code)
	})
}

func TestRunRuleRemoteWriteReloadSetup(t *testing.T) {
	t.Parallel()

	app := extkingpin.NewApp(kingpin.New("test", "test"))
	conf := ruleConfig{
		http: httpConfig{bindAddress: "127.0.0.1:0"},
		grpc: grpcConfig{bindAddress: "127.0.0.1:0"},
		alertmgr: alertMgrConfig{
			alertSourceTemplate: func() *string {
				template := "/graph?g0.expr={{.Expr}}"
				return &template
			}(),
		},
		alertQueryURL:     &url.URL{},
		rwConfig:          extflag.RegisterPathOrContent(app, "remote-write.config", "test"),
		objStoreConfig:    extkingpin.RegisterCommonObjStoreFlags(app, "", false),
		dataDir:           t.TempDir(),
		evalInterval:      time.Second,
		resendDelay:       time.Minute,
		outageTolerance:   time.Hour,
		forGracePeriod:    time.Minute,
		ruleQueueCapacity: 100,
	}

	var g run.Group
	g.Add(func() error { return errors.New("stop test") }, func(error) {})
	err := runRule(
		&g,
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		opentracing.NoopTracer{},
		component.Rule,
		conf,
		make(chan struct{}),
		nil,
		nil,
		nil,
		nil,
		&tsdb.Options{
			MinBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			MaxBlockDuration:  int64(2 * time.Hour / time.Millisecond),
			RetentionDuration: int64(48 * time.Hour / time.Millisecond),
			NoLockfile:        true,
		},
		nil,
	)
	testutil.Ok(t, err)
	testutil.NotOk(t, g.Run())
}

func TestRemoteWriteConfigReloaderErrors(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		loadConfig  func() ([]byte, error)
		applyConfig func(*config.Config) error
	}{
		"load config": {
			loadConfig: func() ([]byte, error) {
				return nil, errors.New("read failed")
			},
		},
		"parse config": {
			loadConfig: func() ([]byte, error) {
				return []byte("invalid: ["), nil
			},
		},
		"apply config": {
			loadConfig: func() ([]byte, error) {
				return []byte("remote_write:\n- url: http://example.com/api/v1/write\n"), nil
			},
			applyConfig: func(*config.Config) error {
				return errors.New("apply failed")
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			reloader := &remoteWriteConfigReloader{
				loadConfig:     tt.loadConfig,
				applyConfig:    tt.applyConfig,
				externalLabels: labels.EmptyLabels(),
			}
			reloadMsg := make(chan error)
			go reloadRemoteWriteConfig(log.NewNopLogger(), reloader, reloadMsg)
			testutil.NotOk(t, <-reloadMsg)
		})
	}
}

func Test_parseFlagLabels(t *testing.T) {
	var tData = []struct {
		s         []string
		expectErr bool
	}{
		{
			s:         []string{`labelName="LabelVal"`, `_label_Name="LabelVal"`, `label_name="LabelVal"`, `LAb_el_Name="LabelValue"`, `lab3l_Nam3="LabelValue"`},
			expectErr: false,
		},
		{
			s:         []string{`label_Name"LabelVal"`}, // Missing "=" separator.
			expectErr: true,
		},
		{
			s:         []string{`label_Name= "LabelVal"`}, // Whitespace invalid syntax.
			expectErr: true,
		},
		{
			s:         []string{`label_name=LabelVal`}, // Missing quotes invalid syntax.
			expectErr: true,
		},
	}
	for _, td := range tData {
		_, err := parseFlagLabels(td.s)
		testutil.Equals(t, err != nil, td.expectErr)
	}
}

func Test_validateTemplate(t *testing.T) {
	tData := []struct {
		template  string
		expectErr bool
	}{
		{
			template:  `/graph?g0.expr={{.Expr}}&g0.tab=1`,
			expectErr: false,
		},
		{
			template:  `/graph?g0.expr={{.Expression}}&g0.tab=1`,
			expectErr: true,
		},
		{
			template:  `another template includes {{.Expr}}`,
			expectErr: false,
		},
	}
	for _, td := range tData {
		err := validateTemplate(td.template)
		testutil.Equals(t, err != nil, td.expectErr)
	}
}

func Test_tableLinkForExpression(t *testing.T) {
	tData := []struct {
		template  string
		expr      string
		expectStr string
		expectErr bool
	}{
		{
			template:  `/graph?g0.expr={{.Expr}}&g0.tab=1`,
			expr:      `up{app="foo"}`,
			expectStr: `/graph?g0.expr=up%7Bapp%3D%22foo%22%7D&g0.tab=1`,
			expectErr: false,
		},
		{
			template:  `/graph?g0.expr={{.Expr}}&g0.tab=1`,
			expr:      `up{app="foo yoo"}`,
			expectStr: `/graph?g0.expr=up%7Bapp%3D%22foo+yoo%22%7D&g0.tab=1`,
			expectErr: false,
		},
		{
			template:  `/graph?g0.expr={{.Expression}}&g0.tab=1`,
			expr:      "test_expr",
			expectErr: true,
		},
		{
			template:  `another template includes {{.Expr}}`,
			expr:      "test_expr",
			expectStr: `another template includes test_expr`,
			expectErr: false,
		},
	}
	for _, td := range tData {
		resStr, err := tableLinkForExpression(td.template, td.expr)
		testutil.Equals(t, err != nil, td.expectErr)
		testutil.Equals(t, resStr, td.expectStr)
	}
}

func TestFilterOutPromQLWarnings(t *testing.T) {
	logger := log.NewNopLogger()
	query := "foo"
	expr, err := extpromql.ParseExpr(`rate(prometheus_build_info[5m])`)
	testutil.Ok(t, err)
	possibleCounterInfo := annotations.NewPossibleNonCounterInfo("foo", expr.PositionRange())
	badBucketLabelWarning := annotations.NewBadBucketLabelWarning("foo", "0.99", expr.PositionRange())
	for _, tc := range []struct {
		name     string
		warnings []string
		expected []string
	}{
		{
			name:     "nil warning",
			expected: make([]string, 0),
		},
		{
			name:     "empty warning",
			warnings: make([]string, 0),
			expected: make([]string, 0),
		},
		{
			name: "no PromQL warning",
			warnings: []string{
				"some_warning_message",
			},
			expected: []string{
				"some_warning_message",
			},
		},
		{
			name: "PromQL warning",
			warnings: []string{
				possibleCounterInfo.Error(),
			},
			expected: make([]string, 0),
		},
		{
			name: "filter out all PromQL warnings",
			warnings: []string{
				possibleCounterInfo.Error(),
				badBucketLabelWarning.Error(),
				"some_warning_message",
			},
			expected: []string{
				"some_warning_message",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := filterOutPromQLWarnings(tc.warnings, logger, query)
			testutil.Equals(t, tc.expected, output)
		})
	}
}

func TestParseRemoteWriteConfig(t *testing.T) {
	extLabels := labels.FromStrings("replica", "0")

	for _, tc := range []struct {
		name      string
		yaml      string
		expectErr bool
	}{
		{
			name: "valid rw2 config with replace relabeling",
			yaml: `remote_write:
  - name: rw2-default
    url: http://127.0.0.1:9090/api/v1/write
    protobuf_message: io.prometheus.write.v2.Request
    write_relabel_configs:
      - source_labels: [__tmp_name]
        target_label: __name__
        action: replace
`,
		},
		{
			name: "duplicate queue name",
			yaml: `remote_write:
  - name: dup
    url: http://127.0.0.1:9090/api/v1/write
  - name: dup
    url: http://127.0.0.1:9091/api/v1/write
`,
			expectErr: true,
		},
		{
			name: "null remote write entry",
			yaml: `remote_write:
  - null
`,
			expectErr: true,
		},
		{
			name: "empty remote write list",
			yaml: `remote_write: []
`,
			expectErr: true,
		},
		{
			name: "invalid write_relabel_configs missing target_label",
			yaml: `remote_write:
  - url: http://127.0.0.1:9090/api/v1/write
    write_relabel_configs:
      - source_labels: [__tmp_name]
        action: replace
`,
			expectErr: true,
		},
		{
			name: "unknown top-level field",
			yaml: `unknown_field: true
remote_write:
  - url: http://127.0.0.1:9090/api/v1/write
`,
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseRemoteWriteConfig([]byte(tc.yaml), extLabels)
			if tc.expectErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, model.UTF8Validation, cfg.GlobalConfig.MetricNameValidationScheme)
			testutil.Equals(t, extLabels, cfg.GlobalConfig.ExternalLabels)
			testutil.Equals(t, 1, len(cfg.RemoteWriteConfigs))
			testutil.Equals(t, remote.WriteV2MessageType, cfg.RemoteWriteConfigs[0].ProtobufMessage)
			testutil.Equals(t, 1, len(cfg.RemoteWriteConfigs[0].WriteRelabelConfigs))
			testutil.Equals(t, model.UTF8Validation, cfg.RemoteWriteConfigs[0].WriteRelabelConfigs[0].NameValidationScheme)
		})
	}
}

func TestOpenStatelessRuleStorage_RemoteWriteV2(t *testing.T) {
	reqCh := make(chan *writev2.Request, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		raw, err := snappy.Decode(nil, compressed)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var req writev2.Request
		if err := req.Unmarshal(raw); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var writtenSamples int
		for _, ts := range req.Timeseries {
			writtenSamples += len(ts.Samples)
		}
		w.Header().Set("X-Prometheus-Remote-Write-Written-Samples", strconv.Itoa(writtenSamples))
		w.Header().Set("X-Prometheus-Remote-Write-Written-Histograms", "0")
		w.Header().Set("X-Prometheus-Remote-Write-Written-Exemplars", "0")
		w.WriteHeader(http.StatusNoContent)
		select {
		case reqCh <- &req:
		default:
		}
	}))
	t.Cleanup(srv.Close)

	rwCfgYAML := fmt.Sprintf(`remote_write:
  - url: %s
    protobuf_message: %s
    queue_config:
      batch_send_deadline: 50ms
    write_relabel_configs:
      - source_labels: [__tmp_name]
        target_label: __name__
        action: replace
      - regex: __tmp_name
        action: labeldrop
`, srv.URL, remote.WriteV2MessageType)

	dataDir := t.TempDir()
	extLabels := labels.FromStrings("replica", "0")

	// Duplicate unnamed remote_write configs pass parseRemoteWriteConfig but fail
	// duplicate hash detection in remoteStore.ApplyConfig; verify agentDB and
	// remoteStore are closed on error so reusing dataDir succeeds without a lock error.
	dupCfgYAML := fmt.Sprintf("remote_write:\n  - url: %s\n  - url: %s\n", srv.URL, srv.URL)
	_, err := openStatelessRuleStorage(log.NewNopLogger(), prometheus.NewRegistry(), dataDir, []byte(dupCfgYAML), extLabels, agent.DefaultOptions())
	testutil.NotOk(t, err)

	store, err := openStatelessRuleStorage(
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		dataDir,
		[]byte(rwCfgYAML),
		extLabels,
		agent.DefaultOptions(),
	)
	testutil.Ok(t, err)
	t.Cleanup(func() {
		testutil.Ok(t, store.Close())
	})

	var (
		req      *writev2.Request
		ts       = time.Now().Add(time.Second).UnixMilli()
		deadline = time.After(3 * time.Second)
		ticker   = time.NewTicker(50 * time.Millisecond)
	)
	defer ticker.Stop()

	for req == nil {
		app := store.Appender(t.Context())
		_, err := app.Append(0, labels.FromStrings("__name__", "rule:placeholder", "__tmp_name", "rule:evaluated_total", "env", "prod"), ts, 42)
		testutil.Ok(t, err)
		testutil.Ok(t, app.Commit())
		ts++

		select {
		case req = <-reqCh:
		case <-ticker.C:
		case <-deadline:
			t.Fatal("timed out waiting for remote write v2 request")
		}
	}

	testutil.Assert(t, len(req.Timeseries) > 0, "expected at least one timeseries")
	gotLabels, err := req.Timeseries[0].ToLabels(&labels.ScratchBuilder{}, req.Symbols)
	testutil.Ok(t, err)
	testutil.Equals(t, labels.FromStrings("__name__", "rule:evaluated_total", "env", "prod", "replica", "0"), gotLabels)
	testutil.Assert(t, len(req.Timeseries[0].Samples) > 0, "expected at least one sample")
	testutil.Equals(t, 42.0, req.Timeseries[0].Samples[0].Value)
}
