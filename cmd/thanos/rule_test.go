// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/efficientgo/core/testutil"
	extflag "github.com/efficientgo/tools/extkingpin"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
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
