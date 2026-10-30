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

	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/agent"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/thanos-io/thanos/pkg/extpromql"
)

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
