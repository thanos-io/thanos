// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"gopkg.in/yaml.v3"

	"github.com/efficientgo/core/testutil"

	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type nopAppendable struct{}

func (n nopAppendable) AppenderV2(_ context.Context) storage.AppenderV2 { return nopAppender{} }

type nopAppender struct{}

func (n nopAppender) Append(storage.SeriesRef, labels.Labels, int64, int64, float64, *histogram.Histogram, *histogram.FloatHistogram, storage.AOptions) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) Commit() error   { return nil }
func (n nopAppender) Rollback() error { return nil }

type nopQueryable struct{}

func (n nopQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return storage.NoopQuerier(), nil
}

// Regression test against https://github.com/thanos-io/thanos/issues/1779.
func TestRun_Subqueries(t *testing.T) {
	dir := t.TempDir()

	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "rule.yaml"), []byte(`
groups:
- name: "rule with subquery"
  partial_response_strategy: "warn"
  rules:
  - record: "test"
    expr: "rate(some_metric[1h:5m] offset 1d)"
`), os.ModePerm))

	var (
		queryDone = make(chan struct{})
		queryOnce sync.Once
		query     string
	)
	thanosRuleMgr := NewManager(
		context.Background(),
		nil,
		dir,
		rules.ManagerOptions{
			Logger:       logutil.GoKitLogToSlog(log.NewLogfmtLogger(os.Stderr)),
			Context:      context.Background(),
			AppendableV2: nopAppendable{},
			Queryable:    nopQueryable{},
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, t time.Time) (vectors promql.Vector, e error) {
				queryOnce.Do(func() {
					query = q
					close(queryDone)
				})
				return promql.Vector{}, nil
			}
		},
		labels.FromStrings("replica", "1"),
		"http://localhost",
	)
	testutil.Ok(t, thanosRuleMgr.Update(1*time.Second, []string{filepath.Join(dir, "rule.yaml")}))

	thanosRuleMgr.Run()
	defer thanosRuleMgr.Stop()

	select {
	case <-time.After(1 * time.Minute):
		t.Fatal("timeout while waiting on rule manager query evaluation")
	case <-queryDone:
	}
	testutil.Equals(t, "rate(some_metric[1h:5m] offset 1d)", query)
}

func TestUpdate_Error_UpdatePartial(t *testing.T) {
	dir := t.TempDir()
	dataDir := t.TempDir()

	err := os.MkdirAll(filepath.Join(dir, "subdir"), 0775)
	testutil.Ok(t, err)

	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "no_strategy.yaml"), []byte(`
groups:
- name: "something1"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "abort.yaml"), []byte(`
groups:
- name: "something2"
  partial_response_strategy: "abort"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "warn.yaml"), []byte(`
groups:
- name: "something3"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "wrong.yaml"), []byte(`
groups:
- name: "something4"
  partial_response_strategy: "afafsdgsdgs" # Err 1
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "combined.yaml"), []byte(`
groups:
- name: "something5"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
- name: "something6"
  partial_response_strategy: "abort"
  rules:
  - alert: "some"
    expr: "up"
- name: "something7"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	// Same filename as the first rule file but different path.
	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "subdir", "no_strategy.yaml"), []byte(`
groups:
- name: "something8"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	reg := prometheus.NewRegistry()

	thanosRuleMgr := NewManager(
		context.Background(),
		reg,
		dataDir,
		rules.ManagerOptions{
			Logger:    logutil.GoKitLogToSlog(log.NewLogfmtLogger(os.Stderr)),
			Queryable: nopQueryable{},
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				return nil, nil
			}
		},
		labels.FromStrings("replica", "1"),
		"http://localhost",
	)
	err = thanosRuleMgr.Update(10*time.Second, []string{
		filepath.Join(dir, "no_strategy.yaml"),
		filepath.Join(dir, "abort.yaml"),
		filepath.Join(dir, "warn.yaml"),
		filepath.Join(dir, "wrong.yaml"),
		filepath.Join(dir, "combined.yaml"),
		filepath.Join(dir, "non_existing.yaml"),
		filepath.Join(dir, "subdir", "no_strategy.yaml"),
	})
	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "wrong.yaml: failed to unmarshal \"afafsdgsdgs\" as 'partial_response_strategy'"), err.Error())
	testutil.Assert(t, strings.Contains(err.Error(), "non_existing.yaml: no such file or directory"), err.Error())

	// Still failed update should load at least partially correct rules.
	// Also, check metrics: Regression test: https://github.com/thanos-io/thanos/issues/3083
	testutil.Equals(t,
		map[string]float64{
			fmt.Sprintf("prometheus_rule_group_rules{rule_group=%s/.tmp-rules/ABORT%s/abort.yaml;something2,strategy=abort}", dataDir, dir):              1,
			fmt.Sprintf("prometheus_rule_group_rules{rule_group=%s/.tmp-rules/ABORT%s/subdir/no_strategy.yaml;something8,strategy=abort}", dataDir, dir): 1,
			fmt.Sprintf("prometheus_rule_group_rules{rule_group=%s/.tmp-rules/ABORT%s/combined.yaml;something6,strategy=abort}", dataDir, dir):           1,
			fmt.Sprintf("prometheus_rule_group_rules{rule_group=%s/.tmp-rules/ABORT%s/combined.yaml;something7,strategy=abort}", dataDir, dir):           1,
			fmt.Sprintf("prometheus_rule_group_rules{rule_group=%s/.tmp-rules/ABORT%s/no_strategy.yaml;something1,strategy=abort}", dataDir, dir):        1,
			fmt.Sprintf("prometheus_rule_group_rules{rule_group=%s/.tmp-rules/WARN%s/combined.yaml;something5,strategy=warn}", dataDir, dir):             1,
			fmt.Sprintf("prometheus_rule_group_rules{rule_group=%s/.tmp-rules/WARN%s/warn.yaml;something3,strategy=warn}", dataDir, dir):                 1,
		},
		extprom.CurrentGaugeValuesFor(t, reg, "prometheus_rule_group_rules"),
	)

	g := thanosRuleMgr.RuleGroups()
	sort.Slice(g, func(i, j int) bool {
		return g[i].Name() < g[j].Name()
	})

	exp := []struct {
		name     string
		file     string
		strategy storepb.PartialResponseStrategy
	}{
		{
			name:     "something1",
			file:     filepath.Join(dir, "no_strategy.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something2",
			file:     filepath.Join(dir, "abort.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something3",
			file:     filepath.Join(dir, "warn.yaml"),
			strategy: storepb.PartialResponseStrategy_WARN,
		},
		{
			name:     "something5",
			file:     filepath.Join(dir, "combined.yaml"),
			strategy: storepb.PartialResponseStrategy_WARN,
		},
		{
			name:     "something6",
			file:     filepath.Join(dir, "combined.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something7",
			file:     filepath.Join(dir, "combined.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something8",
			file:     filepath.Join(dir, "subdir", "no_strategy.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
	}
	testutil.Equals(t, len(exp), len(g))

	for i := range exp {
		t.Run(exp[i].name, func(t *testing.T) {
			testutil.Equals(t, exp[i].strategy, g[i].PartialResponseStrategy)
			testutil.Equals(t, exp[i].name, g[i].Name())

			p := g[i].toProto()
			testutil.Equals(t, exp[i].strategy, p.PartialResponseStrategy)
			testutil.Equals(t, exp[i].name, p.Name)
			testutil.Equals(t, exp[i].file, p.File)
		})
	}
	defer func() {
		// Update creates go routines. We don't need rules mngrs to run, just to parse things, but let it start and stop
		// at the end to correctly test leaked go routines.
		thanosRuleMgr.Run()
		thanosRuleMgr.Stop()
	}()
}

func TestConfigRuleAdapterUnmarshalMarshalYAML(t *testing.T) {
	c := configGroups{}
	testutil.Ok(t, yaml.Unmarshal([]byte(`groups:
- name: something1
  rules:
  - alert: some
    expr: up
  partial_response_strategy: ABORT
  limit: 10
- name: something2
  rules:
  - alert: some
    expr: rate(some_metric[1h:5m] offset 1d)
  partial_response_strategy: WARN
`), &c))
	b, err := yaml.Marshal(c)
	testutil.Ok(t, err)
	testutil.Equals(t, `groups:
    - limit: 10
      name: something1
      rules:
        - alert: some
          expr: up
    - name: something2
      rules:
        - alert: some
          expr: rate(some_metric[1h:5m] offset 1d)
`, string(b))
}

func TestManager_Rules(t *testing.T) {
	dir := t.TempDir()

	curr, err := os.Getwd()
	testutil.Ok(t, err)

	thanosRuleMgr := NewManager(
		context.Background(),
		nil,
		dir,
		rules.ManagerOptions{
			Logger:    logutil.GoKitLogToSlog(log.NewLogfmtLogger(os.Stderr)),
			Queryable: nopQueryable{},
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				return nil, nil
			}
		},
		labels.FromStrings("replica", "test1"),
		"http://localhost",
	)
	testutil.Ok(t, thanosRuleMgr.Update(60*time.Second, []string{
		filepath.Join(curr, "../../examples/alerts/alerts.yaml"),
		filepath.Join(curr, "../../examples/alerts/rules.yaml"),
	}))
	defer func() {
		// Update creates go routines. We don't need rules mngrs to run, just to parse things, but let it start and stop
		// at the end to correctly test leaked go routines.
		thanosRuleMgr.Run()
		thanosRuleMgr.Stop()
	}()
	testRulesAgainstExamples(t, filepath.Join(curr, "../../examples/alerts"), thanosRuleMgr, false)
}

func TestManagerUpdateWithNoRules(t *testing.T) {
	dir := t.TempDir()

	testutil.Ok(t, os.WriteFile(filepath.Join(dir, "no_strategy.yaml"), []byte(`
groups:
- name: "something1"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))

	thanosRuleMgr := NewManager(
		context.Background(),
		nil,
		dir,
		rules.ManagerOptions{
			Logger:    logutil.GoKitLogToSlog(log.NewLogfmtLogger(os.Stderr)),
			Queryable: nopQueryable{},
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				return nil, nil
			}
		},
		labels.EmptyLabels(),
		"http://localhost",
	)

	// We need to run the underlying rule managers to update them more than
	// once (otherwise there's a deadlock).
	thanosRuleMgr.Run()
	t.Cleanup(thanosRuleMgr.Stop)

	err := thanosRuleMgr.Update(1*time.Second, []string{
		filepath.Join(dir, "no_strategy.yaml"),
	})
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(thanosRuleMgr.RuleGroups()))

	err = thanosRuleMgr.Update(1*time.Second, []string{})
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(thanosRuleMgr.RuleGroups()))
}

func TestManagerRunRulesWithRuleGroupLimit(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "with_limit.yaml")
	testutil.Ok(t, os.WriteFile(filename, []byte(`
groups:
- name: "something1"
  interval: 1ms
  limit: 1
  rules:
  - alert: "some"
    expr: "up>0"
    for: 0s
`), os.ModePerm))

	thanosRuleMgr := NewManager(
		context.Background(),
		nil,
		dir,
		rules.ManagerOptions{
			Logger:    logutil.GoKitLogToSlog(log.NewLogfmtLogger(os.Stderr)),
			Queryable: nopQueryable{},
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, ts time.Time) (promql.Vector, error) {
				return []promql.Sample{
					{
						T:      0,
						F:      1,
						Metric: labels.FromStrings("foo", "bar"),
					},
					{
						T:      0,
						F:      1,
						Metric: labels.FromStrings("foo1", "bar1"),
					},
				}, nil
			}
		},
		labels.EmptyLabels(),
		"http://localhost",
	)
	thanosRuleMgr.Run()
	t.Cleanup(thanosRuleMgr.Stop)
	testutil.Ok(t, thanosRuleMgr.Update(time.Millisecond, []string{filename}))
	testutil.Equals(t, 1, len(thanosRuleMgr.protoRuleGroups()))
	testutil.Equals(t, 1, len(thanosRuleMgr.protoRuleGroups()[0].Rules))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	testutil.Ok(t, runutil.Retry(time.Millisecond, ctx.Done(), func() error {
		if thanosRuleMgr.protoRuleGroups()[0].Rules[0].GetAlert().Health != string(rules.HealthBad) {
			return errors.New("expect HealthBad")
		}
		return nil
	}))
	testutil.Equals(t, "exceeded limit of 1 with 2 alerts", thanosRuleMgr.protoRuleGroups()[0].Rules[0].GetAlert().LastError)
}

func TestValidateAndCount_Metadata(t *testing.T) {
	for _, tc := range []struct {
		name      string
		yaml      string
		expRules  int
		expectErr bool
	}{
		{
			name: "valid recording rule metadata",
			yaml: `groups:
- name: rec
  partial_response_strategy: warn
  rules:
  - record: job:http_requests:sum
    expr: sum(http_requests_total) by (job)
    metadata:
      type: counter
      unit: requests
      help: Total HTTP requests by job.
`,
			expRules: 1,
		},
		{
			name: "invalid recording rule metadata type",
			yaml: `groups:
- name: rec
  rules:
  - record: job:http_requests:sum
    expr: sum(http_requests_total) by (job)
    metadata:
      type: not_a_type
`,
			expectErr: true,
		},
		{
			name: "metadata on alerting rule rejected",
			yaml: `groups:
- name: alert
  rules:
  - alert: HighErrorRate
    expr: up == 0
    metadata:
      type: gauge
`,
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, errs := ValidateAndCount(strings.NewReader(tc.yaml))
			if tc.expectErr {
				testutil.NotOk(t, errs.Err())
				return
			}
			testutil.Ok(t, errs.Err())
			testutil.Equals(t, tc.expRules, n)
		})
	}
}

type recordedSample struct {
	lset labels.Labels
	meta metadata.Metadata
}

type recordingAppendable struct {
	mu      sync.Mutex
	samples map[string]recordedSample
}

func (r *recordingAppendable) AppenderV2(_ context.Context) storage.AppenderV2 {
	return &recordingAppender{parent: r}
}

type recordingAppender struct {
	parent  *recordingAppendable
	pending []recordedSample
}

func (a *recordingAppender) Append(_ storage.SeriesRef, lset labels.Labels, _, _ int64, _ float64, _ *histogram.Histogram, _ *histogram.FloatHistogram, opts storage.AOptions) (storage.SeriesRef, error) {
	a.pending = append(a.pending, recordedSample{
		lset: lset.Copy(),
		meta: opts.Metadata,
	})
	return 0, nil
}

func (a *recordingAppender) Commit() error {
	a.parent.mu.Lock()
	defer a.parent.mu.Unlock()
	for _, s := range a.pending {
		a.parent.samples[s.lset.Get(labels.MetricName)] = s
	}
	a.pending = nil
	return nil
}

func (a *recordingAppender) Rollback() error {
	a.pending = nil
	return nil
}

func TestManager_RecordingRuleMetadata(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		enableTypeAndUnitLabels bool
		expLabels               map[string]labels.Labels
		expMeta                 map[string]metadata.Metadata
	}{
		{
			name:                    "type-and-unit-labels=false",
			enableTypeAndUnitLabels: false,
			expLabels: map[string]labels.Labels{
				"job:http_requests:rate5m": labels.FromStrings("__name__", "job:http_requests:rate5m"),
				"job:http_requests_total":  labels.FromStrings("__name__", "job:http_requests_total"),
				"job:feature_state":        labels.FromStrings("__name__", "job:feature_state"),
			},
			expMeta: map[string]metadata.Metadata{
				"job:http_requests:rate5m": {
					Type: model.MetricTypeGauge,
					Unit: "requests_per_second",
					Help: "Per-second rate of HTTP requests.",
				},
				"job:http_requests_total": {
					Type: model.MetricTypeCounter,
				},
				"job:feature_state": {
					Type: model.MetricTypeGauge,
				},
			},
		},
		{
			name:                    "type-and-unit-labels=true",
			enableTypeAndUnitLabels: true,
			expLabels: map[string]labels.Labels{
				"job:http_requests:rate5m": labels.FromStrings("__name__", "job:http_requests:rate5m", "__type__", "gauge", "__unit__", "requests_per_second"),
				"job:http_requests_total":  labels.FromStrings("__name__", "job:http_requests_total", "__type__", "counter"),
				"job:feature_state":        labels.FromStrings("__name__", "job:feature_state", "__type__", "stateset", "__unit__", "seconds"),
			},
			expMeta: map[string]metadata.Metadata{
				"job:http_requests:rate5m": {
					Type: model.MetricTypeGauge,
					Unit: "requests_per_second",
					Help: "Per-second rate of HTTP requests.",
				},
				"job:http_requests_total": {
					Type: model.MetricTypeCounter,
				},
				"job:feature_state": {
					Type: model.MetricTypeStateset,
					Unit: "seconds",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ruleFile := filepath.Join(dir, "rules.yaml")
			testutil.Ok(t, os.WriteFile(ruleFile, []byte(`groups:
- name: metadata_group
  partial_response_strategy: warn
  interval: 10ms
  rules:
  - record: job:http_requests:rate5m
    expr: sum(rate(http_requests_total[5m])) by (job)
    metadata:
      type: gauge
      unit: requests_per_second
      help: Per-second rate of HTTP requests.
  - record: job:http_requests_total
    expr: http_requests_total
  - record: job:feature_state
    expr: feature_state
`), 0600))

			app := &recordingAppendable{samples: make(map[string]recordedSample)}
			mgr := NewManager(
				t.Context(),
				nil,
				dir,
				rules.ManagerOptions{
					Logger:                  logutil.GoKitLogToSlog(log.NewNopLogger()),
					AppendableV2:            app,
					Queryable:               nopQueryable{},
					AppendMetadata:          true,
					AutoRecordingMetadata:   true,
					EnableTypeAndUnitLabels: tc.enableTypeAndUnitLabels,
				},
				func(storepb.PartialResponseStrategy) rules.QueryFunc {
					return func(_ context.Context, q string, ts time.Time) (promql.Vector, error) {
						switch q {
						case "http_requests_total":
							return promql.Vector{{
								T:      ts.UnixMilli(),
								F:      10,
								Metric: labels.FromStrings("__name__", "http_requests_total"),
							}}, nil
						case "feature_state":
							return promql.Vector{{
								T:      ts.UnixMilli(),
								F:      1,
								Metric: labels.FromStrings("__name__", "feature_state", "__type__", "stateset", "__unit__", "seconds"),
							}}, nil
						default:
							return promql.Vector{{
								T:      ts.UnixMilli(),
								F:      2.5,
								Metric: labels.EmptyLabels(),
							}}, nil
						}
					}
				},
				labels.EmptyLabels(),
				"http://localhost",
			)
			mgr.Run()
			t.Cleanup(mgr.Stop)

			testutil.Ok(t, mgr.Update(10*time.Millisecond, []string{ruleFile}))

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			testutil.Ok(t, runutil.Retry(10*time.Millisecond, ctx.Done(), func() error {
				app.mu.Lock()
				defer app.mu.Unlock()
				if len(app.samples) < len(tc.expMeta) {
					return errors.Errorf("waiting for %d recorded samples, got %d", len(tc.expMeta), len(app.samples))
				}
				return nil
			}))

			app.mu.Lock()
			defer app.mu.Unlock()
			for name, expMeta := range tc.expMeta {
				got, ok := app.samples[name]
				testutil.Assert(t, ok, "missing recorded sample for %s", name)
				testutil.Equals(t, expMeta, got.meta)
				testutil.Equals(t, tc.expLabels[name], got.lset)
			}
		})
	}
}
