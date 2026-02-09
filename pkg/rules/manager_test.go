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
	"github.com/prometheus/prometheus/model/exemplar"
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

func (n nopAppendable) Appender(_ context.Context) storage.Appender { return nopAppender{} }

type nopAppender struct{}

func (n nopAppender) SetOptions(opts *storage.AppendOptions) {}

func (n nopAppender) AppendHistogramCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) AppendHistogramSTZeroSample(ref storage.SeriesRef, l labels.Labels, t, st int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) AppendSTZeroSample(ref storage.SeriesRef, l labels.Labels, t, st int64) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) Append(storage.SeriesRef, labels.Labels, int64, float64) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	return 0, nil
}

func (n nopAppender) Commit() error                                        { return nil }
func (n nopAppender) Rollback() error                                      { return nil }
func (n nopAppender) Appender(_ context.Context) (storage.Appender, error) { return n, nil }
func (n nopAppender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, nil
}

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
			Logger:     logutil.GoKitLogToSlog(log.NewLogfmtLogger(os.Stderr)),
			Context:    context.Background(),
			Appendable: nopAppendable{},
			Queryable:  nopQueryable{},
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
