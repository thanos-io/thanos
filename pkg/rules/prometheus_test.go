// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestEnrichRulesWithExtLabels(t *testing.T) {
	extLset := labels.FromStrings("cluster", "prod", "replica", "0")

	groups := []*rulespb.RuleGroup{{
		Rules: []*rulespb.Rule{
			rulespb.NewAlertingRule(&rulespb.Alert{
				Name:   "TestAlert",
				Labels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("severity", "page"))},
				Alerts: []*rulespb.AlertInstance{
					{Labels: labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings("instance", "a"))}},
				},
			}),
		},
	}}

	enrichRulesWithExtLabels(groups, extLset)

	alert := groups[0].Rules[0].GetAlert()
	// The rule definition labels are enriched with the external labels.
	testutil.Equals(t, labels.FromStrings("cluster", "prod", "replica", "0", "severity", "page"), alert.Labels.PromLabels())
	// The individual alert instances are enriched too (the bug fixed in #7327).
	testutil.Equals(t, labels.FromStrings("cluster", "prod", "instance", "a", "replica", "0"), alert.Alerts[0].Labels.PromLabels())
}

func TestPrometheus_Rules_e2e(t *testing.T) {
	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	curr, err := os.Getwd()
	testutil.Ok(t, err)
	root := filepath.Join(curr, "../../")

	p.SetConfig(fmt.Sprintf(`
global:
  external_labels:
    region: eu-west

rule_files:
  - %s/examples/alerts/alerts.yaml
  - %s/examples/alerts/rules.yaml
`, root, root))
	testutil.Ok(t, p.Start(context.Background(), log.NewNopLogger()))

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	promRules := NewPrometheus(u, promclient.NewDefaultClient(), func() labels.Labels {
		return labels.FromStrings("replica", "test1")
	})
	testRulesAgainstExamples(t, filepath.Join(root, "examples/alerts"), promRules, true)
}
