// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
)

func TestPrometheusStore_Rules_e2e(t *testing.T) {
	t.Helper()

	defer leaktest.CheckTimeout(t, 10*time.Second)()

	p, err := e2eutil.NewPrometheus()
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, p.Stop()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	curr, err := os.Getwd()
	testutil.Ok(t, err)
	testutil.Ok(t, p.SetConfig(fmt.Sprintf(`
global:
  external_labels:
    region: eu-west

rule_files:
  - %s/../../examples/alerts/alerts.yaml
  - %s/../../examples/alerts/rules.yaml
`, curr, curr)))
	testutil.Ok(t, p.Start())

	u, err := url.Parse(fmt.Sprintf("http://%s", p.Addr()))
	testutil.Ok(t, err)

	promRules := NewPrometheus(u, promclient.NewDefaultClient())

	someAlert := &rulespb.Rule{Result: &rulespb.Rule_Alert{Alert: &rulespb.Alert{Name: "some"}}}
	someRecording := &rulespb.Rule{Result: &rulespb.Rule_Recording{Recording: &rulespb.RecordingRule{Name: "some"}}}

	for _, tcase := range []struct {
		expected    []*rulespb.RuleGroup
		expectedErr error
	}{
		{
			expected: []*rulespb.RuleGroup{
				{
					Name:                              "thanos-bucket-replicate.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-compact.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-component-absent.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-query.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-receive.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-rule.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-sidecar.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-store.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/alerts.yaml", curr),
					Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-bucket-replicate.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/rules.yaml", curr),
					Rules:                             []*rulespb.Rule{},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-query.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/rules.yaml", curr),
					Rules:                             []*rulespb.Rule{someRecording, someRecording, someRecording, someRecording, someRecording},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name: "thanos-receive.rules", File: fmt.Sprintf("%s/../../examples/alerts/rules.yaml", curr),
					Rules:                             []*rulespb.Rule{someRecording, someRecording, someRecording, someRecording, someRecording, someRecording},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
				{
					Name:                              "thanos-store.rules",
					File:                              fmt.Sprintf("%s/../../examples/alerts/rules.yaml", curr),
					Rules:                             []*rulespb.Rule{someRecording, someRecording, someRecording, someRecording},
					Interval:                          60,
					DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
				},
			},
			// TODO(bwplotka): Potentially add more cases.
		},
	} {
		t.Run("", func(t *testing.T) {
			srv := &rulesServer{ctx: ctx}
			err = promRules.Rules(&rulespb.RulesRequest{}, srv)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}

			// We don't want to be picky, just check what number and types of rules within group are.
			got := srv.groups
			for i, g := range got {
				for j, r := range g.Rules {
					if r.GetAlert() != nil {
						got[i].Rules[j] = someAlert
						continue
					}
					if r.GetRecording() != nil {
						got[i].Rules[j] = someRecording
						continue
					}
					t.Fatalf("Found rule in group %s that is neither recording not alert.", g.Name)
				}
			}

			testutil.Ok(t, err)
			testutil.Equals(t, []error(nil), srv.warnings)
			testutil.Equals(t, tcase.expected, srv.groups)
		})
	}
}
