// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

// testRulesAgainstExamples tests against alerts.yaml and rules.yaml examples.
func testRulesAgainstExamples(t *testing.T, dir string, server rulespb.RulesServer) {
	t.Helper()

	// We don't test internals, just if groups are expected.
	// TODO(bwplotka): Test internals as well, especially labels!
	someAlert := &rulespb.Rule{Result: &rulespb.Rule_Alert{Alert: &rulespb.Alert{Name: "some"}}}
	someRecording := &rulespb.Rule{Result: &rulespb.Rule_Recording{Recording: &rulespb.RecordingRule{Name: "some"}}}

	expected := []*rulespb.RuleGroup{
		{
			Name:                              "thanos-bucket-replicate.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-compact.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-component-absent.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-query.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-receive.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-rule.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-sidecar.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-store.rules",
			File:                              filepath.Join(dir, "alerts.yaml"),
			Rules:                             []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-bucket-replicate.rules",
			File:                              filepath.Join(dir, "rules.yaml"),
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-query.rules",
			File:                              filepath.Join(dir, "rules.yaml"),
			Rules:                             []*rulespb.Rule{someRecording, someRecording, someRecording, someRecording, someRecording},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-receive.rules",
			File:                              filepath.Join(dir, "rules.yaml"),
			Rules:                             []*rulespb.Rule{someRecording, someRecording, someRecording, someRecording, someRecording, someRecording},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                              "thanos-store.rules",
			File:                              filepath.Join(dir, "rules.yaml"),
			Rules:                             []*rulespb.Rule{someRecording, someRecording, someRecording, someRecording},
			Interval:                          60,
			DeprecatedPartialResponseStrategy: storepb.PartialResponseStrategy_WARN, PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
	}

	for _, tcase := range []struct {
		requestedType rulespb.RulesRequest_Type
		expectedErr   error
	}{
		{
			requestedType: rulespb.RulesRequest_ALL,
		},
		{
			requestedType: rulespb.RulesRequest_ALERT,
		},
		{
			requestedType: rulespb.RulesRequest_RECORD,
		},
	} {
		t.Run(tcase.requestedType.String(), func(t *testing.T) {
			groups, w, err := NewGRPCClient(server).Rules(context.Background(), &rulespb.RulesRequest{
				Type: tcase.requestedType,
			})
			testutil.Equals(t, storage.Warnings(nil), w)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)

			expectedForType := expected
			if tcase.requestedType != rulespb.RulesRequest_ALL {
				expectedForType = make([]*rulespb.RuleGroup, len(expected))
				for i, g := range expected {
					expectedForType[i] = proto.Clone(g).(*rulespb.RuleGroup)
					expectedForType[i].Rules = nil

					for _, r := range g.Rules {
						switch tcase.requestedType {
						case rulespb.RulesRequest_ALERT:
							if r.GetAlert() != nil {
								expectedForType[i].Rules = append(expectedForType[i].Rules, someAlert)
							}
						case rulespb.RulesRequest_RECORD:
							if r.GetRecording() != nil {
								expectedForType[i].Rules = append(expectedForType[i].Rules, someRecording)
							}
						}
					}
				}
			}

			got := groups.Groups

			// We don't want to be picky, just check what number and types of rules within group are.
			for i := range got {
				for j, r := range got[i].Rules {
					if r.GetAlert() != nil {
						got[i].Rules[j] = someAlert
						continue
					}
					if r.GetRecording() != nil {
						got[i].Rules[j] = someRecording
						continue
					}
					t.Fatalf("Found rule in group %s that is neither recording not alert.", got[i].Name)
				}
				if len(got[i].Rules) == 0 {
					// Fix, for test purposes.
					got[i].Rules = nil
				}
				// Mask nondeterministic fields.
				got[i].EvaluationDurationSeconds = 0
				got[i].LastEvaluation = time.Time{}

				testutil.Equals(t, expectedForType[i], got[i])
			}
			testutil.Equals(t, expectedForType, got)
		})
	}
}
