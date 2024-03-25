// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil/custom"
)

func TestMain(m *testing.M) {
	custom.TolerantVerifyLeakMain(m)
}

// testRulesAgainstExamples tests against alerts.yaml and rules.yaml examples.
func testRulesAgainstExamples(t *testing.T, dir string, server rulespb.RulesServer) {
	t.Helper()

	// We don't test internals, just if groups are expected.
	// TODO(bwplotka): Test internals as well, especially labels!
	someAlert := &rulespb.Rule{Result: &rulespb.Rule_Alert{Alert: &rulespb.Alert{Name: "some"}}}
	someRecording := &rulespb.Rule{Result: &rulespb.Rule_Recording{Recording: &rulespb.RecordingRule{Name: "some"}}}

	expected := []*rulespb.RuleGroup{
		{
			Name:                    "thanos-bucket-replicate",
			File:                    filepath.Join(dir, "alerts.yaml"),
			Rules:                   []*rulespb.Rule{someAlert, someAlert},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                    "thanos-compact",
			File:                    filepath.Join(dir, "alerts.yaml"),
			Rules:                   []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                    "thanos-component-absent",
			File:                    filepath.Join(dir, "alerts.yaml"),
			Rules:                   []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name: "thanos-query",
			File: filepath.Join(dir, "alerts.yaml"),
			Rules: []*rulespb.Rule{
				someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert,
			},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name: "thanos-receive",
			File: filepath.Join(dir, "alerts.yaml"),
			Rules: []*rulespb.Rule{
				someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert,
			},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                    "thanos-rule",
			File:                    filepath.Join(dir, "alerts.yaml"),
			Rules:                   []*rulespb.Rule{someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert, someAlert},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                    "thanos-sidecar",
			File:                    filepath.Join(dir, "alerts.yaml"),
			Rules:                   []*rulespb.Rule{someAlert, someAlert},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name: "thanos-store",
			File: filepath.Join(dir, "alerts.yaml"),
			Rules: []*rulespb.Rule{
				someAlert, someAlert, someAlert, someAlert,
			},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name:                    "thanos-bucket-replicate.rules",
			File:                    filepath.Join(dir, "rules.yaml"),
			Rules:                   nil,
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name: "thanos-query.rules",
			File: filepath.Join(dir, "rules.yaml"),
			Rules: []*rulespb.Rule{
				someRecording, someRecording, someRecording, someRecording, someRecording,
			},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name: "thanos-receive.rules",
			File: filepath.Join(dir, "rules.yaml"),
			Rules: []*rulespb.Rule{
				someRecording, someRecording, someRecording, someRecording, someRecording, someRecording, someRecording,
			},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			Name: "thanos-store.rules",
			File: filepath.Join(dir, "rules.yaml"),
			Rules: []*rulespb.Rule{
				someRecording, someRecording, someRecording, someRecording,
			},
			Interval:                60,
			PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
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
			groups, w, err := NewGRPCClientWithDedup(server, nil).Rules(context.Background(), &rulespb.RulesRequest{
				Type: tcase.requestedType,
			})
			testutil.Equals(t, annotations.Annotations(nil), w)
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

				t.Run(got[i].Name+" "+path.Base(got[i].File), func(t *testing.T) {
					testutil.Equals(t, expectedForType[i], got[i])
				})
			}
			testutil.Equals(t, expectedForType, got)
		})
	}
}

func TestDedupRules(t *testing.T) {
	for _, tc := range []struct {
		name          string
		rules, want   []*rulespb.Rule
		replicaLabels []string
	}{
		{
			name:  "nil slice",
			rules: nil,
			want:  nil,
		},
		{
			name:  "empty rule slice",
			rules: []*rulespb.Rule{},
			want:  []*rulespb.Rule{},
		},
		{
			name: "single recording rule",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
			},
			want: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
			},
		},
		{
			name: "single alert",
			rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{Name: "a1"}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{Name: "a1"}),
			},
		},
		{
			name: "rule type",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
				rulespb.NewAlertingRule(&rulespb.Alert{Name: "a1"}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{Name: "a1"}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
			},
		},
		{
			name: "rule name",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
				rulespb.NewAlertingRule(&rulespb.Alert{Name: "a1"}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
				rulespb.NewAlertingRule(&rulespb.Alert{Name: "a1"}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{Name: "a1"}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
			},
		},
		{
			name: "rule labels",
			rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
		},
		{
			name: "rule expression",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
		},
		{
			name: "alert duration",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 1.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 1.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 2.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 1.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 2.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name: "a1", Query: "up", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
		},
		{
			name: "alert duration with replicas",
			rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 1.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
						{Name: "replica", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 2.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 1.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:            "a1",
					Query:           "up",
					DurationSeconds: 2.0,
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "a", Value: "1"},
					}}}),
			},
			replicaLabels: []string{"replica"},
		},
		{
			name: "replica labels",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
					{Name: "replica", Value: "3"},
				}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
					{Name: "replica", Value: "1"},
				}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
					{Name: "replica", Value: "2"},
				}}}),
			},
			want: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
				}}}),
			},
			replicaLabels: []string{"replica"},
		},
		{
			name: "ambiguous replica labels",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "replica", Value: "1"},
					{Name: "a", Value: "1"},
				}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "replica", Value: "1"},
				}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "replica", Value: "1"},
					{Name: "a", Value: "2"},
				}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "replica", Value: "1"},
					{Name: "a", Value: "1"},
				}}}),
			},
			want: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
				}}}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "2"},
				}}}),
			},
			replicaLabels: []string{"replica"},
		},
		{
			name: "youngest recording rule",
			rules: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "2"},
					}},
					LastEvaluation: time.Unix(0, 0),
				}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "1"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:  "a1",
					Query: "up",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "3"},
					}},
					LastEvaluation: time.Unix(3, 0),
				}),
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:           "a1",
					Query:          "up",
					LastEvaluation: time.Unix(2, 0),
				}),
			},
			want: []*rulespb.Rule{
				rulespb.NewRecordingRule(&rulespb.RecordingRule{
					Name:           "a1",
					Query:          "up",
					LastEvaluation: time.Unix(3, 0),
				}),
			},
			replicaLabels: []string{"replica"},
		},
		{
			name: "youngest firing alert",
			rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "2"},
					}},
					LastEvaluation: time.Unix(4, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "2"},
						{Name: "foo", Value: "bar"},
					}},
					LastEvaluation: time.Unix(2, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name:           "a2",
					LastEvaluation: time.Unix(2, 0),
					State:          rulespb.AlertState_PENDING,
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "1"},
					}},
					LastEvaluation: time.Unix(3, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a2",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "1"},
					}},
					LastEvaluation: time.Unix(3, 0),
					State:          rulespb.AlertState_PENDING,
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "3"},
					}},
					LastEvaluation: time.Unix(2, 0),
					State:          rulespb.AlertState_FIRING,
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "foo", Value: "bar"},
					}},
					State:          rulespb.AlertState_FIRING,
					LastEvaluation: time.Unix(1, 0),
				}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					State:          rulespb.AlertState_FIRING,
					Name:           "a1",
					LastEvaluation: time.Unix(2, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					State: rulespb.AlertState_FIRING,
					Name:  "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "foo", Value: "bar"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					State:          rulespb.AlertState_PENDING,
					Name:           "a2",
					LastEvaluation: time.Unix(3, 0),
				}),
			},
			replicaLabels: []string{"replica"},
		},
		{
			name: "alerts with different severity",
			rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "1"},
						{Name: "severity", Value: "warning"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "1"},
						{Name: "severity", Value: "critical"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "2"},
						{Name: "severity", Value: "warning"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "2"},
						{Name: "severity", Value: "critical"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "critical"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "severity", Value: "warning"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
			},
			replicaLabels: []string{"replica"},
		},
		{
			name: "alerts with missing replica labels",
			rules: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "1"},
						{Name: "label", Value: "foo"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "replica", Value: "2"},
						{Name: "label", Value: "foo"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "label", Value: "foo"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
			},
			want: []*rulespb.Rule{
				rulespb.NewAlertingRule(&rulespb.Alert{
					Name: "a1",
					Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
						{Name: "label", Value: "foo"},
					}},
					LastEvaluation: time.Unix(1, 0),
				}),
			},
			replicaLabels: []string{"replica"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicaLabels := make(map[string]struct{})
			for _, lbl := range tc.replicaLabels {
				replicaLabels[lbl] = struct{}{}
			}
			testutil.Equals(t, tc.want, dedupRules(tc.rules, replicaLabels))
		})
	}
}

func TestDedupGroups(t *testing.T) {
	for _, tc := range []struct {
		name         string
		groups, want []*rulespb.RuleGroup
	}{
		{
			name:   "no groups",
			groups: nil,
			want:   nil,
		},
		{
			name: "empty group",
			groups: []*rulespb.RuleGroup{
				{Name: "a"},
			},
			want: []*rulespb.RuleGroup{
				{Name: "a"},
			},
		},
		{
			name: "multiple empty groups",
			groups: []*rulespb.RuleGroup{
				{Name: "a"},
				{Name: "b"},
			},
			want: []*rulespb.RuleGroup{
				{Name: "a"},
				{Name: "b"},
			},
		},
		{
			name: "single group",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
		},
		{
			name: "separate groups",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
			},
		},
		{
			name: "duplicate groups",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
				{
					Name: "c",
				},
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
				{
					Name: "c",
				},
			},
		},
		{
			name: "distinct file names",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					File: "foo.yaml",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
				{
					Name: "c",
				},
				{
					Name: "a",
					File: "bar.yaml",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "b2"}),
					},
				},
				{
					Name: "c",
				},
				{
					Name: "a",
					File: "bar.yaml",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
				{
					Name: "a",
					File: "foo.yaml",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a1"}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{Name: "a2"}),
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run(tc.name, func(t *testing.T) {
				testutil.Equals(t, tc.want, dedupGroups(tc.groups))
			})
		})
	}
}

func TestFilterRules(t *testing.T) {
	for _, tc := range []struct {
		name         string
		matcherSets  [][]*labels.Matcher
		groups, want []*rulespb.RuleGroup
	}{
		{
			name:   "no groups",
			groups: nil,
			want:   nil,
		},
		{
			name: "empty group with no matcher",
			groups: []*rulespb.RuleGroup{
				{Name: "a"},
			},
			want: []*rulespb.RuleGroup{
				{Name: "a"},
			},
		},
		{
			name: "multiple empty groups with no matcher",
			groups: []*rulespb.RuleGroup{
				{Name: "a"},
				{Name: "b"},
			},
			want: []*rulespb.RuleGroup{
				{Name: "a"},
				{Name: "b"},
			},
		},
		{
			name: "single group with labeled rules and no matcher",
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "replica", Value: "1"},
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r2", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "otherlabel", Value: "bar"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "replica", Value: "1"},
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r2", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "otherlabel", Value: "bar"},
							}},
						}),
					},
				},
			},
		},
		{
			name:        "single group with labeled rules and matcher",
			matcherSets: [][]*labels.Matcher{{&labels.Matcher{Name: "label", Value: "foo", Type: labels.MatchEqual}}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "replica", Value: "1"},
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r2", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "otherlabel", Value: "bar"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "replica", Value: "1"},
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
			},
		},
		{
			name:        "single group with no match for matcher",
			matcherSets: [][]*labels.Matcher{{&labels.Matcher{Name: "foo", Value: "bar", Type: labels.MatchEqual}}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "replica", Value: "1"},
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r2", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "otherlabel", Value: "bar"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{},
		},
		{
			name:        "single group with templated labels",
			matcherSets: [][]*labels.Matcher{{&labels.Matcher{Name: "templatedlabel", Value: "{{ $externalURL }}", Type: labels.MatchEqual}}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
								{Name: "templatedlabel", Value: "{{ $externalURL }}"},
							}},
						}),
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a2",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{},
		},
		{
			name:        "multiple group with labeled rules and matcher",
			matcherSets: [][]*labels.Matcher{{&labels.Matcher{Name: "label", Value: "foo", Type: labels.MatchEqual}}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1a",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1a", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "otherlabel", Value: "bar"},
							}},
						}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1b",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "some", Value: "label"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1a",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1a", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
			},
		},
		{
			name:        "multiple group with labeled rules and no match",
			matcherSets: [][]*labels.Matcher{{&labels.Matcher{Name: "foo", Value: "bar", Type: labels.MatchEqual}}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1a",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1a", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "otherlabel", Value: "bar"},
							}},
						}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1b",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "some", Value: "label"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{},
		},
		{
			name:        "multiple group with templated labels",
			matcherSets: [][]*labels.Matcher{{&labels.Matcher{Name: "templatedlabel", Value: "{{ $externalURL }}", Type: labels.MatchEqual}}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1a",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "templatedlabel", Value: "{{ $externalURL }}"},
							}},
						}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1b",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "templated", Value: "{{ $externalURL }}"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{},
		},
		{
			name:        "multiple group with templated labels and non templated matcher",
			matcherSets: [][]*labels.Matcher{{&labels.Matcher{Name: "templatedlabel", Value: "foo", Type: labels.MatchEqual}}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1a",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "templatedlabel", Value: "{{ $externalURL }}"},
							}},
						}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1b",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "templated", Value: "{{ $externalURL }}"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{},
		},
		{
			name:        "multiple group with regex matcher",
			matcherSets: [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchRegexp, "label", "f.*")}},
			groups: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1a",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1a", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "otherlabel", Value: "bar"},
							}},
						}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1b",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "some", Value: "label"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
			},
			want: []*rulespb.RuleGroup{
				{
					Name: "a",
					Rules: []*rulespb.Rule{
						rulespb.NewAlertingRule(&rulespb.Alert{
							Name: "a1a",
							Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1a", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
				{
					Name: "b",
					Rules: []*rulespb.Rule{
						rulespb.NewRecordingRule(&rulespb.RecordingRule{
							Name: "r1b", Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
								{Name: "label", Value: "foo"},
							}},
						}),
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testutil.Equals(t, tc.want, filterRules(tc.groups, tc.matcherSets))
		})
	}
}
