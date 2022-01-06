// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rulespb

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/testpromcompatibility"
)

func TestJSONUnmarshalMarshal(t *testing.T) {
	now := time.Now()
	twoHoursAgo := now.Add(2 * time.Hour)

	for _, tcase := range []struct {
		name  string
		input *testpromcompatibility.RuleDiscovery

		expectedProto      *RuleGroups
		expectedErr        error
		expectedJSONOutput string // If empty, expected same one as marshaled input.
	}{
		{
			name:               "Empty JSON",
			input:              &testpromcompatibility.RuleDiscovery{},
			expectedProto:      &RuleGroups{},
			expectedJSONOutput: `{"groups":[]}`,
		},
		{
			name: "one empty group",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name:                    "group1",
						File:                    "file1.yml",
						Interval:                2442,
						LastEvaluation:          now,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "ABORT",
					},
				},
			},
			expectedProto: &RuleGroups{
				Groups: []*RuleGroup{
					{
						Name:                      "group1",
						File:                      "file1.yml",
						Interval:                  2442,
						LastEvaluation:            now,
						EvaluationDurationSeconds: 2.1,
						Limit:                     0,
						PartialResponseStrategy:   storepb.PartialResponseStrategy_ABORT,
						Rules:                     []*Rule{},
					},
				},
			},
		},
		{
			name: "one group with one empty group",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{},
				},
			},
			expectedProto: &RuleGroups{
				Groups: []*RuleGroup{
					{
						PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
					},
				},
			},
			// Different than input due to default enum fields.
			expectedJSONOutput: `{"groups":[{"name":"","file":"","rules":[],"interval":0,"evaluationTime":0,"lastEvaluation":"0001-01-01T00:00:00Z","limit":0,"partialResponseStrategy":"ABORT"}]}`,
		},
		{
			name: "one valid group, with 1 with no rule type",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.RecordingRule{
								Name: "recording1",
							},
						},
						File:                    "file1.yml",
						Interval:                2442,
						LastEvaluation:          now,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "ABORT",
					},
				},
			},
			expectedErr: errors.New("rule: no type field provided: {\"name\":\"recording1\",\"query\":\"\",\"labels\":{},\"health\":\"\",\"evaluationTime\":0,\"lastEvaluation\":\"0001-01-01T00:00:00Z\",\"type\":\"\"}"),
		},
		{
			name: "one valid group, with 1 rule with invalid rule type",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.RecordingRule{
								Name: "recording1",
								Type: "wrong",
							},
						},
						File:                    "file1.yml",
						Interval:                2442,
						LastEvaluation:          now,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "ABORT",
					},
				},
			},
			expectedErr: errors.New("rule: unknown type field provided wrong; {\"name\":\"recording1\",\"query\":\"\",\"labels\":{},\"health\":\"\",\"evaluationTime\":0,\"lastEvaluation\":\"0001-01-01T00:00:00Z\",\"type\":\"wrong\"}"),
		},
		{
			name: "one valid group, with 1 rule with invalid alert state",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.AlertingRule{
								Name:  "alert1",
								Type:  RuleAlertingType,
								State: "sdfsdf",
							},
						},
						File:                    "file1.yml",
						Interval:                2442,
						LastEvaluation:          now,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "ABORT",
					},
				},
			},
			expectedErr: errors.New("rule: alerting rule unmarshal: {\"state\":\"sdfsdf\",\"name\":\"alert1\",\"query\":\"\",\"duration\":0,\"labels\":{},\"annotations\":{},\"alerts\":null,\"health\":\"\",\"evaluationTime\":0,\"lastEvaluation\":\"0001-01-01T00:00:00Z\",\"type\":\"alerting\"}: unknown alertState: \"sdfsdf\""),
		},
		{
			name: "one group with WRONG partial response fields",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name:                    "group1",
						File:                    "file1.yml",
						Interval:                2442,
						LastEvaluation:          now,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "asdfsdfsdfsd",
					},
				},
			},
			expectedErr: errors.New("failed to unmarshal \"asdfsdfsdfsd\" as 'partial_response_strategy'. Possible values are ABORT,WARN"),
		},
		{
			name: "one valid group with 1 alerting rule containing no alerts.",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.AlertingRule{
								Type:  RuleAlertingType,
								Name:  "alert1",
								Query: "up == 0",
								Labels: labels.Labels{
									{Name: "a2", Value: "b2"},
									{Name: "c2", Value: "d2"},
								},
								Annotations: labels.Labels{
									{Name: "ann1", Value: "ann44"},
									{Name: "ann2", Value: "ann33"},
								},
								Health:         "health2",
								LastError:      "1",
								Duration:       60,
								State:          "pending",
								EvaluationTime: 1.1,
							},
						},
						File:                    "file1.yml",
						Interval:                2442,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "ABORT",
					},
				},
			},
			expectedProto: &RuleGroups{
				Groups: []*RuleGroup{
					{
						Name: "group1",
						Rules: []*Rule{
							NewAlertingRule(&Alert{
								Name:  "alert1",
								Query: "up == 0",
								Labels: labelpb.ZLabelSet{
									Labels: []labelpb.ZLabel{
										{Name: "a2", Value: "b2"},
										{Name: "c2", Value: "d2"},
									},
								},
								Annotations: labelpb.ZLabelSet{
									Labels: []labelpb.ZLabel{
										{Name: "ann1", Value: "ann44"},
										{Name: "ann2", Value: "ann33"},
									},
								},
								DurationSeconds:           60,
								State:                     AlertState_PENDING,
								LastError:                 "1",
								Health:                    "health2",
								EvaluationDurationSeconds: 1.1,
							}),
						},
						File:                      "file1.yml",
						Interval:                  2442,
						EvaluationDurationSeconds: 2.1,
						PartialResponseStrategy:   storepb.PartialResponseStrategy_ABORT,
					},
				},
			},
			// Different than input due to the alerts slice being initialized to a zero-length slice instead of nil.
			expectedJSONOutput: `{"groups":[{"name":"group1","file":"file1.yml","rules":[{"state":"pending","name":"alert1","query":"up == 0","duration":60,"labels":{"a2":"b2","c2":"d2"},"annotations":{"ann1":"ann44","ann2":"ann33"},"alerts":[],"health":"health2","lastError":"1","evaluationTime":1.1,"lastEvaluation":"0001-01-01T00:00:00Z","type":"alerting"}],"interval":2442,"evaluationTime":2.1,"lastEvaluation":"0001-01-01T00:00:00Z","limit":0,"partialResponseStrategy":"ABORT"}]}`,
		},
		{
			name: "one valid group, with 1 rule and alert each and second empty group.",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.RecordingRule{
								Type:  RuleRecordingType,
								Query: "up",
								Name:  "recording1",
								Labels: labels.Labels{
									{Name: "a", Value: "b"},
									{Name: "c", Value: "d"},
									{Name: "a", Value: "b"}, // Kind of invalid, but random one will be chosen.
								},
								LastError:      "2",
								Health:         "health",
								LastEvaluation: now.Add(-2 * time.Minute),
								EvaluationTime: 2.6,
							},
							testpromcompatibility.AlertingRule{
								Type:  RuleAlertingType,
								Name:  "alert1",
								Query: "up == 0",
								Labels: labels.Labels{
									{Name: "a2", Value: "b2"},
									{Name: "c2", Value: "d2"},
								},
								Annotations: labels.Labels{
									{Name: "ann1", Value: "ann44"},
									{Name: "ann2", Value: "ann33"},
								},
								Health: "health2",
								Alerts: []*testpromcompatibility.Alert{
									{
										Labels: labels.Labels{
											{Name: "instance1", Value: "1"},
										},
										Annotations: labels.Labels{
											{Name: "annotation1", Value: "2"},
										},
										State:                   "inactive",
										ActiveAt:                nil,
										Value:                   "1",
										PartialResponseStrategy: "WARN",
									},
									{
										Labels:                  nil,
										Annotations:             nil,
										State:                   "firing",
										ActiveAt:                &twoHoursAgo,
										Value:                   "2143",
										PartialResponseStrategy: "ABORT",
									},
								},
								LastError:      "1",
								Duration:       60,
								State:          "pending",
								LastEvaluation: now.Add(-1 * time.Minute),
								EvaluationTime: 1.1,
							},
						},
						File:                    "file1.yml",
						Interval:                2442,
						LastEvaluation:          now,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "ABORT",
					},
					{
						Name:                    "group2",
						File:                    "file2.yml",
						Interval:                242342442,
						LastEvaluation:          now.Add(40 * time.Hour),
						EvaluationTime:          21244.1,
						PartialResponseStrategy: "ABORT",
					},
				},
			},
			expectedProto: &RuleGroups{
				Groups: []*RuleGroup{
					{
						Name: "group1",
						Rules: []*Rule{
							NewRecordingRule(&RecordingRule{
								Query: "up",
								Name:  "recording1",
								Labels: labelpb.ZLabelSet{
									Labels: []labelpb.ZLabel{
										{Name: "a", Value: "b"},
										{Name: "c", Value: "d"},
									},
								},
								LastError:                 "2",
								Health:                    "health",
								LastEvaluation:            now.Add(-2 * time.Minute),
								EvaluationDurationSeconds: 2.6,
							}),
							NewAlertingRule(&Alert{
								Name:  "alert1",
								Query: "up == 0",
								Labels: labelpb.ZLabelSet{
									Labels: []labelpb.ZLabel{
										{Name: "a2", Value: "b2"},
										{Name: "c2", Value: "d2"},
									},
								},
								Annotations: labelpb.ZLabelSet{
									Labels: []labelpb.ZLabel{
										{Name: "ann1", Value: "ann44"},
										{Name: "ann2", Value: "ann33"},
									},
								},
								Alerts: []*AlertInstance{
									{
										Labels: labelpb.ZLabelSet{
											Labels: []labelpb.ZLabel{
												{Name: "instance1", Value: "1"},
											},
										},
										Annotations: labelpb.ZLabelSet{
											Labels: []labelpb.ZLabel{
												{Name: "annotation1", Value: "2"},
											},
										},
										State:                   AlertState_INACTIVE,
										ActiveAt:                nil,
										Value:                   "1",
										PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
									},
									{
										State:                   AlertState_FIRING,
										ActiveAt:                &twoHoursAgo,
										Value:                   "2143",
										PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
									},
								},
								DurationSeconds:           60,
								State:                     AlertState_PENDING,
								LastError:                 "1",
								Health:                    "health2",
								LastEvaluation:            now.Add(-1 * time.Minute),
								EvaluationDurationSeconds: 1.1,
							}),
						},
						File:                      "file1.yml",
						Interval:                  2442,
						LastEvaluation:            now,
						EvaluationDurationSeconds: 2.1,
						PartialResponseStrategy:   storepb.PartialResponseStrategy_ABORT,
					},
					{
						Name:                      "group2",
						File:                      "file2.yml",
						Interval:                  242342442,
						LastEvaluation:            now.Add(40 * time.Hour),
						EvaluationDurationSeconds: 21244.1,
						PartialResponseStrategy:   storepb.PartialResponseStrategy_ABORT,
						Rules:                     []*Rule{},
					},
				},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			jsonInput, err := json.Marshal(tcase.input)
			testutil.Ok(t, err)

			proto := &RuleGroups{}
			err = json.Unmarshal(jsonInput, proto)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)
			fmt.Println(proto.String())
			testutil.Equals(t, tcase.expectedProto.String(), proto.String())

			jsonProto, err := json.Marshal(proto)
			testutil.Ok(t, err)
			if tcase.expectedJSONOutput != "" {
				testutil.Equals(t, tcase.expectedJSONOutput, string(jsonProto))
				return
			}
			testutil.Equals(t, string(jsonInput), string(jsonProto))
		})
	}
}

func TestRulesComparator(t *testing.T) {
	for _, tc := range []struct {
		name   string
		r1, r2 *Rule
		want   int
	}{
		{
			name: "same recording rule",
			r1:   NewRecordingRule(&RecordingRule{Name: "a"}),
			r2:   NewRecordingRule(&RecordingRule{Name: "a"}),
			want: 0,
		},
		{
			name: "same alerting rule",
			r1:   NewAlertingRule(&Alert{Name: "a"}),
			r2:   NewAlertingRule(&Alert{Name: "a"}),
			want: 0,
		},
		{
			name: "different types",
			r1:   NewAlertingRule(&Alert{Name: "a"}),
			r2:   NewRecordingRule(&RecordingRule{Name: "a"}),
			want: -1,
		},
		{
			name: "different names",
			r1:   NewAlertingRule(&Alert{Name: "a"}),
			r2:   NewAlertingRule(&Alert{Name: "b"}),
			want: -1,
		},
		{
			name: "no label before label",
			r1:   NewAlertingRule(&Alert{Name: "a"}),
			r2: NewAlertingRule(&Alert{
				Name: "a",
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
				}}}),
			want: -1,
		},
		{
			name: "label ordering",
			r1: NewAlertingRule(&Alert{
				Name: "a",
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
				}}}),
			r2: NewAlertingRule(&Alert{
				Name: "a",
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "2"},
				}}}),
			want: -1,
		},
		{
			name: "multiple label ordering",
			r1: NewAlertingRule(&Alert{
				Name: "a",
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
				}}}),
			r2: NewAlertingRule(&Alert{
				Name: "a",
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
					{Name: "b", Value: "1"},
				}}}),
			want: -1,
		},
		{
			name: "different durations",
			r1: NewAlertingRule(&Alert{
				Name:            "a",
				DurationSeconds: 0.0,
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
				}}}),
			r2: NewAlertingRule(&Alert{
				Name:            "a",
				DurationSeconds: 1.0,
				Labels: labelpb.ZLabelSet{Labels: []labelpb.ZLabel{
					{Name: "a", Value: "1"},
				}}}),
			want: -1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testutil.Equals(t, tc.want, tc.r1.Compare(tc.r2))
		})
	}
}
