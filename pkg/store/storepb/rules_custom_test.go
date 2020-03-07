// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/rules"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type prometheusAlert struct {
	Labels      labels.Labels `json:"labels"`
	Annotations labels.Labels `json:"annotations"`
	State       string        `json:"state"`
	ActiveAt    *time.Time    `json:"activeAt,omitempty"`
	Value       string        `json:"value"`
}

// Same as https://github.com/prometheus/prometheus/blob/c530b4b456cc5f9ec249f771dff187eb7715dc9b/web/api/v1/api.go#L955
// but with Partial Response.
type prometheusRuleGroup struct {
	Name           string           `json:"name"`
	File           string           `json:"file"`
	Rules          []prometheusRule `json:"rules"`
	Interval       float64          `json:"interval"`
	EvaluationTime float64          `json:"evaluationTime"`
	LastEvaluation time.Time        `json:"lastEvaluation"`

	DeprecatedPartialResponseStrategy string `json:"partial_response_strategy"`
	PartialResponseStrategy           string `json:"PartialResponseStrategy"`
}

// prometheusRuleDiscovery has info for all rules
type prometheusRuleDiscovery struct {
	RuleGroups []*prometheusRuleGroup `json:"groups"`
}

type prometheusRule interface{}

type prometheusAlertingRule struct {
	// State can be "pending", "firing", "inactive".
	State          string             `json:"state"`
	Name           string             `json:"name"`
	Query          string             `json:"query"`
	Duration       float64            `json:"duration"`
	Labels         labels.Labels      `json:"labels"`
	Annotations    labels.Labels      `json:"annotations"`
	Alerts         []*prometheusAlert `json:"alerts"`
	Health         rules.RuleHealth   `json:"health"`
	LastError      string             `json:"lastError,omitempty"`
	EvaluationTime float64            `json:"evaluationTime"`
	LastEvaluation time.Time          `json:"lastEvaluation"`
	// Type of an prometheusAlertingRule is always "alerting".
	Type string `json:"type"`
}

type prometheusRecordingRule struct {
	Name           string           `json:"name"`
	Query          string           `json:"query"`
	Labels         labels.Labels    `json:"labels,omitempty"`
	Health         rules.RuleHealth `json:"health"`
	LastError      string           `json:"lastError,omitempty"`
	EvaluationTime float64          `json:"evaluationTime"`
	LastEvaluation time.Time        `json:"lastEvaluation"`
	// Type of a prometheusRecordingRule is always "recording".
	Type string `json:"type"`
}

func TestJSONUnmarshalMarshal(t *testing.T) {
	for _, tcase := range []struct {
		name  string
		input *prometheusRuleDiscovery

		expectedProto *RuleGroups
	}{
		{
			name:          "Empty JSON",
			input:         &prometheusRuleDiscovery{},
			expectedProto: &RuleGroups{},
		},
		{
			name: "one empty group",
			input: &prometheusRuleDiscovery{
				RuleGroups: []*prometheusRuleGroup{
					{
						Name:                              "group1",
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    time.Now(),
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "warn",
						PartialResponseStrategy:           "abort",
					},
				},
			},
			expectedProto: &RuleGroups{},
		},
		{
			name: "one valid group, with 1 rule and alert each",
			input: &prometheusRuleDiscovery{
				RuleGroups: []*prometheusRuleGroup{
					{
						Name: "group1",
						Rules: []prometheusRule{
							prometheusRecordingRule{
								Name: "recording1",
							},
							prometheusAlertingRule{
								Name: "alert1",
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    time.Now(),
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "warn",
						PartialResponseStrategy:           "abort",
					},
				},
			},
			expectedProto: &RuleGroups{},
		},
		{
			name: "one group, with 1 rule and alert each WRONG partial response fields",
			input: &prometheusRuleDiscovery{
				RuleGroups: []*prometheusRuleGroup{
					{
						Name: "group1",
						Rules: []prometheusRule{
							prometheusRecordingRule{
								Name: "recording1",
							},
							prometheusAlertingRule{
								Name: "alert1",
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    time.Now(),
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "warasdasdasdn",
						PartialResponseStrategy:           "aboadasdasdart",
					},
				},
			},
			expectedProto: &RuleGroups{},
		},
		{
			name: "two valid groups, with 1 rule and alert each",
			input: &prometheusRuleDiscovery{
				RuleGroups: []*prometheusRuleGroup{
					{
						Name: "group1",
						Rules: []prometheusRule{
							prometheusRecordingRule{
								Name: "recording1",
							},
							prometheusAlertingRule{
								Name: "alert1",
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    time.Now(),
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "warn",
						PartialResponseStrategy:           "abort",
					},
					{
						Name: "group2",
						Rules: []prometheusRule{
							prometheusRecordingRule{
								Name: "recording1",
							},
							prometheusAlertingRule{
								Name: "alert1",
							},
						},
						File:                              "file2.yml",
						Interval:                          2412,
						LastEvaluation:                    time.Now(),
						EvaluationTime:                    4.2,
						DeprecatedPartialResponseStrategy: "abort",
						PartialResponseStrategy:           "warn",
					},
				},
			},
			expectedProto: &RuleGroups{},
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			jsonInput, err := json.Marshal(tcase.input)
			testutil.Ok(t, err)

			proto := &RuleGroups{}
			testutil.Ok(t, json.Unmarshal(jsonInput, proto))
			testutil.Equals(t, tcase.expectedProto, proto)

			jsonProto, err := json.Marshal(proto)
			testutil.Ok(t, err)
			fmt.Println(string(jsonInput), string(jsonProto))
			testutil.Equals(t, jsonInput, jsonProto)
		}); !ok {
			return
		}
	}
}
