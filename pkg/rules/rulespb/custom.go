// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rulespb

import (
	"encoding/json"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

const (
	RuleRecordingType = "recording"
	RuleAlertingType  = "alerting"
)

func NewRuleGroupRulesResponse(rg *RuleGroup) *RulesResponse {
	return &RulesResponse{
		Result: &RulesResponse_Group{
			Group: rg,
		},
	}
}

func NewWarningRulesResponse(warning error) *RulesResponse {
	return &RulesResponse{
		Result: &RulesResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

func NewRecordingRule(r *RecordingRule) *Rule {
	return &Rule{
		Result: &Rule_Recording{Recording: r},
	}
}

// Compare compares equal recording rules r1 and r2 and returns:
//
//	< 0 if r1 < r2  if rule r1 is lexically before rule r2
//	  0 if r1 == r2
//	> 0 if r1 > r2  if rule r1 is lexically after rule r2
//
// More formally, the ordering is determined in the following order:
//
// 1. recording rule last evaluation (earlier evaluation comes first)
//
// Note: This method assumes r1 and r2 are logically equal as per Rule#Compare.
func (r1 *RecordingRule) Compare(r2 *RecordingRule) int {
	r1Time := r1.GetLastEvaluation().AsTime()
	r2Time := r2.GetLastEvaluation().AsTime()

	if r1Time.Before(r2Time) {
		return 1
	}

	if r1Time.After(r2Time) {
		return -1
	}

	return 0
}

func NewAlertingRule(a *Alert) *Rule {
	return &Rule{
		Result: &Rule_Alert{Alert: a},
	}
}

func (r *Rule) GetLabelsPromLabels() labels.Labels {
	switch {
	case r.GetRecording() != nil:
		return labelpb.LabelSetToPromLabels(r.GetRecording().GetLabels())
	case r.GetAlert() != nil:
		return labelpb.LabelSetToPromLabels(r.GetAlert().GetLabels())
	default:
		return labels.EmptyLabels()
	}
}

func (r *Rule) SetLabels(ls labels.Labels) {
	labelSet := labelpb.PromLabelsToLabelSet(ls)

	switch {
	case r.GetRecording() != nil:
		r.GetRecording().Labels = labelSet
	case r.GetAlert() != nil:
		r.GetAlert().Labels = labelSet
	}
}

func (r *Rule) GetRuleName() string {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().Name
	case r.GetAlert() != nil:
		return r.GetAlert().Name
	default:
		return ""
	}
}

func (r *Rule) GetRuleQuery() string {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().Query
	case r.GetAlert() != nil:
		return r.GetAlert().Query
	default:
		return ""
	}
}

func (r *Rule) GetRuleLastEvaluation() time.Time {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().GetLastEvaluation().AsTime()
	case r.GetAlert() != nil:
		return r.GetAlert().GetLastEvaluation().AsTime()
	default:
		return time.Time{}
	}
}

// Compare compares recording and alerting rules r1 and r2 and returns:
//
//	< 0 if r1 < r2  if rule r1 is not equal and lexically before rule r2
//	  0 if r1 == r2 if rule r1 is logically equal to r2 (r1 and r2 are the "same" rules)
//	> 0 if r1 > r2  if rule r1 is not equal and lexically after rule r2
//
// More formally, ordering and equality is determined in the following order:
//
// 1. rule type (alerting rules come before recording rules)
// 2. rule name
// 3. rule labels
// 4. rule query
// 5. for alerting rules: duration
//
// Note: this can still leave ordering undetermined for equal rules (x == y).
// For determining ordering of equal rules, use Alert#Compare or RecordingRule#Compare.
func (r1 *Rule) Compare(r2 *Rule) int {
	if r1.GetAlert() != nil && r2.GetRecording() != nil {
		return -1
	}

	if r1.GetRecording() != nil && r2.GetAlert() != nil {
		return 1
	}

	if d := strings.Compare(r1.GetRuleName(), r2.GetRuleName()); d != 0 {
		return d
	}

	if d := labels.Compare(r1.GetLabelsPromLabels(), r2.GetLabelsPromLabels()); d != 0 {
		return d
	}

	if d := strings.Compare(r1.GetRuleQuery(), r2.GetRuleQuery()); d != 0 {
		return d
	}

	if r1.GetAlert() != nil && r2.GetAlert() != nil {
		if d := big.NewFloat(r1.GetAlert().DurationSeconds).Cmp(big.NewFloat(r2.GetAlert().DurationSeconds)); d != 0 {
			return d
		}
	}

	return 0
}

func (r *RuleGroups) MarshalJSON() ([]byte, error) {
	if r.Groups == nil {
		// Ensure that empty slices are marshaled as '[]' and not 'null'.
		return []byte(`{"groups":[]}`), nil
	}
	type plain RuleGroups
	return json.Marshal((*plain)(r))
}

// Compare compares rule group x and y and returns:
//
//	< 0 if x < y   if rule group r1 is not equal and lexically before rule group r2
//	  0 if x == y  if rule group r1 is logically equal to r2 (r1 and r2 are the "same" rule groups)
//	> 0 if x > y   if rule group r1 is not equal and lexically after rule group r2
func (r1 *RuleGroup) Compare(r2 *RuleGroup) int {
	return strings.Compare(r1.Key(), r2.Key())
}

// Key returns the group key similar resembling Prometheus logic.
// See https://github.com/prometheus/prometheus/blob/869f1bc587e667b79721852d5badd9f70a39fc3f/rules/manager.go#L1062-L1065
func (r *RuleGroup) Key() string {
	if r == nil {
		return ""
	}

	return r.File + ";" + r.Name
}

func (m *Rule) UnmarshalJSON(entry []byte) error {
	decider := struct {
		Type string `json:"type"`
	}{}
	if err := json.Unmarshal(entry, &decider); err != nil {
		return errors.Wrapf(err, "rule: type field unmarshal: %v", string(entry))
	}

	switch strings.ToLower(decider.Type) {
	case "recording":
		r := &RecordingRule{}
		if err := json.Unmarshal(entry, r); err != nil {
			return errors.Wrapf(err, "rule: recording rule unmarshal: %v", string(entry))
		}

		m.Result = &Rule_Recording{Recording: r}
	case "alerting":
		r := &Alert{}
		if err := json.Unmarshal(entry, r); err != nil {
			return errors.Wrapf(err, "rule: alerting rule unmarshal: %v", string(entry))
		}

		m.Result = &Rule_Alert{Alert: r}
	case "":
		return errors.Errorf("rule: no type field provided: %v", string(entry))
	default:
		return errors.Errorf("rule: unknown type field provided %s; %v", decider.Type, string(entry))
	}
	return nil
}

func (m *Rule) MarshalJSON() ([]byte, error) {
	if r := m.GetRecording(); r != nil {
		return json.Marshal(r)
	}
	return json.Marshal(m.GetAlert())
}

func (r *RuleGroup) MarshalJSON() ([]byte, error) {
	rules := r.Rules
	if rules == nil {
		// Ensure that empty slices are marshaled as '[]' and not 'null'.
		rules = make([]*Rule, 0)
	}
	var lastEval time.Time
	if r.LastEvaluation != nil {
		lastEval = r.LastEvaluation.AsTime()
	}
	return json.Marshal(struct {
		Name                    string    `json:"name"`
		File                    string    `json:"file"`
		Rules                   []*Rule   `json:"rules"`
		Interval                float64   `json:"interval"`
		EvaluationTime          float64   `json:"evaluationTime"`
		LastEvaluation          time.Time `json:"lastEvaluation"`
		Limit                   int64     `json:"limit"`
		PartialResponseStrategy string    `json:"partialResponseStrategy"`
	}{
		Name:                    r.Name,
		File:                    r.File,
		Rules:                   rules,
		Interval:                r.Interval,
		EvaluationTime:          r.EvaluationDurationSeconds,
		LastEvaluation:          lastEval,
		Limit:                   r.Limit,
		PartialResponseStrategy: r.PartialResponseStrategy.String(),
	})
}

func (r *RuleGroup) UnmarshalJSON(data []byte) error {
	var v struct {
		Name                    string    `json:"name"`
		File                    string    `json:"file"`
		Rules                   []*Rule   `json:"rules"`
		Interval                float64   `json:"interval"`
		EvaluationTime          float64   `json:"evaluationTime"`
		LastEvaluation          time.Time `json:"lastEvaluation"`
		Limit                   int64     `json:"limit"`
		PartialResponseStrategy string    `json:"partialResponseStrategy"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	r.Name = v.Name
	r.File = v.File
	r.Rules = v.Rules
	r.Interval = v.Interval
	r.EvaluationDurationSeconds = v.EvaluationTime
	if !v.LastEvaluation.IsZero() {
		r.LastEvaluation = timestamppb.New(v.LastEvaluation)
	}
	r.Limit = v.Limit
	if err := r.PartialResponseStrategy.UnmarshalJSON([]byte(strconv.Quote(v.PartialResponseStrategy))); err != nil {
		return err
	}
	return nil
}

func (x *AlertState) UnmarshalJSON(entry []byte) error {
	fieldStr, err := strconv.Unquote(string(entry))
	if err != nil {
		return errors.Wrapf(err, "alertState: unquote %v", string(entry))
	}

	if fieldStr == "" {
		return errors.New("empty alertState")
	}

	state, ok := AlertState_value[strings.ToUpper(fieldStr)]
	if !ok {
		return errors.Errorf("unknown alertState: %v", string(entry))
	}
	*x = AlertState(state)
	return nil
}

func (x *AlertState) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(strings.ToLower(x.String()))), nil
}

// labelSetToMap converts a LabelSet to a map for JSON serialization in Prometheus API format.
func labelSetToMap(ls *labelpb.LabelSet) map[string]string {
	if ls == nil {
		return map[string]string{}
	}
	m := make(map[string]string, len(ls.Labels))
	for _, l := range ls.Labels {
		m[l.Name] = l.Value
	}
	return m
}

// mapToLabelSet converts a map from JSON to a LabelSet.
// Labels are sorted by name to ensure deterministic ordering.
func mapToLabelSet(m map[string]string) *labelpb.LabelSet {
	if len(m) == 0 {
		return &labelpb.LabelSet{}
	}
	lbls := make([]*labelpb.Label, 0, len(m))
	for k, v := range m {
		lbls = append(lbls, &labelpb.Label{Name: k, Value: v})
	}
	// Sort labels by name to ensure deterministic ordering
	sort.Slice(lbls, func(i, j int) bool {
		return lbls[i].Name < lbls[j].Name
	})
	return &labelpb.LabelSet{Labels: lbls}
}

// RecordingRule JSON marshaling for Prometheus API compatibility.
func (r *RecordingRule) MarshalJSON() ([]byte, error) {
	var lastEval time.Time
	if r.LastEvaluation != nil {
		lastEval = r.LastEvaluation.AsTime()
	}
	return json.Marshal(struct {
		Name           string            `json:"name"`
		Query          string            `json:"query"`
		Labels         map[string]string `json:"labels"`
		Health         string            `json:"health"`
		LastError      string            `json:"lastError,omitempty"`
		EvaluationTime float64           `json:"evaluationTime"`
		LastEvaluation time.Time         `json:"lastEvaluation"`
		Type           string            `json:"type"`
	}{
		Name:           r.Name,
		Query:          r.Query,
		Labels:         labelSetToMap(r.Labels),
		Health:         r.Health,
		LastError:      r.LastError,
		EvaluationTime: r.EvaluationDurationSeconds,
		LastEvaluation: lastEval,
		Type:           RuleRecordingType,
	})
}

func (r *RecordingRule) UnmarshalJSON(data []byte) error {
	var v struct {
		Name           string            `json:"name"`
		Query          string            `json:"query"`
		Labels         map[string]string `json:"labels"`
		Health         string            `json:"health"`
		LastError      string            `json:"lastError,omitempty"`
		EvaluationTime float64           `json:"evaluationTime"`
		LastEvaluation time.Time         `json:"lastEvaluation"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	r.Name = v.Name
	r.Query = v.Query
	r.Labels = mapToLabelSet(v.Labels)
	r.Health = v.Health
	r.LastError = v.LastError
	r.EvaluationDurationSeconds = v.EvaluationTime
	if !v.LastEvaluation.IsZero() {
		r.LastEvaluation = timestamppb.New(v.LastEvaluation)
	}
	return nil
}

// Alert JSON marshaling for Prometheus API compatibility.
func (a *Alert) MarshalJSON() ([]byte, error) {
	alerts := a.Alerts
	if alerts == nil {
		alerts = make([]*AlertInstance, 0)
	}
	var lastEval time.Time
	if a.LastEvaluation != nil {
		lastEval = a.LastEvaluation.AsTime()
	}
	return json.Marshal(struct {
		State          string            `json:"state"`
		Name           string            `json:"name"`
		Query          string            `json:"query"`
		Duration       float64           `json:"duration"`
		Labels         map[string]string `json:"labels"`
		Annotations    map[string]string `json:"annotations"`
		Alerts         []*AlertInstance  `json:"alerts"`
		Health         string            `json:"health"`
		LastError      string            `json:"lastError,omitempty"`
		EvaluationTime float64           `json:"evaluationTime"`
		LastEvaluation time.Time         `json:"lastEvaluation"`
		KeepFiringFor  float64           `json:"keepFiringFor"`
		Type           string            `json:"type"`
	}{
		State:          strings.ToLower(a.State.String()),
		Name:           a.Name,
		Query:          a.Query,
		Duration:       a.DurationSeconds,
		Labels:         labelSetToMap(a.Labels),
		Annotations:    labelSetToMap(a.Annotations),
		Alerts:         alerts,
		Health:         a.Health,
		LastError:      a.LastError,
		EvaluationTime: a.EvaluationDurationSeconds,
		LastEvaluation: lastEval,
		KeepFiringFor:  a.KeepFiringForSeconds,
		Type:           RuleAlertingType,
	})
}

func (a *Alert) UnmarshalJSON(data []byte) error {
	var v struct {
		State          AlertState        `json:"state"`
		Name           string            `json:"name"`
		Query          string            `json:"query"`
		Duration       float64           `json:"duration"`
		Labels         map[string]string `json:"labels"`
		Annotations    map[string]string `json:"annotations"`
		Alerts         []*AlertInstance  `json:"alerts"`
		Health         string            `json:"health"`
		LastError      string            `json:"lastError,omitempty"`
		EvaluationTime float64           `json:"evaluationTime"`
		LastEvaluation time.Time         `json:"lastEvaluation"`
		KeepFiringFor  float64           `json:"keepFiringFor"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	a.State = v.State
	a.Name = v.Name
	a.Query = v.Query
	a.DurationSeconds = v.Duration
	a.Labels = mapToLabelSet(v.Labels)
	a.Annotations = mapToLabelSet(v.Annotations)
	a.Alerts = v.Alerts
	a.Health = v.Health
	a.LastError = v.LastError
	a.EvaluationDurationSeconds = v.EvaluationTime
	if !v.LastEvaluation.IsZero() {
		a.LastEvaluation = timestamppb.New(v.LastEvaluation)
	}
	a.KeepFiringForSeconds = v.KeepFiringFor
	return nil
}

// AlertInstance JSON marshaling for Prometheus API compatibility.
func (a *AlertInstance) MarshalJSON() ([]byte, error) {
	var activeAt *time.Time
	if a.ActiveAt != nil {
		t := a.ActiveAt.AsTime()
		activeAt = &t
	}
	return json.Marshal(struct {
		Labels                  map[string]string `json:"labels"`
		Annotations             map[string]string `json:"annotations"`
		State                   string            `json:"state"`
		ActiveAt                *time.Time        `json:"activeAt,omitempty"`
		Value                   string            `json:"value"`
		PartialResponseStrategy string            `json:"partialResponseStrategy"`
	}{
		Labels:                  labelSetToMap(a.Labels),
		Annotations:             labelSetToMap(a.Annotations),
		State:                   strings.ToLower(a.State.String()),
		ActiveAt:                activeAt,
		Value:                   a.Value,
		PartialResponseStrategy: a.PartialResponseStrategy.String(),
	})
}

func (a *AlertInstance) UnmarshalJSON(data []byte) error {
	var v struct {
		Labels                  map[string]string `json:"labels"`
		Annotations             map[string]string `json:"annotations"`
		State                   string            `json:"state"`
		ActiveAt                *time.Time        `json:"activeAt,omitempty"`
		Value                   string            `json:"value"`
		PartialResponseStrategy string            `json:"partialResponseStrategy"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if len(v.Labels) > 0 {
		a.Labels = mapToLabelSet(v.Labels)
	}
	if len(v.Annotations) > 0 {
		a.Annotations = mapToLabelSet(v.Annotations)
	}
	if v.State != "" {
		state, ok := AlertState_value[strings.ToUpper(v.State)]
		if !ok {
			return errors.Errorf("unknown alert state: %s", v.State)
		}
		a.State = AlertState(state)
	}
	if v.ActiveAt != nil {
		a.ActiveAt = timestamppb.New(*v.ActiveAt)
	}
	a.Value = v.Value
	if err := a.PartialResponseStrategy.UnmarshalJSON([]byte(strconv.Quote(v.PartialResponseStrategy))); err != nil {
		return err
	}
	return nil
}

// Compare compares alert state x and y and returns:
//
//	< 0 if x < y  (alert state x is more critical than alert state y)
//	  0 if x == y
//	> 0 if x > y  (alert state x is less critical than alert state y)
//
// For sorting this makes sure that more "critical" alert states come first.
func (x AlertState) Compare(y AlertState) int {
	return int(y) - int(x)
}

// Compare compares two equal alerting rules a1 and a2 and returns:
//
//	< 0 if a1 < a2  if rule a1 is lexically before rule a2
//	  0 if a1 == a2
//	> 0 if a1 > a2  if rule a1 is lexically after rule a2
//
// More formally, the ordering is determined in the following order:
//
// 1. alert state
// 2. alert last evaluation (earlier evaluation comes first)
//
// Note: This method assumes a1 and a2 are logically equal as per Rule#Compare.
func (a1 *Alert) Compare(a2 *Alert) int {
	if d := a1.State.Compare(a2.State); d != 0 {
		return d
	}

	a1Time := a1.GetLastEvaluation().AsTime()
	a2Time := a2.GetLastEvaluation().AsTime()

	if a1Time.Before(a2Time) {
		return 1
	}

	if a1Time.After(a2Time) {
		return -1
	}

	return 0
}
