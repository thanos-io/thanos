// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rulespb

import (
	"encoding/json"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	RuleRecordingType = "recording"
	RuleAlertingType  = "alerting"
)

func TimestampToTime(ts *Timestamp) time.Time {
	if ts == nil {
		return time.Unix(0, 0).UTC() // treat nil like the empty Timestamp
	} else {
		return time.Unix(ts.Seconds, int64(ts.Nanos)).UTC()
	}
}

func TimeToTimestamp(t time.Time) *Timestamp {
	ts := &Timestamp{}
	if t.IsZero() {
		ts.Seconds = time.Time{}.Unix()
		return ts
	}
	ts.Seconds = t.Unix()
	ts.Nanos = int32(t.Nanosecond())

	return ts
}

func (m *Timestamp) MarshalJSON() ([]byte, error) {
	ret := TimestampToTime(m)
	return json.Marshal(ret)
}

func (m *Timestamp) UnmarshalJSON(data []byte) error {
	ret := time.Time{}
	err := json.Unmarshal(data, &ret)
	if err != nil {
		return err
	}

	actualTimestamp := TimeToTimestamp(ret)
	m.Seconds = actualTimestamp.Seconds
	m.Nanos = actualTimestamp.Nanos

	return nil
}

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
	if r.GetLabels() == nil {
		r.Labels = &labelpb.ZLabelSet{}
	}
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
	if TimestampToTime(r1.GetLastEvaluation()).Before(TimestampToTime(r2.GetLastEvaluation())) {
		return 1
	}

	if TimestampToTime(r1.GetLastEvaluation()).After(TimestampToTime(r2.GetLastEvaluation())) {
		return -1
	}

	return 0
}

func NewAlertingRule(a *Alert) *Rule {
	if a.GetAnnotations() == nil {
		a.Annotations = &labelpb.ZLabelSet{}
	}
	if a.GetLabels() == nil {
		a.Labels = &labelpb.ZLabelSet{}
	}
	return &Rule{
		Result: &Rule_Alert{Alert: a},
	}
}

func (r *Rule) GetLabels() labels.Labels {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().GetLabels().PromLabels()
	case r.GetAlert() != nil:
		return r.GetAlert().GetLabels().PromLabels()
	default:
		return nil
	}
}

func (r *Rule) SetLabels(ls labels.Labels) {
	var result = &labelpb.ZLabelSet{}

	if len(ls) > 0 {
		result = &labelpb.ZLabelSet{Labels: labelpb.ProtobufLabelsFromPromLabels(ls)}
	}

	switch {
	case r.GetRecording() != nil:
		r.GetRecording().Labels = result
	case r.GetAlert() != nil:
		r.GetAlert().Labels = result
	}
}

func (r *Rule) GetName() string {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().GetName()
	case r.GetAlert() != nil:
		return r.GetAlert().GetName()
	default:
		return ""
	}
}

func (r *Rule) GetQuery() string {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().GetQuery()
	case r.GetAlert() != nil:
		return r.GetAlert().GetQuery()
	default:
		return ""
	}
}

func (r *Rule) GetLastEvaluation() *Timestamp {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().GetLastEvaluation()
	case r.GetAlert() != nil:
		return r.GetAlert().GetLastEvaluation()
	default:
		return nil
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

	if d := strings.Compare(r1.GetName(), r2.GetName()); d != 0 {
		return d
	}

	if d := labels.Compare(r1.GetLabels(), r2.GetLabels()); d != 0 {
		return d
	}

	if d := strings.Compare(r1.GetQuery(), r2.GetQuery()); d != 0 {
		return d
	}

	if r1.GetAlert() != nil && r2.GetAlert() != nil {
		if d := big.NewFloat(r1.GetAlert().GetDurationSeconds()).Cmp(big.NewFloat(r2.GetAlert().GetDurationSeconds())); d != 0 {
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

func (r *RuleGroups) UnmarshalJSON(data []byte) error {
	type plain RuleGroups
	ret := &plain{}
	err := json.Unmarshal(data, &ret)
	if err != nil {
		return err
	}
	if ret.Groups == nil {
		ret.Groups = []*RuleGroup{}
	}

	*r = RuleGroups{
		Groups: ret.Groups,
	}
	return nil
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
		if r.Labels == nil {
			r.Labels = &labelpb.ZLabelSet{}
		}
		m.Result = &Rule_Recording{Recording: r}
	case "alerting":
		r := &Alert{}
		if err := json.Unmarshal(entry, r); err != nil {
			return errors.Wrapf(err, "rule: alerting rule unmarshal: %v", string(entry))
		}
		if r.Annotations == nil {
			r.Annotations = &labelpb.ZLabelSet{}
		}
		if r.Labels == nil {
			r.Labels = &labelpb.ZLabelSet{}
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
		return json.Marshal(struct {
			*RecordingRule
			Type string `json:"type"`
		}{
			RecordingRule: r,
			Type:          RuleRecordingType,
		})
	}
	a := m.GetAlert()
	if a.Alerts == nil {
		// Ensure that empty slices are marshaled as '[]' and not 'null'.
		a.Alerts = make([]*AlertInstance, 0)
	}
	return json.Marshal(struct {
		*Alert
		Type string `json:"type"`
	}{
		Alert: a,
		Type:  RuleAlertingType,
	})
}

func (a *AlertInstance) MarshalJSON() ([]byte, error) {
	if len(a.GetLabels().GetLabels()) == 0 {
		a.Labels = &labelpb.ZLabelSet{}
	}
	if len(a.GetAnnotations().GetLabels()) == 0 {
		a.Annotations = &labelpb.ZLabelSet{}
	}
	aux := struct {
		Labels                  *labelpb.ZLabelSet               `json:"labels"`
		Annotations             *labelpb.ZLabelSet               `json:"annotations"`
		State                   *AlertState                      `json:"state"`
		ActiveAt                *time.Time                       `json:"activeAt,omitempty"`
		Value                   string                           `json:"value"`
		PartialResponseStrategy *storepb.PartialResponseStrategy `json:"partialResponseStrategy"`
	}{
		Labels:                  a.GetLabels(),
		Annotations:             a.GetAnnotations(),
		State:                   &a.State,
		Value:                   a.GetValue(),
		PartialResponseStrategy: &a.PartialResponseStrategy,
	}
	if t := TimestampToTime(a.GetActiveAt()); !t.IsZero() {
		aux.ActiveAt = &t
	}

	return json.Marshal(aux)
}

func (a *AlertInstance) UnmarshalJSON(entry []byte) error {
	type plain AlertInstance
	ret := &plain{}
	err := json.Unmarshal(entry, &ret)
	if err != nil {
		return err
	}

	*a = AlertInstance{
		State:                   ret.State,
		Value:                   ret.Value,
		PartialResponseStrategy: ret.PartialResponseStrategy,
	}
	if ret.ActiveAt != nil {
		a.ActiveAt = ret.ActiveAt
	} else {
		a.ActiveAt = TimeToTimestamp(time.Time{})
	}
	if len(ret.Labels.GetLabels()) > 0 {
		a.Labels = ret.Labels
	}
	if len(ret.Annotations.GetLabels()) > 0 {
		a.Annotations = ret.Annotations
	}
	return nil
}

func (r *RuleGroup) MarshalJSON() ([]byte, error) {
	if r.Rules == nil {
		// Ensure that empty slices are marshaled as '[]' and not 'null'.
		r.Rules = make([]*Rule, 0)
	}
	type plain RuleGroup
	return json.Marshal((*plain)(r))
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
	if d := a1.GetState().Compare(a2.GetState()); d != 0 {
		return d
	}

	if TimestampToTime(a1.GetLastEvaluation()).Before(TimestampToTime(a2.GetLastEvaluation())) {
		return 1
	}

	if TimestampToTime(a1.GetLastEvaluation()).After(TimestampToTime(a2.GetLastEvaluation())) {
		return -1
	}

	return 0
}
