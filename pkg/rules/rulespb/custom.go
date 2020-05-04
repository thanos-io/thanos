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
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb"
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

func (r1 *RecordingRule) Cmp(r2 *RecordingRule) int {
	if r1.LastEvaluation.Before(r2.LastEvaluation) {
		return 1
	}

	if r1.LastEvaluation.After(r2.LastEvaluation) {
		return -1
	}

	return 0
}

func NewAlertingRule(a *Alert) *Rule {
	return &Rule{
		Result: &Rule_Alert{Alert: a},
	}
}

func (r *Rule) GetLabels() []storepb.Label {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().Labels.Labels
	case r.GetAlert() != nil:
		return r.GetAlert().Labels.Labels
	default:
		return nil
	}
}

func (r *Rule) SetLabels(ls []storepb.Label) {
	var result PromLabels

	if len(ls) > 0 {
		result = PromLabels{Labels: ls}
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
		return r.GetRecording().Name
	case r.GetAlert() != nil:
		return r.GetAlert().Name
	default:
		return ""
	}
}

func (r *Rule) GetQuery() string {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().Query
	case r.GetAlert() != nil:
		return r.GetAlert().Query
	default:
		return ""
	}
}

func (r *Rule) GetLastEvaluation() time.Time {
	switch {
	case r.GetRecording() != nil:
		return r.GetRecording().LastEvaluation
	case r.GetAlert() != nil:
		return r.GetAlert().LastEvaluation
	default:
		return time.Time{}
	}
}

func (r1 *Rule) Cmp(r2 *Rule) int {
	if r1.GetAlert() != nil && r2.GetRecording() != nil {
		return -1
	}

	if r1.GetRecording() != nil && r2.GetAlert() != nil {
		return 1
	}

	if d := strings.Compare(r1.GetName(), r2.GetName()); d != 0 {
		return d
	}

	if d := storepb.CompareLabels(r1.GetLabels(), r2.GetLabels()); d != 0 {
		return d
	}

	if d := strings.Compare(r1.GetQuery(), r2.GetQuery()); d != 0 {
		return d
	}

	if r1.GetAlert() != nil && r2.GetAlert() != nil {
		if d := big.NewFloat(r1.GetAlert().DurationSeconds).Cmp(big.NewFloat(r2.GetAlert().DurationSeconds)); d != 0 {
			return d
		}
	}

	return 0
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
		return json.Marshal(struct {
			*RecordingRule
			Type string `json:"type"`
		}{
			RecordingRule: r,
			Type:          RuleRecordingType,
		})
	}
	a := m.GetAlert()
	return json.Marshal(struct {
		*Alert
		Type string `json:"type"`
	}{
		Alert: a,
		Type:  RuleAlertingType,
	})
}

func (x *AlertState) UnmarshalJSON(entry []byte) error {
	fieldStr, err := strconv.Unquote(string(entry))
	if err != nil {
		return errors.Wrapf(err, "alertState: unquote %v", string(entry))
	}

	if len(fieldStr) == 0 {
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
	return []byte(strconv.Quote(x.String())), nil
}

// Cmp compares x and y and returns:
//
//   < 0 if x < y  (alert state x is more critical than alert state y)
//     0 if x == y
//   > 0 if x > y  (alert state x is less critical than alert state y)
//
// For sorting this makes sure that more "critical" alert states come first.
func (x AlertState) Cmp(y AlertState) int {
	return int(y) - int(x)
}

func (a1 *Alert) Cmp(a2 *Alert) int {
	if d := a1.State.Cmp(a2.State); d != 0 {
		return d
	}

	if a1.LastEvaluation.Before(a2.LastEvaluation) {
		return 1
	}

	if a1.LastEvaluation.After(a2.LastEvaluation) {
		return -1
	}

	return 0
}

func (m *PromLabels) UnmarshalJSON(entry []byte) error {
	lbls := labels.Labels{}
	if err := lbls.UnmarshalJSON(entry); err != nil {
		return errors.Wrapf(err, "labels: labels field unmarshal: %v", string(entry))
	}
	m.Labels = storepb.PromLabelsToLabels(lbls)
	sort.Slice(m.Labels, func(i, j int) bool {
		return m.Labels[i].Name < m.Labels[j].Name
	})
	return nil
}

func (m *PromLabels) MarshalJSON() ([]byte, error) {
	return storepb.LabelsToPromLabels(m.Labels).MarshalJSON()
}
