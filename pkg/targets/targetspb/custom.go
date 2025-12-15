// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package targetspb

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func NewTargetsResponse(targets *TargetDiscovery) *TargetsResponse {
	return &TargetsResponse{
		Result: &TargetsResponse_Targets{
			Targets: targets,
		},
	}
}

func NewWarningTargetsResponse(warning error) *TargetsResponse {
	return &TargetsResponse{
		Result: &TargetsResponse_Warning{
			Warning: warning.Error(),
		},
	}
}

func (x *TargetHealth) UnmarshalJSON(entry []byte) error {
	fieldStr, err := strconv.Unquote(string(entry))
	if err != nil {
		return errors.Wrapf(err, "targetHealth: unquote %v", string(entry))
	}

	if fieldStr == "" {
		return errors.New("empty targetHealth")
	}

	state, ok := TargetHealth_value[strings.ToUpper(fieldStr)]
	if !ok {
		return errors.Errorf("unknown targetHealth: %v", string(entry))
	}
	*x = TargetHealth(state)
	return nil
}

func (x *TargetHealth) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(strings.ToLower(x.String()))), nil
}

func (x TargetHealth) Compare(y TargetHealth) int {
	return int(x) - int(y)
}

func (t1 *ActiveTarget) Compare(t2 *ActiveTarget) int {
	if d := strings.Compare(t1.ScrapeUrl, t2.ScrapeUrl); d != 0 {
		return d
	}

	if d := strings.Compare(t1.ScrapePool, t2.ScrapePool); d != 0 {
		return d
	}

	if d := labels.Compare(labelpb.LabelSetToPromLabels(t1.DiscoveredLabels), labelpb.LabelSetToPromLabels(t2.DiscoveredLabels)); d != 0 {
		return d
	}

	if d := labels.Compare(labelpb.LabelSetToPromLabels(t1.Labels), labelpb.LabelSetToPromLabels(t2.Labels)); d != 0 {
		return d
	}

	return 0
}

func (t1 *ActiveTarget) CompareState(t2 *ActiveTarget) int {
	if d := t1.Health.Compare(t2.Health); d != 0 {
		return d
	}

	t1Time := t1.LastScrape.AsTime()
	t2Time := t2.LastScrape.AsTime()

	if t1Time.Before(t2Time) {
		return 1
	}

	if t1Time.After(t2Time) {
		return -1
	}

	return 0
}

func (t1 *DroppedTarget) Compare(t2 *DroppedTarget) int {
	if d := labels.Compare(labelpb.LabelSetToPromLabels(t1.DiscoveredLabels), labelpb.LabelSetToPromLabels(t2.DiscoveredLabels)); d != 0 {
		return d
	}

	return 0
}

func (t *ActiveTarget) SetLabels(ls labels.Labels) {
	t.Labels = labelpb.PromLabelsToLabelSet(ls)
}

func (t *ActiveTarget) SetDiscoveredLabels(ls labels.Labels) {
	t.DiscoveredLabels = labelpb.PromLabelsToLabelSet(ls)
}

func (t *DroppedTarget) SetDiscoveredLabels(ls labels.Labels) {
	t.DiscoveredLabels = labelpb.PromLabelsToLabelSet(ls)
}
