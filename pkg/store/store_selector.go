// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	reMatchAny = "^$"
)

var (
	SupportedRelabelActions = map[relabel.Action]struct{}{relabel.Keep: {}, relabel.Drop: {}, relabel.HashMod: {}, relabel.LabelDrop: {}, relabel.LabelKeep: {}}
)

type StoreSelector struct {
	relabelConfig []*relabel.Config
}

func NewStoreSelector(relabelConfig []*relabel.Config) *StoreSelector {
	return &StoreSelector{
		relabelConfig: relabelConfig,
	}
}

func (sr *StoreSelector) RelabelConfigEnabled() bool {
	return sr.relabelConfig != nil
}

func (sr *StoreSelector) MatchStore(labelSets ...labels.Labels) (bool, []labels.Labels) {
	if !sr.RelabelConfigEnabled() {
		return true, nil
	}
	matchedLabelSets := sr.runRelabelRules(labelSets)
	return len(matchedLabelSets) > 0, matchedLabelSets
}

func (sr *StoreSelector) runRelabelRules(labelSets []labels.Labels) []labels.Labels {
	result := make([]labels.Labels, 0)
	for _, labelSet := range labelSets {
		if _, keep := relabel.Process(labelSet, sr.relabelConfig...); !keep {
			continue
		}

		result = append(result, labelSet)
	}

	return result
}

func (sr *StoreSelector) buildLabelMatchers(labelSets []labels.Labels) []storepb.LabelMatcher {
	labelCounts := make(map[string]int)
	matcherSet := make(map[string]map[string]struct{})
	for _, labelSet := range labelSets {
		for _, lbl := range labelSet {
			if _, ok := matcherSet[lbl.Name]; !ok {
				matcherSet[lbl.Name] = make(map[string]struct{})
			}
			labelCounts[lbl.Name]++
			matcherSet[lbl.Name][lbl.Value] = struct{}{}
		}
	}

	for k := range matcherSet {
		if labelCounts[k] < len(labelSets) {
			matcherSet[k][reMatchAny] = struct{}{}
		}
	}

	matchers := make([]storepb.LabelMatcher, 0, len(matcherSet))
	for k, v := range matcherSet {
		var matchedValues []string
		for val := range v {
			matchedValues = append(matchedValues, val)
		}
		matcher := storepb.LabelMatcher{Type: storepb.LabelMatcher_RE, Name: k, Value: strings.Join(matchedValues, "|")}
		matchers = append(matchers, matcher)
	}

	return matchers
}
