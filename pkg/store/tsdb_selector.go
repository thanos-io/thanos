// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"golang.org/x/exp/maps"

	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const reMatchEmpty = "^$"

var DefaultSelector = noopSelector()

type TSDBSelector struct {
	relabelConfig []*relabel.Config
}

func noopSelector() *TSDBSelector {
	return NewTSDBSelector(nil)
}

func NewTSDBSelector(relabelConfig []*relabel.Config) *TSDBSelector {
	return &TSDBSelector{
		relabelConfig: relabelConfig,
	}
}

// MatchStore returns true if the selector matches the given store label sets.
// If only certain labels match, the method also returns those matching labels.
func (sr *TSDBSelector) MatchStore(store Client) (bool, []labels.Labels) {
	labelSets := store.LabelSets()
	if sr.relabelConfig == nil || len(labelSets) == 0 {
		return true, nil
	}
	matchedLabelSets := sr.runRelabelRules(labelSets)
	return len(matchedLabelSets) > 0, matchedLabelSets
}

func (sr *TSDBSelector) runRelabelRules(labelSets []labels.Labels) []labels.Labels {
	result := make([]labels.Labels, 0)
	for _, labelSet := range labelSets {
		if _, keep := relabel.Process(labelSet, sr.relabelConfig...); !keep {
			continue
		}

		result = append(result, labelSet)
	}

	return result
}

// matchersForLabelSets generates a list of label matchers for the given label sets.
func matchersForLabelSets(labelSets []labels.Labels) []storepb.LabelMatcher {
	var (
		// labelNameCounts tracks how many times a label name appears in the given label
		// sets. This is used to make sure that an explicit empty value matcher is
		// generated when a label name is missing from a label set.
		labelNameCounts = make(map[string]int)
		// labelNameValues contains an entry for each label name and label value
		// combination that is present in the given label sets. This map is used to build
		// out the label matchers.
		labelNameValues = make(map[string]map[string]struct{})
	)
	for _, labelSet := range labelSets {
		for _, lbl := range labelSet {
			if _, ok := labelNameValues[lbl.Name]; !ok {
				labelNameValues[lbl.Name] = make(map[string]struct{})
			}
			labelNameCounts[lbl.Name]++
			labelNameValues[lbl.Name][lbl.Value] = struct{}{}
		}
	}

	// If a label name is missing from a label set, force an empty value matcher for
	// that label name.
	for labelName := range labelNameValues {
		if labelNameCounts[labelName] < len(labelSets) {
			labelNameValues[labelName][reMatchEmpty] = struct{}{}
		}
	}

	matchers := make([]storepb.LabelMatcher, 0, len(labelNameValues))
	for lblName, lblVals := range labelNameValues {
		matcher := storepb.LabelMatcher{
			Name:  lblName,
			Value: strings.Join(maps.Keys(lblVals), "|"),
			Type:  storepb.LabelMatcher_RE,
		}
		matchers = append(matchers, matcher)
	}

	return matchers
}
