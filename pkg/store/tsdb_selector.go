// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"sort"
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

// MatchLabelSets returns true if the given label sets match the TSDBSelector.
// As a second parameter, it returns the matched label sets if they are a subset of the given input.
// Otherwise the second return value is nil.
func (sr *TSDBSelector) MatchLabelSets(labelSets ...labels.Labels) (bool, []labels.Labels) {
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

// MatchersForLabelSets generates a list of label matchers for the given label sets.
func MatchersForLabelSets(labelSets []labels.Labels) []storepb.LabelMatcher {
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
		labelSet.Range(func(l labels.Label) {
			if _, ok := labelNameValues[l.Name]; !ok {
				labelNameValues[l.Name] = make(map[string]struct{})
			}
			labelNameCounts[l.Name]++
			labelNameValues[l.Name][l.Value] = struct{}{}
		})
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
		values := maps.Keys(lblVals)
		sort.Strings(values)
		matcher := storepb.LabelMatcher{
			Name:  lblName,
			Value: strings.Join(values, "|"),
			Type:  storepb.LabelMatcher_RE,
		}
		matchers = append(matchers, matcher)
	}

	return matchers
}
