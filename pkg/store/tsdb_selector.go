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

func (sr *TSDBSelector) MatchStore(labelSets ...labels.Labels) (bool, []labels.Labels) {
	if sr.relabelConfig == nil || len(labelSets) == 0 {
		return true, labelSets
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

func (sr *TSDBSelector) buildTSDBMatchers(labelSets []labels.Labels) []storepb.LabelMatcher {
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
