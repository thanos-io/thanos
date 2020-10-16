// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package containing Zero Copy Labels adapter.

package labelpb

import (
	"sort"
	"strings"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
)

// LabelsFromPromLabels converts Prometheus labels to slice of storepb.Label in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func LabelsFromPromLabels(lset labels.Labels) []Label {
	return *(*[]Label)(unsafe.Pointer(&lset))
}

// LabelsToPromLabels convert slice of storepb.Label to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []Label.
func LabelsToPromLabels(lset []Label) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}

// LabelSetsToPromLabelSets converts slice of storepb.LabelSet to slice of Prometheus labels.
func LabelSetsToPromLabelSets(lss ...LabelSet) []labels.Labels {
	res := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		res = append(res, ls.PromLabels())
	}
	return res
}

// Label is a FullCopyLabel.
// This is quick fix for https://github.com/thanos-io/thanos/issues/3265.
// TODO(bwplotka): Replace with https://github.com/thanos-io/thanos/pull/3279
type Label = FullCopyLabel

// Equal implements proto.Equaler.
func (m *Label) Equal(other Label) bool {
	return m.Name == other.Name && m.Value == other.Value
}

// Compare implements proto.Comparer.
func (m *Label) Compare(other Label) int {
	if c := strings.Compare(m.Name, other.Name); c != 0 {
		return c
	}
	return strings.Compare(m.Value, other.Value)
}

// ExtendLabels extend given labels by extend in labels format.
// The type conversion is done safely, which means we don't modify extend labels underlying array.
//
// In case of existing labels already present in given label set, it will be overwritten by external one.
func ExtendLabels(lset labels.Labels, extend labels.Labels) labels.Labels {
	overwritten := map[string]struct{}{}
	for i, l := range lset {
		if v := extend.Get(l.Name); v != "" {
			lset[i].Value = v
			overwritten[l.Name] = struct{}{}
		}
	}

	for _, l := range extend {
		if _, ok := overwritten[l.Name]; ok {
			continue
		}
		lset = append(lset, l)
	}
	sort.Sort(lset)
	return lset
}

func PromLabelSetsToString(lsets []labels.Labels) string {
	s := []string{}
	for _, ls := range lsets {
		s = append(s, ls.String())
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

func (m *LabelSet) UnmarshalJSON(entry []byte) error {
	lbls := labels.Labels{}
	if err := lbls.UnmarshalJSON(entry); err != nil {
		return errors.Wrapf(err, "labels: labels field unmarshal: %v", string(entry))
	}
	sort.Sort(lbls)
	m.Labels = LabelsFromPromLabels(lbls)
	return nil
}

func (m *LabelSet) MarshalJSON() ([]byte, error) {
	return m.PromLabels().MarshalJSON()
}

// PromLabels return Prometheus labels.Labels without extra allocation.
func (m *LabelSet) PromLabels() labels.Labels {
	return LabelsToPromLabels(m.Labels)
}
