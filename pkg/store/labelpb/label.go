// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package containing proto and JSON serializable Labels and ZLabels (no copy) structs used to
// identify series. This package expose no-copy converters to Prometheus labels.Labels.

package labelpb

import (
	"sort"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
)

var sep = []byte{'\xff'}

func noAllocBytes(buf string) []byte {
	return *(*[]byte)(unsafe.Pointer(&buf))
}

// ProtobufLabelsFromPromLabels converts Prometheus labels to a slice pointers to labelpb.Label.
func ProtobufLabelsFromPromLabels(lset labels.Labels) []*Label {
	labels := make([]*Label, 0, len(lset))
	for _, lbl := range lset {
		labels = append(labels, &Label{Name: lbl.Name, Value: lbl.Value})
	}

	return labels
}

// ZLabelSetFromProtobufLabels converts a slice of pointers to labelpb.Label to a pointer to a ZLabelSet.
func ZLabelSetFromProtobufLabels(lset []*Label) *ZLabelSet {
	if len(lset) == 0 {
		return nil
	}
	return &ZLabelSet{Labels: lset}
}

// ProtobufLabelsToPromLabels converts a slice of pointers labelpb.Label to Prometheus labels.
func ProtobufLabelsToPromLabels(lset []*Label) labels.Labels {
	promLabels := make([]labels.Label, 0, len(lset))
	for _, lbl := range lset {
		promLabels = append(promLabels, labels.Label{Name: lbl.Name, Value: lbl.Value})
	}

	return promLabels
}

// ReAllocZLabelsStrings re-allocates all underlying bytes for string, detaching it from bigger memory pool.
func ReAllocZLabelsStrings(lset *[]*Label) {
	for j, l := range *lset {
		// NOTE: This trick converts from string to byte without copy, but copy when creating string.
		(*lset)[j].Name = string(noAllocBytes(l.Name))
		(*lset)[j].Value = string(noAllocBytes(l.Value))
	}
}

// LabelsFromPromLabels converts Prometheus labels to slice of labelpb.ZLabel in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func LabelsFromPromLabels(lset labels.Labels) []Label {
	return *(*[]Label)(unsafe.Pointer(&lset))
}

// LabelsToPromLabels convert slice of labelpb.ZLabel to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []Label.
func LabelsToPromLabels(lset []Label) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}

// ZLabelSetsToPromLabelSets converts slice of labelpb.ZLabelSet to slice of Prometheus labels.
func ZLabelSetsToPromLabelSets(lss ...*ZLabelSet) []labels.Labels {
	res := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		res = append(res, ls.PromLabels())
	}
	return res
}

// Compare compares two labels.
func (m *Label) Compare(other *Label) int {
	if c := strings.Compare(m.Name, other.Name); c != 0 {
		return c
	}
	return strings.Compare(m.Value, other.Value)
}

// ExtendSortedLabels extend given labels by extend in labels format.
// The type conversion is done safely, which means we don't modify extend labels underlying array.
//
// In case of existing labels already present in given label set, it will be overwritten by external one.
// NOTE: Labels and extend has to be sorted.
func ExtendSortedLabels(lset, extend labels.Labels) labels.Labels {
	ret := make(labels.Labels, 0, len(lset)+len(extend))

	// Inject external labels in place.
	for len(lset) > 0 && len(extend) > 0 {
		d := strings.Compare(lset[0].Name, extend[0].Name)
		if d == 0 {
			// Duplicate, prefer external labels.
			// NOTE(fabxc): Maybe move it to a prefixed version to still ensure uniqueness of series?
			ret = append(ret, extend[0])
			lset, extend = lset[1:], extend[1:]
		} else if d < 0 {
			ret = append(ret, lset[0])
			lset = lset[1:]
		} else if d > 0 {
			ret = append(ret, extend[0])
			extend = extend[1:]
		}
	}

	// Append all remaining elements.
	ret = append(ret, lset...)
	ret = append(ret, extend...)
	return ret
}

func PromLabelSetsToString(lsets []labels.Labels) string {
	s := []string{}
	for _, ls := range lsets {
		s = append(s, ls.String())
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

func (m *ZLabelSet) UnmarshalJSON(entry []byte) error {
	lbls := labels.Labels{}
	if err := lbls.UnmarshalJSON(entry); err != nil {
		return errors.Wrapf(err, "labels: labels field unmarshal: %v", string(entry))
	}
	sort.Sort(lbls)
	m.Labels = ProtobufLabelsFromPromLabels(lbls)
	return nil
}

func (m *ZLabelSet) MarshalJSON() ([]byte, error) {
	return m.PromLabels().MarshalJSON()
}

// PromLabels return Prometheus labels.Labels without extra allocation.
func (m *ZLabelSet) PromLabels() labels.Labels {
	return ProtobufLabelsToPromLabels(m.Labels)
}

// HashWithPrefix returns a hash for the given prefix and labels.
func HashWithPrefix(prefix string, lbls []*Label) uint64 {
	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	b = append(b, prefix...)
	b = append(b, sep[0])

	for i, v := range lbls {
		if len(b)+len(v.Name)+len(v.Value)+2 >= cap(b) {
			// If labels entry is 1KB allocate do not allocate whole entry.
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range lbls[i:] {
				_, _ = h.WriteString(v.Name)
				_, _ = h.Write(sep)
				_, _ = h.WriteString(v.Value)
				_, _ = h.Write(sep)
			}
			return h.Sum64()
		}
		b = append(b, v.Name...)
		b = append(b, sep[0])
		b = append(b, v.Value...)
		b = append(b, sep[0])
	}
	return xxhash.Sum64(b)
}

// ZLabelSets is a sortable list of ZLabelSet. It assumes the label pairs in each ZLabelSet element are already sorted.
type ZLabelSets []*ZLabelSet

func (z ZLabelSets) Len() int { return len(z) }

func (z ZLabelSets) Swap(i, j int) { z[i], z[j] = z[j], z[i] }

func (z ZLabelSets) Less(i, j int) bool {
	l := 0
	r := 0
	var result int
	lenI, lenJ := len(z[i].Labels), len(z[j].Labels)
	for l < lenI && r < lenJ {
		result = z[i].Labels[l].Compare(z[j].Labels[r])
		if result == 0 {
			l++
			r++
			continue
		}
		return result < 0
	}

	return l == lenI
}
