// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package labelpb

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
)

var (
	ErrOutOfOrderLabels = errors.New("out of order labels")
	ErrEmptyLabels      = errors.New("label set contains a label with empty name or value")
	ErrDuplicateLabels  = errors.New("label set contains duplicate label names")

	sep = []byte{'\xff'}
)

// ExtendSortedLabels extend given labels by extend in labels format.
// The type conversion is done safely, which means we don't modify extend labels underlying array.
//
// In case of existing labels already present in given label set, it will be overwritten by external one.
func ExtendSortedLabels(lset, extend labels.Labels) labels.Labels {
	if extend.IsEmpty() {
		return lset.Copy()
	}
	b := labels.NewBuilder(lset)
	extend.Range(func(l labels.Label) {
		b.Set(l.Name, l.Value)
	})
	return b.Labels()
}

func PromLabelSetsToString(lsets []labels.Labels) string {
	s := []string{}
	for _, ls := range lsets {
		s = append(s, ls.String())
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

// ValidateLabels validates label names and values (checks for empty
// names and values, out of order labels and duplicate label names)
// Returns appropriate error if validation fails on a label.
func ValidateLabels(lbls []Label) error {
	if len(lbls) == 0 {
		return ErrEmptyLabels
	}

	// Check first label.
	l0 := lbls[0]
	if l0.Name == "" || l0.Value == "" {
		return ErrEmptyLabels
	}

	// Iterate over the rest, check each for empty / duplicates and
	// check lexicographical (alphabetically) ordering.
	for _, l := range lbls[1:] {
		if l.Name == "" || l.Value == "" {
			return ErrEmptyLabels
		}

		if l.Name == l0.Name {
			return ErrDuplicateLabels
		}

		if l.Name < l0.Name {
			return ErrOutOfOrderLabels
		}
		l0 = l
	}

	return nil
}

func PromLabelsToLabelpbLabels(lbls labels.Labels) []Label {
	if lbls.Len() == 0 {
		return []Label(nil)
	}
	lset := make([]Label, 0, lbls.Len())
	lbls.Range(func(l labels.Label) {
		lset = append(lset, Label{Name: l.Name, Value: l.Value})
	})

	return lset
}

func LabelpbLabelsToPromLabels(lbls []Label) labels.Labels {
	if len(lbls) == 0 {
		return labels.Labels(nil)
	}
	lblSlice := make([]string, 0, len(lbls)*2)
	for _, l := range lbls {
		lblSlice = append(lblSlice, l.Name, l.Value)
	}
	return labels.FromStrings(lblSlice...)
}

func (l *Label) Equal(other Label) bool {
	return l.Name == other.Name && l.Value == other.Value
}

func (m *Label) Compare(other Label) int {
	if c := strings.Compare(m.Name, other.Name); c != 0 {
		return c
	}
	return strings.Compare(m.Value, other.Value)
}

func (m *LabelSet) PromLabels() labels.Labels {
	return LabelpbLabelsToPromLabels(m.Labels)
}

func LabelpbLabelSetsToPromLabels(lss ...LabelSet) []labels.Labels {
	res := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		res = append(res, ls.PromLabels())
	}
	return res
}

// HashWithPrefix returns a hash for the given prefix and labels.
func HashWithPrefix(prefix string, lbls []Label) uint64 {
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
				_, _ = h.Write([]byte(v.Name))
				_, _ = h.Write(sep)
				_, _ = h.Write([]byte(v.Value))
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

type LabelSets []LabelSet

func (z LabelSets) Len() int { return len(z) }

func (z LabelSets) Swap(i, j int) { z[i], z[j] = z[j], z[i] }

func (z LabelSets) Less(i, j int) bool {
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

func (m *Label) UnmarshalJSON(entry []byte) error {
	f := Label(*m)
	if err := json.Unmarshal(entry, &f); err != nil {
		return errors.Wrapf(err, "labels: label field unmarshal: %v", string(entry))
	}
	*m = Label(f)
	return nil
}

func (m *LabelSet) UnmarshalJSON(entry []byte) error {
	lbls := labels.Labels{}
	if err := lbls.UnmarshalJSON(entry); err != nil {
		return errors.Wrapf(err, "labels: labels field unmarshal: %v", string(entry))
	}
	m.Labels = PromLabelsToLabelpbLabels(lbls)
	return nil
}

func (m *LabelSet) MarshalJSON() ([]byte, error) {
	return m.PromLabels().MarshalJSON()
}
