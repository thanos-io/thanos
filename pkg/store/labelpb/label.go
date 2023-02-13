// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package containing proto and JSON serializable Labels and ZLabels (no copy) structs used to
// identify series. This package expose no-copy converters to Prometheus labels.Labels.

package labelpb

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"go4.org/intern"
)

var (
	ErrOutOfOrderLabels = errors.New("out of order labels")
	ErrEmptyLabels      = errors.New("label set contains a label with empty name or value")
	ErrDuplicateLabels  = errors.New("label set contains duplicate label names")

	sep = []byte{'\xff'}
)

func noAllocString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

func noAllocBytes(buf string) []byte {
	return *(*[]byte)(unsafe.Pointer(&buf))
}

// ZLabelsFromPromLabels converts Prometheus labels to slice of labelpb.ZLabel in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func ZLabelsFromPromLabels(lset labels.Labels) []ZLabel {
	return *(*[]ZLabel)(unsafe.Pointer(&lset))
}

// ZLabelsToPromLabels convert slice of labelpb.ZLabel to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []ZLabel.
// NOTE: Use with care. ZLabels holds memory from the whole protobuf unmarshal, so the returned
// Prometheus Labels will hold this memory as well.
func ZLabelsToPromLabels(lset []ZLabel) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}

// ReAllocAndInternZLabelsStrings re-allocates all underlying bytes for string, detaching it from bigger memory pool.
// If `intern` is set to true, the method will use interning, i.e. reuse already allocated strings, to make the reallocation
// method more efficient.
//
// This is primarily intended to be used before labels are written into TSDB which can hold label strings in the memory long term.
func ReAllocZLabelsStrings(lset *[]ZLabel, intern bool) {
	if intern {
		for j, l := range *lset {
			(*lset)[j].Name = detachAndInternLabelString(l.Name)
			(*lset)[j].Value = detachAndInternLabelString(l.Value)
		}
		return
	}

	for j, l := range *lset {
		(*lset)[j].Name = string(noAllocBytes(l.Name))
		(*lset)[j].Value = string(noAllocBytes(l.Value))
	}
}

// internLabelString is a helper method to intern a label string or,
// if the string was previously interned, it returns the existing
// reference and asserts it to a string.
func internLabelString(s string) string {
	return intern.GetByString(s).Get().(string)
}

// detachAndInternLabelString reallocates the label string to detach it
// from a bigger memory pool and interns the string.
func detachAndInternLabelString(s string) string {
	return internLabelString(string(noAllocBytes(s)))
}

// ZLabelSetsToPromLabelSets converts slice of labelpb.ZLabelSet to slice of Prometheus labels.
func ZLabelSetsToPromLabelSets(lss ...ZLabelSet) []labels.Labels {
	res := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		res = append(res, ls.PromLabels())
	}
	return res
}

// ZLabelSetsFromPromLabels converts []labels.labels to []labelpb.ZLabelSet.
func ZLabelSetsFromPromLabels(lss ...labels.Labels) []ZLabelSet {
	sets := make([]ZLabelSet, 0, len(lss))
	for _, ls := range lss {
		set := ZLabelSet{
			Labels: make([]ZLabel, 0, len(ls)),
		}
		for _, lbl := range ls {
			set.Labels = append(set.Labels, ZLabel{
				Name:  lbl.Name,
				Value: lbl.Value,
			})
		}
		sets = append(sets, set)
	}

	return sets
}

// ZLabel is a Label (also easily transformable to Prometheus labels.Labels) that can be unmarshalled from protobuf
// reusing the same memory address for string bytes.
// NOTE: While unmarshalling it uses exactly same bytes that were allocated for protobuf. This mean that *whole* protobuf
// bytes will be not GC-ed as long as ZLabels are referenced somewhere. Use it carefully, only for short living
// protobuf message processing.
type ZLabel Label

func (m *ZLabel) MarshalTo(data []byte) (int, error) {
	f := Label(*m)
	return f.MarshalTo(data)
}

func (m *ZLabel) MarshalToSizedBuffer(data []byte) (int, error) {
	f := Label(*m)
	return f.MarshalToSizedBuffer(data)
}

// Unmarshal unmarshalls gRPC protobuf into ZLabel struct. ZLabel string is directly using bytes passed in `data`.
// To use it add (gogoproto.customtype) = "github.com/thanos-io/thanos/pkg/store/labelpb.ZLabel" to proto field tag.
// NOTE: This exists in internal Google protobuf implementation, but not in open source one: https://news.ycombinator.com/item?id=23588882
func (m *ZLabel) Unmarshal(data []byte) error {
	l := len(data)

	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ZLabel: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ZLabel: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = noAllocString(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = noAllocString(data[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *ZLabel) UnmarshalJSON(entry []byte) error {
	f := Label(*m)
	if err := json.Unmarshal(entry, &f); err != nil {
		return errors.Wrapf(err, "labels: label field unmarshal: %v", string(entry))
	}
	*m = ZLabel(f)
	return nil
}

func (m *ZLabel) Marshal() ([]byte, error) {
	f := Label(*m)
	return f.Marshal()
}

func (m *ZLabel) MarshalJSON() ([]byte, error) {
	return json.Marshal(Label(*m))
}

// Size implements proto.Sizer.
func (m *ZLabel) Size() (n int) {
	f := Label(*m)
	return f.Size()
}

// Equal implements proto.Equaler.
func (m *ZLabel) Equal(other ZLabel) bool {
	return m.Name == other.Name && m.Value == other.Value
}

// Compare implements proto.Comparer.
func (m *ZLabel) Compare(other ZLabel) int {
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
	m.Labels = ZLabelsFromPromLabels(lbls)
	return nil
}

func (m *ZLabelSet) MarshalJSON() ([]byte, error) {
	return m.PromLabels().MarshalJSON()
}

// PromLabels return Prometheus labels.Labels without extra allocation.
func (m *ZLabelSet) PromLabels() labels.Labels {
	return ZLabelsToPromLabels(m.Labels)
}

// DeepCopy copies labels and each label's string to separate bytes.
func DeepCopy(lbls []ZLabel) []ZLabel {
	ret := make([]ZLabel, len(lbls))
	for i := range lbls {
		ret[i].Name = string(noAllocBytes(lbls[i].Name))
		ret[i].Value = string(noAllocBytes(lbls[i].Value))
	}
	return ret
}

// HashWithPrefix returns a hash for the given prefix and labels.
func HashWithPrefix(prefix string, lbls []ZLabel) uint64 {
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

// ValidateLabels validates label names and values (checks for empty
// names and values, out of order labels and duplicate label names)
// Returns appropriate error if validation fails on a label.
func ValidateLabels(lbls []ZLabel) error {
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

// ZLabelSets is a sortable list of ZLabelSet. It assumes the label pairs in each ZLabelSet element are already sorted.
type ZLabelSets []ZLabelSet

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
