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
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/easyproto"
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

// Error variables for protobuf parsing.
var (
	ErrInvalidLengthTypes        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTypes          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTypes = fmt.Errorf("proto: unexpected end of group")
)

func noAllocString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

func noAllocBytes(buf string) []byte {
	return *(*[]byte)(unsafe.Pointer(&buf))
}

// ZLabel is a Label (also easily transformable to Prometheus labels.Labels) that can be unmarshalled from protobuf
// reusing the same memory address for string bytes.
// NOTE: While unmarshalling it uses exactly same bytes that were allocated for protobuf. This mean that *whole* protobuf
// bytes will be not GC-ed as long as ZLabels are referenced somewhere. Use it carefully, only for short living
// protobuf message processing.
//
// ZLabel has the same memory layout as labels.Label from Prometheus, allowing zero-copy conversions.
type ZLabel struct {
	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

// Reset resets the ZLabel.
func (m *ZLabel) Reset() { *m = ZLabel{} }

// String returns the string representation.
func (m *ZLabel) String() string {
	return fmt.Sprintf("%s=%q", m.Name, m.Value)
}

// ProtoMessage marks this as a protobuf message.
func (*ZLabel) ProtoMessage() {}

// ZLabelsFromPromLabels converts Prometheus labels to slice of labelpb.ZLabel.
// With -tags slicelabels, labels.Labels is a slice of labels.Label which has
// the same memory layout as ZLabel, allowing zero-copy conversion.
func ZLabelsFromPromLabels(lset labels.Labels) []ZLabel {
	return *(*[]ZLabel)(unsafe.Pointer(&lset))
}

// ZLabelsToPromLabels converts slice of labelpb.ZLabel to Prometheus labels.Labels.
// With -tags slicelabels, labels.Labels is a slice of labels.Label which has
// the same memory layout as ZLabel, allowing zero-copy conversion.
func ZLabelsToPromLabels(lset []ZLabel) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}

// ReAllocZLabelsStrings re-allocates all underlying bytes for string, detaching it from bigger memory pool.
// If `intern` is set to true, the method will use interning, i.e. reuse already allocated strings, to make the reallocation
// method more efficient.
//
// This is primarily intended to be used before labels are written into TSDB which can hold label strings in the memory long term.
func ReAllocZLabelsStrings(lset *[]ZLabel, intern bool) {
	if intern {
		for j := range *lset {
			(*lset)[j].Name = detachAndInternLabelString((*lset)[j].Name)
			(*lset)[j].Value = detachAndInternLabelString((*lset)[j].Value)
		}
		return
	}

	for j := range *lset {
		(*lset)[j].Name = string(noAllocBytes((*lset)[j].Name))
		(*lset)[j].Value = string(noAllocBytes((*lset)[j].Value))
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

// ZLabelSet is a set of ZLabels.
type ZLabelSet struct {
	Labels []ZLabel `protobuf:"bytes,1,rep,name=labels,proto3" json:"labels"`
}

// Reset resets the ZLabelSet.
func (m *ZLabelSet) Reset() { *m = ZLabelSet{} }

// String returns the string representation.
func (m *ZLabelSet) String() string {
	return fmt.Sprintf("%v", m.Labels)
}

// ProtoMessage marks this as a protobuf message.
func (*ZLabelSet) ProtoMessage() {}

// ZLabelSetsToPromLabelSets converts slice of labelpb.ZLabelSet to slice of Prometheus labels.
func ZLabelSetsToPromLabelSets(lss ...ZLabelSet) []labels.Labels {
	res := make([]labels.Labels, 0, len(lss))
	for i := range lss {
		res = append(res, lss[i].PromLabels())
	}
	return res
}

// ZLabelSetsFromPromLabels converts []labels.labels to []labelpb.ZLabelSet.
func ZLabelSetsFromPromLabels(lss ...labels.Labels) []ZLabelSet {
	sets := make([]ZLabelSet, 0, len(lss))
	for _, ls := range lss {
		set := ZLabelSet{
			Labels: make([]ZLabel, 0, ls.Len()),
		}
		ls.Range(func(lbl labels.Label) {
			set.Labels = append(set.Labels, ZLabel{
				Name:  lbl.Name,
				Value: lbl.Value,
			})
		})
		sets = append(sets, set)
	}

	return sets
}

// MarshalTo marshals the ZLabel to the given byte slice.
func (m *ZLabel) MarshalTo(data []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(data[:size])
}

// MarshalToSizedBuffer marshals the ZLabel to a sized buffer.
func (m *ZLabel) MarshalToSizedBuffer(data []byte) (int, error) {
	i := len(data)
	if len(m.Value) > 0 {
		i -= len(m.Value)
		copy(data[i:], m.Value)
		i = encodeVarintTypes(data, i, uint64(len(m.Value)))
		i--
		data[i] = 0x12
	}
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(data[i:], m.Name)
		i = encodeVarintTypes(data, i, uint64(len(m.Name)))
		i--
		data[i] = 0xa
	}
	return len(data) - i, nil
}

// Unmarshal unmarshalls gRPC protobuf into ZLabel struct. ZLabel string is directly using bytes passed in `data`.
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

// UnmarshalJSON unmarshals JSON into ZLabel.
func (m *ZLabel) UnmarshalJSON(entry []byte) error {
	var tmp struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(entry, &tmp); err != nil {
		return errors.Wrapf(err, "labels: label field unmarshal: %v", string(entry))
	}
	m.Name = tmp.Name
	m.Value = tmp.Value
	return nil
}

// Marshal marshals the ZLabel to bytes.
func (m *ZLabel) Marshal() ([]byte, error) {
	size := m.Size()
	data := make([]byte, size)
	n, err := m.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// MarshalJSON marshals the ZLabel to JSON.
func (m *ZLabel) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}{
		Name:  m.Name,
		Value: m.Value,
	})
}

// Size returns the size of the ZLabel when serialized.
func (m *ZLabel) Size() (n int) {
	if m == nil {
		return 0
	}
	l := len(m.Name)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	return n
}

// Equal checks if two ZLabels are equal.
func (m *ZLabel) Equal(other ZLabel) bool {
	return m.Name == other.Name && m.Value == other.Value
}

// Compare compares two ZLabels.
func (m *ZLabel) Compare(other ZLabel) int {
	if c := strings.Compare(m.Name, other.Name); c != 0 {
		return c
	}
	return strings.Compare(m.Value, other.Value)
}

// ZLabelSet marshal/unmarshal methods

// Marshal marshals the ZLabelSet to bytes.
func (m *ZLabelSet) Marshal() ([]byte, error) {
	size := m.Size()
	data := make([]byte, size)
	n, err := m.MarshalToSizedBuffer(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// MarshalTo marshals the ZLabelSet to the given byte slice.
func (m *ZLabelSet) MarshalTo(data []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(data[:size])
}

// MarshalToSizedBuffer marshals the ZLabelSet to a sized buffer.
func (m *ZLabelSet) MarshalToSizedBuffer(data []byte) (int, error) {
	i := len(data)
	if len(m.Labels) > 0 {
		for iNdEx := len(m.Labels) - 1; iNdEx >= 0; iNdEx-- {
			size := m.Labels[iNdEx].Size()
			i -= size
			if _, err := m.Labels[iNdEx].MarshalTo(data[i:]); err != nil {
				return 0, err
			}
			i = encodeVarintTypes(data, i, uint64(size))
			i--
			data[i] = 0xa
		}
	}
	return len(data) - i, nil
}

// Unmarshal unmarshals bytes into ZLabelSet.
func (m *ZLabelSet) Unmarshal(data []byte) error {
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
			return fmt.Errorf("proto: ZLabelSet: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ZLabelSet: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Labels = append(m.Labels, ZLabel{})
			if err := m.Labels[len(m.Labels)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(data[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
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

// Size returns the size of the ZLabelSet when serialized.
func (m *ZLabelSet) Size() (n int) {
	if m == nil {
		return 0
	}
	if len(m.Labels) > 0 {
		for _, e := range m.Labels {
			l := e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	return n
}

func (m *ZLabelSet) UnmarshalJSON(entry []byte) error {
	lbls := labels.Labels{}
	if err := lbls.UnmarshalJSON(entry); err != nil {
		return errors.Wrapf(err, "labels: labels field unmarshal: %v", string(entry))
	}
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

// Helper functions for protobuf encoding/decoding.

func encodeVarintTypes(data []byte, offset int, v uint64) int {
	offset -= sovTypes(v)
	base := offset
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return base
}

func sovTypes(x uint64) int {
	return (sovTypesLen(x) + 6) / 7
}

func sovTypesLen(x uint64) int {
	n := 0
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n + 1
}

func skipTypes(data []byte) (n int, err error) {
	l := len(data)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTypes
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if data[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTypes
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTypes
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTypes
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

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

// HashWithPrefixFromProto returns a hash for the given prefix and protobuf v2 labels.
func HashWithPrefixFromProto(prefix string, lbls []*Label) uint64 {
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

// ValidateLabelsFromProto validates label names and values for proto []*Label type.
func ValidateLabelsFromProto(lbls []*Label) error {
	if len(lbls) == 0 {
		return ErrEmptyLabels
	}

	// Check first label.
	l0 := lbls[0]
	if l0 == nil || l0.Name == "" || l0.Value == "" {
		return ErrEmptyLabels
	}

	// Iterate over the rest, check each for empty / duplicates and
	// check lexicographical (alphabetically) ordering.
	for _, l := range lbls[1:] {
		if l == nil || l.Name == "" || l.Value == "" {
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

// LabelSetToPromLabels converts a *LabelSet (protobuf v2 generated type) to Prometheus labels.Labels.
func LabelSetToPromLabels(ls *LabelSet) labels.Labels {
	if ls == nil || len(ls.Labels) == 0 {
		return labels.EmptyLabels()
	}
	b := labels.NewScratchBuilder(len(ls.Labels))
	for _, l := range ls.Labels {
		b.Add(l.Name, l.Value)
	}
	return b.Labels()
}

// PromLabelsToLabelSet converts Prometheus labels.Labels to *LabelSet (protobuf v2 generated type).
func PromLabelsToLabelSet(ls labels.Labels) *LabelSet {
	if ls.IsEmpty() {
		return nil
	}
	result := &LabelSet{
		Labels: make([]*Label, 0, ls.Len()),
	}
	ls.Range(func(l labels.Label) {
		result.Labels = append(result.Labels, &Label{Name: l.Name, Value: l.Value})
	})
	return result
}

// LabelSetsToPromLabelSets converts a slice of *LabelSet to a slice of Prometheus labels.Labels.
func LabelSetsToPromLabelSets(lss []*LabelSet) []labels.Labels {
	result := make([]labels.Labels, 0, len(lss))
	for _, ls := range lss {
		result = append(result, LabelSetToPromLabels(ls))
	}
	return result
}

// PromLabelSetsToLabelSets converts a slice of Prometheus labels.Labels to a slice of *LabelSet.
func PromLabelSetsToLabelSets(lss []labels.Labels) []*LabelSet {
	result := make([]*LabelSet, 0, len(lss))
	for _, ls := range lss {
		result = append(result, PromLabelsToLabelSet(ls))
	}
	return result
}

// PromLabelsToLabels converts Prometheus labels.Labels to []*Label (protobuf v2 generated type).
func PromLabelsToLabels(ls labels.Labels) []*Label {
	if ls.IsEmpty() {
		return nil
	}
	result := make([]*Label, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		result = append(result, &Label{Name: l.Name, Value: l.Value})
	})
	return result
}

// LabelsToPromLabels converts []*Label (protobuf v2 generated type) to Prometheus labels.Labels.
func LabelsToPromLabels(lbls []*Label) labels.Labels {
	if len(lbls) == 0 {
		return labels.EmptyLabels()
	}
	b := labels.NewScratchBuilder(len(lbls))
	for _, l := range lbls {
		b.Add(l.Name, l.Value)
	}
	return b.Labels()
}

// ReAllocLabelStrings re-allocates all underlying bytes for string in []*Label,
// detaching it from bigger memory pool.
func ReAllocLabelStrings(lset *[]*Label) {
	for j := range *lset {
		(*lset)[j].Name = string(noAllocBytes((*lset)[j].Name))
		(*lset)[j].Value = string(noAllocBytes((*lset)[j].Value))
	}
}

// DeepCopyLabels copies []*Label and each label's string to separate bytes.
func DeepCopyLabels(lbls []*Label) []*Label {
	ret := make([]*Label, len(lbls))
	for i := range lbls {
		ret[i] = &Label{
			Name:  string(noAllocBytes(lbls[i].Name)),
			Value: string(noAllocBytes(lbls[i].Value)),
		}
	}
	return ret
}

// ZLabelSetsToLabelSets converts []ZLabelSet to []*LabelSet (protobuf v2 generated type).
func ZLabelSetsToLabelSets(zss []ZLabelSet) []*LabelSet {
	result := make([]*LabelSet, len(zss))
	for i, zs := range zss {
		result[i] = ZLabelSetToLabelSet(zs)
	}
	return result
}

// ZLabelSetToLabelSet converts a single ZLabelSet to *LabelSet (protobuf v2 generated type).
func ZLabelSetToLabelSet(zs ZLabelSet) *LabelSet {
	if len(zs.Labels) == 0 {
		return nil
	}
	result := &LabelSet{
		Labels: make([]*Label, len(zs.Labels)),
	}
	for i, zl := range zs.Labels {
		result.Labels[i] = &Label{Name: zl.Name, Value: zl.Value}
	}
	return result
}

// ZLabelsToLabels converts []ZLabel to []*Label (protobuf v2 generated type).
func ZLabelsToLabels(zls []ZLabel) []*Label {
	result := make([]*Label, len(zls))
	for i, zl := range zls {
		result[i] = &Label{Name: zl.Name, Value: zl.Value}
	}
	return result
}

// LabelsToZLabels converts []*Label (protobuf v2 generated type) to []ZLabel.
func LabelsToZLabels(ls []*Label) []ZLabel {
	result := make([]ZLabel, len(ls))
	for i, l := range ls {
		if l != nil {
			result[i] = ZLabel{Name: l.Name, Value: l.Value}
		}
	}
	return result
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

type CustomLabelset labels.Labels

var builderPool = &sync.Pool{
	New: func() any {
		b := labels.NewScratchBuilder(8)
		return &b
	},
}

func (l *CustomLabelset) UnmarshalProtobuf(src []byte) (err error) {
	b := builderPool.Get().(*labels.ScratchBuilder)
	b.Reset()

	defer builderPool.Put(b)

	var fc easyproto.FieldContext

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return errors.Wrap(err, "unmarshal next field")
		}

		if fc.FieldNum != 1 {
			return fmt.Errorf("expected field 1, got %d", fc.FieldNum)
		}

		dat, ok := fc.MessageData()
		if !ok {
			return fmt.Errorf("expected message data for field %d", fc.FieldNum)
		}

		var n, v string
		var msgFc easyproto.FieldContext
		for len(dat) > 0 {
			dat, err = msgFc.NextField(dat)
			if err != nil {
				return errors.Wrap(err, "unmarshal next field in message")
			}

			switch msgFc.FieldNum {
			case 1:
				n, ok = msgFc.String()
				if !ok {
					return fmt.Errorf("expected string data for field %d", msgFc.FieldNum)
				}
			case 2:
				v, ok = msgFc.String()
				if !ok {
					return fmt.Errorf("expected string data for field %d", msgFc.FieldNum)
				}
			default:
				return fmt.Errorf("unexpected field %d in label message", msgFc.FieldNum)
			}
		}

		b.Add(n, v)

	}

	*l = CustomLabelset(b.Labels())
	return nil
}
