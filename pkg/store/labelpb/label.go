// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package containing Zero Copy Labels adapter.

package labelpb

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
)

func noAllocString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

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

// Label is a labels.Label that can be marshaled to/from protobuf reusing the same
// memory address for string bytes.
type Label labels.Label

func (m *Label) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(data[:size])
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *Label) MarshalTo(data []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(data[:size])
}

func (m *Label) MarshalToSizedBuffer(data []byte) (int, error) {
	i := len(data)
	_ = i
	var l int
	_ = l
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

func (m *Label) Unmarshal(data []byte) error {
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
			return fmt.Errorf("proto: Label: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Label: illegal tag %d (wire type %d)", fieldNum, wire)
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

func (m *Label) UnmarshalJSON(entry []byte) error {
	l := FullCopyLabel{}

	if err := json.Unmarshal(entry, &l); err != nil {
		return errors.Wrapf(err, "labels: label field unmarshal: %v", string(entry))
	}
	m.Name = l.Name
	m.Value = l.Value
	return nil
}

func (m *Label) MarshalJSON() ([]byte, error) {
	return json.Marshal(&FullCopyLabel{Name: m.Name, Value: m.Value})
}

// Size implements proto.Sizer.
func (m *Label) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovTypes(uint64(l))
	}
	return n
}

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
