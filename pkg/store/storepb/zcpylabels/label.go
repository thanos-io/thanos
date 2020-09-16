// Package containing Zero Copy Labels adapter.

package zcpylabels

import (
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/prometheus/prometheus/pkg/labels"
)

func noAllocString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

// LabelsFromPromLabels converts Thanos proto no alloc labels to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func LabelsFromPromLabels(lset labels.Labels) []Label {
	return *(*[]Label)(unsafe.Pointer(&lset))
}

// LabelsToPromLabels converts Prometheus labels to Thanos proto no alloc labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []NoAllocLabels
func LabelsToPromLabels(lset ...Label) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}

// Label is a labels.Label that can be marshalled to/from protobuf reusing the same
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
