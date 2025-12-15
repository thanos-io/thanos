// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Package labelpb contains proto and JSON serializable Labels and LabelSet structs
// used to identify series. This package intentionally does not import
// github.com/prometheus/prometheus/model/labels to avoid import cycles.
// For conversion functions between labelpb types and Prometheus labels,
// see the labelpb/convert package.

package labelpb

import (
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
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

// NoAllocString converts a byte slice to a string without allocation.
// The returned string shares memory with the input slice.
// This is exported for use by other packages that need zero-copy string conversion.
func NoAllocString(buf []byte) string {
	return noAllocString(buf)
}

// UnmarshalZeroCopy unmarshals a Label from protobuf bytes without copying string data.
// The Label's Name and Value strings will point directly into the provided data buffer.
//
// WARNING: The data buffer must not be modified or freed while the Label is in use.
// This is intended for short-lived processing where the protobuf buffer outlives the Label.
func (m *Label) UnmarshalZeroCopy(data []byte) error {
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

// ReAllocLabelStrings re-allocates all underlying bytes for string in []*Label,
// detaching it from bigger memory pool.
// If `intern` is set to true, the method will use interning, i.e. reuse already allocated strings.
func ReAllocLabelStrings(lset []*Label, doIntern bool) {
	if doIntern {
		for j := range lset {
			lset[j].Name = detachAndInternLabelString(lset[j].Name)
			lset[j].Value = detachAndInternLabelString(lset[j].Value)
		}
		return
	}

	for j := range lset {
		lset[j].Name = string(noAllocBytes(lset[j].Name))
		lset[j].Value = string(noAllocBytes(lset[j].Value))
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

// ValidateLabels validates label names and values for proto []*Label type.
func ValidateLabels(lbls []*Label) error {
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

// LabelSets is a sortable list of LabelSet. It assumes the label pairs in each LabelSet element are already sorted.
type LabelSets []*LabelSet

func (z LabelSets) Len() int { return len(z) }

func (z LabelSets) Swap(i, j int) { z[i], z[j] = z[j], z[i] }

func (z LabelSets) Less(i, j int) bool {
	l := 0
	r := 0
	var result int
	lenI, lenJ := len(z[i].Labels), len(z[j].Labels)
	for l < lenI && r < lenJ {
		result = CompareLabels(z[i].Labels[l], z[j].Labels[r])
		if result == 0 {
			l++
			r++
			continue
		}
		return result < 0
	}

	return l == lenI
}

// CompareLabels compares two labels by name, then by value.
func CompareLabels(a, b *Label) int {
	if c := strings.Compare(a.Name, b.Name); c != 0 {
		return c
	}
	return strings.Compare(a.Value, b.Value)
}
