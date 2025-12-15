// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prompb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

// Error variables for protobuf parsing.
var (
	errInvalidLength = fmt.Errorf("proto: negative length found during unmarshaling")
	errIntOverflow   = fmt.Errorf("proto: integer overflow")
)

// UnmarshalZeroCopy unmarshals a TimeSeries from protobuf bytes without copying label string data.
// The TimeSeries' Labels will have Name and Value strings that point directly into the provided data buffer.
//
// WARNING: The data buffer must not be modified or freed while the TimeSeries is in use.
// This is intended for short-lived processing where the protobuf buffer outlives the TimeSeries.
func (m *TimeSeries) UnmarshalZeroCopy(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return errIntOverflow
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
			return fmt.Errorf("proto: TimeSeries: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TimeSeries: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1: // labels
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return errIntOverflow
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
				return errInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return errInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			lbl := &labelpb.Label{}
			if err := lbl.UnmarshalZeroCopy(data[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Labels = append(m.Labels, lbl)
			iNdEx = postIndex
		case 2: // samples
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return errIntOverflow
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
				return errInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return errInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			sample := &Sample{}
			if err := unmarshalSample(data[iNdEx:postIndex], sample); err != nil {
				return err
			}
			m.Samples = append(m.Samples, sample)
			iNdEx = postIndex
		case 3: // exemplars - use standard unmarshal
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Exemplars", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return errIntOverflow
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
				return errInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return errInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Exemplars = append(m.Exemplars, &Exemplar{})
			if err := m.Exemplars[len(m.Exemplars)-1].UnmarshalVT(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4: // histograms - use standard unmarshal
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Histograms", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return errIntOverflow
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
				return errInvalidLength
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return errInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Histograms = append(m.Histograms, &Histogram{})
			if err := m.Histograms[len(m.Histograms)-1].UnmarshalVT(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skip(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return errInvalidLength
			}
			if (iNdEx + skippy) < 0 {
				return errInvalidLength
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

// unmarshalSample unmarshals a Sample from protobuf bytes.
// Samples don't contain strings, so no zero-copy optimization needed.
func unmarshalSample(data []byte, m *Sample) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return errIntOverflow
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
		switch fieldNum {
		case 1: // value (fixed64/double)
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			if iNdEx+8 > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = math.Float64frombits(binary.LittleEndian.Uint64(data[iNdEx:]))
			iNdEx += 8
		case 2: // timestamp (varint)
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamp", wireType)
			}
			var v int64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return errIntOverflow
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Timestamp = v
		default:
			skippy, err := skip(data[iNdEx-1:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return errInvalidLength
			}
			iNdEx += skippy - 1
		}
	}
	return nil
}

// skip returns the number of bytes to skip for an unknown field.
func skip(data []byte) (int, error) {
	l := len(data)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, errIntOverflow
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
					return 0, errIntOverflow
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
					return 0, errIntOverflow
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
				return 0, errInvalidLength
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, fmt.Errorf("proto: unexpected end of group")
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, errInvalidLength
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}
