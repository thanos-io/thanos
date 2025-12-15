// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"io"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// Error variables for protobuf parsing.
var (
	errInvalidLength = fmt.Errorf("proto: negative length found during unmarshaling")
	errIntOverflow   = fmt.Errorf("proto: integer overflow")
)

// UnmarshalZeroCopy unmarshals a WriteRequest from protobuf bytes without copying label string data.
// The WriteRequest's TimeSeries Labels will have Name and Value strings that point directly into
// the provided data buffer.
//
// WARNING: The data buffer must not be modified or freed while the WriteRequest is in use.
// This is intended for short-lived processing where the protobuf buffer outlives the WriteRequest.
func (m *WriteRequest) UnmarshalZeroCopy(data []byte) error {
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
			return fmt.Errorf("proto: WriteRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: WriteRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1: // timeseries
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timeseries", wireType)
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
			ts := &prompb.TimeSeries{}
			if err := ts.UnmarshalZeroCopy(data[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Timeseries = append(m.Timeseries, ts)
			iNdEx = postIndex
		case 2: // tenant
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tenant", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return errIntOverflow
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
				return errInvalidLength
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return errInvalidLength
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			// Tenant is typically short-lived, so we can use zero-copy here too
			m.Tenant = labelpb.NoAllocString(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 3: // replica
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Replica", wireType)
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
			m.Replica = v
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

// UnmarshalZeroCopy unmarshals a Series from protobuf bytes without copying label string data.
// The Series' Labels will have Name and Value strings that point directly into the provided data buffer.
//
// WARNING: The data buffer must not be modified or freed while the Series is in use.
// This is intended for short-lived processing where the protobuf buffer outlives the Series.
func (m *Series) UnmarshalZeroCopy(data []byte) error {
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
			return fmt.Errorf("proto: Series: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Series: illegal tag %d (wire type %d)", fieldNum, wire)
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
		case 2: // chunks
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
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
			// Use standard unmarshal for AggrChunk as it doesn't contain strings
			chunk := &AggrChunk{}
			if err := chunk.UnmarshalVT(data[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Chunks = append(m.Chunks, chunk)
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
