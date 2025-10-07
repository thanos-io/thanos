// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prompb

import (
	encoding_binary "encoding/binary"
	fmt "fmt"
	io "io"
	math "math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

func (h Histogram) IsFloatHistogram() bool {
	_, ok := h.GetCount().(*Histogram_CountFloat)
	return ok
}

func FromProtoHistogram(h Histogram) *histogram.FloatHistogram {
	if h.IsFloatHistogram() {
		return FloatHistogramProtoToFloatHistogram(h)
	} else {
		return HistogramProtoToFloatHistogram(h)
	}
}

type Exemplar struct {
	// Optional, can be empty.
	Labels labels.Labels `protobuf:"bytes,1,rep,name=labels,proto3" json:"labels"`
	Value  float64       `protobuf:"fixed64,2,opt,name=value,proto3" json:"value,omitempty"`
	// timestamp is in ms format, see pkg/timestamp/timestamp.go for
	// conversion from time.Time to Prometheus timestamp.
	Timestamp int64 `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

// TimeSeries represents samples and labels for a single time series.
type TimeSeries struct {
	// Labels have to be sorted by label names and without duplicated label names.
	// TODO(bwplotka): Don't use zero copy ZLabels, see https://github.com/thanos-io/thanos/pull/3279 for details.
	Labels     labels.Labels `protobuf:"bytes,1,rep,name=labels,proto3" json:"labels"`
	Samples    []Sample      `protobuf:"bytes,2,rep,name=samples,proto3" json:"samples"`
	Exemplars  []Exemplar    `protobuf:"bytes,3,rep,name=exemplars,proto3" json:"exemplars"`
	Histograms []Histogram   `protobuf:"bytes,4,rep,name=histograms,proto3" json:"histograms"`
}

// ChunkedSeries represents single, encoded time series.
type ChunkedSeries struct {
	// Labels should be sorted.
	Labels labels.Labels `protobuf:"bytes,1,rep,name=labels,proto3,customtype=github.com/thanos-io/thanos/pkg/store/labelpb.ZLabel" json:"labels"`
	// Chunks will be in start time order and may overlap.
	Chunks []Chunk `protobuf:"bytes,2,rep,name=chunks,proto3" json:"chunks"`
}

func (m *Exemplar) GetLabels() labels.Labels {
	if m != nil {
		return m.Labels
	}
	return labels.EmptyLabels()
}

func (m *TimeSeries) GetLabels() labels.Labels {
	if m != nil {
		return m.Labels
	}
	return labels.EmptyLabels()
}

func (m *ChunkedSeries) GetLabels() labels.Labels {
	if m != nil {
		return m.Labels
	}
	return labels.EmptyLabels()
}

func (m *Exemplar) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Timestamp != 0 {
		i = encodeVarintTypes(dAtA, i, uint64(m.Timestamp))
		i--
		dAtA[i] = 0x18
	}
	if m.Value != 0 {
		i -= 8
		encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(m.Value))))
		i--
		dAtA[i] = 0x11
	}
	if m.Labels.Len() > 0 {
		var err error

		i, err = labelpb.MarshalToSizedBufferPromLabels(m.Labels, dAtA[:i])
		if err != nil {
			return 0, err
		}
	}
	return len(dAtA) - i, nil
}

func (m *TimeSeries) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Histograms) > 0 {
		for iNdEx := len(m.Histograms) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Histograms[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTypes(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x22
		}
	}
	if len(m.Exemplars) > 0 {
		for iNdEx := len(m.Exemplars) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Exemplars[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTypes(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	if len(m.Samples) > 0 {
		for iNdEx := len(m.Samples) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Samples[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTypes(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if m.Labels.Len() > 0 {
		var err error

		i, err = labelpb.MarshalToSizedBufferPromLabels(m.Labels, dAtA[:i])
		if err != nil {
			return 0, err
		}
	}
	return len(dAtA) - i, nil
}

func (m *ChunkedSeries) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Chunks) > 0 {
		for iNdEx := len(m.Chunks) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Chunks[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTypes(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if m.Labels.Len() > 0 {
		var err error

		i, err = labelpb.MarshalToSizedBufferPromLabels(m.Labels, dAtA[:i])
		if err != nil {
			return 0, err
		}
	}
	return len(dAtA) - i, nil
}

func (m *TimeSeries) Unmarshal(dAtA []byte) error {
	var labelsStart, labelsEnd int = -1, -1

	l := len(dAtA)
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
			b := dAtA[iNdEx]
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
		if fieldNum == 1 && labelsStart == -1 {
			labelsStart = preIndex
		}
		if fieldNum != 1 && labelsStart != -1 && labelsEnd == -1 {
			labelsEnd = preIndex
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
				b := dAtA[iNdEx]
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
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
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
			m.Samples = append(m.Samples, Sample{})
			if err := m.Samples[len(m.Samples)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Exemplars", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
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
			m.Exemplars = append(m.Exemplars, Exemplar{})
			if err := m.Exemplars[len(m.Exemplars)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Histograms", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
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
			m.Histograms = append(m.Histograms, Histogram{})
			if err := m.Histograms[len(m.Histograms)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
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

	if labelsEnd == -1 && labelsStart == -1 {
		return fmt.Errorf("labels subslice not found")
	}

	lbls, err := labelpb.UnmarshalProtobuf(dAtA[labelsStart:labelsEnd], 1)
	if err != nil {
		return err
	}

	m.Labels = lbls
	return nil
}

func (m *Exemplar) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Labels.Len() > 0 {
		n += labelpb.PromLabelsSize(m.Labels)
	}
	if m.Value != 0 {
		n += 9
	}
	if m.Timestamp != 0 {
		n += 1 + sovTypes(uint64(m.Timestamp))
	}
	return n
}

func (m *TimeSeries) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Labels.Len() > 0 {
		n += labelpb.PromLabelsSize(m.Labels)
	}
	if len(m.Samples) > 0 {
		for _, e := range m.Samples {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	if len(m.Exemplars) > 0 {
		for _, e := range m.Exemplars {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	if len(m.Histograms) > 0 {
		for _, e := range m.Histograms {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	return n
}

func (m *ChunkedSeries) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Labels.Len() > 0 {
		n += labelpb.PromLabelsSize(m.Labels)
	}
	if len(m.Chunks) > 0 {
		for _, e := range m.Chunks {
			l = e.Size()
			n += 1 + l + sovTypes(uint64(l))
		}
	}
	return n
}

func (m *Exemplar) Unmarshal(dAtA []byte) error {
	var labelsStart, labelsEnd int = -1, -1
	const labelsFieldNum = 1

	l := len(dAtA)
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
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Exemplar: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Exemplar: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		if fieldNum == labelsFieldNum && labelsStart == -1 {
			labelsStart = preIndex
		}
		if fieldNum != labelsFieldNum && labelsStart != -1 && labelsEnd == -1 {
			labelsEnd = preIndex
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
				b := dAtA[iNdEx]
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
			iNdEx = postIndex
		case 2:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			m.Value = float64(math.Float64frombits(v))
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamp", wireType)
			}
			m.Timestamp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Timestamp |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
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
	if labelsEnd == -1 && labelsStart == -1 {
		return fmt.Errorf("labels subslice not found")
	}

	lbls, err := labelpb.UnmarshalProtobuf(dAtA[labelsStart:labelsEnd], labelsFieldNum)
	if err != nil {
		return err
	}

	m.Labels = lbls
	return nil
}

func (m *ChunkedSeries) Unmarshal(dAtA []byte) error {
	var labelsStart, labelsEnd int = -1, -1
	const labelsFieldNum = 1

	l := len(dAtA)
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
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ChunkedSeries: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ChunkedSeries: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		if fieldNum == labelsFieldNum && labelsStart == -1 {
			labelsStart = preIndex
		}
		if fieldNum != labelsFieldNum && labelsStart != -1 && labelsEnd == -1 {
			labelsEnd = preIndex
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
				b := dAtA[iNdEx]
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
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
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
			m.Chunks = append(m.Chunks, Chunk{})
			if err := m.Chunks[len(m.Chunks)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(dAtA[iNdEx:])
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

	if labelsEnd == -1 && labelsStart == -1 {
		return fmt.Errorf("labels subslice not found")
	}

	lbls, err := labelpb.UnmarshalProtobuf(dAtA[labelsStart:labelsEnd], labelsFieldNum)
	if err != nil {
		return err
	}

	m.Labels = lbls
	return nil
}
