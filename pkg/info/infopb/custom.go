package infopb

import (
	fmt "fmt"
	io "io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

type InfoResponse struct {
	LabelSets     []labels.Labels `protobuf:"bytes,1,rep,name=label_sets,json=labelSets,proto3" json:"label_sets"`
	ComponentType string          `protobuf:"bytes,2,opt,name=ComponentType,proto3" json:"ComponentType,omitempty"`
	// StoreInfo holds the metadata related to Store API if exposed by the component otherwise it will be null.
	Store *StoreInfo `protobuf:"bytes,3,opt,name=store,proto3" json:"store,omitempty"`
	// RulesInfo holds the metadata related to Rules API if exposed by the component otherwise it will be null.
	Rules *RulesInfo `protobuf:"bytes,4,opt,name=rules,proto3" json:"rules,omitempty"`
	// MetricMetadataInfo holds the metadata related to Metadata API if exposed by the component otherwise it will be null.
	MetricMetadata *MetricMetadataInfo `protobuf:"bytes,5,opt,name=metric_metadata,json=metricMetadata,proto3" json:"metric_metadata,omitempty"`
	// TargetsInfo holds the metadata related to Targets API if exposed by the component otherwise it will be null.
	Targets *TargetsInfo `protobuf:"bytes,6,opt,name=targets,proto3" json:"targets,omitempty"`
	// ExemplarsInfo holds the metadata related to Exemplars API if exposed by the component otherwise it will be null.
	Exemplars *ExemplarsInfo `protobuf:"bytes,7,opt,name=exemplars,proto3" json:"exemplars,omitempty"`
	// QueryAPIInfo holds the metadata related to Query API if exposed by the component, otherwise it will be null.
	Query *QueryAPIInfo `protobuf:"bytes,8,opt,name=query,proto3" json:"query,omitempty"`
}

type TSDBInfo struct {
	Labels  labels.Labels `protobuf:"bytes,1,opt,name=labels,proto3" json:"labels"`
	MinTime int64         `protobuf:"varint,2,opt,name=min_time,json=minTime,proto3" json:"min_time,omitempty"`
	MaxTime int64         `protobuf:"varint,3,opt,name=max_time,json=maxTime,proto3" json:"max_time,omitempty"`
}

func (m *InfoResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Query != nil {
		{
			size, err := m.Query.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintRpc(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x42
	}
	if m.Exemplars != nil {
		{
			size, err := m.Exemplars.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintRpc(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x3a
	}
	if m.Targets != nil {
		{
			size, err := m.Targets.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintRpc(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x32
	}
	if m.MetricMetadata != nil {
		{
			size, err := m.MetricMetadata.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintRpc(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x2a
	}
	if m.Rules != nil {
		{
			size, err := m.Rules.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintRpc(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x22
	}
	if m.Store != nil {
		{
			size, err := m.Store.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintRpc(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x1a
	}
	if len(m.ComponentType) > 0 {
		i -= len(m.ComponentType)
		copy(dAtA[i:], m.ComponentType)
		i = encodeVarintRpc(dAtA, i, uint64(len(m.ComponentType)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.LabelSets) > 0 {
		for iNdEx := len(m.LabelSets) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := labelpb.MarshalToSizedBufferPromLabels(m.LabelSets[iNdEx], dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintRpc(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *TSDBInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.MaxTime != 0 {
		i = encodeVarintRpc(dAtA, i, uint64(m.MaxTime))
		i--
		dAtA[i] = 0x18
	}
	if m.MinTime != 0 {
		i = encodeVarintRpc(dAtA, i, uint64(m.MinTime))
		i--
		dAtA[i] = 0x10
	}
	{
		size, err := labelpb.MarshalToSizedBufferPromLabels(m.Labels, dAtA[:i])
		if err != nil {
			return 0, err
		}
		i -= size
		i = encodeVarintRpc(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0xa
	return len(dAtA) - i, nil
}

func (m *InfoResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.LabelSets) > 0 {
		for _, e := range m.LabelSets {
			n += labelpb.PromLabelsSize(e)
		}
	}
	l = len(m.ComponentType)
	if l > 0 {
		n += 1 + l + sovRpc(uint64(l))
	}
	if m.Store != nil {
		l = m.Store.Size()
		n += 1 + l + sovRpc(uint64(l))
	}
	if m.Rules != nil {
		l = m.Rules.Size()
		n += 1 + l + sovRpc(uint64(l))
	}
	if m.MetricMetadata != nil {
		l = m.MetricMetadata.Size()
		n += 1 + l + sovRpc(uint64(l))
	}
	if m.Targets != nil {
		l = m.Targets.Size()
		n += 1 + l + sovRpc(uint64(l))
	}
	if m.Exemplars != nil {
		l = m.Exemplars.Size()
		n += 1 + l + sovRpc(uint64(l))
	}
	if m.Query != nil {
		l = m.Query.Size()
		n += 1 + l + sovRpc(uint64(l))
	}
	return n
}

func (m *TSDBInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l

	n += labelpb.PromLabelsSize(m.Labels)
	if m.MinTime != 0 {
		n += 1 + sovRpc(uint64(m.MinTime))
	}
	if m.MaxTime != 0 {
		n += 1 + sovRpc(uint64(m.MaxTime))
	}
	return n
}

func (m *InfoResponse) Unmarshal(dAtA []byte) error {
	var labelsStart, labelsEnd int = -1, -1

	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
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
			return fmt.Errorf("proto: InfoResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: InfoResponse: illegal tag %d (wire type %d)", fieldNum, wire)
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
				return fmt.Errorf("proto: wrong wireType = %d for field LabelSets", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}

			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ComponentType", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.ComponentType = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Store", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Store == nil {
				m.Store = &StoreInfo{}
			}
			if err := m.Store.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Rules", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Rules == nil {
				m.Rules = &RulesInfo{}
			}
			if err := m.Rules.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field MetricMetadata", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.MetricMetadata == nil {
				m.MetricMetadata = &MetricMetadataInfo{}
			}
			if err := m.MetricMetadata.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Targets", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Targets == nil {
				m.Targets = &TargetsInfo{}
			}
			if err := m.Targets.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Exemplars", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Exemplars == nil {
				m.Exemplars = &ExemplarsInfo{}
			}
			if err := m.Exemplars.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 8:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Query", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Query == nil {
				m.Query = &QueryAPIInfo{}
			}
			if err := m.Query.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthRpc
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

	lbls, err := labelpb.UnmarshalLabelsetsProtobuf(dAtA[labelsStart:labelsEnd], 1, 1)
	if err != nil {
		return err
	}

	m.LabelSets = lbls
	return nil
}

func (m *TSDBInfo) Unmarshal(dAtA []byte) error {
	var labelsStart, labelsEnd int = -1, -1

	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
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
			return fmt.Errorf("proto: TSDBInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TSDBInfo: illegal tag %d (wire type %d)", fieldNum, wire)
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
					return ErrIntOverflowRpc
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
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRpc
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MinTime", wireType)
			}
			m.MinTime = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MinTime |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxTime", wireType)
			}
			m.MaxTime = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxTime |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthRpc
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

	lbls, err := labelpb.UnmarshalProtobuf(dAtA[labelsStart:labelsEnd], 1)
	if err != nil {
		return err
	}

	m.Labels = lbls
	return nil
}
