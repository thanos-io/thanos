// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writev2

import (
	fmt "fmt"
	io "io"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func (m *Request) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: Request: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Request: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Symbols", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Symbols = append(m.Symbols, unsafe.String(unsafe.SliceData(dAtA[iNdEx:postIndex]), len(dAtA[iNdEx:postIndex])))
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timeseries", wireType)
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
			m.Timeseries = append(m.Timeseries, TimeSeries{})
			if err := m.Timeseries[len(m.Timeseries)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
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
	return nil
}

// ToV1Series desymbolizes and validates a Remote Write 2.0 TimeSeries into a v1 prompb.TimeSeries.
func (m TimeSeries) ToV1Series(symbols []string) (prompb.TimeSeries, error) {
	if len(m.LabelsRefs) == 0 || len(m.LabelsRefs)%2 != 0 {
		return prompb.TimeSeries{}, errors.New("invalid labels_refs length")
	}
	if int(m.Metadata.HelpRef) >= len(symbols) || int(m.Metadata.UnitRef) >= len(symbols) {
		return prompb.TimeSeries{}, errors.New("metadata symbol reference out of bounds")
	}
	if len(m.Samples) == 0 && len(m.Histograms) == 0 && len(m.Exemplars) == 0 {
		return prompb.TimeSeries{}, errors.New("time series must contain at least one sample, histogram, or exemplar")
	}
	if len(m.Samples) > 0 && len(m.Histograms) > 0 {
		return prompb.TimeSeries{}, errors.New("time series cannot contain both float samples and histograms")
	}

	v1Ts := prompb.TimeSeries{
		Labels: make([]labelpb.ZLabel, 0, len(m.LabelsRefs)/2),
	}
	var hasMetricName bool
	for i := 0; i < len(m.LabelsRefs); i += 2 {
		nameRef, valRef := int(m.LabelsRefs[i]), int(m.LabelsRefs[i+1])
		if nameRef >= len(symbols) || valRef >= len(symbols) {
			return prompb.TimeSeries{}, errors.New("label symbol reference out of bounds")
		}
		name, val := symbols[nameRef], symbols[valRef]
		if name == labels.MetricName && val != "" {
			hasMetricName = true
		}
		v1Ts.Labels = append(v1Ts.Labels, labelpb.ZLabel{
			Name:  name,
			Value: val,
		})
	}
	if err := labelpb.ValidateLabels(v1Ts.Labels); err != nil {
		return prompb.TimeSeries{}, errors.Wrap(err, "invalid series labels")
	}
	if !hasMetricName {
		return prompb.TimeSeries{}, errors.New("time series missing __name__ label")
	}

	if len(m.Samples) > 0 {
		v1Ts.Samples = make([]prompb.Sample, 0, len(m.Samples))
		for _, v2s := range m.Samples {
			v1Ts.Samples = append(v1Ts.Samples, prompb.Sample{
				Timestamp: v2s.Timestamp,
				Value:     v2s.Value,
			})
		}
	}

	if len(m.Exemplars) > 0 {
		v1Ts.Exemplars = make([]prompb.Exemplar, 0, len(m.Exemplars))
		for _, e := range m.Exemplars {
			if len(e.LabelsRefs)%2 != 0 {
				return prompb.TimeSeries{}, errors.New("invalid exemplar labels_refs length")
			}
			v1Exemplar := prompb.Exemplar{
				Value:     e.Value,
				Timestamp: e.Timestamp,
				Labels:    make([]labelpb.ZLabel, 0, len(e.LabelsRefs)/2),
			}
			for i := 0; i < len(e.LabelsRefs); i += 2 {
				nameRef, valRef := int(e.LabelsRefs[i]), int(e.LabelsRefs[i+1])
				if nameRef >= len(symbols) || valRef >= len(symbols) {
					return prompb.TimeSeries{}, errors.New("exemplar label symbol reference out of bounds")
				}
				v1Exemplar.Labels = append(v1Exemplar.Labels, labelpb.ZLabel{
					Name:  symbols[nameRef],
					Value: symbols[valRef],
				})
			}
			if len(v1Exemplar.Labels) > 0 {
				if err := labelpb.ValidateLabels(v1Exemplar.Labels); err != nil {
					return prompb.TimeSeries{}, errors.Wrap(err, "invalid exemplar labels")
				}
			}
			v1Ts.Exemplars = append(v1Ts.Exemplars, v1Exemplar)
		}
	}

	if len(m.Histograms) > 0 {
		v1Ts.Histograms = make([]prompb.Histogram, 0, len(m.Histograms))
		for _, h := range m.Histograms {
			v1Histogram := prompb.Histogram{
				Sum:            h.Sum,
				Schema:         h.Schema,
				ZeroThreshold:  h.ZeroThreshold,
				NegativeSpans:  translateV2SpansToV1(h.NegativeSpans),
				NegativeDeltas: h.NegativeDeltas,
				NegativeCounts: h.NegativeCounts,
				PositiveSpans:  translateV2SpansToV1(h.PositiveSpans),
				PositiveDeltas: h.PositiveDeltas,
				PositiveCounts: h.PositiveCounts,
				ResetHint:      prompb.Histogram_ResetHint(h.ResetHint),
				Timestamp:      h.Timestamp,
				CustomValues:   h.CustomValues,
			}

			switch c := h.Count.(type) {
			case *Histogram_CountInt:
				v1Histogram.Count = &prompb.Histogram_CountInt{CountInt: c.CountInt}
			case *Histogram_CountFloat:
				v1Histogram.Count = &prompb.Histogram_CountFloat{CountFloat: c.CountFloat}
			}

			switch zc := h.ZeroCount.(type) {
			case *Histogram_ZeroCountInt:
				v1Histogram.ZeroCount = &prompb.Histogram_ZeroCountInt{ZeroCountInt: zc.ZeroCountInt}
			case *Histogram_ZeroCountFloat:
				v1Histogram.ZeroCount = &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: zc.ZeroCountFloat}
			}

			if v1Histogram.IsFloatHistogram() {
				if err := prompb.FloatHistogramProtoToFloatHistogram(v1Histogram).Validate(); err != nil {
					return prompb.TimeSeries{}, errors.Wrap(err, "invalid float histogram")
				}
			} else {
				if err := prompb.HistogramProtoToHistogram(v1Histogram).Validate(); err != nil {
					return prompb.TimeSeries{}, errors.Wrap(err, "invalid histogram")
				}
			}

			v1Ts.Histograms = append(v1Ts.Histograms, v1Histogram)
		}
	}

	return v1Ts, nil
}

func translateV2SpansToV1(spans []BucketSpan) []prompb.BucketSpan {
	if len(spans) == 0 {
		return nil
	}
	out := make([]prompb.BucketSpan, len(spans))
	for i, s := range spans {
		out[i] = prompb.BucketSpan{Offset: s.Offset, Length: s.Length}
	}
	return out
}
