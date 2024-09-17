// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package cortexpb

import (
	stdjson "encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
)

func LabelPairToModelMetric(labels []*LabelPair) model.Metric {
	m := make(model.Metric, len(labels))
	for _, l := range labels {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}

	return m
}

func LabelMapToCortexMetric(lbls map[string]string) []*LabelPair {
	labels := make([]*LabelPair, 0, len(lbls))
	for ln, lv := range lbls {
		labels = append(labels, &LabelPair{
			Name:  []byte(ln),
			Value: []byte(lv),
		})
	}
	sort.Slice(labels, func(i, j int) bool {
		return strings.Compare(string(labels[i].Name), string(labels[j].Name)) < 0
	})

	return labels
}

func ModelMetricToCortexMetric(m model.Metric) []*LabelPair {
	labels := make([]*LabelPair, 0, len(m))
	for ln, lv := range m {
		labels = append(labels, &LabelPair{
			Name:  []byte(ln),
			Value: []byte(lv),
		})
	}
	sort.Slice(labels, func(i, j int) bool {
		return strings.Compare(string(labels[i].Name), string(labels[j].Name)) < 0
	})

	return labels
}

// isTesting is only set from tests to get special behaviour to verify that custom sample encode and decode is used,
// both when using jsonitor or standard json package.
var isTesting = false

// MarshalJSON implements json.Marshaler.
func (s *Sample) MarshalJSON() ([]byte, error) {
	if isTesting && math.IsNaN(s.Value) {
		return nil, fmt.Errorf("test sample")
	}

	t, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(model.Time(s.TimestampMs))
	if err != nil {
		return nil, err
	}
	v, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(model.SampleValue(s.Value))
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("[%s,%s]", t, v)), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Sample) UnmarshalJSON(b []byte) error {
	var t model.Time
	var v model.SampleValue
	vs := [...]stdjson.Unmarshaler{&t, &v}
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(b, &vs); err != nil {
		return err
	}
	s.TimestampMs = int64(t)
	s.Value = float64(v)

	if isTesting && math.IsNaN(float64(v)) {
		return fmt.Errorf("test sample")
	}
	return nil
}

func SampleJsoniterEncode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	sample := (*Sample)(ptr)

	if isTesting && math.IsNaN(sample.Value) {
		stream.Error = fmt.Errorf("test sample")
		return
	}

	stream.WriteArrayStart()
	stream.WriteFloat64(float64(sample.TimestampMs) / float64(time.Second/time.Millisecond))
	stream.WriteMore()
	stream.WriteString(model.SampleValue(sample.Value).String())
	stream.WriteArrayEnd()
}

func SampleJsoniterDecode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	if !iter.ReadArray() {
		iter.ReportError("cortexpb.Sample", "expected [")
		return
	}

	t := model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond))

	if !iter.ReadArray() {
		iter.ReportError("cortexpb.Sample", "expected ,")
		return
	}

	bs := iter.ReadStringAsSlice()
	ss := *(*string)(unsafe.Pointer(&bs))
	v, err := strconv.ParseFloat(ss, 64)
	if err != nil {
		iter.ReportError("cortexpb.Sample", err.Error())
		return
	}

	if isTesting && math.IsNaN(v) {
		iter.Error = fmt.Errorf("test sample")
		return
	}

	if iter.ReadArray() {
		iter.ReportError("cortexpb.Sample", "expected ]")
	}

	*(*Sample)(ptr) = Sample{
		TimestampMs: int64(t),
		Value:       v,
	}
}

func init() {
	jsoniter.RegisterTypeEncoderFunc("cortexpb.Sample", SampleJsoniterEncode, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("cortexpb.Sample", SampleJsoniterDecode)
}
