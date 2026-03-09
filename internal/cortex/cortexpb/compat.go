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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/internal/cortex/util"
)

// FromLabelAdaptersToLabels converts []LabelAdapter to labels.Labels.
func FromLabelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	if len(ls) == 0 {
		return labels.EmptyLabels()
	}
	b := labels.NewScratchBuilder(len(ls))
	for _, l := range ls {
		b.Add(l.Name, l.Value)
	}
	b.Sort()
	return b.Labels()
}

// FromLabelsToLabelAdapters converts labels.Labels to []LabelAdapter.
func FromLabelsToLabelAdapters(ls labels.Labels) []LabelAdapter {
	if ls.IsEmpty() {
		return nil
	}
	result := make([]LabelAdapter, 0, ls.Len())
	ls.Range(func(l labels.Label) {
		result = append(result, LabelAdapter{Name: l.Name, Value: l.Value})
	})
	return result
}

// FromLabelAdaptersToMetric converts []LabelAdapter to a model.Metric.
// Don't do this on any performance sensitive paths.
func FromLabelAdaptersToMetric(ls []LabelAdapter) model.Metric {
	return util.LabelsToMetric(FromLabelAdaptersToLabels(ls))
}

// FromMetricsToLabelAdapters converts model.Metric to []LabelAdapter.
// Don't do this on any performance sensitive paths.
// The result is sorted.
func FromMetricsToLabelAdapters(metric model.Metric) []LabelAdapter {
	result := make([]LabelAdapter, 0, len(metric))
	for k, v := range metric {
		result = append(result, LabelAdapter{
			Name:  string(k),
			Value: string(v),
		})
	}
	sort.Sort(byLabel(result)) // The labels should be sorted upon initialisation.
	return result
}

type byLabel []LabelAdapter

func (s byLabel) Len() int           { return len(s) }
func (s byLabel) Less(i, j int) bool { return strings.Compare(s[i].Name, s[j].Name) < 0 }
func (s byLabel) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// isTesting is only set from tests to get special behaviour to verify that custom sample encode and decode is used,
// both when using jsonitor or standard json package.
var isTesting = false

// MarshalJSON implements json.Marshaler.
func (s Sample) MarshalJSON() ([]byte, error) {
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
	return fmt.Appendf(nil, "[%s,%s]", t, v), nil
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
