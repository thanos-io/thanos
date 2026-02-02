// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/easyproto"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// GetWriteRequestUnmarshaler returns a WriteRequestUnmarshaler from the pool.
//
// Return it via PutWriteRequestUnmarshaler when done processing the WriteRequest.
// The WriteRequest and all its nested slices are only valid until Put is called.
func GetWriteRequestUnmarshaler() *WriteRequestUnmarshaler {
	v := wruPool.Get()
	if v == nil {
		return &WriteRequestUnmarshaler{}
	}
	return v.(*WriteRequestUnmarshaler)
}

// PutWriteRequestUnmarshaler returns wru to the pool.
//
// The caller must not access any data from the WriteRequest after this call.
func PutWriteRequestUnmarshaler(wru *WriteRequestUnmarshaler) {
	wru.Reset()
	wruPool.Put(wru)
}

var wruPool sync.Pool

// WriteRequestUnmarshaler is a reusable unmarshaler for WriteRequest protobuf messages.
//
// It uses object pooling to minimize allocations during high-throughput ingestion.
// The unmarshaled WriteRequest references the original protobuf bytes for strings
// (zero-copy), so the source bytes must remain valid while the WriteRequest is in use.
type WriteRequestUnmarshaler struct {
	wr WriteRequest

	// Pooled slices - kept across unmarshals, only length is reset.
	timeseriesPool     []prompb.TimeSeries
	labelsPool         []labelpb.ZLabel // For timeseries labels.
	exemplarLabelsPool []labelpb.ZLabel // Separate pool for exemplar labels.
	samplesPool        []prompb.Sample
	exemplarsPool      []prompb.Exemplar
	histogramsPool     []prompb.Histogram

	// For histogram fields.
	bucketSpansPool []prompb.BucketSpan
	int64Pool       []int64
	float64Pool     []float64
}

// Reset resets wru for reuse.
func (wru *WriteRequestUnmarshaler) Reset() {
	// Clear WriteRequest fields.
	wru.wr.Tenant = ""
	wru.wr.Replica = 0
	wru.wr.Timeseries = nil

	// Clear timeseries pool - reset nested slices to nil to release references.
	for i := range wru.timeseriesPool {
		wru.timeseriesPool[i].Labels = nil
		wru.timeseriesPool[i].Samples = nil
		wru.timeseriesPool[i].Exemplars = nil
		wru.timeseriesPool[i].Histograms = nil
	}
	wru.timeseriesPool = wru.timeseriesPool[:0]

	// Clear labels pool - clear strings to release references to protobuf bytes.
	for i := range wru.labelsPool {
		wru.labelsPool[i].Name = ""
		wru.labelsPool[i].Value = ""
	}
	wru.labelsPool = wru.labelsPool[:0]

	// Clear exemplar labels pool.
	for i := range wru.exemplarLabelsPool {
		wru.exemplarLabelsPool[i].Name = ""
		wru.exemplarLabelsPool[i].Value = ""
	}
	wru.exemplarLabelsPool = wru.exemplarLabelsPool[:0]

	// Clear samples pool.
	for i := range wru.samplesPool {
		wru.samplesPool[i] = prompb.Sample{}
	}
	wru.samplesPool = wru.samplesPool[:0]

	// Clear exemplars pool.
	for i := range wru.exemplarsPool {
		wru.exemplarsPool[i].Labels = nil
		wru.exemplarsPool[i].Value = 0
		wru.exemplarsPool[i].Timestamp = 0
	}
	wru.exemplarsPool = wru.exemplarsPool[:0]

	// Clear histograms pool.
	for i := range wru.histogramsPool {
		wru.histogramsPool[i] = prompb.Histogram{}
	}
	wru.histogramsPool = wru.histogramsPool[:0]

	// Clear histogram helper pools.
	for i := range wru.bucketSpansPool {
		wru.bucketSpansPool[i] = prompb.BucketSpan{}
	}
	wru.bucketSpansPool = wru.bucketSpansPool[:0]

	wru.int64Pool = wru.int64Pool[:0]
	wru.float64Pool = wru.float64Pool[:0]
}

// UnmarshalProtobuf parses protobuf-encoded src into WriteRequest.
//
// The returned WriteRequest is valid until Reset() or PutWriteRequestUnmarshaler() is called.
// The src slice must remain valid for the lifetime of the returned WriteRequest,
// as string fields (Tenant, label names/values) reference the original bytes directly.
func (wru *WriteRequestUnmarshaler) UnmarshalProtobuf(src []byte) (*WriteRequest, error) {
	wru.Reset()

	// WriteRequest proto:
	// message WriteRequest {
	//   repeated TimeSeries timeseries = 1;
	//   string tenant = 2;
	//   int64 replica = 3;
	// }

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, fmt.Errorf("cannot read next field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // repeated TimeSeries timeseries = 1
			data, ok := fc.MessageData()
			if !ok {
				return nil, fmt.Errorf("cannot read timeseries data")
			}
			if err := wru.unmarshalTimeSeries(data); err != nil {
				return nil, fmt.Errorf("cannot unmarshal timeseries: %w", err)
			}

		case 2: // string tenant = 2
			tenant, ok := fc.String()
			if !ok {
				return nil, fmt.Errorf("cannot read tenant")
			}
			wru.wr.Tenant = tenant

		case 3: // int64 replica = 3
			replica, ok := fc.Int64()
			if !ok {
				return nil, fmt.Errorf("cannot read replica")
			}
			wru.wr.Replica = replica
		}
	}

	// Assign the pooled timeseries slice to the WriteRequest.
	wru.wr.Timeseries = wru.timeseriesPool

	return &wru.wr, nil
}

func (wru *WriteRequestUnmarshaler) unmarshalTimeSeries(src []byte) error {
	// Grow timeseries pool.
	if len(wru.timeseriesPool) < cap(wru.timeseriesPool) {
		wru.timeseriesPool = wru.timeseriesPool[:len(wru.timeseriesPool)+1]
	} else {
		wru.timeseriesPool = append(wru.timeseriesPool, prompb.TimeSeries{})
	}
	ts := &wru.timeseriesPool[len(wru.timeseriesPool)-1]

	// Track where labels/samples start in pools for this timeseries.
	labelsStart := len(wru.labelsPool)
	samplesStart := len(wru.samplesPool)
	exemplarsStart := len(wru.exemplarsPool)
	histogramsStart := len(wru.histogramsPool)

	// TimeSeries proto:
	// message TimeSeries {
	//   repeated Label labels = 1;
	//   repeated Sample samples = 2;
	//   repeated Exemplar exemplars = 3;
	//   repeated Histogram histograms = 4;
	// }

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read timeseries field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // repeated Label labels = 1
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read label data")
			}
			if err := wru.unmarshalLabel(data); err != nil {
				return fmt.Errorf("cannot unmarshal label: %w", err)
			}

		case 2: // repeated Sample samples = 2
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read sample data")
			}
			if err := wru.unmarshalSample(data); err != nil {
				return fmt.Errorf("cannot unmarshal sample: %w", err)
			}

		case 3: // repeated Exemplar exemplars = 3
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read exemplar data")
			}
			if err := wru.unmarshalExemplar(data); err != nil {
				return fmt.Errorf("cannot unmarshal exemplar: %w", err)
			}

		case 4: // repeated Histogram histograms = 4
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read histogram data")
			}
			if err := wru.unmarshalHistogram(data); err != nil {
				return fmt.Errorf("cannot unmarshal histogram: %w", err)
			}
		}
	}

	// Assign slices from pool (these are windows into the pooled slices).
	ts.Labels = wru.labelsPool[labelsStart:]
	ts.Samples = wru.samplesPool[samplesStart:]
	ts.Exemplars = wru.exemplarsPool[exemplarsStart:]
	ts.Histograms = wru.histogramsPool[histogramsStart:]

	return nil
}

func (wru *WriteRequestUnmarshaler) unmarshalLabel(src []byte) error {
	// Grow labels pool.
	if len(wru.labelsPool) < cap(wru.labelsPool) {
		wru.labelsPool = wru.labelsPool[:len(wru.labelsPool)+1]
	} else {
		wru.labelsPool = append(wru.labelsPool, labelpb.ZLabel{})
	}
	lbl := &wru.labelsPool[len(wru.labelsPool)-1]

	// Label proto:
	// message Label {
	//   string name = 1;
	//   string value = 2;
	// }

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read label field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // string name = 1
			name, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read label name")
			}
			lbl.Name = name // Zero-copy: points into src.

		case 2: // string value = 2
			value, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read label value")
			}
			lbl.Value = value // Zero-copy: points into src.
		}
	}
	return nil
}

func (wru *WriteRequestUnmarshaler) unmarshalSample(src []byte) error {
	// Grow samples pool.
	if len(wru.samplesPool) < cap(wru.samplesPool) {
		wru.samplesPool = wru.samplesPool[:len(wru.samplesPool)+1]
	} else {
		wru.samplesPool = append(wru.samplesPool, prompb.Sample{})
	}
	s := &wru.samplesPool[len(wru.samplesPool)-1]

	// Sample proto:
	// message Sample {
	//   double value = 1;
	//   int64 timestamp = 2;
	// }

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read sample field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // double value = 1 (fixed64 wire type)
			value, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read sample value")
			}
			s.Value = value

		case 2: // int64 timestamp = 2 (varint wire type)
			ts, ok := fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read sample timestamp")
			}
			s.Timestamp = ts
		}
	}
	return nil
}

func (wru *WriteRequestUnmarshaler) unmarshalExemplar(src []byte) error {
	// Grow exemplars pool.
	if len(wru.exemplarsPool) < cap(wru.exemplarsPool) {
		wru.exemplarsPool = wru.exemplarsPool[:len(wru.exemplarsPool)+1]
	} else {
		wru.exemplarsPool = append(wru.exemplarsPool, prompb.Exemplar{})
	}
	ex := &wru.exemplarsPool[len(wru.exemplarsPool)-1]

	// Track where labels start for this exemplar (using separate pool).
	labelsStart := len(wru.exemplarLabelsPool)

	// Exemplar proto:
	// message Exemplar {
	//   repeated Label labels = 1;
	//   double value = 2;
	//   int64 timestamp = 3;
	// }

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read exemplar field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // repeated Label labels = 1
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read exemplar label data")
			}
			if err := wru.unmarshalExemplarLabel(data); err != nil {
				return fmt.Errorf("cannot unmarshal exemplar label: %w", err)
			}

		case 2: // double value = 2
			value, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read exemplar value")
			}
			ex.Value = value

		case 3: // int64 timestamp = 3
			ts, ok := fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read exemplar timestamp")
			}
			ex.Timestamp = ts
		}
	}

	// Assign labels from exemplar labels pool.
	ex.Labels = wru.exemplarLabelsPool[labelsStart:]

	return nil
}

func (wru *WriteRequestUnmarshaler) unmarshalExemplarLabel(src []byte) error {
	// Grow exemplar labels pool.
	if len(wru.exemplarLabelsPool) < cap(wru.exemplarLabelsPool) {
		wru.exemplarLabelsPool = wru.exemplarLabelsPool[:len(wru.exemplarLabelsPool)+1]
	} else {
		wru.exemplarLabelsPool = append(wru.exemplarLabelsPool, labelpb.ZLabel{})
	}
	lbl := &wru.exemplarLabelsPool[len(wru.exemplarLabelsPool)-1]

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read exemplar label field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // string name = 1
			name, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read exemplar label name")
			}
			lbl.Name = name

		case 2: // string value = 2
			value, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read exemplar label value")
			}
			lbl.Value = value
		}
	}
	return nil
}

func (wru *WriteRequestUnmarshaler) unmarshalHistogram(src []byte) error {
	// Grow histograms pool.
	if len(wru.histogramsPool) < cap(wru.histogramsPool) {
		wru.histogramsPool = wru.histogramsPool[:len(wru.histogramsPool)+1]
	} else {
		wru.histogramsPool = append(wru.histogramsPool, prompb.Histogram{})
	}
	h := &wru.histogramsPool[len(wru.histogramsPool)-1]
	*h = prompb.Histogram{} // Reset to zero value.

	// Track where bucket spans and deltas/counts start.
	negSpansStart := len(wru.bucketSpansPool)
	posSpansStart := -1 // Will be set when we see positive spans.
	negDeltasStart := len(wru.int64Pool)
	posDeltasStart := -1
	negCountsStart := len(wru.float64Pool)
	posCountsStart := -1
	customValuesStart := -1

	// Histogram proto (simplified, showing key fields):
	// message Histogram {
	//   oneof count { uint64 count_int = 1; double count_float = 2; }
	//   double sum = 3;
	//   sint32 schema = 4;
	//   double zero_threshold = 5;
	//   oneof zero_count { uint64 zero_count_int = 6; double zero_count_float = 7; }
	//   repeated BucketSpan negative_spans = 8;
	//   repeated sint64 negative_deltas = 9;
	//   repeated double negative_counts = 10;
	//   repeated BucketSpan positive_spans = 11;
	//   repeated sint64 positive_deltas = 12;
	//   repeated double positive_counts = 13;
	//   ResetHint reset_hint = 14;
	//   int64 timestamp = 15;
	//   repeated double custom_values = 16;
	// }

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read histogram field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // uint64 count_int = 1
			countInt, ok := fc.Uint64()
			if !ok {
				return fmt.Errorf("cannot read count_int")
			}
			h.Count = &prompb.Histogram_CountInt{CountInt: countInt}

		case 2: // double count_float = 2
			countFloat, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read count_float")
			}
			h.Count = &prompb.Histogram_CountFloat{CountFloat: countFloat}

		case 3: // double sum = 3
			sum, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read sum")
			}
			h.Sum = sum

		case 4: // sint32 schema = 4
			schema, ok := fc.Sint32()
			if !ok {
				return fmt.Errorf("cannot read schema")
			}
			h.Schema = schema

		case 5: // double zero_threshold = 5
			zt, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read zero_threshold")
			}
			h.ZeroThreshold = zt

		case 6: // uint64 zero_count_int = 6
			zci, ok := fc.Uint64()
			if !ok {
				return fmt.Errorf("cannot read zero_count_int")
			}
			h.ZeroCount = &prompb.Histogram_ZeroCountInt{ZeroCountInt: zci}

		case 7: // double zero_count_float = 7
			zcf, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read zero_count_float")
			}
			h.ZeroCount = &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: zcf}

		case 8: // repeated BucketSpan negative_spans = 8
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read negative_spans data")
			}
			if err := wru.unmarshalBucketSpan(data); err != nil {
				return fmt.Errorf("cannot unmarshal negative_span: %w", err)
			}

		case 9: // repeated sint64 negative_deltas = 9 (packed)
			var ok bool
			wru.int64Pool, ok = fc.UnpackSint64s(wru.int64Pool)
			if !ok {
				// Try single value.
				val, ok := fc.Sint64()
				if !ok {
					return fmt.Errorf("cannot read negative_deltas")
				}
				wru.int64Pool = append(wru.int64Pool, val)
			}

		case 10: // repeated double negative_counts = 10 (packed)
			var ok bool
			wru.float64Pool, ok = fc.UnpackDoubles(wru.float64Pool)
			if !ok {
				// Try single value.
				val, ok := fc.Double()
				if !ok {
					return fmt.Errorf("cannot read negative_counts")
				}
				wru.float64Pool = append(wru.float64Pool, val)
			}

		case 11: // repeated BucketSpan positive_spans = 11
			if posSpansStart < 0 {
				posSpansStart = len(wru.bucketSpansPool)
			}
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read positive_spans data")
			}
			if err := wru.unmarshalBucketSpan(data); err != nil {
				return fmt.Errorf("cannot unmarshal positive_span: %w", err)
			}

		case 12: // repeated sint64 positive_deltas = 12 (packed)
			if posDeltasStart < 0 {
				posDeltasStart = len(wru.int64Pool)
			}
			var ok bool
			wru.int64Pool, ok = fc.UnpackSint64s(wru.int64Pool)
			if !ok {
				val, ok := fc.Sint64()
				if !ok {
					return fmt.Errorf("cannot read positive_deltas")
				}
				wru.int64Pool = append(wru.int64Pool, val)
			}

		case 13: // repeated double positive_counts = 13 (packed)
			if posCountsStart < 0 {
				posCountsStart = len(wru.float64Pool)
			}
			var ok bool
			wru.float64Pool, ok = fc.UnpackDoubles(wru.float64Pool)
			if !ok {
				val, ok := fc.Double()
				if !ok {
					return fmt.Errorf("cannot read positive_counts")
				}
				wru.float64Pool = append(wru.float64Pool, val)
			}

		case 14: // ResetHint reset_hint = 14
			rh, ok := fc.Int32()
			if !ok {
				return fmt.Errorf("cannot read reset_hint")
			}
			h.ResetHint = prompb.Histogram_ResetHint(rh)

		case 15: // int64 timestamp = 15
			ts, ok := fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read timestamp")
			}
			h.Timestamp = ts

		case 16: // repeated double custom_values = 16 (packed)
			if customValuesStart < 0 {
				customValuesStart = len(wru.float64Pool)
			}
			var ok bool
			wru.float64Pool, ok = fc.UnpackDoubles(wru.float64Pool)
			if !ok {
				val, ok := fc.Double()
				if !ok {
					return fmt.Errorf("cannot read custom_values")
				}
				wru.float64Pool = append(wru.float64Pool, val)
			}
		}
	}

	// Assign bucket spans.
	if posSpansStart < 0 {
		posSpansStart = len(wru.bucketSpansPool)
	}
	h.NegativeSpans = nilIfEmptyBucketSpans(wru.bucketSpansPool[negSpansStart:posSpansStart])
	h.PositiveSpans = nilIfEmptyBucketSpans(wru.bucketSpansPool[posSpansStart:])

	// Assign deltas.
	if posDeltasStart < 0 {
		posDeltasStart = len(wru.int64Pool)
	}
	h.NegativeDeltas = nilIfEmptyInt64(wru.int64Pool[negDeltasStart:posDeltasStart])
	h.PositiveDeltas = nilIfEmptyInt64(wru.int64Pool[posDeltasStart:])

	// Assign counts and custom values.
	// This is trickier because negative_counts, positive_counts, and custom_values
	// all go into the same float64Pool. We need to track boundaries.
	if posCountsStart < 0 {
		posCountsStart = len(wru.float64Pool)
	}
	if customValuesStart < 0 {
		customValuesStart = len(wru.float64Pool)
	}

	h.NegativeCounts = nilIfEmptyFloat64(wru.float64Pool[negCountsStart:posCountsStart])
	h.PositiveCounts = nilIfEmptyFloat64(wru.float64Pool[posCountsStart:customValuesStart])
	h.CustomValues = nilIfEmptyFloat64(wru.float64Pool[customValuesStart:])

	return nil
}

// Helper functions to return nil instead of empty slices for protobuf compatibility.
func nilIfEmptyBucketSpans(s []prompb.BucketSpan) []prompb.BucketSpan {
	if len(s) == 0 {
		return nil
	}
	return s
}

func nilIfEmptyInt64(s []int64) []int64 {
	if len(s) == 0 {
		return nil
	}
	return s
}

func nilIfEmptyFloat64(s []float64) []float64 {
	if len(s) == 0 {
		return nil
	}
	return s
}

func (wru *WriteRequestUnmarshaler) unmarshalBucketSpan(src []byte) error {
	// Grow bucket spans pool.
	if len(wru.bucketSpansPool) < cap(wru.bucketSpansPool) {
		wru.bucketSpansPool = wru.bucketSpansPool[:len(wru.bucketSpansPool)+1]
	} else {
		wru.bucketSpansPool = append(wru.bucketSpansPool, prompb.BucketSpan{})
	}
	bs := &wru.bucketSpansPool[len(wru.bucketSpansPool)-1]

	// BucketSpan proto:
	// message BucketSpan {
	//   sint32 offset = 1;
	//   uint32 length = 2;
	// }

	var fc easyproto.FieldContext
	var err error

	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read bucket_span field: %w", err)
		}

		switch fc.FieldNum {
		case 1: // sint32 offset = 1
			offset, ok := fc.Sint32()
			if !ok {
				return fmt.Errorf("cannot read bucket_span offset")
			}
			bs.Offset = offset

		case 2: // uint32 length = 2
			length, ok := fc.Uint32()
			if !ok {
				return fmt.Errorf("cannot read bucket_span length")
			}
			bs.Length = length
		}
	}
	return nil
}
