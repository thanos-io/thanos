// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package writecapnp

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/pool"
)

var symbolsPool = pool.MustNewBucketedPool[string](256, 65536, 2, 0)

type HistogramSample struct {
	Timestamp      int64
	Histogram      *histogram.Histogram
	FloatHistogram *histogram.FloatHistogram
}

type FloatSample struct {
	Value     float64
	Timestamp int64
}

type Series struct {
	Labels     labels.Labels
	Samples    []FloatSample
	Histograms []HistogramSample
	Exemplars  []exemplar.Exemplar
}

type Request struct {
	i       int
	symbols *[]string
	builder labels.ScratchBuilder
	series  TimeSeries_List
}

func NewRequest(wr WriteRequest) (*Request, error) {
	ts, err := wr.TimeSeries()
	if err != nil {
		return nil, err
	}
	symTable, err := wr.Symbols()
	if err != nil {
		return nil, err
	}
	data, err := symTable.Data()
	if err != nil {
		return nil, err
	}
	offsets, err := symTable.Offsets()
	if err != nil {
		return nil, err
	}

	strings, _ := symbolsPool.Get(offsets.Len())
	start := uint32(0)
	for i := 0; i < offsets.Len(); i++ {
		end := offsets.At(i)
		if start == end {
			*strings = append(*strings, "")
		} else {
			b := data[start:end]
			*strings = append(*strings, unsafe.String(&b[0], len(b)))
		}
		start = end
	}

	return &Request{
		i:       -1,
		symbols: strings,
		series:  ts,
		builder: labels.NewScratchBuilder(8),
	}, nil
}

func (s *Request) Next() bool {
	s.i++
	return s.i < s.series.Len()
}

func (s *Request) At(t *Series) error {
	lbls, err := s.series.At(s.i).Labels()
	if err != nil {
		return err
	}

	s.builder.Reset()
	for i := 0; i < lbls.Len(); i++ {
		lbl := lbls.At(i)
		s.builder.Add((*s.symbols)[lbl.Name()], (*s.symbols)[lbl.Value()])
	}
	s.builder.Overwrite(&t.Labels)

	samples, err := s.series.At(s.i).Samples()
	if err != nil {
		return err
	}
	t.Samples = t.Samples[:0]
	for i := 0; i < samples.Len(); i++ {
		sample := samples.At(i)
		t.Samples = append(t.Samples, FloatSample{
			Value:     sample.Value(),
			Timestamp: sample.Timestamp(),
		})
	}

	histograms, err := s.series.At(s.i).Histograms()
	if err != nil {
		return err
	}
	t.Histograms = t.Histograms[:0]
	for i := 0; i < histograms.Len(); i++ {
		h, err := s.readHistogram(histograms.At(i))
		if err != nil {
			return err
		}
		t.Histograms = append(t.Histograms, h)
	}

	exemplars, err := s.series.At(s.i).Exemplars()
	if err != nil {
		return err
	}
	t.Exemplars = t.Exemplars[:0]
	for i := 0; i < exemplars.Len(); i++ {
		ex, err := s.readExemplar(s.symbols, exemplars.At(i))
		if err != nil {
			return err
		}
		t.Exemplars = append(t.Exemplars, ex)
	}
	return nil
}

func (s *Request) readHistogram(src Histogram) (HistogramSample, error) {
	var (
		h   *histogram.Histogram
		fh  *histogram.FloatHistogram
		err error
	)
	if src.Count().Which() == Histogram_count_Which_countInt {
		h = &histogram.Histogram{
			CounterResetHint: histogram.CounterResetHint(src.ResetHint()),
			Count:            src.Count().CountInt(),
			Sum:              src.Sum(),
			Schema:           src.Schema(),
			ZeroThreshold:    src.ZeroThreshold(),
			ZeroCount:        src.ZeroCount().ZeroCountInt(),
		}
		h.PositiveSpans, h.NegativeSpans, err = createSpans(src)
		if err != nil {
			return HistogramSample{}, err
		}

		positiveDeltas, err := src.PositiveDeltas()
		if err != nil {
			return HistogramSample{}, err
		}
		if positiveDeltas.Len() > 0 {
			h.PositiveBuckets = make([]int64, positiveDeltas.Len())
			for i := 0; i < positiveDeltas.Len(); i++ {
				h.PositiveBuckets[i] = positiveDeltas.At(i)
			}
		}

		negativeDeltas, err := src.NegativeDeltas()
		if err != nil {
			return HistogramSample{}, err
		}
		if negativeDeltas.Len() > 0 {
			h.NegativeBuckets = make([]int64, negativeDeltas.Len())
			for i := 0; i < negativeDeltas.Len(); i++ {
				h.NegativeBuckets[i] = negativeDeltas.At(i)
			}
		}
	} else {
		fh = &histogram.FloatHistogram{
			CounterResetHint: histogram.CounterResetHint(src.ResetHint()),
			Count:            src.Count().CountFloat(),
			Sum:              src.Sum(),
			Schema:           src.Schema(),
			ZeroThreshold:    src.ZeroThreshold(),
			ZeroCount:        src.ZeroCount().ZeroCountFloat(),
		}
		fh.PositiveSpans, fh.NegativeSpans, err = createSpans(src)
		if err != nil {
			return HistogramSample{}, err
		}

		positiveCounts, err := src.PositiveCounts()
		if err != nil {
			return HistogramSample{}, err
		}
		if positiveCounts.Len() > 0 {
			fh.PositiveBuckets = make([]float64, positiveCounts.Len())
			for i := 0; i < positiveCounts.Len(); i++ {
				fh.PositiveBuckets[i] = positiveCounts.At(i)
			}
		}

		negativeCounts, err := src.NegativeCounts()
		if err != nil {
			return HistogramSample{}, err
		}
		if negativeCounts.Len() > 0 {
			fh.NegativeBuckets = make([]float64, negativeCounts.Len())
			for i := 0; i < negativeCounts.Len(); i++ {
				fh.NegativeBuckets[i] = negativeCounts.At(i)
			}
		}
	}

	return HistogramSample{
		Timestamp:      src.Timestamp(),
		Histogram:      h,
		FloatHistogram: fh,
	}, nil
}

type spanGetter interface {
	PositiveSpans() (BucketSpan_List, error)
	NegativeSpans() (BucketSpan_List, error)
}

func createSpans(src spanGetter) ([]histogram.Span, []histogram.Span, error) {
	positiveSpans, err := src.PositiveSpans()
	if err != nil {
		return nil, nil, err
	}
	negativeSpans, err := src.NegativeSpans()
	if err != nil {
		return nil, nil, err
	}
	return copySpans(positiveSpans), copySpans(negativeSpans), nil
}

func copySpans(src BucketSpan_List) []histogram.Span {
	spans := make([]histogram.Span, src.Len())
	for i := 0; i < src.Len(); i++ {
		spans[i].Offset = src.At(i).Offset()
		spans[i].Length = src.At(i).Length()
	}
	return spans
}

func (s *Request) readExemplar(symbols *[]string, e Exemplar) (exemplar.Exemplar, error) {
	ex := exemplar.Exemplar{}
	lbls, err := e.Labels()
	if err != nil {
		return ex, err
	}

	builder := labels.ScratchBuilder{}
	for i := 0; i < lbls.Len(); i++ {
		builder.Add((*symbols)[lbls.At(i).Name()], (*symbols)[lbls.At(i).Value()])
	}
	ex.Labels = builder.Labels()
	ex.Value = e.Value()
	ex.Ts = e.Timestamp()
	return ex, nil
}

func (s *Request) Close() error {
	symbolsPool.Put(s.symbols)
	return nil
}
