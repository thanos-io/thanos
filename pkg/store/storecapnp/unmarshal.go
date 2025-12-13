// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecapnp

import (
	"unsafe"

	"capnproto.org/go/capnp/v3"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

var symbolsPool = pool.MustNewBucketedPool[string](256, 65536, 2, 0)

// SymbolsDecoder decodes symbol table from capnp messages.
type SymbolsDecoder struct {
	symbols *[]string
}

// NewSymbolsDecoder creates a new symbol table decoder from a capnp Symbols struct.
func NewSymbolsDecoder(syms Symbols) (*SymbolsDecoder, error) {
	data, err := syms.Data()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols data")
	}
	offsets, err := syms.Offsets()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols offsets")
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

	return &SymbolsDecoder{symbols: strings}, nil
}

// Get returns the symbol at the given index.
func (d *SymbolsDecoder) Get(idx uint32) string {
	if d.symbols == nil || int(idx) >= len(*d.symbols) {
		return ""
	}
	return (*d.symbols)[idx]
}

// Close releases the symbol table back to the pool.
func (d *SymbolsDecoder) Close() {
	if d.symbols != nil {
		symbolsPool.Put(d.symbols)
		d.symbols = nil
	}
}

// UnmarshalSeriesRequest unmarshals a capnp SeriesRequest from bytes.
func UnmarshalSeriesRequest(data []byte) (*storepb.SeriesRequest, error) {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal capnp message")
	}
	return ParseSeriesRequest(msg)
}

// UnmarshalSeriesRequestPacked unmarshals a packed capnp SeriesRequest from bytes.
func UnmarshalSeriesRequestPacked(data []byte) (*storepb.SeriesRequest, error) {
	msg, err := capnp.UnmarshalPacked(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal packed capnp message")
	}
	return ParseSeriesRequest(msg)
}

// ParseSeriesRequest parses a capnp SeriesRequest from a message.
func ParseSeriesRequest(msg *capnp.Message) (*storepb.SeriesRequest, error) {
	capnpReq, err := ReadRootSeriesRequest(msg)
	if err != nil {
		return nil, errors.Wrap(err, "read root series request")
	}

	symbols, err := capnpReq.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseSeriesRequestFrom(capnpReq, decoder)
}

// ParseSeriesRequestFrom parses a capnp SeriesRequest using a pre-existing symbols decoder.
func ParseSeriesRequestFrom(capnpReq SeriesRequest, decoder *SymbolsDecoder) (*storepb.SeriesRequest, error) {
	req := &storepb.SeriesRequest{
		MinTime:                 capnpReq.MinTime(),
		MaxTime:                 capnpReq.MaxTime(),
		MaxResolutionWindow:     capnpReq.MaxResolutionWindow(),
		PartialResponseDisabled: capnpReq.PartialResponseDisabled(),
		PartialResponseStrategy: storepb.PartialResponseStrategy(capnpReq.PartialResponseStrategy()),
		SkipChunks:              capnpReq.SkipChunks(),
		Step:                    capnpReq.Step(),
		Range:                   capnpReq.Range(),
		Limit:                   capnpReq.Limit(),
	}

	// Parse hints
	if capnpReq.HasHints() {
		hintsData, err := capnpReq.Hints()
		if err != nil {
			return nil, errors.Wrap(err, "get hints")
		}
		if len(hintsData) > 0 {
			req.Hints = &types.Any{}
			if err := req.Hints.Unmarshal(hintsData); err != nil {
				return nil, errors.Wrap(err, "unmarshal hints")
			}
		}
	}

	// Parse matchers
	if capnpReq.HasMatchers() {
		matchers, err := capnpReq.Matchers()
		if err != nil {
			return nil, errors.Wrap(err, "get matchers")
		}
		req.Matchers = make([]storepb.LabelMatcher, matchers.Len())
		for i := 0; i < matchers.Len(); i++ {
			m := matchers.At(i)
			req.Matchers[i] = storepb.LabelMatcher{
				Type:  storepb.LabelMatcher_Type(m.Type()),
				Name:  decoder.Get(m.Name()),
				Value: decoder.Get(m.Value()),
			}
		}
	}

	// Parse aggregates
	if capnpReq.HasAggregates() {
		aggregates, err := capnpReq.Aggregates()
		if err != nil {
			return nil, errors.Wrap(err, "get aggregates")
		}
		req.Aggregates = make([]storepb.Aggr, aggregates.Len())
		for i := 0; i < aggregates.Len(); i++ {
			req.Aggregates[i] = storepb.Aggr(aggregates.At(i))
		}
	}

	// Parse QueryHints
	if capnpReq.HasQueryHints() {
		qh, err := capnpReq.QueryHints()
		if err != nil {
			return nil, errors.Wrap(err, "get query hints")
		}
		req.QueryHints = &storepb.QueryHints{
			StepMillis: qh.StepMillis(),
		}

		if qh.HasFunc() {
			f, err := qh.Func()
			if err != nil {
				return nil, errors.Wrap(err, "get func")
			}
			req.QueryHints.Func = &storepb.Func{
				Name: decoder.Get(f.Name()),
			}
		}

		if qh.HasGrouping() {
			g, err := qh.Grouping()
			if err != nil {
				return nil, errors.Wrap(err, "get grouping")
			}
			req.QueryHints.Grouping = &storepb.Grouping{
				By: g.By(),
			}
			if g.HasLabels() {
				labels, err := g.Labels()
				if err != nil {
					return nil, errors.Wrap(err, "get grouping labels")
				}
				req.QueryHints.Grouping.Labels = make([]string, labels.Len())
				for i := 0; i < labels.Len(); i++ {
					req.QueryHints.Grouping.Labels[i] = decoder.Get(labels.At(i))
				}
			}
		}

		if qh.HasRange() {
			r, err := qh.Range()
			if err != nil {
				return nil, errors.Wrap(err, "get range")
			}
			req.QueryHints.Range = &storepb.Range{
				Millis: r.Millis(),
			}
		}
	}

	// Parse ShardInfo
	if capnpReq.HasShardInfo() {
		si, err := capnpReq.ShardInfo()
		if err != nil {
			return nil, errors.Wrap(err, "get shard info")
		}
		req.ShardInfo = &storepb.ShardInfo{
			ShardIndex:  si.ShardIndex(),
			TotalShards: si.TotalShards(),
			By:          si.By(),
		}
		if si.HasLabels() {
			labels, err := si.Labels()
			if err != nil {
				return nil, errors.Wrap(err, "get shard labels")
			}
			req.ShardInfo.Labels = make([]string, labels.Len())
			for i := 0; i < labels.Len(); i++ {
				req.ShardInfo.Labels[i] = decoder.Get(labels.At(i))
			}
		}
	}

	// Parse WithoutReplicaLabels
	if capnpReq.HasWithoutReplicaLabels() {
		wrl, err := capnpReq.WithoutReplicaLabels()
		if err != nil {
			return nil, errors.Wrap(err, "get without replica labels")
		}
		req.WithoutReplicaLabels = make([]string, wrl.Len())
		for i := 0; i < wrl.Len(); i++ {
			req.WithoutReplicaLabels[i] = decoder.Get(wrl.At(i))
		}
	}

	return req, nil
}

// UnmarshalSeriesResponse unmarshals a capnp SeriesResponse from bytes.
func UnmarshalSeriesResponse(data []byte) (*storepb.SeriesResponse, error) {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal capnp message")
	}
	return ParseSeriesResponse(msg)
}

// UnmarshalSeriesResponsePacked unmarshals a packed capnp SeriesResponse from bytes.
func UnmarshalSeriesResponsePacked(data []byte) (*storepb.SeriesResponse, error) {
	msg, err := capnp.UnmarshalPacked(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal packed capnp message")
	}
	return ParseSeriesResponse(msg)
}

// ParseSeriesResponse parses a capnp SeriesResponse from a message.
func ParseSeriesResponse(msg *capnp.Message) (*storepb.SeriesResponse, error) {
	capnpResp, err := ReadRootSeriesResponse(msg)
	if err != nil {
		return nil, errors.Wrap(err, "read root series response")
	}

	symbols, err := capnpResp.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseSeriesResponseFrom(capnpResp, decoder)
}

// ParseSeriesResponseFrom parses a capnp SeriesResponse using a pre-existing symbols decoder.
func ParseSeriesResponseFrom(capnpResp SeriesResponse, decoder *SymbolsDecoder) (*storepb.SeriesResponse, error) {
	resp := &storepb.SeriesResponse{}

	switch capnpResp.Which() {
	case SeriesResponse_Which_series:
		series, err := capnpResp.Series()
		if err != nil {
			return nil, errors.Wrap(err, "get series")
		}
		pbSeries, err := parseSeries(series, decoder)
		if err != nil {
			return nil, errors.Wrap(err, "parse series")
		}
		resp.Result = &storepb.SeriesResponse_Series{Series: pbSeries}

	case SeriesResponse_Which_warning:
		warning, err := capnpResp.Warning()
		if err != nil {
			return nil, errors.Wrap(err, "get warning")
		}
		resp.Result = &storepb.SeriesResponse_Warning{Warning: warning}

	case SeriesResponse_Which_hints:
		hintsData, err := capnpResp.Hints()
		if err != nil {
			return nil, errors.Wrap(err, "get hints")
		}
		hints := &types.Any{}
		if err := hints.Unmarshal(hintsData); err != nil {
			return nil, errors.Wrap(err, "unmarshal hints")
		}
		resp.Result = &storepb.SeriesResponse_Hints{Hints: hints}
	}

	return resp, nil
}

// InfoResult contains the parsed info response data.
type InfoResult struct {
	LabelSets                    []labels.Labels
	MinTime                      int64
	MaxTime                      int64
	SupportsSharding             bool
	SupportsWithoutReplicaLabels bool
	TSDBInfos                    []infopb.TSDBInfo
}

// ParseInfoResponseFrom parses a capnp InfoResponse using a pre-existing symbols decoder.
func ParseInfoResponseFrom(capnpResp InfoResponse, decoder *SymbolsDecoder) (*InfoResult, error) {
	result := &InfoResult{}

	// Parse store info
	if capnpResp.HasStoreInfo() {
		storeInfo, err := capnpResp.StoreInfo()
		if err != nil {
			return nil, errors.Wrap(err, "get store info")
		}
		result.MinTime = storeInfo.MinTime()
		result.MaxTime = storeInfo.MaxTime()
		result.SupportsSharding = storeInfo.SupportsSharding()
		result.SupportsWithoutReplicaLabels = storeInfo.SupportsWithoutReplicaLabels()

		// Parse TSDB infos
		if storeInfo.HasTsdbInfos() {
			tsdbInfos, err := storeInfo.TsdbInfos()
			if err != nil {
				return nil, errors.Wrap(err, "get tsdb infos")
			}
			result.TSDBInfos = make([]infopb.TSDBInfo, tsdbInfos.Len())
			for i := 0; i < tsdbInfos.Len(); i++ {
				ti := tsdbInfos.At(i)
				result.TSDBInfos[i].MinTime = ti.MinTime()
				result.TSDBInfos[i].MaxTime = ti.MaxTime()

				if ti.HasLabels() {
					tsdbLabels, err := ti.Labels()
					if err != nil {
						return nil, errors.Wrap(err, "get tsdb labels")
					}
					if tsdbLabels.HasLabels() {
						lbls, err := tsdbLabels.Labels()
						if err != nil {
							return nil, errors.Wrap(err, "get tsdb labels list")
						}
						result.TSDBInfos[i].Labels.Labels = make([]labelpb.ZLabel, lbls.Len())
						for j := 0; j < lbls.Len(); j++ {
							lbl := lbls.At(j)
							result.TSDBInfos[i].Labels.Labels[j] = labelpb.ZLabel{
								Name:  decoder.Get(lbl.Name()),
								Value: decoder.Get(lbl.Value()),
							}
						}
					}
				}
			}
		}
	}

	// Parse label sets
	if capnpResp.HasLabelSets() {
		labelSets, err := capnpResp.LabelSets()
		if err != nil {
			return nil, errors.Wrap(err, "get label sets")
		}
		result.LabelSets = make([]labels.Labels, labelSets.Len())
		for i := 0; i < labelSets.Len(); i++ {
			ls := labelSets.At(i)
			if ls.HasLabels() {
				lbls, err := ls.Labels()
				if err != nil {
					return nil, errors.Wrap(err, "get label set labels")
				}
				builder := labels.NewScratchBuilder(lbls.Len())
				for j := 0; j < lbls.Len(); j++ {
					lbl := lbls.At(j)
					builder.Add(decoder.Get(lbl.Name()), decoder.Get(lbl.Value()))
				}
				result.LabelSets[i] = builder.Labels()
			}
		}
	}

	return result, nil
}

func parseSeries(capnpSeries Series, decoder *SymbolsDecoder) (*storepb.Series, error) {
	series := &storepb.Series{}

	// Parse labels
	if capnpSeries.HasLabels() {
		labels, err := capnpSeries.Labels()
		if err != nil {
			return nil, errors.Wrap(err, "get labels")
		}
		series.Labels = make([]labelpb.ZLabel, labels.Len())
		for i := 0; i < labels.Len(); i++ {
			lbl := labels.At(i)
			series.Labels[i] = labelpb.ZLabel{
				Name:  decoder.Get(lbl.Name()),
				Value: decoder.Get(lbl.Value()),
			}
		}
	}

	// Parse chunks
	if capnpSeries.HasChunks() {
		chunks, err := capnpSeries.Chunks()
		if err != nil {
			return nil, errors.Wrap(err, "get chunks")
		}
		series.Chunks = make([]storepb.AggrChunk, chunks.Len())
		for i := 0; i < chunks.Len(); i++ {
			chunk := chunks.At(i)
			aggrChunk, err := parseAggrChunk(chunk)
			if err != nil {
				return nil, errors.Wrapf(err, "parse chunk %d", i)
			}
			series.Chunks[i] = *aggrChunk
		}
	}

	return series, nil
}

func parseAggrChunk(capnpChunk AggrChunk) (*storepb.AggrChunk, error) {
	chunk := &storepb.AggrChunk{
		MinTime: capnpChunk.MinTime(),
		MaxTime: capnpChunk.MaxTime(),
	}

	if capnpChunk.HasRaw() {
		raw, err := capnpChunk.Raw()
		if err != nil {
			return nil, errors.Wrap(err, "get raw chunk")
		}
		chunk.Raw, err = parseChunk(raw)
		if err != nil {
			return nil, errors.Wrap(err, "parse raw chunk")
		}
	}

	if capnpChunk.HasCount() {
		count, err := capnpChunk.Count()
		if err != nil {
			return nil, errors.Wrap(err, "get count chunk")
		}
		chunk.Count, err = parseChunk(count)
		if err != nil {
			return nil, errors.Wrap(err, "parse count chunk")
		}
	}

	if capnpChunk.HasSum() {
		sum, err := capnpChunk.Sum()
		if err != nil {
			return nil, errors.Wrap(err, "get sum chunk")
		}
		chunk.Sum, err = parseChunk(sum)
		if err != nil {
			return nil, errors.Wrap(err, "parse sum chunk")
		}
	}

	if capnpChunk.HasMin() {
		min, err := capnpChunk.Min()
		if err != nil {
			return nil, errors.Wrap(err, "get min chunk")
		}
		chunk.Min, err = parseChunk(min)
		if err != nil {
			return nil, errors.Wrap(err, "parse min chunk")
		}
	}

	if capnpChunk.HasMax() {
		max, err := capnpChunk.Max()
		if err != nil {
			return nil, errors.Wrap(err, "get max chunk")
		}
		chunk.Max, err = parseChunk(max)
		if err != nil {
			return nil, errors.Wrap(err, "parse max chunk")
		}
	}

	if capnpChunk.HasCounter() {
		counter, err := capnpChunk.Counter()
		if err != nil {
			return nil, errors.Wrap(err, "get counter chunk")
		}
		chunk.Counter, err = parseChunk(counter)
		if err != nil {
			return nil, errors.Wrap(err, "parse counter chunk")
		}
	}

	return chunk, nil
}

func parseChunk(capnpChunk Chunk) (*storepb.Chunk, error) {
	data, err := capnpChunk.Data()
	if err != nil {
		return nil, errors.Wrap(err, "get chunk data")
	}

	// Make a copy of the data since capnp data is tied to the message lifetime
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return &storepb.Chunk{
		Type: storepb.Chunk_Encoding(capnpChunk.Type()),
		Data: dataCopy,
		Hash: capnpChunk.Hash(),
	}, nil
}

// UnmarshalLabelNamesRequest unmarshals a capnp LabelNamesRequest from bytes.
func UnmarshalLabelNamesRequest(data []byte) (*storepb.LabelNamesRequest, error) {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal capnp message")
	}
	return ParseLabelNamesRequest(msg)
}

// ParseLabelNamesRequest parses a capnp LabelNamesRequest from a message.
func ParseLabelNamesRequest(msg *capnp.Message) (*storepb.LabelNamesRequest, error) {
	capnpReq, err := ReadRootLabelNamesRequest(msg)
	if err != nil {
		return nil, errors.Wrap(err, "read root label names request")
	}

	symbols, err := capnpReq.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseLabelNamesRequestFrom(capnpReq, decoder)
}

// ParseLabelNamesRequestFrom parses a capnp LabelNamesRequest using a pre-existing symbols decoder.
func ParseLabelNamesRequestFrom(capnpReq LabelNamesRequest, decoder *SymbolsDecoder) (*storepb.LabelNamesRequest, error) {
	req := &storepb.LabelNamesRequest{
		PartialResponseDisabled: capnpReq.PartialResponseDisabled(),
		PartialResponseStrategy: storepb.PartialResponseStrategy(capnpReq.PartialResponseStrategy()),
		Start:                   capnpReq.Start(),
		End:                     capnpReq.End(),
		Limit:                   capnpReq.Limit(),
	}

	if capnpReq.HasHints() {
		hintsData, err := capnpReq.Hints()
		if err != nil {
			return nil, errors.Wrap(err, "get hints")
		}
		if len(hintsData) > 0 {
			req.Hints = &types.Any{}
			if err := req.Hints.Unmarshal(hintsData); err != nil {
				return nil, errors.Wrap(err, "unmarshal hints")
			}
		}
	}

	if capnpReq.HasMatchers() {
		matchers, err := capnpReq.Matchers()
		if err != nil {
			return nil, errors.Wrap(err, "get matchers")
		}
		req.Matchers = make([]storepb.LabelMatcher, matchers.Len())
		for i := 0; i < matchers.Len(); i++ {
			m := matchers.At(i)
			req.Matchers[i] = storepb.LabelMatcher{
				Type:  storepb.LabelMatcher_Type(m.Type()),
				Name:  decoder.Get(m.Name()),
				Value: decoder.Get(m.Value()),
			}
		}
	}

	if capnpReq.HasWithoutReplicaLabels() {
		wrl, err := capnpReq.WithoutReplicaLabels()
		if err != nil {
			return nil, errors.Wrap(err, "get without replica labels")
		}
		req.WithoutReplicaLabels = make([]string, wrl.Len())
		for i := 0; i < wrl.Len(); i++ {
			req.WithoutReplicaLabels[i] = decoder.Get(wrl.At(i))
		}
	}

	return req, nil
}

// UnmarshalLabelNamesResponse unmarshals a capnp LabelNamesResponse from bytes.
func UnmarshalLabelNamesResponse(data []byte) (*storepb.LabelNamesResponse, error) {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal capnp message")
	}
	return ParseLabelNamesResponse(msg)
}

// ParseLabelNamesResponse parses a capnp LabelNamesResponse from a message.
func ParseLabelNamesResponse(msg *capnp.Message) (*storepb.LabelNamesResponse, error) {
	capnpResp, err := ReadRootLabelNamesResponse(msg)
	if err != nil {
		return nil, errors.Wrap(err, "read root label names response")
	}

	symbols, err := capnpResp.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseLabelNamesResponseFrom(capnpResp, decoder)
}

// ParseLabelNamesResponseFrom parses a capnp LabelNamesResponse using a pre-existing symbols decoder.
func ParseLabelNamesResponseFrom(capnpResp LabelNamesResponse, decoder *SymbolsDecoder) (*storepb.LabelNamesResponse, error) {
	resp := &storepb.LabelNamesResponse{}

	if capnpResp.HasNames() {
		names, err := capnpResp.Names()
		if err != nil {
			return nil, errors.Wrap(err, "get names")
		}
		resp.Names = make([]string, names.Len())
		for i := 0; i < names.Len(); i++ {
			resp.Names[i] = decoder.Get(names.At(i))
		}
	}

	if capnpResp.HasWarnings() {
		warnings, err := capnpResp.Warnings()
		if err != nil {
			return nil, errors.Wrap(err, "get warnings")
		}
		resp.Warnings = make([]string, warnings.Len())
		for i := 0; i < warnings.Len(); i++ {
			w, err := warnings.At(i)
			if err != nil {
				return nil, errors.Wrapf(err, "get warning %d", i)
			}
			resp.Warnings[i] = w
		}
	}

	if capnpResp.HasHints() {
		hintsData, err := capnpResp.Hints()
		if err != nil {
			return nil, errors.Wrap(err, "get hints")
		}
		if len(hintsData) > 0 {
			resp.Hints = &types.Any{}
			if err := resp.Hints.Unmarshal(hintsData); err != nil {
				return nil, errors.Wrap(err, "unmarshal hints")
			}
		}
	}

	return resp, nil
}

// UnmarshalLabelValuesRequest unmarshals a capnp LabelValuesRequest from bytes.
func UnmarshalLabelValuesRequest(data []byte) (*storepb.LabelValuesRequest, error) {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal capnp message")
	}
	return ParseLabelValuesRequest(msg)
}

// ParseLabelValuesRequest parses a capnp LabelValuesRequest from a message.
func ParseLabelValuesRequest(msg *capnp.Message) (*storepb.LabelValuesRequest, error) {
	capnpReq, err := ReadRootLabelValuesRequest(msg)
	if err != nil {
		return nil, errors.Wrap(err, "read root label values request")
	}

	symbols, err := capnpReq.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseLabelValuesRequestFrom(capnpReq, decoder)
}

// ParseLabelValuesRequestFrom parses a capnp LabelValuesRequest using a pre-existing symbols decoder.
func ParseLabelValuesRequestFrom(capnpReq LabelValuesRequest, decoder *SymbolsDecoder) (*storepb.LabelValuesRequest, error) {
	req := &storepb.LabelValuesRequest{
		Label:                   decoder.Get(capnpReq.Label()),
		PartialResponseDisabled: capnpReq.PartialResponseDisabled(),
		PartialResponseStrategy: storepb.PartialResponseStrategy(capnpReq.PartialResponseStrategy()),
		Start:                   capnpReq.Start(),
		End:                     capnpReq.End(),
		Limit:                   capnpReq.Limit(),
	}

	if capnpReq.HasHints() {
		hintsData, err := capnpReq.Hints()
		if err != nil {
			return nil, errors.Wrap(err, "get hints")
		}
		if len(hintsData) > 0 {
			req.Hints = &types.Any{}
			if err := req.Hints.Unmarshal(hintsData); err != nil {
				return nil, errors.Wrap(err, "unmarshal hints")
			}
		}
	}

	if capnpReq.HasMatchers() {
		matchers, err := capnpReq.Matchers()
		if err != nil {
			return nil, errors.Wrap(err, "get matchers")
		}
		req.Matchers = make([]storepb.LabelMatcher, matchers.Len())
		for i := 0; i < matchers.Len(); i++ {
			m := matchers.At(i)
			req.Matchers[i] = storepb.LabelMatcher{
				Type:  storepb.LabelMatcher_Type(m.Type()),
				Name:  decoder.Get(m.Name()),
				Value: decoder.Get(m.Value()),
			}
		}
	}

	if capnpReq.HasWithoutReplicaLabels() {
		wrl, err := capnpReq.WithoutReplicaLabels()
		if err != nil {
			return nil, errors.Wrap(err, "get without replica labels")
		}
		req.WithoutReplicaLabels = make([]string, wrl.Len())
		for i := 0; i < wrl.Len(); i++ {
			req.WithoutReplicaLabels[i] = decoder.Get(wrl.At(i))
		}
	}

	return req, nil
}

// UnmarshalLabelValuesResponse unmarshals a capnp LabelValuesResponse from bytes.
func UnmarshalLabelValuesResponse(data []byte) (*storepb.LabelValuesResponse, error) {
	msg, err := capnp.Unmarshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal capnp message")
	}
	return ParseLabelValuesResponse(msg)
}

// ParseLabelValuesResponse parses a capnp LabelValuesResponse from a message.
func ParseLabelValuesResponse(msg *capnp.Message) (*storepb.LabelValuesResponse, error) {
	capnpResp, err := ReadRootLabelValuesResponse(msg)
	if err != nil {
		return nil, errors.Wrap(err, "read root label values response")
	}

	symbols, err := capnpResp.Symbols()
	if err != nil {
		return nil, errors.Wrap(err, "get symbols")
	}
	decoder, err := NewSymbolsDecoder(symbols)
	if err != nil {
		return nil, errors.Wrap(err, "new symbols decoder")
	}
	defer decoder.Close()

	return ParseLabelValuesResponseFrom(capnpResp, decoder)
}

// ParseLabelValuesResponseFrom parses a capnp LabelValuesResponse using a pre-existing symbols decoder.
func ParseLabelValuesResponseFrom(capnpResp LabelValuesResponse, decoder *SymbolsDecoder) (*storepb.LabelValuesResponse, error) {
	resp := &storepb.LabelValuesResponse{}

	if capnpResp.HasValues() {
		values, err := capnpResp.Values()
		if err != nil {
			return nil, errors.Wrap(err, "get values")
		}
		resp.Values = make([]string, values.Len())
		for i := 0; i < values.Len(); i++ {
			resp.Values[i] = decoder.Get(values.At(i))
		}
	}

	if capnpResp.HasWarnings() {
		warnings, err := capnpResp.Warnings()
		if err != nil {
			return nil, errors.Wrap(err, "get warnings")
		}
		resp.Warnings = make([]string, warnings.Len())
		for i := 0; i < warnings.Len(); i++ {
			w, err := warnings.At(i)
			if err != nil {
				return nil, errors.Wrapf(err, "get warning %d", i)
			}
			resp.Warnings[i] = w
		}
	}

	if capnpResp.HasHints() {
		hintsData, err := capnpResp.Hints()
		if err != nil {
			return nil, errors.Wrap(err, "get hints")
		}
		if len(hintsData) > 0 {
			resp.Hints = &types.Any{}
			if err := resp.Hints.Unmarshal(hintsData); err != nil {
				return nil, errors.Wrap(err, "unmarshal hints")
			}
		}
	}

	return resp, nil
}
