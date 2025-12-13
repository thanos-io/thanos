// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecapnp

import (
	"capnproto.org/go/capnp/v3"
	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// MarshalSeriesRequest marshals a protobuf SeriesRequest to capnp bytes.
func MarshalSeriesRequest(req *storepb.SeriesRequest) ([]byte, error) {
	capnpReq, err := BuildSeriesRequest(req)
	if err != nil {
		return nil, err
	}
	return capnpReq.Message().Marshal()
}

// MarshalSeriesRequestPacked marshals a protobuf SeriesRequest to packed capnp bytes.
func MarshalSeriesRequestPacked(req *storepb.SeriesRequest) ([]byte, error) {
	capnpReq, err := BuildSeriesRequest(req)
	if err != nil {
		return nil, err
	}
	return capnpReq.Message().MarshalPacked()
}

// BuildSeriesRequest builds a capnp SeriesRequest from a protobuf SeriesRequest.
func BuildSeriesRequest(req *storepb.SeriesRequest) (SeriesRequest, error) {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return SeriesRequest{}, err
	}

	capnpReq, err := NewRootSeriesRequest(seg)
	if err != nil {
		return SeriesRequest{}, err
	}

	if err := BuildSeriesRequestInto(capnpReq, req); err != nil {
		return SeriesRequest{}, err
	}

	return capnpReq, nil
}

// BuildSeriesRequestInto builds a capnp SeriesRequest in place from a protobuf SeriesRequest.
func BuildSeriesRequestInto(capnpReq SeriesRequest, req *storepb.SeriesRequest) error {
	builder := newSymbolsBuilder()

	// Set scalar fields
	capnpReq.SetMinTime(req.MinTime)
	capnpReq.SetMaxTime(req.MaxTime)
	capnpReq.SetMaxResolutionWindow(req.MaxResolutionWindow)
	capnpReq.SetPartialResponseDisabled(req.PartialResponseDisabled)
	capnpReq.SetPartialResponseStrategy(PartialResponseStrategy(req.PartialResponseStrategy))
	capnpReq.SetSkipChunks(req.SkipChunks)
	capnpReq.SetStep(req.Step)
	capnpReq.SetRange(req.Range)
	capnpReq.SetLimit(req.Limit)

	// Set hints (opaque data)
	if req.Hints != nil {
		hintsData, err := req.Hints.Marshal()
		if err != nil {
			return errors.Wrap(err, "marshal hints")
		}
		if err := capnpReq.SetHints(hintsData); err != nil {
			return errors.Wrap(err, "set hints")
		}
	}

	// Build matchers
	if len(req.Matchers) > 0 {
		matchers, err := capnpReq.NewMatchers(int32(len(req.Matchers)))
		if err != nil {
			return errors.Wrap(err, "new matchers")
		}
		for i, m := range req.Matchers {
			matcher := matchers.At(i)
			matcher.SetType(LabelMatcher_Type(m.Type))
			matcher.SetName(builder.addEntry(m.Name))
			matcher.SetValue(builder.addEntry(m.Value))
		}
	}

	// Build aggregates
	if len(req.Aggregates) > 0 {
		aggregates, err := capnpReq.NewAggregates(int32(len(req.Aggregates)))
		if err != nil {
			return errors.Wrap(err, "new aggregates")
		}
		for i, a := range req.Aggregates {
			aggregates.Set(i, Aggr(a))
		}
	}

	// Build QueryHints
	if req.QueryHints != nil {
		qh, err := capnpReq.NewQueryHints()
		if err != nil {
			return errors.Wrap(err, "new query hints")
		}
		qh.SetStepMillis(req.QueryHints.StepMillis)

		if req.QueryHints.Func != nil {
			f, err := qh.NewFunc()
			if err != nil {
				return errors.Wrap(err, "new func")
			}
			f.SetName(builder.addEntry(req.QueryHints.Func.Name))
		}

		if req.QueryHints.Grouping != nil {
			g, err := qh.NewGrouping()
			if err != nil {
				return errors.Wrap(err, "new grouping")
			}
			g.SetBy(req.QueryHints.Grouping.By)
			if len(req.QueryHints.Grouping.Labels) > 0 {
				labels, err := g.NewLabels(int32(len(req.QueryHints.Grouping.Labels)))
				if err != nil {
					return errors.Wrap(err, "new grouping labels")
				}
				for i, lbl := range req.QueryHints.Grouping.Labels {
					labels.Set(i, builder.addEntry(lbl))
				}
			}
		}

		if req.QueryHints.Range != nil {
			r, err := qh.NewRange()
			if err != nil {
				return errors.Wrap(err, "new range")
			}
			r.SetMillis(req.QueryHints.Range.Millis)
		}
	}

	// Build ShardInfo
	if req.ShardInfo != nil {
		si, err := capnpReq.NewShardInfo()
		if err != nil {
			return errors.Wrap(err, "new shard info")
		}
		si.SetShardIndex(req.ShardInfo.ShardIndex)
		si.SetTotalShards(req.ShardInfo.TotalShards)
		si.SetBy(req.ShardInfo.By)
		if len(req.ShardInfo.Labels) > 0 {
			labels, err := si.NewLabels(int32(len(req.ShardInfo.Labels)))
			if err != nil {
				return errors.Wrap(err, "new shard labels")
			}
			for i, lbl := range req.ShardInfo.Labels {
				labels.Set(i, builder.addEntry(lbl))
			}
		}
	}

	// Build WithoutReplicaLabels
	if len(req.WithoutReplicaLabels) > 0 {
		wrl, err := capnpReq.NewWithoutReplicaLabels(int32(len(req.WithoutReplicaLabels)))
		if err != nil {
			return errors.Wrap(err, "new without replica labels")
		}
		for i, lbl := range req.WithoutReplicaLabels {
			wrl.Set(i, builder.addEntry(lbl))
		}
	}

	// Build symbols table
	symbols, err := capnpReq.NewSymbols()
	if err != nil {
		return errors.Wrap(err, "new symbols")
	}
	if err := marshalSymbols(builder, symbols); err != nil {
		return errors.Wrap(err, "marshal symbols")
	}

	return nil
}

// MarshalSeriesResponse marshals a protobuf SeriesResponse to capnp bytes.
func MarshalSeriesResponse(resp *storepb.SeriesResponse) ([]byte, error) {
	capnpResp, err := BuildSeriesResponse(resp)
	if err != nil {
		return nil, err
	}
	return capnpResp.Message().Marshal()
}

// MarshalSeriesResponsePacked marshals a protobuf SeriesResponse to packed capnp bytes.
func MarshalSeriesResponsePacked(resp *storepb.SeriesResponse) ([]byte, error) {
	capnpResp, err := BuildSeriesResponse(resp)
	if err != nil {
		return nil, err
	}
	return capnpResp.Message().MarshalPacked()
}

// BuildSeriesResponse builds a capnp SeriesResponse from a protobuf SeriesResponse.
func BuildSeriesResponse(resp *storepb.SeriesResponse) (SeriesResponse, error) {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return SeriesResponse{}, err
	}

	capnpResp, err := NewRootSeriesResponse(seg)
	if err != nil {
		return SeriesResponse{}, err
	}

	if err := BuildSeriesResponseInto(capnpResp, resp); err != nil {
		return SeriesResponse{}, err
	}

	return capnpResp, nil
}

// BuildSeriesResponseInto builds a capnp SeriesResponse in place from a protobuf SeriesResponse.
func BuildSeriesResponseInto(capnpResp SeriesResponse, resp *storepb.SeriesResponse) error {
	builder := newSymbolsBuilder()

	switch r := resp.Result.(type) {
	case *storepb.SeriesResponse_Series:
		series, err := capnpResp.NewSeries()
		if err != nil {
			return errors.Wrap(err, "new series")
		}
		if err := marshalSeries(series, r.Series, builder); err != nil {
			return errors.Wrap(err, "marshal series")
		}

	case *storepb.SeriesResponse_Warning:
		if err := capnpResp.SetWarning(r.Warning); err != nil {
			return errors.Wrap(err, "set warning")
		}

	case *storepb.SeriesResponse_Hints:
		hintsData, err := r.Hints.Marshal()
		if err != nil {
			return errors.Wrap(err, "marshal hints")
		}
		if err := capnpResp.SetHints(hintsData); err != nil {
			return errors.Wrap(err, "set hints")
		}
	}

	// Build symbols table
	symbols, err := capnpResp.NewSymbols()
	if err != nil {
		return errors.Wrap(err, "new symbols")
	}
	if err := marshalSymbols(builder, symbols); err != nil {
		return errors.Wrap(err, "marshal symbols")
	}

	return nil
}

func marshalSeries(capnpSeries Series, pbSeries *storepb.Series, builder *symbolsBuilder) error {
	// Marshal labels
	if len(pbSeries.Labels) > 0 {
		labels, err := capnpSeries.NewLabels(int32(len(pbSeries.Labels)))
		if err != nil {
			return errors.Wrap(err, "new labels")
		}
		if err := marshalLabels(labels, pbSeries.Labels, builder); err != nil {
			return errors.Wrap(err, "marshal labels")
		}
	}

	// Marshal chunks
	if len(pbSeries.Chunks) > 0 {
		chunks, err := capnpSeries.NewChunks(int32(len(pbSeries.Chunks)))
		if err != nil {
			return errors.Wrap(err, "new chunks")
		}
		for i, pbChunk := range pbSeries.Chunks {
			chunk := chunks.At(i)
			if err := marshalAggrChunk(chunk, &pbChunk); err != nil {
				return errors.Wrapf(err, "marshal chunk %d", i)
			}
		}
	}

	return nil
}

func marshalLabels(labels Label_List, pbLabels []labelpb.ZLabel, builder *symbolsBuilder) error {
	for i, pbLabel := range pbLabels {
		label := labels.At(i)
		label.SetName(builder.addEntry(pbLabel.Name))
		label.SetValue(builder.addEntry(pbLabel.Value))
	}
	return nil
}

func marshalAggrChunk(chunk AggrChunk, pbChunk *storepb.AggrChunk) error {
	chunk.SetMinTime(pbChunk.MinTime)
	chunk.SetMaxTime(pbChunk.MaxTime)

	if pbChunk.Raw != nil {
		raw, err := chunk.NewRaw()
		if err != nil {
			return errors.Wrap(err, "new raw chunk")
		}
		if err := marshalChunk(raw, pbChunk.Raw); err != nil {
			return errors.Wrap(err, "marshal raw chunk")
		}
	}

	if pbChunk.Count != nil {
		count, err := chunk.NewCount()
		if err != nil {
			return errors.Wrap(err, "new count chunk")
		}
		if err := marshalChunk(count, pbChunk.Count); err != nil {
			return errors.Wrap(err, "marshal count chunk")
		}
	}

	if pbChunk.Sum != nil {
		sum, err := chunk.NewSum()
		if err != nil {
			return errors.Wrap(err, "new sum chunk")
		}
		if err := marshalChunk(sum, pbChunk.Sum); err != nil {
			return errors.Wrap(err, "marshal sum chunk")
		}
	}

	if pbChunk.Min != nil {
		min, err := chunk.NewMin()
		if err != nil {
			return errors.Wrap(err, "new min chunk")
		}
		if err := marshalChunk(min, pbChunk.Min); err != nil {
			return errors.Wrap(err, "marshal min chunk")
		}
	}

	if pbChunk.Max != nil {
		max, err := chunk.NewMax()
		if err != nil {
			return errors.Wrap(err, "new max chunk")
		}
		if err := marshalChunk(max, pbChunk.Max); err != nil {
			return errors.Wrap(err, "marshal max chunk")
		}
	}

	if pbChunk.Counter != nil {
		counter, err := chunk.NewCounter()
		if err != nil {
			return errors.Wrap(err, "new counter chunk")
		}
		if err := marshalChunk(counter, pbChunk.Counter); err != nil {
			return errors.Wrap(err, "marshal counter chunk")
		}
	}

	return nil
}

func marshalChunk(chunk Chunk, pbChunk *storepb.Chunk) error {
	chunk.SetType(ChunkEncoding(pbChunk.Type))
	if err := chunk.SetData(pbChunk.Data); err != nil {
		return errors.Wrap(err, "set chunk data")
	}
	chunk.SetHash(pbChunk.Hash)
	return nil
}

// MarshalLabelNamesRequest marshals a protobuf LabelNamesRequest to capnp bytes.
func MarshalLabelNamesRequest(req *storepb.LabelNamesRequest) ([]byte, error) {
	capnpReq, err := BuildLabelNamesRequest(req)
	if err != nil {
		return nil, err
	}
	return capnpReq.Message().Marshal()
}

// BuildLabelNamesRequest builds a capnp LabelNamesRequest from a protobuf LabelNamesRequest.
func BuildLabelNamesRequest(req *storepb.LabelNamesRequest) (LabelNamesRequest, error) {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return LabelNamesRequest{}, err
	}

	capnpReq, err := NewRootLabelNamesRequest(seg)
	if err != nil {
		return LabelNamesRequest{}, err
	}

	if err := BuildLabelNamesRequestInto(capnpReq, req); err != nil {
		return LabelNamesRequest{}, err
	}

	return capnpReq, nil
}

// BuildLabelNamesRequestInto builds a capnp LabelNamesRequest in place.
func BuildLabelNamesRequestInto(capnpReq LabelNamesRequest, req *storepb.LabelNamesRequest) error {
	builder := newSymbolsBuilder()

	capnpReq.SetPartialResponseDisabled(req.PartialResponseDisabled)
	capnpReq.SetPartialResponseStrategy(PartialResponseStrategy(req.PartialResponseStrategy))
	capnpReq.SetStart(req.Start)
	capnpReq.SetEnd(req.End)
	capnpReq.SetLimit(req.Limit)

	if req.Hints != nil {
		hintsData, err := req.Hints.Marshal()
		if err != nil {
			return errors.Wrap(err, "marshal hints")
		}
		if err := capnpReq.SetHints(hintsData); err != nil {
			return errors.Wrap(err, "set hints")
		}
	}

	if len(req.Matchers) > 0 {
		matchers, err := capnpReq.NewMatchers(int32(len(req.Matchers)))
		if err != nil {
			return errors.Wrap(err, "new matchers")
		}
		for i, m := range req.Matchers {
			matcher := matchers.At(i)
			matcher.SetType(LabelMatcher_Type(m.Type))
			matcher.SetName(builder.addEntry(m.Name))
			matcher.SetValue(builder.addEntry(m.Value))
		}
	}

	if len(req.WithoutReplicaLabels) > 0 {
		wrl, err := capnpReq.NewWithoutReplicaLabels(int32(len(req.WithoutReplicaLabels)))
		if err != nil {
			return errors.Wrap(err, "new without replica labels")
		}
		for i, lbl := range req.WithoutReplicaLabels {
			wrl.Set(i, builder.addEntry(lbl))
		}
	}

	symbols, err := capnpReq.NewSymbols()
	if err != nil {
		return errors.Wrap(err, "new symbols")
	}
	if err := marshalSymbols(builder, symbols); err != nil {
		return errors.Wrap(err, "marshal symbols")
	}

	return nil
}

// MarshalLabelNamesResponse marshals a protobuf LabelNamesResponse to capnp bytes.
func MarshalLabelNamesResponse(resp *storepb.LabelNamesResponse) ([]byte, error) {
	capnpResp, err := BuildLabelNamesResponse(resp)
	if err != nil {
		return nil, err
	}
	return capnpResp.Message().Marshal()
}

// BuildLabelNamesResponse builds a capnp LabelNamesResponse from a protobuf LabelNamesResponse.
func BuildLabelNamesResponse(resp *storepb.LabelNamesResponse) (LabelNamesResponse, error) {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return LabelNamesResponse{}, err
	}

	capnpResp, err := NewRootLabelNamesResponse(seg)
	if err != nil {
		return LabelNamesResponse{}, err
	}

	if err := BuildLabelNamesResponseInto(capnpResp, resp); err != nil {
		return LabelNamesResponse{}, err
	}

	return capnpResp, nil
}

// BuildLabelNamesResponseInto builds a capnp LabelNamesResponse in place.
func BuildLabelNamesResponseInto(capnpResp LabelNamesResponse, resp *storepb.LabelNamesResponse) error {
	builder := newSymbolsBuilder()

	if len(resp.Names) > 0 {
		names, err := capnpResp.NewNames(int32(len(resp.Names)))
		if err != nil {
			return errors.Wrap(err, "new names")
		}
		for i, name := range resp.Names {
			names.Set(i, builder.addEntry(name))
		}
	}

	if len(resp.Warnings) > 0 {
		warnings, err := capnpResp.NewWarnings(int32(len(resp.Warnings)))
		if err != nil {
			return errors.Wrap(err, "new warnings")
		}
		for i, w := range resp.Warnings {
			if err := warnings.Set(i, w); err != nil {
				return errors.Wrapf(err, "set warning %d", i)
			}
		}
	}

	if resp.Hints != nil {
		hintsData, err := resp.Hints.Marshal()
		if err != nil {
			return errors.Wrap(err, "marshal hints")
		}
		if err := capnpResp.SetHints(hintsData); err != nil {
			return errors.Wrap(err, "set hints")
		}
	}

	symbols, err := capnpResp.NewSymbols()
	if err != nil {
		return errors.Wrap(err, "new symbols")
	}
	if err := marshalSymbols(builder, symbols); err != nil {
		return errors.Wrap(err, "marshal symbols")
	}

	return nil
}

// MarshalLabelValuesRequest marshals a protobuf LabelValuesRequest to capnp bytes.
func MarshalLabelValuesRequest(req *storepb.LabelValuesRequest) ([]byte, error) {
	capnpReq, err := BuildLabelValuesRequest(req)
	if err != nil {
		return nil, err
	}
	return capnpReq.Message().Marshal()
}

// BuildLabelValuesRequest builds a capnp LabelValuesRequest from a protobuf LabelValuesRequest.
func BuildLabelValuesRequest(req *storepb.LabelValuesRequest) (LabelValuesRequest, error) {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return LabelValuesRequest{}, err
	}

	capnpReq, err := NewRootLabelValuesRequest(seg)
	if err != nil {
		return LabelValuesRequest{}, err
	}

	if err := BuildLabelValuesRequestInto(capnpReq, req); err != nil {
		return LabelValuesRequest{}, err
	}

	return capnpReq, nil
}

// BuildLabelValuesRequestInto builds a capnp LabelValuesRequest in place.
func BuildLabelValuesRequestInto(capnpReq LabelValuesRequest, req *storepb.LabelValuesRequest) error {
	builder := newSymbolsBuilder()

	capnpReq.SetLabel(builder.addEntry(req.Label))
	capnpReq.SetPartialResponseDisabled(req.PartialResponseDisabled)
	capnpReq.SetPartialResponseStrategy(PartialResponseStrategy(req.PartialResponseStrategy))
	capnpReq.SetStart(req.Start)
	capnpReq.SetEnd(req.End)
	capnpReq.SetLimit(req.Limit)

	if req.Hints != nil {
		hintsData, err := req.Hints.Marshal()
		if err != nil {
			return errors.Wrap(err, "marshal hints")
		}
		if err := capnpReq.SetHints(hintsData); err != nil {
			return errors.Wrap(err, "set hints")
		}
	}

	if len(req.Matchers) > 0 {
		matchers, err := capnpReq.NewMatchers(int32(len(req.Matchers)))
		if err != nil {
			return errors.Wrap(err, "new matchers")
		}
		for i, m := range req.Matchers {
			matcher := matchers.At(i)
			matcher.SetType(LabelMatcher_Type(m.Type))
			matcher.SetName(builder.addEntry(m.Name))
			matcher.SetValue(builder.addEntry(m.Value))
		}
	}

	if len(req.WithoutReplicaLabels) > 0 {
		wrl, err := capnpReq.NewWithoutReplicaLabels(int32(len(req.WithoutReplicaLabels)))
		if err != nil {
			return errors.Wrap(err, "new without replica labels")
		}
		for i, lbl := range req.WithoutReplicaLabels {
			wrl.Set(i, builder.addEntry(lbl))
		}
	}

	symbols, err := capnpReq.NewSymbols()
	if err != nil {
		return errors.Wrap(err, "new symbols")
	}
	if err := marshalSymbols(builder, symbols); err != nil {
		return errors.Wrap(err, "marshal symbols")
	}

	return nil
}

// MarshalLabelValuesResponse marshals a protobuf LabelValuesResponse to capnp bytes.
func MarshalLabelValuesResponse(resp *storepb.LabelValuesResponse) ([]byte, error) {
	capnpResp, err := BuildLabelValuesResponse(resp)
	if err != nil {
		return nil, err
	}
	return capnpResp.Message().Marshal()
}

// BuildLabelValuesResponse builds a capnp LabelValuesResponse from a protobuf LabelValuesResponse.
func BuildLabelValuesResponse(resp *storepb.LabelValuesResponse) (LabelValuesResponse, error) {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return LabelValuesResponse{}, err
	}

	capnpResp, err := NewRootLabelValuesResponse(seg)
	if err != nil {
		return LabelValuesResponse{}, err
	}

	if err := BuildLabelValuesResponseInto(capnpResp, resp); err != nil {
		return LabelValuesResponse{}, err
	}

	return capnpResp, nil
}

// BuildLabelValuesResponseInto builds a capnp LabelValuesResponse in place.
func BuildLabelValuesResponseInto(capnpResp LabelValuesResponse, resp *storepb.LabelValuesResponse) error {
	builder := newSymbolsBuilder()

	if len(resp.Values) > 0 {
		values, err := capnpResp.NewValues(int32(len(resp.Values)))
		if err != nil {
			return errors.Wrap(err, "new values")
		}
		for i, val := range resp.Values {
			values.Set(i, builder.addEntry(val))
		}
	}

	if len(resp.Warnings) > 0 {
		warnings, err := capnpResp.NewWarnings(int32(len(resp.Warnings)))
		if err != nil {
			return errors.Wrap(err, "new warnings")
		}
		for i, w := range resp.Warnings {
			if err := warnings.Set(i, w); err != nil {
				return errors.Wrapf(err, "set warning %d", i)
			}
		}
	}

	if resp.Hints != nil {
		hintsData, err := resp.Hints.Marshal()
		if err != nil {
			return errors.Wrap(err, "marshal hints")
		}
		if err := capnpResp.SetHints(hintsData); err != nil {
			return errors.Wrap(err, "set hints")
		}
	}

	symbols, err := capnpResp.NewSymbols()
	if err != nil {
		return errors.Wrap(err, "new symbols")
	}
	if err := marshalSymbols(builder, symbols); err != nil {
		return errors.Wrap(err, "marshal symbols")
	}

	return nil
}

// Symbol table utilities

func marshalSymbols(builder *symbolsBuilder, symbols Symbols) error {
	offsets, err := symbols.NewOffsets(builder.len())
	if err != nil {
		return err
	}
	data := make([]byte, builder.symbolsSize)
	for k, entry := range builder.table {
		end := entry.start + uint32(len(k))
		copy(data[entry.start:end], k)
		offsets.Set(int(entry.index), end)
	}

	return symbols.SetData(data)
}

type symbolsBuilder struct {
	table       map[string]tableEntry
	symbolsSize uint32
}

func newSymbolsBuilder() *symbolsBuilder {
	return &symbolsBuilder{
		table: make(map[string]tableEntry),
	}
}

func (s *symbolsBuilder) addEntry(item string) uint32 {
	entry, ok := s.table[item]
	if ok {
		return entry.index
	}
	entry = tableEntry{
		index: uint32(len(s.table)),
		start: s.symbolsSize,
	}
	s.symbolsSize += uint32(len(item))
	s.table[item] = entry
	return entry.index
}

func (s *symbolsBuilder) len() int32 {
	return int32(len(s.table))
}

type tableEntry struct {
	index uint32
	start uint32
}

// BuildInfoResponseFromProto builds a capnp InfoResponse from a protobuf InfoResponse.
func BuildInfoResponseFromProto(capnpResp InfoResponse, pbResp *infopb.InfoResponse) error {
	builder := newSymbolsBuilder()

	// Build label sets
	if len(pbResp.LabelSets) > 0 {
		capnpLabelSets, err := capnpResp.NewLabelSets(int32(len(pbResp.LabelSets)))
		if err != nil {
			return errors.Wrap(err, "new label sets")
		}
		for i, ls := range pbResp.LabelSets {
			capnpLs := capnpLabelSets.At(i)
			if len(ls.Labels) > 0 {
				labels, err := capnpLs.NewLabels(int32(len(ls.Labels)))
				if err != nil {
					return errors.Wrap(err, "new labels")
				}
				if err := marshalLabels(labels, ls.Labels, builder); err != nil {
					return errors.Wrap(err, "marshal labels")
				}
			}
		}
	}

	// Build store info
	if pbResp.Store != nil {
		capnpStoreInfo, err := capnpResp.NewStoreInfo()
		if err != nil {
			return errors.Wrap(err, "new store info")
		}
		capnpStoreInfo.SetMinTime(pbResp.Store.MinTime)
		capnpStoreInfo.SetMaxTime(pbResp.Store.MaxTime)
		capnpStoreInfo.SetSupportsSharding(pbResp.Store.SupportsSharding)
		capnpStoreInfo.SetSupportsWithoutReplicaLabels(pbResp.Store.SupportsWithoutReplicaLabels)

		// Build TSDB infos
		if len(pbResp.Store.TsdbInfos) > 0 {
			tsdbInfos, err := capnpStoreInfo.NewTsdbInfos(int32(len(pbResp.Store.TsdbInfos)))
			if err != nil {
				return errors.Wrap(err, "new tsdb infos")
			}
			for i, ti := range pbResp.Store.TsdbInfos {
				capnpTi := tsdbInfos.At(i)
				capnpTi.SetMinTime(ti.MinTime)
				capnpTi.SetMaxTime(ti.MaxTime)

				// Build TSDB labels
				if len(ti.Labels.Labels) > 0 {
					tsdbLabels, err := capnpTi.NewLabels()
					if err != nil {
						return errors.Wrap(err, "new tsdb labels")
					}
					labels, err := tsdbLabels.NewLabels(int32(len(ti.Labels.Labels)))
					if err != nil {
						return errors.Wrap(err, "new tsdb labels list")
					}
					if err := marshalLabels(labels, ti.Labels.Labels, builder); err != nil {
						return errors.Wrap(err, "marshal tsdb labels")
					}
				}
			}
		}
	}

	// Build symbols table
	symbols, err := capnpResp.NewSymbols()
	if err != nil {
		return errors.Wrap(err, "new symbols")
	}
	if err := marshalSymbols(builder, symbols); err != nil {
		return errors.Wrap(err, "marshal symbols")
	}

	return nil
}
